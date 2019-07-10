/*
 * Copyright 2014–2018 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.impl.storage

import slamdata.Predef._

import quasar.concurrent.BlockingContext
import quasar.contrib.scalaz.MonadError_

import cats.arrow.FunctionK
import cats.effect._
import cats.effect.concurrent.{Ref, Deferred}
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.applicative._
import cats.syntax.functor._
import cats.syntax.either._
import cats.instances.either._
import cats.instances.option._
import cats.syntax.traverse._
import cats.syntax.foldable._
import cats.instances.list._

import fs2.Stream
import fs2.concurrent.InspectableQueue
import scalaz.syntax.tag._

import io.atomix.cluster.ClusterMembershipService
import io.atomix.cluster.messaging.ClusterCommunicationService
import io.atomix.cluster.{Member, MemberId}

import scala.collection.JavaConverters._
import java.util.concurrent.{Executors, Executor, ConcurrentMap}
import scala.concurrent.ExecutionContext
import java.util.function.{Consumer, Function}
import scala.concurrent.duration._
import java.nio.charset.StandardCharsets

import AEStore._
import AtomixSetup._

final class AEStore[F[_]: ConcurrentEffect: ContextShift](
    name: String,
    communication: ClusterCommunicationService,
    membership: ClusterMembershipService,
    sendingRef: Ref[F, Pending],
    underlying: ConcurrentMap[Key, Value],
    blockingPool: BlockingContext)(
    implicit timer: Timer[F])
    extends IndexedStore[F, Key, RawValue] {

  val MaxEvents: Int = 50
  val AdTimeout: FiniteDuration = new FiniteDuration(500, MILLISECONDS)
  val PurgeTimeout: FiniteDuration = new FiniteDuration(200, MILLISECONDS)
  val SyncTimeout: FiniteDuration = new FiniteDuration(200, MILLISECONDS)
  val BatchInterval: Int = 50
  val CollectingInterval: Int = 100

  private val F = Sync[F]

  def localId: F[MemberId] = F.delay(membership.getLocalMember.id)

  def peers: F[Set[Member]] = F.delay {
    membership.getMembers.asScala.to[Set]
  }

  private def evalOnPool[A](fa: F[A]): F[A] =
    ContextShift[F].evalOn[A](blockingPool.unwrap)(fa)

  private def evalStreamOnPool[A](s: Stream[F, A]): Stream[F, A] =
    s.translate(new FunctionK[F, F] {
      def apply[A](fa: F[A]): F[A] = evalOnPool(fa)
    })

  def entries: Stream[F, (Key, RawValue)] = for {
    iterator <- Stream.eval(evalOnPool(F.delay(underlying.entrySet.iterator.asScala)))
    entry <- evalStreamOnPool(
      Stream.fromIterator[F, java.util.Map.Entry[Key, Value]](iterator))
    key = entry.getKey
    value <- raw(entry.getValue) match {
      case None => Stream.empty
      case Some(a) => Stream.emit(a)
    }
  } yield (key, value)

  def lookup(k: Key): F[Option[RawValue]] = evalOnPool(F.delay{
    Option(underlying.get(k)).flatMap(raw(_))
  })

  def insert(k: Key, v: RawValue): F[Unit] = evalOnPool(for {
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    value: Value = Tagged(v, currentTime)
    valueBytes = encodeValue(value)
    _ <- F.delay(underlying.put(k, value))
    id <- localId
    _ <- sendingRef.modify((x: Pending) => (Pending(x.items.updated(k, value), x.inited, currentTime), ()))
  } yield ())

  def delete(k: Key): F[Boolean] = evalOnPool(for {
    res <- F.delay(Option(underlying.remove(k)).nonEmpty)
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    _ <- sendingRef.modify((x: Pending) => (Pending(x.items.updated(k, Tombstone(currentTime)), x.inited, currentTime), ()))
  } yield res)


  // SENDING ADS
  def sendingAdStream: F[Stream[F, Unit]] = F.delay {
    (Stream.eval(sendAd) *> Stream.sleep(AdTimeout)).repeat
  }

  private def sendAd: F[Unit] = for {
    pids <- peers.map(_.map(_.id))
    ad <- prepareAd
    id <- localId
    _ <- F.delay(println(s"prepared ::: ${ad}"))
    _ <- multicast(communication, Advertisement(name), encodeAdvertisement(ad), pids - id).unlessA((pids - id).isEmpty)
    _ <- ContextShift[F].shift
  } yield ()

  private def prepareAd: F[AdvertisementPayload] = localId.map { (id: MemberId) =>
    val mp = scala.collection.mutable.Map[Key, Long]()
    underlying.forEach { (k: String, v: Value) =>
      mp.updated(k, timestamp(v)); ()
    }
    AdvertisementPayload(mp.toMap[Key, Long], id)
  }

  // RECEIVING ADS
  def advertisement: F[Stream[F, AdvertisementPayload]] = {
    val fStream = InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, Advertisement(name), 256) *> queue.dequeue
      }
    fStream.map(decodeStream(decodeAdvertisement))
  }

  def handleAdvertisement(ad: AdvertisementPayload): F[Unit] = {
    val initMap = underlying.entrySet.iterator.asScala.map((e: java.util.Map.Entry[Key, Value]) => (e.getKey, e.getValue)).toMap
    val init: (List[Key], ValueMap) = (List(), initMap)
    val result = ad.items.toList.foldM[F, (List[Key], ValueMap)](init){ (acc, v) => (acc, v) match {
      case ((requesting, returning), (k, v)) =>
        val mbCurrent = Option(underlying.get(k))
        if (mbCurrent.map(timestamp(_)).getOrElse(0L) < v) {
          F.delay((k :: requesting, returning))
        } else for {
          current <- mbCurrent match {
            case Some(a) => Sync[F].delay(a)
            case None => timer.clock.monotonic(MILLISECONDS).map(Tombstone(_))
          }
        } yield (requesting, returning.updated(k, current))
    }}
    for {
      (requesting, returning) <- result
      id <- localId
      _ <- unicast(communication, RequestUpdate(name), encodeUpdateRequest(UpdateRequest(requesting, id)), ad.local)
      _ <- ContextShift[F].shift
      _ <- unicast(communication, Update(name), encodeValueMap(returning), ad.local)
      _ <- ContextShift[F].shift
    } yield ()
  }

  def advertisementHandled: F[Stream[F, Unit]] =
    advertisement.map(_.evalMap(handleAdvertisement(_)))

  // TOMBSTONES PURGING
  def purgeTombstones: F[Stream[F, Unit]] =
    F.delay((Stream.sleep(PurgeTimeout) *> Stream.eval(purge)).repeat)

  private def purge: F[Unit] =
    F.delay { underlying.synchronized {
      underlying.forEach { (k: Key, v: Value) =>
        if (raw(v).isEmpty) underlying.remove(k); ()
      }}}

  // RECEIVING UPDATES (both via initialization and update messages)
  def updateReceived: F[Stream[F, ValueMap]] = {
    val fStream = InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, Update(name), 256) *> queue.dequeue
    }
    fStream.map(decodeStream(decodeValueMap))
  }

  def updateHandler(mp: ValueMap): F[Unit] = mp.toList.traverse_{
    case (k, newVal) =>
      val oldVal: Option[Value] = Option(underlying.get(k))
      F.delay(underlying.put(k, newVal)).whenA(oldVal.map(timestamp(_)).getOrElse(0L) < timestamp(newVal))
  }

  def updateHandled: F[Stream[F, Unit]] =
    updateReceived.map(_.evalMap(updateHandler))

  // REQUESTING FOR UPDATES

  def updateRequesting: F[Stream[F, UpdateRequest]] = {
    val fStream = InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, RequestUpdate(name), 128) *> queue.dequeue
    }
    fStream.map(decodeStream(decodeUpdateRequest))
  }

  def updateRequestedHandler(req: UpdateRequest): F[Unit] = {
    val payload: ValueMap = req.items.foldLeft(Map[Key, Value]()) { (acc: ValueMap, k: Key) =>
      Option(underlying.get(k)) match {
        case None => acc
        case Some(toInsert) => acc.updated(k, toInsert)
      }
    }
    unicast(communication, Update(name), encodeValueMap(payload), req.local) *> ContextShift[F].shift
  }

  def updateRequestHandled: F[Stream[F, Unit]] =
    updateRequesting.map(_.evalMap(updateRequestedHandler(_)))

  // SYNCHRONIZATION

  def synchronization: F[Stream[F, Unit]] =
    F.delay((Stream.eval(notifyPeersF) *> Stream.sleep(SyncTimeout)).repeat)

  def notifyPeersF: F[Unit] = for {
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    pids <- peers.map(_.map(_.id))
    msg <- sendingRef.modify(sendingRefUpdate(currentTime))
    id <- localId
    _ <- msg.traverse_(multicast(communication, Update(name), _, pids - id))
    _ <- ContextShift[F].shift
  } yield ()

  private def sendingRefUpdate(now: Long)(inp: Pending): (Pending, Option[Array[Byte]]) = inp match {
    case Pending(lst, _, _) if lst.isEmpty => (inp, None)
    case Pending(_, _, last) if now - last < BatchInterval => (inp, None)
    case Pending(lst, _, _) if lst.size > MaxEvents => (Pending(Map(), now, now), Some(encodeValueMap(lst)))
    case Pending(_, init, _) if now - init < CollectingInterval => (inp, None)
    case Pending(lst, _, _) => (Pending(Map(), now, now), Some(encodeValueMap(lst)))
  }

  // PRIVATE

  private def run(action: F[Unit]) =
    ConcurrentEffect[F].runAsync(action)(_ => IO.unit).unsafeRunSync

  private def handler(eventName: String, cb: Array[Byte] => Unit): F[Unit] =
    cfToAsync(communication.subscribe[Array[Byte]](
      eventName,
      cb(_),
      blockingContextExecutor(blockingPool))) as (())

  private def enqueue(q: InspectableQueue[F, Array[Byte]], eventType: Message, maxItems: Int): Stream[F, Unit] =
    Stream.eval(handler(printMessage(eventType), { (a: Array[Byte]) => run {
      q.getSize.flatMap((size: Int) => {
        q.enqueue1(a).whenA(size < maxItems)
      })}}))

  private def unsubscribe(eventName: String): F[Unit] = F.delay {
    communication.unsubscribe(eventName)
  }

  private def close: F[Unit] =
    List(RequestUpdate(name), Update(name), Advertisement(name)).traverse_ { (m: Message) =>
      unsubscribe(printMessage(m))
    }

}

object AEStore {
  type Key = String
  type ValueMap = Map[Key, Value]
  type RawValue = String
  type RawMap = Map[Key, RawValue]

  def decodeStream[F[_]: Sync, A, B](decode: A => Option[B])(inp: Stream[F, A]): Stream[F, B] =
    inp.flatMap { (a: A) => decode(a) match {
      case None => Stream.empty
      case Some(b) => Stream.emit(b)
    }}

  trait Message extends Product with Serializable
  final case class RequestUpdate(name: String) extends Message
  final case class Update(name: String) extends Message
  final case class Advertisement(name: String) extends Message

  def printMessage(m: Message): String = m match {
    case RequestUpdate(n) => s"aestore::requestUpdate::${n}"
    case Update(n) => s"aestore::update::${n}"
    case Advertisement(n) => s"aestore::advertisement::${n}"
  }

  def unicast[F[_]: Async: ContextShift](
      comm: ClusterCommunicationService,
      message: Message,
      payload: Array[Byte],
      target: MemberId)
      : F[Unit] = {
    cfToAsync(comm.unicast(
      printMessage(message),
      payload,
      target)) as (())
  }

  def multicast[F[_]: Sync](
      comm: ClusterCommunicationService,
      message: Message,
      payload: Array[Byte],
      targets: Set[MemberId])
      : F[Unit] = Sync[F].delay {
    if (targets.isEmpty) ()
    else
      comm.multicast(
        printMessage(message),
        payload,
        targets.to[scala.collection.mutable.Set].asJava)
  }

  import java.nio.ByteBuffer

  final case class Pending(
      items: ValueMap,
      inited: Long,
      lastAdded: Long)
      extends Product with Serializable


  final case class AdvertisementPayload(
      items: Map[Key, Long],
      local: MemberId)
      extends Product with Serializable

  final case class UpdateRequest(
      items: List[Key],
      local: MemberId)
      extends Product with Serializable

  def encodeUpdateRequest(ur: UpdateRequest): Array[Byte] = {
    val itemBytes = ur.items.map(_.getBytes)
    val size = itemBytes.foldLeft(0)((x: Int, a: Array[Byte]) => x + a.length)
    val buffer = ByteBuffer.allocate(size + itemBytes.length * 4)
    itemBytes.foreach { (as: Array[Byte]) => buffer.putInt(as.length).put(as) }
    encodeMemberId(ur.local) ++ buffer.array
  }

  def decodeUpdateRequest(as: Array[Byte]): Option[UpdateRequest] = {
    def read(buffer: ByteBuffer): String = {
      val size = buffer.getInt
      val res = Array.ofDim[Byte](size)
      buffer.get(res)
      new String(res, "UTF-8")
    }
    def go(buffer: ByteBuffer, acc: Option[List[String]]): Option[List[String]] = {
      if (buffer.remaining == 0) acc
      else {
        val newAcc: Option[List[String]] = for {
          lst <- acc
          k <- scala.Either.catchNonFatal(read(buffer)).toOption
        } yield k :: lst
        go(buffer, newAcc)
      }
    }
    val inp = ByteBuffer.wrap(as)
    for {
      id <- decodeMemberId(inp)
      items <- go(inp, Some(List()))
    } yield UpdateRequest(items, id)
  }


  def encodeAdvertisement(ad: AdvertisementPayload): Array[Byte] = {
    val res = scala.collection.mutable.ArrayBuffer.empty[Byte]
    ad.items.foreach {
      case (k, v) =>
        val kb = k.getBytes
        val item = ByteBuffer.allocate(kb.length + 12)
          .putInt(kb.length)
          .put(kb)
          .putLong(v)
          .array
        res ++= item
    }
    encodeMemberId(ad.local) ++ (res.toArray: Array[Byte])
  }

  def decodeAdvertisement(as: Array[Byte]): Option[AdvertisementPayload] = {
    def read(buffer: ByteBuffer): String = {
      val size = buffer.getInt
      val res = Array.ofDim[Byte](size)
      buffer.get(res)
      new String(res, StandardCharsets.UTF_8)
    }

    def go(buffer: ByteBuffer, acc: Option[Map[Key, Long]]): Option[Map[Key, Long]] = {
      acc match {
        case None => None
        case Some(mp) if buffer.remaining == 0 => Some(mp)
        case Some(mp) =>
          val newAcc: Option[Map[Key, Long]] = for {
            mp <- acc
            k <- scala.Either.catchNonFatal(read(buffer)).toOption
            v <- scala.Either.catchNonFatal(buffer.getLong()).toOption
          } yield mp.updated(k, v)
          go(buffer, newAcc)
      }
    }
    val inp = ByteBuffer.wrap(as)
    for {
      mid <- decodeMemberId(inp)
      items <- go(inp, Some(Map()))
    } yield AdvertisementPayload(items, mid)
  }

  def encodeMemberId(mid: MemberId): Array[Byte] = {
    val a = mid.id.getBytes("utf-8")
    ByteBuffer.allocate(a.length + 4)
      .putInt(a.length)
      .put(a)
      .array
  }

  def decodeMemberId(buffer: ByteBuffer): Option[MemberId] = scala.Either.catchNonFatal({
    val size = buffer.getInt
    val arr = Array.ofDim[Byte](size)
    buffer.get(arr)
    MemberId.from(new String(arr, java.nio.charset.Charset.forName("UTF-8")))
  }).toOption

  def encodeValueMap(items: ValueMap): Array[Byte] = {
    val res = scala.collection.mutable.ArrayBuffer.empty[Byte]
    items.foreach {
      case (k, v) =>
        val valueBytes = encodeValue(v)
        val kb = k.getBytes
        val item = ByteBuffer.allocate(valueBytes.length + kb.length + 8)
          .putInt(kb.length)
          .put(kb)
          .putInt(valueBytes.length)
          .put(valueBytes)
          .array
        res ++= item
    }
    res.toArray

  }

  def decodeValueMap(bytes: Array[Byte]): Option[ValueMap] = {
    def readKey(buffer: ByteBuffer): String = {
      val size = buffer.getInt
      val res = Array.ofDim[Byte](size)
      buffer.get(res)
      new String(res, StandardCharsets.UTF_8)
    }
    def readValue (buffer: ByteBuffer): Option[Value] = {
      val size = buffer.getInt
      val res = Array.ofDim[Byte](size)
      buffer.get(res)
      decodeValue(res)
    }
    def go(buffer: ByteBuffer, acc: Option[Map[Key, Value]]): Option[Map[Key, Value]] = {
      acc match {
        case None => None
        case Some(mp) if buffer.remaining == 0 => Some(mp)
        case Some(mp) =>
          val newAcc = for {
            k <- scala.Either.catchNonFatal(readKey(buffer)).toOption
            v <- scala.Either.catchNonFatal(readValue(buffer)).toOption.flatten
          } yield mp.updated(k, v)
          go(buffer, newAcc)
      }
    }
    go(ByteBuffer.wrap(bytes), Some(Map()))
  }

  trait Value extends Product with Serializable
  final case class Tombstone(timestamp: Long) extends Value
  final case class Tagged(raw: RawValue, timestamp: Long) extends Value

  def raw(v: Value): Option[RawValue] = v match {
    case Tombstone(_) => None
    case Tagged(raw, _) => Some(raw)
  }

  def timestamp(v: Value): Long = v match {
    case Tombstone(ts) => ts
    case Tagged(_, ts) => ts
  }

  def encodeValue(v: Value): Array[Byte] = v match {
    case Tagged(v, ts) =>
      val rawBytes = v.getBytes
      ByteBuffer
        .allocate(9)
        .put(0x0: Byte)
        .putLong(ts)
        .array ++ rawBytes
    case Tombstone(timestamp) =>
      ByteBuffer
        .allocate(9)
        .put(0xf: Byte)
        .putLong(timestamp)
        .array
  }

  def decodeValue(bytes: Array[Byte]): Option[Value] =
    if (bytes.length < 9) None
    else {
      val buffer = ByteBuffer.wrap(bytes)
      val flag: Byte = buffer.get
      val ts = buffer.getLong()
      if (flag == 0x0) {
        val arr: Array[Byte] = Array.ofDim(bytes.length - 9)
        buffer.get(arr)
        Some(Tagged(new String(arr, "UTF-8"), ts))
      } else {
        Some(Tombstone(ts))
      }
    }

  def blockingContextExecutor(pool: BlockingContext): Executor = new Executor {
    def execute(r: java.lang.Runnable) = pool.unwrap.execute(r)
  }

  def apply[F[_]: ConcurrentEffect: ContextShift](
      name: String,
      communication: ClusterCommunicationService,
      membership: ClusterMembershipService,
      underlying: ConcurrentMap[Key, Value],
      pool: BlockingContext)(
      implicit timer: Timer[F])
      : Resource[F, IndexedStore[F, Key, RawValue]] = {

    val F = ConcurrentEffect[F]

    val init: F[(IndexedStore[F, Key, RawValue], Deferred[F, Either[Throwable, Unit]])] = for {
      currentTime <- timer.clock.monotonic(MILLISECONDS)
      sendingRef <- Ref.of[F, Pending](Pending(Map(), currentTime, currentTime))
      store <- Sync[F].delay(new AEStore[F](name, communication, membership, sendingRef, underlying, pool))
      stopper <- Deferred.tryable[F, Either[Throwable, Unit]]
      // Streams!
      adReceiver <- store.advertisementHandled.map(_.interruptWhen(stopper))
      adSender <- store.sendingAdStream.map(_.interruptWhen(stopper))
      purger <- store.purgeTombstones.map(_.interruptWhen(stopper))
      updates <- store.updateHandled.map(_.interruptWhen(stopper))
      updateRequester <- store.updateRequestHandled.map(_.interruptWhen(stopper))
      synching <- store.synchronization.map(_.interruptWhen(stopper))
      // Run them!
      fibs <- List(adReceiver, adSender, purger, updates, updateRequester, synching).traverse { (s: Stream[F, Unit]) =>
        F.start(ContextShift[F].evalOn(pool.unwrap)(s.compile.drain))
      }
    } yield (store, stopper)
    def finish(pair: (IndexedStore[F, Key, RawValue], Deferred[F, Either[Throwable, Unit]])): F[Unit] =
      pair._2.complete(Right(()))
    Resource.make(init)(finish).map(_._1)
  }

}
