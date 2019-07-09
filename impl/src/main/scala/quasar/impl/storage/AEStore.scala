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
      thr = new Throwable("Incorrect bytes in underlying map insidec AEStore")
    } yield (entry.getKey, entry.getValue.raw) //scala.Either.fromOption(decodeValue(entry.getValue), thr).map((entry.getKey, _))
//    eitherStream.rethrow.map {
//      case (x, y) => (x, y.bytes)
//    }

  def lookup(k: Key): F[Option[RawValue]] = evalOnPool(F.delay{
    Option(underlying.get(k)).map(_.raw)
  })

  def insert(k: Key, v: RawValue): F[Unit] = evalOnPool(for {
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    value = Value(v, currentTime)
    valueBytes = encodeValue(value)
    _ <- F.delay(underlying.put(k, value))
    _ <- sendingRef.modify {
      case Pending(lst, time, firstAdded) =>
        (AEStore.Pending(lst.updated(k, value), time, if (lst.isEmpty) currentTime else firstAdded), ())
    }
  } yield ())

  def delete(k: Key): F[Boolean] = evalOnPool(for {
    res <- F.delay(Option(underlying.remove(k)).nonEmpty)
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    _ <- sendingRef.modify {
      case Pending(lst, time, firstAdded) =>
        // TODO, tombstone separate constructor
        (Pending(lst.updated(k, Value("", currentTime)), time, if (lst.isEmpty) currentTime else firstAdded), ())
    }
  } yield res)


  // SENDING ADS
  def sendingAdStream: F[Stream[F, Unit]] = F.delay {
    val sendAndWait = Stream.eval(sendAd) *> Stream.sleep(new FiniteDuration(100, MILLISECONDS))
    Stream.sleep(new FiniteDuration(100, MILLISECONDS)) *> sendAndWait.repeat
  }

  private def sendAd: F[Unit] = for {
    pids <- peers.map(_.map(_.id))
    ad <- prepareAd
    _ <- multicast(communication, Advertisement(name), encodeAdvertisement(ad), pids)
    _ <- ContextShift[F].shift
  } yield ()

  private def prepareAd: F[AdvertisementPayload] = localId.map { (id: MemberId) =>
    val mp = scala.collection.mutable.Map[Key, Long]()
    underlying.forEach { (k: String, v: Value) =>
      // TODO
//      case None => throw new Throwable("incorrect underlying")
//      case Some(value) =>
      mp.updated(k, v.timestamp); ()
    }
    AdvertisementPayload(mp.toMap[Key, Long], id)
  }
//  }

  // RECEIVING ADS
  def advertisement: F[Stream[F, AdvertisementPayload]] = {
    val fStream = InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, Advertisement(name), 256) *> queue.dequeue
    }
    fStream.map(decodeStream(decodeAdvertisement))
  }

  def handleAdvertisement(ad: AdvertisementPayload): F[Unit] = {
    val init: (List[Key], ValueMap) = (List(), Map())
    val result = ad.items.toList.foldM[F, (List[Key], ValueMap)](init){ (acc, v) => (acc, v) match {
      case ((requesting, returning), (k, v)) =>
        val mbCurrent = Option(underlying.get(k)) //.flatMap(decodeValue(_))
        if (mbCurrent.map(_.timestamp).getOrElse(0L) < v) {
          F.delay((k :: requesting, returning))
        } else for {
          current <- mbCurrent match {
            case Some(a) => Sync[F].delay(a)
            case None => timer.clock.monotonic(MILLISECONDS).map(Value("", _))
          }
        } yield (requesting, returning.updated(k, current))
    }}
    for {
      (requesting, returning) <- result
      id <- localId
      _ <- unicast(communication, RequestUpdate(name), encodeUpdateRequest(UpdateRequest(requesting, id)), ad.local)
      _ <- ContextShift[F].shift
      _ <- unicast(communication, Update(name), encodePendingEvents(returning), ad.local)
      _ <- ContextShift[F].shift
    } yield ()
  }

  def advertisementHandled: F[Stream[F, Unit]] =
    advertisement.map(_.evalMap(handleAdvertisement(_)))

  // TOMBSTONES PURGING
  def purgeTombstones: F[Stream[F, Unit]] =
    F.delay((Stream.sleep(new FiniteDuration(100, MILLISECONDS)) *> Stream.eval(purge)).repeat)

  private def purge: F[Unit] = F.delay { underlying.synchronized {
    underlying.forEach { (k: Key, v: Value) => //decodeValue(v) match {
//      case Some(Value(payload, _)) if (!payload.isEmpty) => ()
//      case _ =>
      underlying.remove(k); () }
//    }}
  }}

  // BOOTSTRAPPING
  def requestBootstrap: F[Unit] = for {
    id <- localId
    pids <- peers.map(_.map(_.id))
    _ <- multicast(communication, RequestBootstrap(name), encodeMemberId(id), pids)
    _ <- ContextShift[F].shift
  } yield ()

  def bootstrapping: F[Stream[F, MemberId]] = {
    val fStream = InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, RequestBootstrap(name), 256) *> queue.dequeue
    }
    fStream.map(_.map(decodeMemberId(_)))
  }
  def bootstrapHandler(requester: MemberId): F[Unit] = {
    def group(m: ValueMap, groups: List[ValueMap]): List[ValueMap] = {
      val item = m.take(MaxEvents)
      val remaining = m.drop(MaxEvents)
      if (remaining.size == 0)
        item :: groups
      else
        group(remaining, item :: groups)
    }
    val groups = group(underlying.asScala.toMap, List())
    groups.traverse_ { (mp: ValueMap) =>
      unicast(communication, Bootstrap(name), encodePendingEvents(mp), requester) *> ContextShift[F].shift
    }
  }
  def bootstrapHandled: F[Stream[F, Unit]] =
    bootstrapping.map(_.evalMap(bootstrapHandler(_)))

  // RECEIVING UPDATES (both via initialization and update messages)
  def initializing: F[Stream[F, ValueMap]] = {
    val fStream = InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, Bootstrap(name), 32) *> queue.dequeue
    }
    fStream.map(decodeStream(decodePendingEvents))
  }
  def updateReceived: F[Stream[F, ValueMap]] = {
    val fStream = InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, Update(name), 128) *> queue.dequeue
    }
    fStream.map(decodeStream(decodePendingEvents))
  }
  def updateHandler(mp: ValueMap): F[Unit] = {
    println(s"mp ::: $mp")
    mp.toList.traverse_ {
      case (k, newVal) =>
        val oldVal: Option[Value] = Option(underlying.get(k))//.flatMap(decodeValue(_))
          F.delay(underlying.put(k, newVal)).whenA {
            oldVal.map(_.timestamp).getOrElse(0L) < newVal.timestamp
          }
    }
  }

  def updateHandled: F[Stream[F, Unit]] = for {
    init <- initializing
    updates <- updateReceived
  } yield init.merge(updates).evalMap(updateHandler)

  // REQUESTING FOR UPDATES

  def updateRequesting: F[Stream[F, UpdateRequest]] = {
    val fStream = InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, RequestUpdate(name), 128) *> queue.dequeue
    }
    fStream.map(decodeStream(decodeUpdateRequest))
  }

  def updateRequestedHandler(req: UpdateRequest): F[Unit] = {
    val payload: ValueMap = req.items.foldLeft(Map[Key, Value]()) { (acc: ValueMap, k: Key) =>
      Option(underlying.get(k)) /*.flatMap(decodeValue(_)) */ match {
        case None => acc
        case Some(toInsert) => acc.updated(k, toInsert)
      }
    }
    unicast(communication, Update(name), encodePendingEvents(payload), req.local) *> ContextShift[F].shift
  }

  def updateRequestHandled: F[Stream[F, Unit]] =
    updateRequesting.map(_.evalMap(updateRequestedHandler(_)))

  // SYNCHRONIZATION

  def synchronization: F[Stream[F, Unit]] =
    F.delay((Stream.eval(notifyPeersF) *> Stream.sleep(new FiniteDuration(10, MILLISECONDS))).repeat)

  def notifyPeersF: F[Unit] = for {
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    AEStore.Pending(lst, lastTime, firstAdded) <- sendingRef.get
    shouldUpdate = (currentTime - firstAdded > 50) || (lst.size > 50) || (currentTime - lastTime > 200)
//    _ <- sendingRef.set(Pending(lst, currentTime, firstAdded)).whenA(lst.isEmpty)
    _ <- (sendToAll *> sendingRef.set(Pending(Map(), currentTime, 0L))).whenA(shouldUpdate)
  } yield ()

  private def sendToAll: F[Unit] = for {
    pids <- peers.map(_.map(_.id))
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    msg <- sendingRef.modify {
      case x@Pending(lst, currentTime, firstAdded) =>
        println(s"lst ::: $lst")
        (AEStore.Pending(Map(), currentTime, 0L), encodePendingEvents(lst))
    }
    _ <- multicast(communication, Update(name), msg, pids)
    _ <- ContextShift[F].shift
  } yield ()



  // PRIVATE

  private def run(action: F[Unit]) =
    ConcurrentEffect[F].runAsync(action)(_ => IO.unit).unsafeRunSync

  private def handler(eventName: String, cb: Array[Byte] => Unit): F[Unit] = for {
    _ <- cfToAsync(communication.subscribe[Array[Byte]](
      eventName,
      (x: Array[Byte]) => {
        cb(x)
      },
      blockingContextExecutor(blockingPool)))
  } yield ()

  private def enqueue(q: InspectableQueue[F, Array[Byte]], eventType: Message, maxItems: Int): Stream[F, Unit] = {
    Stream.eval(handler(printMessage(eventType), { (a: Array[Byte]) => run {
      q.getSize.flatMap((size: Int) => q.enqueue1(a).whenA(size < maxItems))
    }}))
  }

  private def unsubscribe(eventName: String): F[Unit] = F.delay {
    communication.unsubscribe(eventName)
  }

  private def close: F[Unit] =
    List(RequestBootstrap(name), Bootstrap(name), RequestUpdate(name), Update(name), Advertisement(name)).traverse_ { (m: Message) =>
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
      case None => Stream.raiseError[F](new Throwable("can't decode"))
      case Some(b) => Stream.emit(b)
    }}

  trait Message extends Product with Serializable
  final case class RequestBootstrap(name: String) extends Message
  final case class Bootstrap(name: String) extends Message
  final case class RequestUpdate(name: String) extends Message
  final case class Update(name: String) extends Message
  final case class Advertisement(name: String) extends Message

  def printMessage(m: Message): String = m match {
    case RequestBootstrap(n) => s"aestore::requestBootrap::${n}"
    case Bootstrap(n) => s"aestore::bootstrap::${n}"
    case RequestUpdate(n) => s"aestore::requestUpdate::${n}"
    case Update(n) => s"aestore::update::${n}"
    case Advertisement(n) => s"aestore::advertisement::${n}"
  }

  def unicast[F[_]: Async: ContextShift](
      comm: ClusterCommunicationService,
      message: Message,
      payload: Array[Byte],
      target: MemberId)
      : F[Unit] =
    cfToAsync(comm.unicast(
      printMessage(message),
      payload,
      target)) as (())

  def multicast[F[_]: Sync](
      comm: ClusterCommunicationService,
      message: Message,
      payload: Array[Byte],
      targets: Set[MemberId])
      : F[Unit] = Sync[F].delay {
    comm.multicast(
      printMessage(message),
      payload,
      targets.to[scala.collection.mutable.Set].asJava)
  }

  import java.nio.ByteBuffer

  final case class Pending(
      items: ValueMap,
      lastSend: Long,
      firstAdded: Long)
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
    buffer.array ++ encodeMemberId(ur.local)
  }

  def decodeUpdateRequest(as: Array[Byte]): Option[UpdateRequest] = {
    def read(buffer: ByteBuffer): Array[Byte] = {
      val size = buffer.getInt
      val res = Array[Byte]()
      buffer.get(res, 0, size)
      res
    }
    def go(buffer: ByteBuffer, acc: Option[List[Array[Byte]]]): Option[List[Array[Byte]]] = {
      if (buffer.remaining == 0) acc
      else {
        val newAcc: Option[List[Array[Byte]]] = for {
          lst <- acc
          k <- scala.Either.catchNonFatal(read(buffer)).toOption
        } yield k :: lst
        go(buffer, newAcc)
      }
    }
    val inp = ByteBuffer.wrap(as)
    val lst = go(inp, Some(List())).map(_.map(new String(_, StandardCharsets.UTF_8)))
    val memberArr = Array[Byte]()
    inp.get(memberArr)
    lst.map(UpdateRequest(_, decodeMemberId(memberArr)))
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
    res ++= encodeMemberId(ad.local)
    res.toArray
  }

  def decodeAdvertisement(as: Array[Byte]): Option[AdvertisementPayload] = {
    def read(buffer: ByteBuffer): String = {
      val size = buffer.getInt
      val res = Array[Byte]()
      buffer.get(res, 0, size)
      new String(res, StandardCharsets.UTF_8)
    }
    def go(buffer: ByteBuffer, acc: Option[Map[Key, Long]]): Option[Map[Key, Long]] = {
      if (buffer.remaining == 0) acc
      else {
        val newAcc: Option[Map[Key, Long]] = for {
          mp <- acc
          k <- scala.Either.catchNonFatal(read(buffer)).toOption
          v <- scala.Either.catchNonFatal(buffer.getLong()).toOption
        } yield mp.updated(k, v)
        go(buffer, newAcc)
      }
    }
    val inp = ByteBuffer.wrap(as)
    val items = go(inp, Some(Map()))
    val memberArr = Array[Byte]()
    inp.get(memberArr)
    items.map(AdvertisementPayload(_, decodeMemberId(memberArr)))
  }

  def encodeMemberId(mid: MemberId): Array[Byte] =
    mid.id.getBytes("utf-8")

  def decodeMemberId(bytes: Array[Byte]): MemberId =
    MemberId.from(new String(bytes, java.nio.charset.Charset.forName("UTF-8")))

  def encodePendingEvents(items: Map[Key, Value]): Array[Byte] = {
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

  def decodePendingEvents(bytes: Array[Byte]): Option[Map[Key, Value]] = {
    def readKey(buffer: ByteBuffer): String = {
      val size = buffer.getInt
      val res = Array[Byte]()
      buffer.get(res, 0, size)
      new String(res, StandardCharsets.UTF_8)
    }
    def readValue (buffer: ByteBuffer): Option[Value] = {
      val size = buffer.getInt
      val res = Array[Byte]()
      buffer.get(res, 0, size)
      decodeValue(res)
    }
    def go(buffer: ByteBuffer, acc: Option[Map[Key, Value]]): Option[Map[Key, Value]] = {
      if (buffer.remaining == 0) acc
      else {
        val newAcc = for {
          mp <- acc
          k <- scala.Either.catchNonFatal(readKey(buffer)).toOption
          v <- scala.Either.catchNonFatal(readValue(buffer)).toOption.flatten
        } yield mp.updated(k, v)
        go(buffer, newAcc)
      }
    }
    go(ByteBuffer.wrap(bytes), Some(Map()))
  }

  final case class Value(raw: RawValue, timestamp: Long) extends Product with Serializable

  def encodeValue(v: Value): Array[Byte] = {
    val rawBytes = v.raw.getBytes
    ByteBuffer.allocate(8).putLong(v.timestamp).array ++ rawBytes
  }

  def decodeValue(bytes: Array[Byte]): Option[Value] =
    if (bytes.length < 8) None
    else {
      val buffer = ByteBuffer.wrap(bytes)
      val ts = buffer.getLong()
      val arr: Array[Byte] = Array()
      buffer.get(arr)
      Some(Value(new String(arr, "UTF-8"), ts))
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
      sendingRef <- Ref.of[F, Pending](Pending(Map(), 0L, 0L))
      store <- Sync[F].delay(new AEStore[F](name, communication, membership, sendingRef, underlying, pool))
      stopper <- Deferred.tryable[F, Either[Throwable, Unit]]
      // Streams!
      adReceiver <- store.advertisementHandled.map(_.interruptWhen(stopper))
      adSender <- store.sendingAdStream.map(_.interruptWhen(stopper))
      purger <- store.purgeTombstones.map(_.interruptWhen(stopper))
      bootstrapper <- store.bootstrapHandled.map(_.interruptWhen(stopper))
      updates <- store.updateHandled.map(_.interruptWhen(stopper))
      updateRequester <- store.updateRequestHandled.map(_.interruptWhen(stopper))
      synching <- store.synchronization.map(_.interruptWhen(stopper))
      // Run them!
      fibs <- List(adReceiver, adSender, purger, bootstrapper, updates, updateRequester, synching).traverse { (s: Stream[F, Unit]) =>
        F.start(ContextShift[F].evalOn(pool.unwrap)(s.compile.drain))
      }
    } yield (store, stopper)
    def finish(pair: (IndexedStore[F, Key, RawValue], Deferred[F, Either[Throwable, Unit]])): F[Unit] =
      pair._2.complete(Right(()))
    Resource.make(init)(finish).map(_._1)
  }

}
