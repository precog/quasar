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
import cats.effect.concurrent.Ref
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.applicative._
import cats.syntax.functor._
import cats.syntax.either._
import cats.instances.either._
import cats.instances.option._
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
import java.util.function.Consumer
import scala.concurrent.duration._

import AEStore._

final class AEStore[F[_]: ConcurrentEffect: ContextShift](
    name: String,
    communication: ClusterCommunicationService,
    membership: ClusterMembershipService,
    sendingRef: Ref[F, Pending],
    underlying: ConcurrentMap[Array[Byte], Array[Byte]],
    blockingPool: BlockingContext)(
    implicit
      timer: Timer[F])

    extends IndexedStore[F, Array[Byte], Array[Byte]] {

  val LocalId: MemberId = membership.getLocalMember.id
  val MaxEvents: Int = 50


  private def evalOnPool[A](fa: F[A]): F[A] =
    ContextShift[F].evalOn[A](blockingPool.unwrap)(fa)

  private def evalStreamOnPool[A](s: Stream[F, A]): Stream[F, A] =
    s.translate(new FunctionK[F, F] {
      def apply[A](fa: F[A]): F[A] = evalOnPool(fa)
    })

  def entries: Stream[F, (Array[Byte], Array[Byte])] = {
    val eitherStream: Stream[F, Either[Throwable, (Array[Byte], Value)]] = for {
      iterator <- Stream.eval(evalOnPool(Sync[F].delay(underlying.entrySet.iterator.asScala)))
      entry <- evalStreamOnPool(
        Stream.fromIterator[F, java.util.Map.Entry[Array[Byte], Array[Byte]]](iterator))
      thr = new Throwable("Incorrect bytes in underlying map insidec AEStore")
    } yield scala.Either.fromOption(decodeValue(entry.getValue), thr).map((entry.getKey, _))
    eitherStream.rethrow.map {
      case (x, y) => (x, y.bytes)
    }
  }

  def lookup(k: Array[Byte]): F[Option[Array[Byte]]] = evalOnPool(Sync[F].delay{
    Option(underlying.get(k)).flatMap(decodeValue(_)).map(_.bytes)
  })

  def insert(k: Array[Byte], v: Array[Byte]): F[Unit] = evalOnPool(for {
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    value = Value(v, currentTime)
    valueBytes = encodeValue(value)
    _ <- Sync[F].delay(underlying.put(k, valueBytes))
    _ <- sendingRef.modify {
      case Pending(lst, time, firstAdded) =>
        (AEStore.Pending(lst.updated(k, value), time, if (lst.isEmpty) currentTime else firstAdded), ())
    }
  } yield ())

  def delete(k: Array[Byte]): F[Boolean] = evalOnPool(for {
    res <- Sync[F].delay(Option(underlying.remove(k)).nonEmpty)
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    _ <- sendingRef.modify {
      case Pending(lst, time, firstAdded) =>
        (Pending(lst.updated(k, Value(Array(), currentTime)), time, if (lst.isEmpty) currentTime else firstAdded), ())
    }
  } yield res)

  private def peers: F[Set[Member]] = Sync[F].delay {
    membership.getMembers.asScala.to[Set]
  }

  def advertisement: F[Stream[F, Unit]] = Sync[F].delay {
    val sendAndWait = Stream.eval(sendAd) *> Stream.sleep(new FiniteDuration(100, MILLISECONDS))
    Stream.sleep(new FiniteDuration(5000, MILLISECONDS)) *> sendAndWait.repeat
  }
  val AdMessage: String = "aestore::advertisement"

  private def sendAd: F[Unit] = for {
    pids <- peers.map(_.map(_.id))
    ad <- prepareAd
    _ <- Sync[F].delay(communication.multicast(
      AdMessage,
      encodeAdvertisement(ad),
      pids.to[scala.collection.mutable.Set].asJava))
  } yield ()

  private def prepareAd: F[Advertisement] = Sync[F].delay {
    val mp = scala.collection.mutable.Map[Array[Byte], Long]()
    underlying.forEach { (k: Array[Byte], v: Array[Byte]) => decodeValue(v) match {
      // TODO
      case None => throw new Throwable("incorrect underlying")
      case Some(value) => mp.updated(k, value.timestamp); ()
    }}
    Advertisement(mp.toMap[Array[Byte], Long], LocalId)
  }

  def listenAE: F[Stream[F, Array[Byte]]] =
    InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, AdMessage, 256) *> queue.dequeue
    }

  def handleAd(bytes: Array[Byte]): F[Unit] = {
    val mbad: Option[Advertisement] = decodeAdvertisement(bytes)
    mbad match {
      case None => Sync[F].raiseError(new Throwable("???"))
      case Some(ad) =>
        val result = ad.items.toList.foldM[F, (List[Array[Byte]], Map[Array[Byte], Value])]((List[Array[Byte]](), Map[Array[Byte], Value]())){ (acc, v) => (acc, v) match {
          case ((requesting, returning), (k, v)) =>
            val mbCurrent = Option(underlying.get(k)).flatMap(decodeValue(_))
            if (mbCurrent.map(_.timestamp).getOrElse(0L) < v) {
              Sync[F].delay((k :: requesting, returning))
            } else for {
              current <- mbCurrent match {
                case Some(a) => Sync[F].delay(a)
                case None => timer.clock.monotonic(MILLISECONDS).map(Value(Array(), _))
              }
            } yield (requesting, returning.updated(k, current))
        }}
        for {
          (requesting, returning) <- result
          _ <- Sync[F].delay(communication.unicast(
            UpdateRequestMessage,
            encodeUpdateRequest(UpdateRequest(requesting, LocalId)),
            ad.local))
          _ <- Sync[F].delay(communication.unicast(
            UpdateReceivedMessage,
            encodePendingEvents(returning),
            ad.local))
        } yield ()
    }
  }

  def purgeTombstones: F[Stream[F, Unit]] =
    Sync[F].delay((Stream.sleep(new FiniteDuration(100, MILLISECONDS)) *> Stream.eval(purge)).repeat)

  private def purge: F[Unit] = Sync[F].delay {
    println("PURGING TOMBSTONES")
  }

  private def run(action: F[Unit]) =
    ConcurrentEffect[F].runAsync(action)(_ => IO.unit).unsafeRunSync

  private def handler[A](eventName: String, cb: A => Unit): Unit = {
    val consumer: Consumer[A] = (a) => cb(a)
    communication.subscribe(
      eventName,
      consumer,
      blockingContextExecutor(blockingPool))
    ()
  }

  private def enqueue[A](q: InspectableQueue[F, A], eventName: String, maxItems: Int): Stream[F, Unit] =
    Stream.eval(Sync[F].delay(handler[A](eventName, { (a: A) => run {
      q.getSize.flatMap((size: Int) => q.enqueue1(a).whenA(size < maxItems))
    }})))

  private def unsubscribe(eventName: String): F[Unit] = Sync[F].delay {
    communication.unsubscribe(eventName)
  }

  val BootstrapMessage: String = s"aestore::bootstrapping"
  def bootstrapping: F[Stream[F, Array[Byte]]] =
    InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, BootstrapMessage, 256) *> queue.dequeue
    }
  def bootstrapHandler(as: Array[Byte]): F[Unit] = {
    val requester: MemberId = decodeMemberId(as)
    def group(m: Map[Array[Byte], Array[Byte]], groups: List[Map[Array[Byte], Array[Byte]]]): List[Map[Array[Byte], Array[Byte]]] = {
      val item = m.take(MaxEvents)
      val remaining = m.drop(MaxEvents)
      if (remaining.size == 0)
        item :: groups
      else
        group(remaining, item :: groups)
    }
    val groups = group(underlying.asScala.toMap, List())
    groups.traverse_ { (mp: Map[Array[Byte], Array[Byte]]) => Sync[F].delay {
      communication.unicast(
        InitMessage,
        encodeBootstrap(mp),
        requester);
      ()
    }}
  }

  val InitMessage: String = s"aestore::initializing"
  def initializing: F[Stream[F, Array[Byte]]] =
    InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, InitMessage, 32) *> queue.dequeue
    }
  def updateHandler(as: Array[Byte]): F[Unit] = {
    val pendingEvents: Option[Map[Array[Byte], Value]] = decodePendingEvents(as)
    pendingEvents match {
      case None => Sync[F].raiseError(new Throwable("Incorrect message received"))
      case Some(mp) => mp.toList.traverse_ {
        case (k, newVal) =>
          val oldVal: Option[Value] = Option(underlying.get(k)).flatMap(decodeValue(_))
          if (oldVal.map(_.timestamp).getOrElse(0L) < newVal.timestamp) {
            Sync[F].delay(underlying.put(k, encodeValue(newVal))) as (())
          } else {
            Sync[F].delay(())
          }
      }
    }
  }

  val UpdateRequestMessage: String = s"aestore::update-request::${name}"
  def updateRequesing: F[Stream[F, Array[Byte]]] =
    InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, UpdateRequestMessage, 128) *> queue.dequeue
    }
  def updateRequestedHandler(as: Array[Byte]): F[Unit] = decodeUpdateRequest(as) match {
    case None => Sync[F].raiseError(new Throwable(">>>"))
    case Some(req) =>
      val payload: Map[Array[Byte], Value] = req.items.foldLeft(Map[Array[Byte], Value]()) { (acc: Map[Array[Byte], Value], k: Array[Byte]) =>
        Option(underlying.get(k)).flatMap(decodeValue(_)) match {
          case None => acc
          case Some(toInsert) => acc.updated(k, toInsert)
        }
      }
      Sync[F].delay(communication.unicast(
        UpdateReceivedMessage,
        encodePendingEvents(payload),
        req.local))
  }

  val UpdateReceivedMessage: String = s"aestore::updated::${name}"
  def updateReceived: F[Stream[F, Array[Byte]]] =
    InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, UpdateReceivedMessage, 128) *> queue.dequeue
    }

  private def sendToAll: F[Unit] = for {
    pids <- peers.map(_.map(_.id))
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    msg <- sendingRef.modify {
      case x@Pending(lst, currentTime, firstAdded) =>
        (AEStore.Pending(Map(), currentTime, 0L), encodePendingEvents(lst))
    }
    _ <- Sync[F].delay(communication.multicast(
      UpdateReceivedMessage,
      msg,
      pids.to[scala.collection.mutable.Set].asJava))
  } yield ()

  def notifyPeersF: F[Unit] = for {
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    AEStore.Pending(lst, lastTime, firstAdded) <- sendingRef.get
    _ <- if (lst.isEmpty) {
      sendingRef.set(Pending(lst, currentTime, firstAdded))
    } else if (currentTime - firstAdded > 50) {
      sendToAll *> sendingRef.set(Pending(Map(), currentTime, 0L))
    } else if (lst.size > 50) {
      sendToAll *> sendingRef.set(Pending(Map(), currentTime, 0L))
    } else if (currentTime - lastTime > 200) {
      sendToAll *> sendingRef.set(Pending(Map(), currentTime, 0L))
    } else { Sync[F].delay(()) }
  } yield ()

  def notifyingPeers: Stream[F, Unit] =
    (Stream.eval(notifyPeersF) *> Stream.sleep(new FiniteDuration(10, MILLISECONDS))).repeat

  def bootstrap: F[Unit] =
    peers.flatMap(_.toList.traverse_(requestBootrap(_)))

  def requestBootrap(member: Member): F[Unit] = Sync[F].delay {
    communication.unicast(
      BootstrapMessage,
      encodeMemberId(LocalId),
      member.id);
    ()
  }
}

object AEStore {
  import java.nio.ByteBuffer

  final case class Pending(
      items: Map[Array[Byte], Value],
      lastSend: Long,
      firstAdded: Long)
      extends Product with Serializable


  final case class Advertisement(
      items: Map[Array[Byte], Long],
      local: MemberId)
      extends Product with Serializable

  final case class UpdateRequest(
      items: List[Array[Byte]],
      local: MemberId)
      extends Product with Serializable

  def encodeUpdateRequest(r: UpdateRequest): Array[Byte] = {
    val size = r.items.foldLeft(0)((x: Int, a: Array[Byte]) => x + a.length)
    val buffer = ByteBuffer.allocate(size + r.items.length * 4)
    r.items.foreach { (as: Array[Byte]) => buffer.putInt(as.length).put(as) }
    buffer.array ++ encodeMemberId(r.local)
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
    val lst = go(inp, Some(List()))
    val memberArr = Array[Byte]()
    inp.get(memberArr)
    lst.map(UpdateRequest(_, decodeMemberId(memberArr)))
  }


  def encodeAdvertisement(ad: Advertisement): Array[Byte] = {
    val res = scala.collection.mutable.ArrayBuffer.empty[Byte]
    ad.items.foreach {
      case (k, v) =>
        val item = ByteBuffer.allocate(k.length + 12)
          .putInt(k.length)
          .put(k)
          .putLong(v)
          .array
        res ++= item
    }
    res ++= encodeMemberId(ad.local)
    res.toArray
  }

  def decodeAdvertisement(as: Array[Byte]): Option[Advertisement] = {
    def read(buffer: ByteBuffer): Array[Byte] = {
      val size = buffer.getInt
      val res = Array[Byte]()
      buffer.get(res, 0, size)
      res
    }
    def go(buffer: ByteBuffer, acc: Option[Map[Array[Byte], Long]]): Option[Map[Array[Byte], Long]] = {
      if (buffer.remaining == 0) acc
      else {
        val newAcc: Option[Map[Array[Byte], Long]] = for {
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
    items.map(Advertisement(_, decodeMemberId(memberArr)))
  }

  def encodeMemberId(mid: MemberId): Array[Byte] =
    mid.id.getBytes("utf-8")

  def decodeMemberId(bytes: Array[Byte]): MemberId =
    MemberId.from(new String(bytes, java.nio.charset.Charset.forName("UTF-8")))

  def encodeBootstrap(items: Map[Array[Byte], Array[Byte]]): Array[Byte] = {
    val res = scala.collection.mutable.ArrayBuffer.empty[Byte]
    items.foreach {
      case (k, v) =>
        val item = ByteBuffer.allocate(v.length + k.length + 8)
          .putInt(k.length)
          .put(k)
          .putInt(v.length)
          .put(v)
          .array
        res ++= item
    }
    res.toArray
  }

  def decodeBootstrap(bytes: Array[Byte]): Option[Map[Array[Byte], Array[Byte]]] = {
    def read(buffer: ByteBuffer): Array[Byte] = {
      val size = buffer.getInt
      val res = Array[Byte]()
      buffer.get(res, 0, size)
      res
    }
    def go(buffer: ByteBuffer, acc: Option[Map[Array[Byte], Array[Byte]]]): Option[Map[Array[Byte], Array[Byte]]] = {
      if (buffer.remaining == 0) acc
      else {
        val newAcc: Option[Map[Array[Byte], Array[Byte]]] = for {
          mp <- acc
          k <- scala.Either.catchNonFatal(read(buffer)).toOption
          v <- scala.Either.catchNonFatal(read(buffer)).toOption
        } yield mp.updated(k, v)
        go(buffer, newAcc)
      }
    }
    go(ByteBuffer.wrap(bytes), Some(Map()))
  }

  def encodePendingEvents(items: Map[Array[Byte], Value]): Array[Byte] = {
    val res = scala.collection.mutable.ArrayBuffer.empty[Byte]
    items.foreach {
      case (k, v) =>
        val valueBytes = encodeValue(v)
        val item = ByteBuffer.allocate(valueBytes.length + k.length + 8)
          .putInt(k.length)
          .put(k)
          .putInt(valueBytes.length)
          .put(valueBytes)
          .array
        res ++= item
    }
    res.toArray

  }

  def decodePendingEvents(bytes: Array[Byte]): Option[Map[Array[Byte], Value]] = {
    def readKey(buffer: ByteBuffer): Array[Byte] = {
      val size = buffer.getInt
      val res = Array[Byte]()
      buffer.get(res, 0, size)
      res
    }
    def readValue (buffer: ByteBuffer): Option[Value] = {
      val size = buffer.getInt
      val res = Array[Byte]()
      buffer.get(res, 0, size)
      decodeValue(res)
    }
    def go(buffer: ByteBuffer, acc: Option[Map[Array[Byte], Value]]): Option[Map[Array[Byte], Value]] = {
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

  final case class Value(bytes: Array[Byte], timestamp: Long) extends Product with Serializable

  def encodeValue(v: Value): Array[Byte] = ByteBuffer.allocate(8).putLong(v.timestamp).array ++ v.bytes

  def decodeValue(bytes: Array[Byte]): Option[Value] =
    if (bytes.length < 8) None
    else {
      val buffer = ByteBuffer.wrap(bytes)
      val ts = buffer.getLong()
      val arr: Array[Byte] = Array()
      buffer.get(arr)
      Some(Value(arr, ts))
    }

  def blockingContextExecutor(pool: BlockingContext): Executor = new Executor {
    def execute(r: java.lang.Runnable) = pool.unwrap.execute(r)
  }

  /* OK, so basically we need the following things
   * x) event streams which ignores events if there are too much of them, they are handled on blocking pool
   * x) timed streams which sends advertisements and purge tombstone commands, they are evaled on blocking pool, preferrable single threaded
   * x) batch sender, that is the thing that accumulates events and sends them together to all peers selected by specific function.
   * Note that default peer selection is either one random node or all nodes (it's 6a.m. I'm a bit tired), and this should be somehow
   * adjustable: cluster size?, config?
   * The sender should work on blocking pool too.
   *
   * The `items` field in the atomix lib should be injectable via constructor and be kinda parameterizable
   * 1) We need a function from `Map[K, V]` to `Map[Array[Byte], Array[Byte]]` at least for strings
   * 2) After loading instead of requesting updates with only memberid we can request data with loaded values. We might also bootstrap things
   * using a couple of, this also changes updates communication a bit.
   * 3) `Serializer[A]`, `MapValue` serializer
   *
   *  There is no need in any builder or other fancy interfaces, `AtomixCluster` have all services needed here.
   */

  def apply[F[_]: ConcurrentEffect: ContextShift](
      name: String,
      communication: ClusterCommunicationService,
      membership: ClusterMembershipService,
      underlying: ConcurrentMap[Array[Byte], Array[Byte]],
      handlePool: BlockingContext,
      sendingPool: ExecutionContext,
      syncContext: ExecutionContext)(
      implicit timer: Timer[F])
      : F[IndexedStore[F, Array[Byte], Array[Byte]]] = {
    val BootstrapMessage: String = s"aestore::bootstrapping::${name}"
    val InitMessage: String = s"aestore::initializing::${name}"
    val AntiEntropyMessage: String = s"aestore::anti-entropy::${name}"
    val UpdateRequestMessage: String = s"aestore::update-request::${name}"
    val UpdateReceivedMessage: String = s"aestore::updated::${name}"
    for {
      sendingRef <- Ref.of[F, Pending](Pending(Map(), 0L, 0L))
      store <- Sync[F].delay(new AEStore[F](name, communication, membership, sendingRef, underlying, handlePool))
    } yield store
  }
}
