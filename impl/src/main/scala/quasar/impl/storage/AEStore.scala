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

import cats.arrow.FunctionK
import cats.effect._
import cats.effect.concurrent.{Ref, Deferred}
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.applicative._
import cats.syntax.functor._
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
import java.util.concurrent.{Executor, ConcurrentMap}
import scala.concurrent.duration._

import scodec._
import scodec.codecs._
import scodec.codecs.implicits._
import scodec.bits.{ByteVector, BitVector}

import AEStore._
import AtomixSetup._

final class AEStore[F[_]: ConcurrentEffect: ContextShift, K: Codec, V: Codec](
    name: String,
    communication: ClusterCommunicationService,
    membership: ClusterMembershipService,
    sendingRef: Ref[F, Pending[K, V]],
    underlying: IndexedStore[F, K, MapValue[V]],
    blockingPool: BlockingContext)(
    implicit timer: Timer[F])
    extends IndexedStore[F, K, V] {

  val MaxEvents: Int = 50
  val AdTimeout: FiniteDuration = new FiniteDuration(500, MILLISECONDS)
  val PurgeTimeout: FiniteDuration = new FiniteDuration(200, MILLISECONDS)
  val SyncTimeout: FiniteDuration = new FiniteDuration(200, MILLISECONDS)
  val BatchInterval: Int = 50
  val CollectingInterval: Int = 100

  private val F = Sync[F]

  def localId: F[MemberId] = F.delay {
    membership.getLocalMember.id
  }

  def peers: F[Set[Member]] = F.delay {
    membership.getMembers.asScala.to[Set]
  }

  private def evalOnPool[A](fa: F[A]): F[A] =
    ContextShift[F].evalOn[A](blockingPool.unwrap)(fa)

  private def evalStreamOnPool[A](s: Stream[F, A]): Stream[F, A] =
    s.translate(new FunctionK[F, F] {
      def apply[A](fa: F[A]): F[A] = evalOnPool(fa)
    })

  def entries: Stream[F, (K, V)] =
    underlying.entries.map({ case (k, v) => raw(v).map((k, _))}).unNone

  def lookup(k: K): F[Option[V]] =
    underlying.lookup(k).map(_.flatMap(raw(_)))

  def insert(k: K, v: V): F[Unit] = evalOnPool(for {
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    value: MapValue[V] = Tagged(v, currentTime)
    _ <- underlying.insert(k, value)
    id <- localId
    _ <- sendingRef.modify((x: Pending[K, V]) => (Pending(x.items.updated(k, value), x.inited, currentTime), ()))
  } yield ())

  def delete(k: K): F[Boolean] = evalOnPool(for {
    res <- underlying.delete(k)
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    _ <- sendingRef.modify((x: Pending[K, V]) => (Pending(x.items.updated(k, Tombstone(currentTime)), x.inited, currentTime), ()))
  } yield res)


  // SENDING ADS
  def sendingAdStream: F[Stream[F, Unit]] =
    F.delay((Stream.eval(sendAd) *> Stream.sleep(AdTimeout)).repeat)

  private def sendAd: F[Unit] = for {
    pids <- peers.map(_.map(_.id))
    ad <- prepareAd
    id <- localId
    _ <- multicast(communication, Advertisement(name), ad, pids - id).unlessA((pids - id).isEmpty)
    _ <- ContextShift[F].shift
  } yield ()

  private def prepareAd: F[AdvertisementPayload[K]] = for {
    lst <- underlying.entries.map({ case (k, v) => (k, timestamp(v))}).compile.toList
    id <- localId
  } yield AdvertisementPayload(lst.toMap, id)


  // RECEIVING ADS
  def handleAdvertisement(ad: AdvertisementPayload[K]): F[Unit] = {
    type Accum = (List[K], Map[K, MapValue[V]])

    val fInitMap: F[Map[K, MapValue[V]]] =
      underlying
        .entries
        .map({ case (k, v) => ad.items.get(k) as ((k, v)) })
        .unNone
        .compile
        .toList
        .map(_.toMap)

    val fInit: F[Accum] = fInitMap.map((List(), _))

    def result(init: Accum)  = ad.items.toList.foldM[F, Accum](init){ (acc, v) => (acc, v) match {
      case ((requesting, returning), (k, v)) => for {
        mbCurrent <- underlying.lookup(k)
        res <- if (mbCurrent.fold(0L)(timestamp(_)) < v) {
          F.delay((k :: requesting, returning))
        } else for {
          (current: MapValue[V]) <- mbCurrent.fold(
            timer.clock.monotonic(MILLISECONDS).map((x: Long) => Tombstone(x): MapValue[V]))(
            F.delay(_))
          } yield (requesting, returning.updated(k, current))
        } yield res
    }}
    for {
      init <- fInit
      (requesting, returning) <- result(init)
      id <- localId
      _ <- unicast(communication, RequestUpdate(name), UpdateRequest(requesting, id), ad.local)
      _ <- ContextShift[F].shift
      _ <- unicast(communication, Update(name), returning, ad.local)
      _ <- ContextShift[F].shift
    } yield ()
  }

  def advertisementHandled: F[Stream[F, Unit]] =
    subscriptionStream[AdvertisementPayload[K]](Advertisement(name), 256)
      .map(_.evalMap(handleAdvertisement(_)))

  // TOMBSTONES PURGING
  def purgeTombstones: F[Stream[F, Unit]] =
    F.delay((Stream.sleep(PurgeTimeout) *> Stream.eval(purge)).repeat)

  private def purge: F[Unit] = underlying.synchronized {
    underlying.entries.evalMap({ case (k, v) =>
      underlying.delete(k).whenA(raw(v).isEmpty)
    }).compile.drain
  }

  // RECEIVING UPDATES (both via initialization and update messages)

  def updateHandler(mp: Map[K, MapValue[V]]): F[Unit] = mp.toList.traverse_ {
    case (k, newVal) => for {
      v <- underlying.lookup(k)
      ts = v.fold(0L)(timestamp(_))
      _ <- underlying.insert(k, newVal).whenA(ts < timestamp(newVal))
    } yield (())
  }

  def updateHandled: F[Stream[F, Unit]] =
    subscriptionStream[Map[K, MapValue[V]]](Update(name), 256)
      .map(_.evalMap(updateHandler(_)))

  // REQUESTING FOR UPDATES

  def updateRequestedHandler(req: UpdateRequest[K]): F[Unit] = for {
    payload <- req.items.foldM(Map[K, MapValue[V]]()) { (acc: Map[K, MapValue[V]], k: K) =>
      underlying.lookup(k).map(_.fold(acc)(acc.updated(k, _)))
    }
    _ <- unicast(communication, Update(name), payload, req.local)
    _ <- ContextShift[F].shift
  } yield (())

  def updateRequestHandled: F[Stream[F, Unit]] =
    subscriptionStream[UpdateRequest[K]](RequestUpdate(name), 128)
      .map(_.evalMap(updateRequestedHandler(_)))

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

  private def sendingRefUpdate(now: Long)(inp: Pending[K, V]): (Pending[K, V], Option[Map[K, MapValue[V]]]) = inp match {
    case Pending(lst, _, _) if lst.isEmpty => (inp, None)
    case Pending(_, _, last) if now - last < BatchInterval => (inp, None)
    case Pending(lst, _, _) if lst.size > MaxEvents => (Pending(Map(), now, now), Some(lst))
    case Pending(_, init, _) if now - init < CollectingInterval => (inp, None)
    case Pending(lst, _, _) => (Pending(Map(), now, now), Some(lst))
  }

  // PRIVATE

  def subscriptionStream[P: Codec](msg: Message, limit: Int): F[Stream[F, P]] =
    InspectableQueue.unbounded[F, P].map((queue: InspectableQueue[F, P]) =>
      enqueue[P](queue, msg, limit) *> queue.dequeue)

  private def run(action: F[Unit]) =
    ConcurrentEffect[F].runAsync(action)(_ => IO.unit).unsafeRunSync

  private def handler(eventName: String, cb: BitVector => Unit): F[Unit] =
    cfToAsync(communication.subscribe[Array[Byte]](
      eventName,
      (x: Array[Byte]) => cb(ByteVector(x).bits),
      blockingContextExecutor(blockingPool))) as (())

  private def enqueue[P: Codec](q: InspectableQueue[F, P], eventType: Message, maxItems: Int): Stream[F, Unit] = {
    val C = Codec[P]
    Stream.eval(handler(printMessage(eventType), { (a: BitVector) => C.decode(a) match {
      case Attempt.Failure(_) => ()
      case Attempt.Successful(d) => run(for {
        size <- q.getSize
        _ <- q.enqueue1(d.value).whenA(size < maxItems)
      } yield())
    }}))
  }

  private def unsubscribe(eventName: String): F[Unit] = F.delay {
    communication.unsubscribe(eventName)
  }

  private def close: F[Unit] =
    List(RequestUpdate(name), Update(name), Advertisement(name)).traverse_ { (m: Message) =>
      unsubscribe(printMessage(m))
    }
}

object AEStore {
  def decodeStream[F[_]: Sync, A: Codec](inp: Stream[F, BitVector]): Stream[F, A] =
    inp.flatMap { (a: BitVector) => Codec[A].decode(a) match {
      case Attempt.Failure(_) => Stream.empty
      case Attempt.Successful(d) => Stream.emit(d.value)
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

  def unicast[F[_]: Async: ContextShift, P: Codec](
      comm: ClusterCommunicationService,
      message: Message,
      payload: P,
      target: MemberId)
      : F[Unit] =
    Codec[P].encode(payload).map((b: BitVector) => {
      cfToAsync(comm.unicast(
        printMessage(message),
        b.toByteArray,
        target)) as (())
    }).getOrElse(Sync[F].delay(()))


  def multicast[F[_]: Sync, P: Codec](
      comm: ClusterCommunicationService,
      message: Message,
      payload: P,
      targets: Set[MemberId])
      : F[Unit] = Sync[F].delay {
    if (targets.isEmpty) ()
    else
      Codec[P].encode(payload).map((b: BitVector) => {
        comm.multicast(
          printMessage(message),
          b.toByteArray,
          targets.to[scala.collection.mutable.Set].asJava)
      }).getOrElse(())
  }

  final case class Pending[K, V](
      items: Map[K, MapValue[V]],
      inited: Long,
      lastAdded: Long)
      extends Product with Serializable

  final case class AdvertisementPayload[K](
      items: Map[K, Long],
      local: MemberId)
      extends Product with Serializable

  trait MapValue[+A] extends Product with Serializable
  final case class Tombstone(timestamp: Long) extends MapValue[Nothing]
  final case class Tagged[A](raw: A, timestamp: Long) extends MapValue[A]

  def raw[A](v: MapValue[A]): Option[A] = v match {
    case Tombstone(_) => None
    case Tagged(raw, _) => Some(raw)
  }

  def timestamp(v: MapValue[_]): Long = v match {
    case Tombstone(ts) => ts
    case Tagged(_, ts) => ts
  }

  implicit val memberIdCodec: Codec[MemberId] =
    utf8_32.xmap(MemberId.from, _.id)

  implicit def mapCodec[K, V](implicit k: Codec[K], v: Codec[V]): Codec[Map[K, V]] =
    listOfN(int32, k ~ v).xmap(_.toMap, _.toList)

  implicit def mapValueCodec[A](implicit a: Codec[A]): Codec[MapValue[A]] =
    either(bool, int64, a ~ int64).xmapc({
      case Left(t) => Tombstone(t)
      case Right((v, t)) => Tagged(v, t)
    })({
      case Tombstone(t) => Left(t)
      case Tagged(v, t) => Right((v, t))
    })

  implicit def advertisementPayloadCodec[K: Codec]: Codec[AdvertisementPayload[K]] =
    (Codec[Map[K, Long]] ~ memberIdCodec).xmapc({
      case (mp, mid) => AdvertisementPayload[K](mp, mid)
    })({
      case AdvertisementPayload(mp, mid) => (mp, mid)
    })

  final case class UpdateRequest[K](
      items: List[K],
      local: MemberId)
      extends Product with Serializable

  implicit def updateRequestCodec[K](implicit k: Codec[K]): Codec[UpdateRequest[K]] =
    (listOfN(int32, k) ~ memberIdCodec).xmapc({
      case (l, id) => UpdateRequest[K](l, id)
    })({
      case UpdateRequest(l, id) => (l, id)
    })

  def blockingContextExecutor(pool: BlockingContext): Executor = new Executor {
    def execute(r: java.lang.Runnable) = pool.unwrap.execute(r)
  }

  def apply[F[_]: ConcurrentEffect: ContextShift, K: Codec, V: Codec](
      name: String,
      communication: ClusterCommunicationService,
      membership: ClusterMembershipService,
      underlying: IndexedStore[F, K, MapValue[V]],
      pool: BlockingContext)(
      implicit timer: Timer[F])
      : Resource[F, IndexedStore[F, K, V]] = {

    val F = ConcurrentEffect[F]

    val init: F[(IndexedStore[F, K, V], Deferred[F, Either[Throwable, Unit]])] = for {
      currentTime <- timer.clock.monotonic(MILLISECONDS)
      sendingRef <- Ref.of[F, Pending[K, V]](Pending(Map(), currentTime, currentTime))
      store <- Sync[F].delay(new AEStore[F, K, V](name, communication, membership, sendingRef, underlying, pool))
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
    def finish(pair: (IndexedStore[F, K, V], Deferred[F, Either[Throwable, Unit]])): F[Unit] =
      pair._2.complete(Right(()))
    Resource.make(init)(finish).map(_._1)
  }
}
