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

import cats.Functor
import cats.effect._
import cats.effect.concurrent.{Ref, Deferred}
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.eq._
import cats.syntax.applicative._
import cats.syntax.functor._
import cats.instances.option._
import cats.syntax.traverse._
import cats.syntax.foldable._
import cats.instances.list._
import cats.instances.int._

import fs2.Stream
import fs2.concurrent.InspectableQueue
import scalaz.syntax.tag._

import io.atomix.cluster.ClusterMembershipService
import io.atomix.cluster.messaging.ClusterCommunicationService
import io.atomix.cluster.{Member, MemberId}

import scala.collection.JavaConverters._
import java.util.concurrent.Executor
import java.util.function.BiConsumer
import scala.concurrent.duration._

import scodec._
import scodec.codecs._
import scodec.codecs.implicits._
import scodec.bits.{ByteVector, BitVector}
import io.atomix.utils.net.Address

import AEStore._
import AtomixSetup._
import Message._
import MapValue._


final case class AEStoreConfig(
  maxEvents: Long,
  adTimeout: Long,
  purgeTimeout: Long,
  tombstoneLiveFor: Long,
  syncTimeout: Long,
  batchInterval: Long,
  collectingInterval: Long,
  updateRequestLimit: Int,
  updateLimit: Int,
  adLimit: Int)

object AEStoreConfig {
  val default: AEStoreConfig = AEStoreConfig(
    maxEvents = 50L,
    adTimeout = 30L,
    purgeTimeout = 30L,
    tombstoneLiveFor = 1000L,
    syncTimeout = 50L,
    batchInterval = 20L,
    collectingInterval = 200L,
    updateRequestLimit = 128,
    updateLimit = 128,
    adLimit = 128)
}

final class AEStore[F[_]: ConcurrentEffect: ContextShift: Timer, K: Codec, V: Codec](
    name: String,
    cluster:Cluster[F],
    sendingRef: Ref[F, Pending[K, V]],
    underlying: IndexedStore[F, K, MapValue[V]],
    config: AEStoreConfig,
    blockingPool: BlockingContext)
    extends IndexedStore[F, K, V] {

  private val F = Sync[F]

  private def ms(inp: Long): FiniteDuration = new FiniteDuration(inp, MILLISECONDS)

  def entries: Stream[F, (K, V)] =
    underlying.entries.map({ case (k, v) => raw(v).map((k, _))}).unNone

  def lookup(k: K): F[Option[V]] =
    underlying.lookup(k).map(_.flatMap(raw(_)))

  def insert(k: K, v: V): F[Unit] = for {
    value <- tagged[F, V](v)
    _ <- underlying.insert(k, value)
    _ <- pend(k, value)
  } yield ()

  def delete(k: K): F[Boolean] = for {
    res <- underlying.delete(k)
    tmb <- tombstone[F, V]
    _ <- pend(k, tmb)
  } yield res

  private def pend(k: K, v: MapValue[V]): F[Unit] =
    sendingRef.modify((x: Pending[K, V]) => (Pending(x.items.updated(k, v), x.inited, timestamp(v)), ()))

  // SENDING ADS
  def sendingAdStream: F[Stream[F, Unit]] =
    F.delay((Stream.eval(sendAd) *> Stream.sleep(ms(config.adTimeout))).repeat)

  private def sendAd: F[Unit] =
    prepareAd.flatMap(cluster.gossip(Advertisement(name), _))

  private def prepareAd: F[Map[K, Long]] =
    underlying.entries.map({ case (k, v) => (k, timestamp(v))}).compile.toList.map(_.toMap)

  // RECEIVING ADS
  private def handleAdvertisement(id: cluster.Id, ad: Map[K, Long]): F[Unit] = {
    type Accum = (List[K], Map[K, MapValue[V]])

    // sender has no idea about this keys
    val fInitMap: F[Map[K, MapValue[V]]] =
      underlying
        .entries
        .map({ case (k, v) => ad.get(k) as ((k, v)) })
        .unNone
        .compile
        .toList
        .map(_.toMap)

    val fInit: F[Accum] = fInitMap.map((List(), _))

    // for every key in advertisement
    def result(init: Accum)  = ad.toList.foldM[F, Accum](init){ (acc, v) => (acc, v) match {
      case ((requesting, returning), (k, v)) => for {
        // we have the following value
        mbCurrent <- underlying.lookup(k)
        res <- if (mbCurrent.fold(0L)(timestamp(_)) < v) {
          // and it's older than in adsender node -> update request
          ((k :: requesting, returning)).pure[F]
        } else {
          // and we we have newer value -> update response
          mbCurrent.fold(tombstone[F, V])(_.pure[F]).map((v: MapValue[V]) => (requesting, returning.updated(k, v)))
        }
      } yield res
    }}
    for {
      init <- fInit
      (requesting, returning) <- result(init)
      _ <- cluster.unicast(RequestUpdate(name), requesting, id)
      _ <- cluster.unicast(Update(name), returning, id)
    } yield ()
  }

  def advertisementHandled: F[Stream[F, Unit]] =
    cluster.subscribe[Map[K, Long]](Advertisement(name), config.adLimit)
      .map(_.evalMap(Function.tupled(handleAdvertisement(_, _))(_)))

  // TOMBSTONES PURGING
  def purgeTombstones: F[Stream[F, Unit]] =
    // don't start purging immediately
    F.delay((Stream.sleep(ms(config.purgeTimeout)) *> Stream.eval(purge)).repeat)

  private def purge: F[Unit] = underlying.synchronized {
    underlying.entries.evalMap({ case (k, v) => for {
      now <- Timer[F].clock.monotonic(MILLISECONDS)
      _ <- underlying.delete(k).whenA(raw(v).isEmpty && now - timestamp(v) > config.tombstoneLiveFor)
    } yield () }).compile.drain
  }

  // RECEIVING UPDATES

  private def updateHandler(id: cluster.Id, mp: Map[K, MapValue[V]]): F[Unit] = mp.toList.traverse_ {
    case (k, newVal) => for {
      v <- underlying.lookup(k)
      ts = v.fold(0L)(timestamp(_))
      _ <- underlying.insert(k, newVal).whenA(ts < timestamp(newVal))
    } yield (())
  }

  def updateHandled: F[Stream[F, Unit]] =
    cluster.subscribe[Map[K, MapValue[V]]](Update(name), config.updateLimit)
      .map(_.evalMap(Function.tupled(updateHandler(_, _))(_)))

  // REQUESTING FOR UPDATES

  private def updateRequestedHandler(id: cluster.Id, req: List[K]): F[Unit] = for {
    payload <- req.foldM(Map[K, MapValue[V]]())((acc: Map[K, MapValue[V]], k: K) => for {
      uv <- underlying.lookup(k)
      // we've been requested for update and if we don't have a value in the key return tombstone
      toInsert <- uv.fold(tombstone[F, V])(_.pure[F])
    } yield acc.updated(k, toInsert))
    _ <- cluster.unicast(Update(name), payload, id)
  } yield (())

  def updateRequestHandled: F[Stream[F, Unit]] =
    cluster.subscribe[List[K]](RequestUpdate(name), config.updateRequestLimit)
      .map(_.evalMap(Function.tupled(updateRequestedHandler(_, _))(_)))

  // SYNCHRONIZATION

  def synchronization: F[Stream[F, Unit]] =
    // syncing should start immediately (the first run is effectively bootstrapping
    F.delay((Stream.eval(notifyPeersF) *> Stream.sleep(ms(config.syncTimeout))).repeat)

  private def notifyPeersF: F[Unit] = for {
    currentTime <- Timer[F].clock.monotonic(MILLISECONDS)
    msg <- sendingRef.modify(sendingRefUpdate(currentTime))
    _ <- msg.traverse_(cluster.gossip(Update(name), _))
  } yield ()

  // This is suboptimal, because batchInterval, maxEvents and collectingInterval could conflict
  private def sendingRefUpdate(now: Long)(inp: Pending[K, V]): (Pending[K, V], Option[Map[K, MapValue[V]]]) = inp match {
    // no updates, do nothing
    case Pending(lst, _, _) if lst.isEmpty => (inp, None)
    // we're still accumulating --> postpone
    case Pending(_, _, last) if now - last < config.batchInterval => (inp, None)
    // there's enough of events --> send it
    case Pending(lst, _, _) if lst.size > config.maxEvents => (Pending(Map(), now, now), Some(lst))
    // we haven't been accumulating for a while --> postpone it
    case Pending(_, init, _) if now - init < config.collectingInterval => (inp, None)
    // we've been accumulating for a while --> send
    case Pending(lst, _, _) => (Pending(Map(), now, now), Some(lst))
  }
}

// This abstracts discovering in running cluster
abstract class Membership[F[_], Id] {
  def localId: F[Id]
  def peers: F[Set[Id]]
  def random: F[Set[Id]]
  def byAddress(addr: Address): F[Option[Id]]
}

object Membership {
  def atomix[F[_]: Sync](service: ClusterMembershipService): Membership[F, MemberId] = new Membership[F, MemberId] {
    val F = Sync[F]
    def localId: F[MemberId] =
      F.delay(service.getLocalMember.id)

    def peers: F[Set[MemberId]] =
      F.delay(service.getMembers.asScala.to[Set].map(_.id))

    def random: F[Set[MemberId]] =
      peers

    @SuppressWarnings(Array("org.wartremover.warts.Equals"))
    def byAddress(addr: Address): F[Option[MemberId]] = {
      def theSame(check: Member): F[Boolean] = for {
        checkAddr <- F.delay(check.address.address(true))
        inpAddress <- F.delay(addr.address(true))
      } yield checkAddr.equals(inpAddress) && check.address.port === addr.port
      service.getMembers.asScala.toList.findM(theSame(_)).map(_.map(_.id))
    }
  }
}

// This abstracts sending and receiving messages in cluster
abstract class Communication[F[_], Id] {
  def unicast[P: Codec](msg: Message, payload: P, target: Id): F[Unit]
  def multicast[P: Codec](msg: Message, payload: P, targets: Set[Id]): F[Unit]
  def subscribe[P: Codec](msg: Message, limit: Int): F[Stream[F, (Id, P)]]
}
object Communication {
  def atomix[F[_]: ConcurrentEffect: ContextShift](
      service: ClusterCommunicationService,
      membership: Membership[F, MemberId],
      pool: BlockingContext)
      : Communication[F, MemberId] = new Communication[F, MemberId] {
    val F = ConcurrentEffect[F]
    def unicast[P: Codec](msg: Message, payload: P, target: MemberId): F[Unit] =
      Codec[P].encode(payload).map((b: BitVector) => {
        cfToAsync(service.unicast(
          printMessage(msg),
          b.toByteArray,
          target)) as (())
      }).getOrElse(F.delay(()))

    def multicast[P: Codec](msg: Message, payload: P, targets: Set[MemberId]): F[Unit] = F.delay {
      if (targets.isEmpty) ()
      else
        Codec[P].encode(payload).map((b: BitVector) => {
          service.multicast(
            printMessage(msg),
            b.toByteArray,
            targets.asJava)
        }).getOrElse(())
    }

    def subscribe[P: Codec](msg: Message, limit: Int): F[Stream[F, (MemberId, P)]] =
      InspectableQueue.unbounded[F, (MemberId, P)]
        .map((queue: InspectableQueue[F, (MemberId, P)]) => enqueue[P](queue, msg, limit) *> queue.dequeue)
        .map(_.onFinalize(F.delay(service.unsubscribe(printMessage(msg)))))

    private def enqueue[P: Codec](q: InspectableQueue[F, (MemberId, P)], eventType: Message, maxItems: Int): Stream[F, Unit] = {
      Stream.eval(handler(printMessage(eventType), { (addr: Address, a: BitVector) => Codec[P].decode(a) match {
        case Attempt.Failure(_) => ()
        case Attempt.Successful(d) => run(for {
          mid <- membership.byAddress(addr)
          size <- q.getSize
          _ <- mid.traverse_((id: MemberId) => q.enqueue1((id, d.value)).whenA(size < maxItems))
        } yield())
      }}))
    }
    private def handler(eventName: String, cb: (Address, BitVector) => Unit): F[Unit] = {
      val biconsumer: BiConsumer[Address, Array[Byte]] = (addr, bytes) => {
        cb(addr, ByteVector(bytes).bits)
      }
      cfToAsync(service.subscribe[Array[Byte]](
        eventName,
        biconsumer,
        blockingContextExecutor(pool))) as (())
    }

    private def run(action: F[Unit]) =
      F.runAsync(action)(_ => IO.unit).unsafeRunSync
  }
}

// This abstracts sending/receiving messages in cluster with some node identifier
abstract class Cluster[F[_]] {
  type Id
  def gossip[P: Codec](message: Message, p: P): F[Unit]
  def subscribe[P: Codec](msg: Message, limit: Int): F[Stream[F, (Id, P)]]
  def unicast[P: Codec](msg: Message, p: P, id: Id): F[Unit]
}

object Cluster {
  def atomix[F[_]: ConcurrentEffect: ContextShift](
      com: ClusterCommunicationService,
      mem: ClusterMembershipService,
      pool: BlockingContext)
      : Cluster[F] = new Cluster[F] {

    type Id = MemberId
    val membership: Membership[F, Id] = Membership.atomix[F](mem)
    val communication: Communication[F, Id] = Communication.atomix[F](com, membership, pool)

    def gossip[P: Codec](msg: Message, p: P): F[Unit] = for {
      targets <- membership.random
      _ <- communication.multicast(msg, p, targets)
      _ <- ContextShift[F].shift
    } yield ()

    def unicast[P: Codec](msg: Message, p: P, id: Id): F[Unit] =
      communication.unicast(msg, p, id) *> ContextShift[F].shift

    def subscribe[P: Codec](msg: Message, limit: Int): F[Stream[F, (Id, P)]] =
      communication.subscribe(msg, limit)
  }
}

// Anything tagged with local timestamp
trait MapValue[+A] extends Product with Serializable

object MapValue {
  final case class Tombstone(timestamp: Long) extends MapValue[Nothing]
  final case class Tagged[A](raw: A, timestamp: Long) extends MapValue[A]

  def tombstone[F[_]: Functor: Timer, A]: F[MapValue[A]] =
    Timer[F].clock.monotonic(MILLISECONDS).map(Tombstone(_))

  def tagged[F[_]: Functor: Timer, A](raw: A): F[MapValue[A]] =
    Timer[F].clock.monotonic(MILLISECONDS).map(Tagged(raw, _))

  def raw[A](v: MapValue[A]): Option[A] = v match {
    case Tombstone(_) => None
    case Tagged(raw, _) => Some(raw)
  }

  def timestamp(v: MapValue[_]): Long = v match {
    case Tombstone(ts) => ts
    case Tagged(_, ts) => ts
  }

  implicit def mapValueCodec[A](implicit a: Codec[A]): Codec[MapValue[A]] =
    either(bool, int64, a ~ int64).xmapc({
      case Left(t) => Tombstone(t)
      case Right((v, t)) => Tagged(v, t)
    })({
      case Tombstone(t) => Left(t)
      case Tagged(v, t) => Right((v, t))
    })
}

// Anti-entropy messages
trait Message extends Product with Serializable

object Message {
  final case class RequestUpdate(name: String) extends Message
  final case class Update(name: String) extends Message
  final case class Advertisement(name: String) extends Message

  def printMessage(m: Message): String = m match {
    case RequestUpdate(n) => s"aestore::requestUpdate::${n}"
    case Update(n) => s"aestore::update::${n}"
    case Advertisement(n) => s"aestore::advertisement::${n}"
  }
}

final case class Pending[K, V](
    items: Map[K, MapValue[V]],
    inited: Long,
    lastAdded: Long)
    extends Product with Serializable

object AEStore {
  implicit def mapCodec[K, V](implicit k: Codec[K], v: Codec[V]): Codec[Map[K, V]] =
    listOfN(int32, k ~ v).xmap(_.toMap, _.toList)

  def blockingContextExecutor(pool: BlockingContext): Executor = new Executor {
    def execute(r: java.lang.Runnable) = pool.unwrap.execute(r)
  }

  def apply[F[_]: ConcurrentEffect: ContextShift, K: Codec, V: Codec](
      name: String,
      commService: ClusterCommunicationService,
      memService: ClusterMembershipService,
      underlying: IndexedStore[F, K, MapValue[V]],
      pool: BlockingContext)(
      implicit timer: Timer[F])
      : Resource[F, IndexedStore[F, K, V]] = {

    val init: F[(IndexedStore[F, K, V], Deferred[F, Either[Throwable, Unit]])] = for {
      currentTime <- timer.clock.monotonic(MILLISECONDS)
      sendingRef <- Ref.of[F, Pending[K, V]](Pending(Map(), currentTime, currentTime))
      cluster = Cluster.atomix(commService, memService, pool)
      store <- Sync[F].delay(new AEStore[F, K, V](
        name,
        cluster,
        sendingRef,
        underlying,
        AEStoreConfig.default,
        pool))
      stopper <- Deferred.tryable[F, Either[Throwable, Unit]]
      // Streams!
      adReceiver <- store.advertisementHandled.map(_.interruptWhen(stopper))
      adSender <- store.sendingAdStream.map(_.interruptWhen(stopper))
      purger <- store.purgeTombstones.map(_.interruptWhen(stopper))
      updates <- store.updateHandled.map(_.interruptWhen(stopper))
      updateRequester <- store.updateRequestHandled.map(_.interruptWhen(stopper))
      synching <- store.synchronization.map(_.interruptWhen(stopper))
      // Run them! (parJoin doesn't work btw)
      _ <- List(
        adReceiver,
        adSender,
        purger,
        updates,
        updateRequester,
        synching)
      .traverse { (s: Stream[F, Unit]) =>
        ConcurrentEffect[F].start(ContextShift[F].evalOn(pool.unwrap)(s.compile.drain))
      }
    } yield (store, stopper)
    def finish(pair: (IndexedStore[F, K, V], Deferred[F, Either[Throwable, Unit]])): F[Unit] =
      pair._2.complete(Right(()))
    Resource.make(init)(finish).map(_._1)
  }
}
