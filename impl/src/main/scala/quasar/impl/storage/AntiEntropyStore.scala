/*
 * Copyright 2014–2019 SlamData Inc.
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

import quasar.impl.cluster.{Timestamped, Cluster, Message}, Timestamped._, Message._

import cats.~>
import cats.effect._
import cats.effect.concurrent.{Deferred, Semaphore}
import cats.instances.list._
import cats.syntax.applicative._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._

import fs2.Stream

import scodec.Codec
import scodec.codecs.{listOfN, int32}
import scodec.codecs.implicits._

import scala.concurrent.duration._

import AntiEntropyStore._

final class AntiEntropyStore[F[_]: ConcurrentEffect: ContextShift: Timer, K: Codec, V: Codec](
    name: String,
    cluster: Cluster[F, Message],
    store: TimestampedStore[F, K, V],
    gates: Gates[F],
    config: AntiEntropyStoreConfig)
    extends IndexedStore[F, K, V] {

  private val F = Sync[F]
  private val underlying: IndexedStore[F, K, Timestamped[V]] = store.underlying

  private def ms(inp: Long): FiniteDuration = new FiniteDuration(inp, MILLISECONDS)

  def entries: Stream[F, (K, V)] =
    store.entries

  def lookup(k: K): F[Option[V]] =
    store.lookup(k)

  def insert(k: K, v: V): F[Unit] =
    gates.in(store.insert(k, v))

  def delete(k: K): F[Boolean] =
    gates.in(store.delete(k))

  // SENDING ADS
  def sendingAdStream: Stream[F, Unit] =
    (Stream.eval(sendAd) *> Stream.sleep(ms(config.adTimeoutMillis))).repeat

  private def sendAd: F[Unit] =
    store.timestamps.flatMap { (ts: Map[K, Long]) =>
      cluster.gossip(Advertisement(name), ts).unlessA(ts.isEmpty)
    }

  // RECEIVING ADS
  private def handleAdvertisement(id: cluster.Id, ad: Map[K, Long]): F[Unit] = {
    type Accum = (List[K], Map[K, Timestamped[V]])

    // Note, that we don't handle keys aren't presented in advertisement
    // They are handled when this node sends its advertisement instead.
    // for every key in advertisement having the most recent modification timestamp map
    def result(init: Accum, timestamps: Map[K, Long])  = ad.toList.foldM[F, Accum](init){
      case (acc @ (requesting, returning), (k, incoming)) => for {
        // when this key was modified last time
        current <- timestamps.get(k).getOrElse(0L).pure[F]
        res <- if (current < incoming) {
          // current value is older than incoming, add key to update request
          ((k :: requesting, returning)).pure[F]
        } else underlying.lookup(k).map {
          case None =>
            acc // impossible in theory, but can't figure out how to handle this gracefully
          case Some(having) =>
            // Value we have is newer than incoming, create update message
            (requesting, returning.updated(k, having))
        }

      } yield res
    }
    for {
      timestamps <- store.timestamps
      (requesting, returning) <- result((List(), Map.empty), timestamps)
      _ <- cluster.unicast(RequestUpdate(name), requesting, id).unlessA(requesting.isEmpty)
      _ <- cluster.unicast(Update(name), returning, id).unlessA(returning.isEmpty)
    } yield ()
  }

  def advertisementHandled: F[Stream[F, Unit]] =
    cluster.subscribe[Map[K, Long]](Advertisement(name), config.adLimit)
      .map(_.evalMap(Function.tupled(handleAdvertisement(_, _))(_)))

  // TOMBSTONES PURGING
  def purgeTombstones: Stream[F, Unit] =
    (Stream.sleep(ms(config.purgeTimeoutMillis)) *> Stream.eval(purge)).repeat

  private def purge: F[Unit] = {
    val purgingStream: Stream[F, Unit] = underlying.entries.evalMap { case (k, v) => for {
      now <- Timer[F].clock.realTime(MILLISECONDS)
      _ <- underlying.delete(k).whenA(raw(v).isEmpty && now - timestamp(v) > config.tombstoneLiveForMillis)
    } yield () }
    gates.strict(purgingStream.compile.drain)
  }

  // RECEIVING UPDATES

  private def updateHandler(id: cluster.Id, mp: Map[K, Timestamped[V]]): F[Unit] = mp.toList.traverse_ {
    case (k, newVal) => for {
      v <- underlying.lookup(k)
      ts = v.fold(0L)(timestamp(_))
      _ <- gates.in(underlying.insert(k, newVal).whenA(ts < timestamp(newVal)))
    } yield (())
  }

  def updateHandled: F[Stream[F, Unit]] =
    cluster.subscribe[Map[K, Timestamped[V]]](Update(name), config.updateLimit)
      .map(_.evalMap(Function.tupled(updateHandler(_, _))(_)))

  // REQUESTING FOR UPDATES

  private def updateRequestedHandler(id: cluster.Id, req: List[K]): F[Unit] = for {
    payload <- subMap(req)
    _ <- cluster.unicast(Update(name), payload, id).unlessA(payload.isEmpty)
  } yield (())

  private def subMap(req: List[K]): F[Map[K, Timestamped[V]]] =
    req.foldM(Map[K, Timestamped[V]]())((acc: Map[K, Timestamped[V]], k: K) => for {
      uv <- underlying.lookup(k)
      // we've been requested for update and if we don't have a value in the key return tombstone
      toInsert <- uv.fold(tombstone[F, V])(_.pure[F])
    } yield acc.updated(k, toInsert))

  def updateRequestHandled: F[Stream[F, Unit]] =
    cluster.subscribe[List[K]](RequestUpdate(name), config.updateRequestLimit)
      .map(_.evalMap(Function.tupled(updateRequestedHandler(_, _))(_)))

  // INITIALIZATION
  def requestInitialization: F[Unit] =
    cluster.random(RequestInit(name), ())

  def initialization: Stream[F, Unit] =
    (Stream.eval(requestInitialization) *> Stream.sleep(ms(config.adTimeoutMillis))).repeat

  def initRequestHandled: F[Stream[F, Unit]] =
    cluster.subscribe[Unit](RequestInit(name), config.updateRequestLimit)
      .map(_.evalMap(x => initRequestHandler(x._1)))

  def initRequestHandler(id: cluster.Id): F[Unit] =
    underlying.entries.compile.toList.flatMap { (lst: List[(K, Timestamped[V])]) =>
      cluster.unicast(Init(name), lst.toMap, id)
    }

  def initHandled(stopper: Deferred[F, Either[Throwable, Unit]]): F[Stream[F, Unit]] =
    cluster.subscribe[Map[K, Timestamped[V]]](Init(name), config.updateLimit)
      .map(_.evalMap {
        case (id, mp) => for {
          _ <- updateHandler(id, mp)
          _ <- Sync[F].suspend(stopper.complete(Right(())))
        } yield ()
      })

  // BROADCASTING UPDATES
  def broadcastUpdates: Stream[F, Unit] =
    store.updates
      .groupWithin(config.updateBroadcastBatch, config.updateBroadcastMillis.milliseconds)
      .map(_.toList.toMap)
      .evalMap(cluster.broadcast(Update(name), _))
}

object AntiEntropyStore {
  final class Gates[F[_]: Bracket[?[_], Throwable]](semaphore: Semaphore[F]) {
    def in[A](fa: F[A]): F[A] =
      Bracket[F, Throwable].guarantee(semaphore.acquire *> fa)(semaphore.release)

    def strict[A](fa: F[A]): F[A] =
      Bracket[F, Throwable].guarantee(semaphore.acquireN(Int.MaxValue) *> fa)(semaphore.releaseN(Int.MaxValue))
  }

  object Gates {
    def apply[F[_]: Concurrent: Bracket[?[_], Throwable]]: F[Gates[F]] =
      Semaphore[F](Int.MaxValue) map (new Gates(_))
  }

  implicit def mapCodec[K, V](implicit k: Codec[K], v: Codec[V]): Codec[Map[K, V]] =
    listOfN(int32, k ~ v).xmap(_.toMap, _.toList)

  def apply[F[_]: ConcurrentEffect: ContextShift, K: Codec, V: Codec](
      config: AntiEntropyStoreConfig,
      name: String,
      cluster: Cluster[F, Message],
      underlying: TimestampedStore[F, K, V],
      blocker: Blocker)(
      implicit timer: Timer[F])
      : Resource[F, IndexedStore[F, K, V]] = {
    val res: F[Resource[F, IndexedStore[F, K, V]]] = for {
      gates <- Gates[F]
      store <- Sync[F].delay(new AntiEntropyStore[F, K, V](
        name,
        cluster,
        underlying,
        gates,
        config))
      stopper <- Deferred[F, Either[Throwable, Unit]]
      empty <- cluster.isEmpty
      _ <- stopper.complete(Right(())).whenA(empty)

      initRequest <- store.initRequestHandled
      init <- store.initHandled(stopper)
      adReceiver <- store.advertisementHandled
      updates <- store.updateHandled
      updateRequester <- store.updateRequestHandled
    } yield {
      val merged = Stream.emits(List(
        adReceiver,
        store.sendingAdStream,
        store.purgeTombstones,
        store.broadcastUpdates,
        initRequest,
        updates,
        updateRequester,
        store.initialization.interruptWhen(stopper),
        init.interruptWhen(stopper))).parJoinUnbounded
      val storeStream = Stream.emit[F, IndexedStore[F, K, V]](store)
      for {
        resource <- storeStream.concurrently(merged).compile.resource.lastOrError
        _ <- Resource.liftF(stopper.get)
      } yield resource
    }
    Resource.suspend(res).mapK[F](λ[F ~> F](ContextShift[F].blockOn(blocker)(_)))
  }
  def default[F[_]: ConcurrentEffect: ContextShift, K: Codec, V: Codec](
      name: String,
      cluster: Cluster[F, Message],
      underlying: TimestampedStore[F, K, V],
      blocker: Blocker)(
      implicit timer: Timer[F])
      : Resource[F, IndexedStore[F, K, V]] =
    apply[F, K, V](AntiEntropyStoreConfig.default, name, cluster, underlying, blocker)
}
