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
import quasar.impl.cluster.{Timestamped, Cluster, Message}, Timestamped._, Message._

import cats.~>
import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.instances.list._
import cats.instances.option._
import cats.syntax.applicative._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._

import fs2.Stream

import scalaz.syntax.tag._

import scodec.Codec
import scodec.codecs.{listOfN, int32}
import scodec.codecs.implicits._

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

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
    prepareAd.flatMap(cluster.gossip(Advertisement(name), _))

  private def prepareAd: F[Map[K, Long]] =
    underlying.entries.map({ case (k, v) => (k, timestamp(v))}).compile.toList.map(_.toMap)

  // RECEIVING ADS
  private def handleAdvertisement(id: cluster.Id, ad: Map[K, Long]): F[Unit] = {
    type Accum = (List[K], Map[K, Timestamped[V]])

    // sender has no idea about this keys
    val fInitMap: F[Map[K, Timestamped[V]]] =
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
          mbCurrent.fold(tombstone[F, V])(_.pure[F]).map((v: Timestamped[V]) => (requesting, returning.updated(k, v)))
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
    _ <- cluster.unicast(Update(name), payload, id)
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
      name: String,
      cluster: Cluster[F, Message],
      underlying: TimestampedStore[F, K, V],
      pool: BlockingContext)(
      implicit timer: Timer[F])
      : Resource[F, IndexedStore[F, K, V]] = {

    val res: F[Resource[F, IndexedStore[F, K, V]]] = for {
      currentTime <- timer.clock.realTime(MILLISECONDS)
      gates <- Gates[F]
      store <- Sync[F].delay(new AntiEntropyStore[F, K, V](
        name,
        cluster,
        underlying,
        gates,
        AntiEntropyStoreConfig.default))
      adReceiver <- store.advertisementHandled
      updates <- store.updateHandled
      updateRequester <- store.updateRequestHandled
    } yield {
      val merged = Stream.emits(List(
        adReceiver,
        store.sendingAdStream,
        store.purgeTombstones,
        updates,
        updateRequester)).parJoinUnbounded
      val storeStream = Stream.emit[F, IndexedStore[F, K, V]](store)
      storeStream.concurrently(merged).compile.resource.lastOrError
    }

    Resource.liftF(res).flatten.mapK[F](new ~>[F, F] {
      def apply[A](fa: F[A]): F[A] = ContextShift[F].evalOn(pool.unwrap)(fa)
    })
  }

  final case class AntiEntropyStoreConfig(
    maxEvents: Long,
    adTimeoutMillis: Long,
    purgeTimeoutMillis: Long,
    tombstoneLiveForMillis: Long,
    updateRequestLimit: Int,
    updateLimit: Int,
    adLimit: Int)

  object AntiEntropyStoreConfig {
    val default: AntiEntropyStoreConfig = AntiEntropyStoreConfig(
      maxEvents = 50L,
      adTimeoutMillis = 30L,
      purgeTimeoutMillis = 30L,
      tombstoneLiveForMillis = 1000L,
      updateRequestLimit = 128,
      updateLimit = 128,
      adLimit = 128)
  }
}
