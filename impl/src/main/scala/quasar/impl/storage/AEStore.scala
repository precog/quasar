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

import cats.effect._
import cats.effect.concurrent.Deferred
import cats.instances.list._
import cats.instances.option._
import cats.syntax.applicative._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.traverse._

import fs2.Stream

import scalaz.syntax.tag._

import scodec.Codec
import scodec.codecs.{listOfN, int32}
import scodec.codecs.implicits._

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import AEStore._

final class AEStore[F[_]: ConcurrentEffect: ContextShift: Timer, K: Codec, V: Codec](
    name: String,
    cluster: Cluster[F, Message],
    store: TimestampedStore[F, K, V],
    config: AEStoreConfig)
    extends IndexedStore[F, K, V] {

  private val F = Sync[F]
  private val underlying: IndexedStore[F, K, Timestamped[V]] = store.underlying

  private def ms(inp: Long): FiniteDuration = new FiniteDuration(inp, MILLISECONDS)

  def entries: Stream[F, (K, V)] =
    store.entries

  def lookup(k: K): F[Option[V]] =
    store.lookup(k)

  def insert(k: K, v: V): F[Unit] =
    store.insert(k, v)

  def delete(k: K): F[Boolean] =
    store.delete(k)

  // SENDING ADS
  def sendingAdStream: F[Stream[F, Unit]] =
    F.delay((Stream.eval(sendAd) *> Stream.sleep(ms(config.adTimeout))).repeat)

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

  private def updateHandler(id: cluster.Id, mp: Map[K, Timestamped[V]]): F[Unit] = mp.toList.traverse_ {
    case (k, newVal) => for {
      v <- underlying.lookup(k)
      ts = v.fold(0L)(timestamp(_))
      _ <- underlying.insert(k, newVal).whenA(ts < timestamp(newVal))
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

object AEStore {
  implicit def mapCodec[K, V](implicit k: Codec[K], v: Codec[V]): Codec[Map[K, V]] =
    listOfN(int32, k ~ v).xmap(_.toMap, _.toList)

  def apply[F[_]: ConcurrentEffect: ContextShift, K: Codec, V: Codec](
      name: String,
      cluster: Cluster[F, Message],
      underlying: TimestampedStore[F, K, V],
      pool: BlockingContext)(
      implicit timer: Timer[F])
      : Resource[F, IndexedStore[F, K, V]] = {

    val init: F[(IndexedStore[F, K, V], Deferred[F, Either[Throwable, Unit]])] = for {
      currentTime <- timer.clock.monotonic(MILLISECONDS)
      store <- Sync[F].delay(new AEStore[F, K, V](
        name,
        cluster,
        underlying,
        AEStoreConfig.default))

      stopper <- Deferred.tryable[F, Either[Throwable, Unit]]
      // Streams!
      adReceiver <- store.advertisementHandled.map(_.interruptWhen(stopper))
      adSender <- store.sendingAdStream.map(_.interruptWhen(stopper))
      purger <- store.purgeTombstones.map(_.interruptWhen(stopper))
      updates <- store.updateHandled.map(_.interruptWhen(stopper))
      updateRequester <- store.updateRequestHandled.map(_.interruptWhen(stopper))
      // Run them! (parJoin doesn't work for some reason)
      _ <- List(
        adReceiver,
        adSender,
        purger,
        updates,
        updateRequester)
      .traverse { (s: Stream[F, Unit]) =>
        ConcurrentEffect[F].start(ContextShift[F].evalOn(pool.unwrap)(s.compile.drain))
      }
    } yield (store, stopper)
    def finish(pair: (IndexedStore[F, K, V], Deferred[F, Either[Throwable, Unit]])): F[Unit] =
      pair._2.complete(Right(()))
    Resource.make(init)(finish).map(_._1)
  }

  final case class AEStoreConfig(
    maxEvents: Long,
    adTimeout: Long,
    purgeTimeout: Long,
    tombstoneLiveFor: Long,
    updateRequestLimit: Int,
    updateLimit: Int,
    adLimit: Int)

  object AEStoreConfig {
    val default: AEStoreConfig = AEStoreConfig(
      maxEvents = 50L,
      adTimeout = 30L,
      purgeTimeout = 30L,
      tombstoneLiveFor = 1000L,
      updateRequestLimit = 128,
      updateLimit = 128,
      adLimit = 128)
  }
}
