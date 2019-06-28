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
import cats.effect.{Sync, ContextShift, Async}
import cats.syntax.apply._
import cats.syntax.applicative._
import cats.syntax.functor._
import cats.syntax.flatMap._
import fs2.Stream
import scalaz.syntax.tag._

import org.apache.ignite.IgniteCache
import org.apache.ignite.lang.IgniteFuture

import scala.collection.JavaConverters._
import javax.cache.Cache.Entry
import scala.collection.JavaConverters._

final class IgniteStore[F[_]: Async: ContextShift, K, V](
    cache: IgniteCache[K, V], pool: BlockingContext)
    extends IndexedStore[F, K, V] {

  import IgniteStore._

  private val F = Sync[F]

  def entries: Stream[F, (K, V)] = for {
    iterator <- Stream.eval(evalOnPool(F.delay(cache.iterator.asScala)))
    _ <- Stream.eval(F.delay(println(iterator)))
    _ <- Stream.eval(F.delay(println(iterator.next)))
    entry <- evalStreamOnPool(Stream.fromIterator[F, Entry[K, V]](iterator))
  } yield (entry.getKey, entry.getValue)

  def lookup(k: K): F[Option[V]] =
    igfToF(cache.getAsync(k)) map (Option(_))

  def insert(k: K, v: V): F[Unit] =
    igfToF(cache.putAsync(k, v)) as (())

  def delete(k: K): F[Boolean] =
    igfToF(cache.removeAsync(k)) map (_.booleanValue)

  private def evalOnPool[A](fa: F[A]): F[A] =
    ContextShift[F].evalOn[A](pool.unwrap)(fa)

  private def evalStreamOnPool[A](s: Stream[F, A]): Stream[F, A] =
    s translate new FunctionK[F, F] {
      def apply[A](fa: F[A]): F[A] = evalOnPool(fa)
    }
}

object IgniteStore {
  def apply[F[_]: Async: ContextShift, K, V](
      cache: IgniteCache[K, V],
      pool: BlockingContext)
      : IndexedStore[F, K, V] = {
    new IgniteStore(cache, pool)
  }

  def igfToF[F[_]: Async: ContextShift, A](igf: IgniteFuture[A]): F[A] = {
    if (igf.isDone) {
      println(igf.get)
      igf.get.pure[F]
    }
    else
      Async[F].async { (cb: Either[Throwable, A] => Unit) =>
        val _ = igf.listen { (ig: IgniteFuture[A]) => cb(Right(ig.get)) }
      } productL ContextShift[F].shift
  }
}
