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

import cats.effect.concurrent.TryableDeferred
import quasar.contrib.scalaz.MonadState_

import fs2.Stream
import scalaz.{IMap, Monad, Order, Scalaz}, Scalaz._

/** An indexed store backed by an immutable map. */
object PureIndexedStore {
  private [this] def pureDeferred[F[_], A](a: A)(implicit F: Monad[F]): TryableDeferred[F, A] =
    new TryableDeferred[F, A] {
      def get: F[A] = F.point(a)
      def complete(x: A): F[Unit] = F.point(())
      def tryGet: F[Option[A]] = F.point(Some(a))
    }


  def apply[F[_]: Monad, I: Order, V](implicit F: MonadState_[F, IMap[I, V]])
      : IndexedStore[F, I, V] =
    new IndexedStore[F, I, V] {
      val entries =
        Stream.eval(F.get).flatMap(m => Stream.emits(m.toList))

      def lookup(i: I): F[Option[V]] =
        F.gets(_.lookup(i))

      def insert(i: I, v: V): F[TryableDeferred[F, Unit]] =
        F.modify(_.insert(i, v)) map ((x: Unit) => pureDeferred(x))

      def delete(i: I): F[TryableDeferred[F, Boolean]] =
        for {
          m <- F.get

          r = m.updateLookupWithKey(i, (_, _) => None)
          (old, m2) = r

          _ <- F.put(m2)
        } yield pureDeferred(old.isDefined)
    }
}
