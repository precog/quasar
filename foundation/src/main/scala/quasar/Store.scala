/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar

import slamdata.Predef._

import quasar.higher.HFunctor

import cats.Applicative
import cats.effect.Concurrent
import cats.effect.concurrent.MVar
import cats.implicits._

import scalaz.~>

trait Store[F[_], I, V] {
  /** Retrieve the value at the specified index. */
  def lookup(i: I): F[Option[V]]

  /** Associate the given value with the specified index, replaces any
    * existing association.
    */
  def insert(i: I, v: V): F[Unit]

  /** Remove any value associated with the specified index, returning whether
    * it existed.
    */
  def delete(i: I): F[Boolean]
}

object Store {
  /* An in-memory `Store` without any persistence. */
  def ephemeral[F[_]: Concurrent, I, V]: F[Store[F, I, V]] =
    MVar.of(Map.empty[I, V]) map { mvar =>
      new Store[F, I, V] {
        def lookup(i: I): F[Option[V]] =
          mvar.read.map(_.get(i))

        def insert(i: I, v: V): F[Unit] =
          for {
            store <- mvar.take
            _ <- mvar.put(store + (i -> v))
          } yield ()

        def delete(i: I): F[Boolean] =
          for {
            store <- mvar.take
            _ <- mvar.put(store - i)
          } yield store.contains(i)
      }
    }

  /** An empty `Store` that ignores all inserts. */
  def void[F[_]: Applicative, I, V] =
    new Store[F, I, V] {
      def lookup(i: I) = none[V].pure[F]
      def insert(i: I, v: V) = ().pure[F]
      def delete(i: I) = false.pure[F]
    }

  implicit def hFunctor[I, V]: HFunctor[Store[?[_], I, V]] =
    new HFunctor[Store[?[_], I, V]] {
      def hmap[A[_], B[_]](fa: Store[A, I, V])(f: A ~> B): Store[B, I, V] =
        new Store[B, I, V] {
          def lookup(i: I): B[Option[V]] =
            f(fa.lookup(i))

          def insert(i: I, v: V): B[Unit] =
            f(fa.insert(i, v))

          def delete(i: I): B[Boolean] =
            f(fa.delete(i))
        }
    }
}
