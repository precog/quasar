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

import quasar.impl.cluster.Timestamped, Timestamped._

import cats.FlatMap
import cats.effect.Timer
import cats.syntax.functor._
import cats.syntax.flatMap._

import fs2.Stream

final case class TimestampedStore[F[_]: FlatMap: Timer, K, V](
    underlying: IndexedStore[F, K, Timestamped[V]])
    extends IndexedStore[F, K, V] {

  def entries: Stream[F, (K, V)] =
    underlying.entries.map({ case (k, v) => raw(v).map((k, _))}).unNone

  def lookup(k: K): F[Option[V]] =
    underlying.lookup(k).map(_.flatMap(raw(_)))

  def insert(k: K, v: V): F[Unit] =
    tagged[F, V](v).flatMap(underlying.insert(k, _))

  def delete(k: K): F[Boolean] = for {
    was <- underlying.lookup(k)
    tmb <- tombstone[F, V]
    res <- underlying.insert(k, tmb)
  } yield was.flatMap(raw(_)).nonEmpty
}
