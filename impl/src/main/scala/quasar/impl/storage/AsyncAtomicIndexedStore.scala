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

import cats.effect._
import cats.syntax.functor._
import cats.syntax.applicative._
import cats.syntax.flatMap._

import fs2.Stream

import io.atomix.core.map.AsyncAtomicMap
import io.atomix.core.iterator.AsyncIterator
import io.atomix.utils.time.Versioned

class AsyncAtomicIndexedStore[F[_]: Async: ContextShift, K, V, M <: AsyncAtomicMap[K, V]](underlying: M)
    extends IndexedStore[F, K, V] {

  import AtomixSetup._
  import AsyncAtomicIndexedStore._

  def entries: Stream[F, (K, V)] = for {
    iterator <- Stream.bracket(Sync[F].delay(underlying.entrySet.iterator))(
      (it: AsyncIterator[java.util.Map.Entry[K, Versioned[V]]]) => cfToAsync(it.close()) as (()))
    entry <- fromIterator[F, java.util.Map.Entry[K, Versioned[V]]](iterator)
  } yield (entry.getKey, entry.getValue.value)

  def lookup(k: K): F[Option[V]] =
    cfToAsync(underlying get k) map ((v: Versioned[V]) => Option(v) map (_.value))

  def insert(k: K, v: V): F[Unit] =
    cfToAsync(underlying.put(k, v)) as (())

  def delete(k: K): F[Boolean] =
    cfToAsync(underlying.remove(k)) map { (x: Versioned[V]) => Option(x).nonEmpty }
}

object AsyncAtomicIndexedStore {
  import AtomixSetup._
  def fromIterator[F[_]: ContextShift: Async, A](iterator: AsyncIterator[A]): Stream[F, A] = {
    def getNext(i: AsyncIterator[A]): F[Option[(A, AsyncIterator[A])]] = for {
      hasNext <- cfToAsync(i.hasNext())
      step <- if (hasNext.booleanValue) cfToAsync(i.next()) map (a => Option((a, i))) else None.pure[F]
    } yield step
    Stream.unfoldEval(iterator)(getNext)
  }

  def apply[F[_]: Async: ContextShift, K, V, M <: AsyncAtomicMap[K, V]](underlying: M): AsyncAtomicIndexedStore[F, K, V, M] =
    new AsyncAtomicIndexedStore[F, K, V, M](underlying)

}
