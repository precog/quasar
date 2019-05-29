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
import cats.syntax.apply._
import cats.syntax.flatMap._

import fs2.Stream

import io.atomix.core.map.AsyncAtomicMap
import io.atomix.core.iterator.AsyncIterator
import io.atomix.utils.time.Versioned

import java.util.concurrent.CompletableFuture

final class AsyncAtomicIndexedStore[F[_]: Async: ContextShift, K, V](
    mp: AsyncAtomicMap[K, V])
    extends IndexedStore[F, K, V] {

  import AsyncAtomicIndexedStore._

  def entries: Stream[F, (K, V)] = for {
    iterator <- Stream.bracket(Sync[F].delay(mp.entrySet.iterator))(
      (it: AsyncIterator[java.util.Map.Entry[K, Versioned[V]]]) => toF(it.close()) as (()))
    entry <- fromIterator[F, java.util.Map.Entry[K, Versioned[V]]](iterator)
  } yield (entry.getKey, entry.getValue.value)

  def lookup(k: K): F[Option[V]] =
    toF(mp get k) map ((v: Versioned[V]) => Option(v.value))

  def insert(k: K, v: V): F[Unit] =
    toF(mp.put(k, v)) as (())

  def delete(k: K): F[Boolean] =
    toF(mp.remove(k)) map { (x: Versioned[V]) => Option(x.value).nonEmpty }
}

object AsyncAtomicIndexedStore {
  def apply[F[_]: Async: ContextShift, K, V](mp: AsyncAtomicMap[K, V]): IndexedStore[F, K, V] =
    new AsyncAtomicIndexedStore(mp)

  def fromIterator[F[_]: ContextShift: Async, A](iterator: AsyncIterator[A]): Stream[F, A] = {
    def getNext(i: AsyncIterator[A]): F[Option[(A, AsyncIterator[A])]] = for {
      hasNext <- toF(i.hasNext())
      step <- if (hasNext.booleanValue) toF(i.next()) map (a => Option((a, i))) else None.pure[F]
    } yield step
    Stream.unfoldEval(iterator)(getNext)
  }

  def toF[F[_]: Async: ContextShift, A](cf: CompletableFuture[A]): F[A] = {
    if (cf.isDone)
      cf.get.pure[F]
    else
      Async[F].async { cb: (Either[Throwable, A] => Unit) =>
        val _ = cf.whenComplete { (res: A, t: Throwable) => cb(Option(t).toLeft(res)) }
      } productL ContextShift[F].shift
  }
}
