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
import cats.effect.concurrent.{TryableDeferred, Deferred}
import cats.syntax.functor._
import cats.syntax.applicative._
import cats.syntax.apply._
import cats.syntax.flatMap._

import fs2.concurrent.Queue
import fs2.Stream

import io.atomix.core.map.{AsyncDistributedMap, MapEvent, MapEventListener}, MapEvent.Type._
import io.atomix.core.iterator.AsyncIterator

import java.util.concurrent.CompletableFuture

final class AsyncDistributedMapStore[F[_]: ConcurrentEffect: ContextShift, K, V](
    mp: AsyncDistributedMap[K, V])
    extends IndexedStore[F, K, V] {

  import AsyncDistributedMapStore._

  def entries: Stream[F, (K, V)] = for {
    iterator <- Stream.bracket(Sync[F].delay(mp.entrySet.iterator))(
      (it: AsyncIterator[java.util.Map.Entry[K, V]]) => toF(it.close()) as (()))
    entry <- fromIterator[F, java.util.Map.Entry[K, V]](iterator)
  } yield (entry.getKey, entry.getValue)

  def lookup(k: K): F[Option[V]] =
    toF(mp get k) map (Option(_))

  def insert(k: K, v: V): F[TryableDeferred[F, Unit]] = {
    val cf: CompletableFuture[Unit] = mp.put(k, v).thenApply((x: V) => ())
    toDeferred(cf)
  }

  def delete(k: K): F[TryableDeferred[F, Boolean]] = {
    val cf: CompletableFuture[Boolean] = mp.remove(k).thenApply((x: V) => Option(x).nonEmpty)
    toDeferred(cf)
  }
}

object AsyncDistributedMapStore {
  import IndexedStore.Event, Event._

  def apply[F[_]: ConcurrentEffect: ContextShift, K, V](
      mp: AsyncDistributedMap[K, V])
      : IndexedStore[F, K, V] =
    new AsyncDistributedMapStore(mp)

  def eventStream[F[_]: ConcurrentEffect: ContextShift, K, V](mp: AsyncDistributedMap[K, V]): Stream[F, Event[K, V]] = {
    val F = ConcurrentEffect[F]
    def run(f: F[Unit]): Unit = F.runAsync(f)(_ => IO.unit).unsafeRunSync
    def listener(cb: Event[K, V] => Unit): MapEventListener[K, V] = { event => event.`type` match {
      case INSERT => cb(Insert(event.key, event.newValue))
      case UPDATE => cb(Insert(event.key, event.newValue))
      case REMOVE => cb(Delete(event.key))
    }}

    for {
      q <- Stream.eval(Queue.circularBuffer[F, Event[K, V]](128))
      handler = listener(x => run(q.enqueue1(x)))
      _ <- Stream.eval(toF(mp addListener handler))
      res <- q.dequeue.onFinalize(toF(mp removeListener handler) as (()))
    } yield res
  }

  def fromIterator[F[_]: ContextShift: Async, A](iterator: AsyncIterator[A]): Stream[F, A] = {
    def getNext(i: AsyncIterator[A]): F[Option[(A, AsyncIterator[A])]] = for {
      hasNext <- toF(i.hasNext())
      step <- if (hasNext.booleanValue) toF(i.next()) map (a => Option((a, i))) else None.pure[F]
    } yield step
    Stream.unfoldEval(iterator)(getNext)
  }

  private[this] def asyncCompletableFuture[F[_]: Async, A](cf: CompletableFuture[A]): F[A] =
    Async[F].async { (cb: (Either[Throwable, A] => Unit)) =>
      val _ = cf.whenComplete { (res: A, t: Throwable) => cb(Option(t).toLeft(res)) }
    }

  def toF[F[_]: Async: ContextShift, A](cf: CompletableFuture[A]): F[A] = {
    if (cf.isDone) cf.get.pure[F]
    else asyncCompletableFuture(cf) productL ContextShift[F].shift
  }

  def toDeferred[F[_]: Concurrent: ContextShift, A](cf: CompletableFuture[A]): F[TryableDeferred[F, A]] = {
    if (cf.isDone) for {
      d <- Deferred.tryable[F, A]
      _ <- d.complete(cf.get)
    } yield d
    else for {
      d <- Deferred.tryable[F, A]
      _ <- Concurrent[F].start(for {
        a <- asyncCompletableFuture(cf)
        _ <- d.complete(a)
      } yield ())
      _ <- ContextShift[F].shift
    } yield d
  }
}
