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
import cats.syntax.functor._

import monocle.Prism

import quasar.contrib.scalaz.MonadError_

import fs2.Stream
import scalaz.{~>, Bind, Functor, InvariantFunctor, Monad, Scalaz, Applicative}, Scalaz._

trait IndexedStore[F[_], I, V] {
  /** All values in the store paired with their index. */
  def entries: Stream[F, (I, V)]

  /** Retrieve the value at the specified index. */
  def lookup(i: I): F[Option[V]]

  /** Associate the given value with the specified index, replaces any
    * existing association.
    */
  def insert(i: I, v: V): F[TryableDeferred[F, Unit]]

  /** Remove any value associated with the specified index, returning whether
    * it existed.
    */
  def delete(i: I): F[TryableDeferred[F, Boolean]]
}

object IndexedStore extends IndexedStoreInstances {
  sealed trait Event[K, +V] extends Product with Serializable
  object Event {
    final case class Insert[K, V](k: K, v: V) extends Event[K, V]
    final case class Delete[K](k: K) extends Event[K, Nothing]
  }

  def consumeEvent[F[_]: Functor, K, V](store: IndexedStore[F, K, V], event: Event[K, V]): F[Unit] = event match {
    case Event.Insert(k, v) => store.insert(k, v) as (())
    case Event.Delete(k) => store.delete(k) as (())
  }

  /** Transform the index of a store. */
  def xmapIndex[F[_]: Functor, I, V, J](
      s: IndexedStore[F, I, V])(
      f: I => J)(
      g: J => I)
      : IndexedStore[F, J, V] =
    new IndexedStore[F, J, V] {
      def entries: Stream[F, (J, V)] =
        s.entries.map(_.leftMap(f))

      def lookup(j: J): F[Option[V]] =
        s.lookup(g(j))

      def insert(j: J, v: V): F[TryableDeferred[F, Unit]] =
        s.insert(g(j), v)

      def delete(j: J): F[TryableDeferred[F, Boolean]] =
        s.delete(g(j))
    }

  /** Effectfully transform the index of a store. */
  def xmapIndexF[F[_], I, V, J](
      s: IndexedStore[F, I, V])(
      f: I => F[J])(
      g: J => F[I])(
      implicit F: Bind[F])
      : IndexedStore[F, J, V] =
    new IndexedStore[F, J, V] {
      def entries: Stream[F, (J, V)] =
        s.entries flatMap {
          case (i, v) => Stream.eval(F.map(f(i))((_, v)))
        }

      def lookup(j: J): F[Option[V]] =
        F.bind(g(j))(s.lookup)

      def insert(j: J, v: V): F[TryableDeferred[F, Unit]] =
        F.bind(g(j))(s.insert(_, v))

      def delete(j: J): F[TryableDeferred[F, Boolean]] =
        F.bind(g(j))(s.delete)
    }

  /** Effectfully transform the value of a store. */
  def xmapValueF[F[_], I, A, B](
      s: IndexedStore[F, I, A])(
      f: A => F[B])(
      g: B => F[A])(
      implicit F: Monad[F])
      : IndexedStore[F, I, B] =
    new IndexedStore[F, I, B] {
      def entries: Stream[F, (I, B)] =
        s.entries flatMap {
          case (i, a) => Stream.eval(F.map(f(a))((i, _)))
        }

      def lookup(i: I): F[Option[B]] =
        F.bind(s.lookup(i))(_.traverse(f))

      def insert(i: I, v: B): F[TryableDeferred[F, Unit]] =
        F.bind(g(v))(s.insert(i, _))

      def delete(i: I): F[TryableDeferred[F, Boolean]] =
        s.delete(i)
    }

  private def decodeP[F[_]: Applicative, A, B, E](
      mkError: A => E)(
      prism: Prism[A, B])(
      implicit F: MonadError_[F, E]): A => F[B] =
    a => prism.getOption(a) match {
      case None => F.raiseError(mkError(a))
      case Some(b) => b.point[F]
    }

  def transformIndex[F[_]: Monad: MonadError_[?[_], E], I, II, V, E](
      mkError: I => E)(
      s: IndexedStore[F, I, V],
      prism: Prism[I, II])
      : IndexedStore[F, II, V] =
    xmapIndexF(s)(
      decodeP[F, I, II, E](mkError)(prism))(
      i => prism(i).point[F])

  def transformValue[F[_]: Monad: MonadError_[?[_], E], I, V, VV, E](
      mkError: V => E)(
      s: IndexedStore[F, I, V],
      prism: Prism[V, VV])
      : IndexedStore[F, I, VV] =
    xmapValueF(s)(
      decodeP[F, V, VV, E](mkError)(prism))(
      v => prism(v).point[F])
}

sealed abstract class IndexedStoreInstances {
  private[this] def mapTryableDeferred[F[_], G[_], A](d: TryableDeferred[F, A], f: F ~> G): TryableDeferred[G, A] =
    new TryableDeferred[G, A] {
      override def get: G[A] = f(d.get)
      override def complete(a: A): G[Unit] = f(d.complete(a))
      override def tryGet: G[Option[A]] = f(d.tryGet)
    }

  implicit def valueInvariantFunctor[F[_]: Functor, I]: InvariantFunctor[IndexedStore[F, I, ?]] =
    new InvariantFunctor[IndexedStore[F, I, ?]] {
      def xmap[A, B](fa: IndexedStore[F, I, A], f: A => B, g: B => A): IndexedStore[F, I, B] =
        new IndexedStore[F, I, B] {
          def entries: Stream[F, (I, B)] =
            fa.entries.map(_.map(f))

          def lookup(i: I): F[Option[B]] =
            fa.lookup(i).map(_.map(f))

          def insert(i: I, v: B): F[TryableDeferred[F, Unit]] =
            fa.insert(i, g(v))

          def delete(i: I): F[TryableDeferred[F, Boolean]] =
            fa.delete(i)
        }
    }
}
