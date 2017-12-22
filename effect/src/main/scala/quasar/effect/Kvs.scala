/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.effect

import slamdata.Predef._
import quasar.fp._

import scalaz._, Scalaz._

/** Provides the ability to read, write and delete from a store of values
  * indexed by keys.
  *
  * @tparam K the type of keys used to index values
  * @tparam V the type of values in the store
  */
trait Kvs[F[_], K, V] {
  def keys: F[Vector[K]]
  def get(k: K): F[Option[V]]
  def put(k: K, v: V): F[Unit]
  def compareAndPut(k: K, expect: Option[V], update: V): F[Boolean]
  def delete(k: K): F[Unit]
}

object Kvs extends KvsInstances {
  def apply[F[_], K, V](implicit K: Kvs[F, K, V]): Kvs[F, K, V] = K

  def forTrans[F[_]: Monad, K, V, T[_[_], _]: MonadTrans](implicit K: Kvs[F, K, V]): Kvs[T[F, ?], K, V] =
    new Kvs[T[F, ?], K, V] {
      def keys                                    = K.keys.liftM[T]
      def get(k: K)                               = K.get(k).liftM[T]
      def put(k: K, v: V)                         = K.put(k, v).liftM[T]
      def compareAndPut(k: K, e: Option[V], u: V) = K.compareAndPut(k, e, u).liftM[T]
      def delete(k: K)                            = K.delete(k).liftM[T]
    }
}

sealed abstract class KvsInstances extends KvsInstances0 {
  implicit def keyValueStoreKvs[K, V]: Kvs[KeyValueStore[K, V, ?], K, V] =
    new Kvs[KeyValueStore[K, V, ?], K, V] {
      def keys                                    = KeyValueStore.Keys()
      def get(k: K)                               = KeyValueStore.Get(k)
      def put(k: K, v: V)                         = KeyValueStore.Put(k, v)
      def compareAndPut(k: K, e: Option[V], u: V) = KeyValueStore.CompareAndPut(k, e, u)
      def delete(k: K)                            = KeyValueStore.Delete(k)
    }

  implicit def freeKvs[K, V, F[_], S[_] <: ACopK](implicit F: Kvs[F, K, V], I: F :<<: S): Kvs[Free[S, ?], K, V] =
    new Kvs[Free[S, ?], K, V] {
      def keys                                    = Free.liftF(I(F.keys))
      def get(k: K)                               = Free.liftF(I(F.get(k)))
      def put(k: K, v: V)                         = Free.liftF(I(F.put(k, v)))
      def compareAndPut(k: K, e: Option[V], u: V) = Free.liftF(I(F.compareAndPut(k, e, u)))
      def delete(k: K)                            = Free.liftF(I(F.delete(k)))
    }
}

sealed abstract class KvsInstances0 {
  implicit def eitherTKvs[K, V, E, F[_]: Monad: Kvs[?[_], K, V]]: Kvs[EitherT[F, E, ?], K, V] =
    Kvs.forTrans[F, K, V, EitherT[?[_], E, ?]]

  implicit def readerTKvs[K, V, R, F[_]: Monad: Kvs[?[_], K, V]]: Kvs[ReaderT[F, R, ?], K, V] =
    Kvs.forTrans[F, K, V, ReaderT[?[_], R, ?]]

  implicit def stateTKvs[K, V, S, F[_]: Monad: Kvs[?[_], K, V]]: Kvs[StateT[F, S, ?], K, V] =
    Kvs.forTrans[F, K, V, StateT[?[_], S, ?]]

  implicit def writerTKvs[K, V, W: Monoid, F[_]: Monad: Kvs[?[_], K, V]]: Kvs[WriterT[F, W, ?], K, V] =
    Kvs.forTrans[F, K, V, WriterT[?[_], W, ?]]
}
