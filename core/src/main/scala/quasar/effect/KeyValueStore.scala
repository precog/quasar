/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.fp.TaskRef

import monocle.Lens
import scalaz.{Lens => _, _}
import scalaz.concurrent.Task
import scalaz.syntax.monad._
import scalaz.syntax.id._

/** Provides the ability to read, write and delete from a store of values
  * indexed by keys.
  *
  * @tparam K the type of keys used to index values
  * @tparam V the type of values in the store
  */
sealed trait KeyValueStore[K, V, A]

object KeyValueStore {
  // NB: Switch to cursor style terms for Keys once backing stores exist where all keys won't fit into memory.
  final case class Keys[K, V]()
    extends KeyValueStore[K, V, Vector[K]]

  final case class Get[K, V](k: K)
    extends KeyValueStore[K, V, Option[V]]

  final case class Put[K, V](k: K, v: V)
    extends KeyValueStore[K, V, Unit]

  final case class CompareAndPut[K, V](k: K, expect: Option[V], update: V)
    extends KeyValueStore[K, V, Boolean]

  final case class Delete[K, V](k: K)
    extends KeyValueStore[K, V, Unit]

  final class Ops[K, V, S[_]](implicit S: KeyValueStore[K, V, ?] :<: S)
    extends LiftedOps[KeyValueStore[K, V, ?], S] {

    /** Similar to `alterS`, but returns the updated value. */
    def alter(k: K, f: Option[V] => V): F[V] =
      alterS(k, v => f(v).squared)

    /** Atomically associates the given key with the first part of the result
      * of applying the given function to the value currently associated with
      * the key, returning the second part of the result.
      */
    def alterS[A](k: K, f: Option[V] => (V, A)): F[A] =
      for {
        cur       <- get(k).run
        (nxt, a0) =  f(cur)
        updated   <- compareAndPut(k, cur, nxt)
        a         <- if (updated) a0.point[F] else alterS(k, f)
      } yield a

    /** Returns whether a value is associated with the given key. */
    def contains(k: K): F[Boolean] =
      get(k).isDefined

    /** Associate `update` with the given key if the current value at the key
      * is `expect`, passing `None` for `expect` indicates that they key is
      * expected not to be associated with a value. Returns whether the value
      * was updated.
      */
    def compareAndPut(k: K, expect: Option[V], update: V): F[Boolean] =
      lift(CompareAndPut(k, expect, update))

    /** Remove any associated with the given key. */
    def delete(k: K): F[Unit] =
      lift(Delete(k))

    /** Returns the current value associated with the given key. */
    def get(k: K): OptionT[F, V] =
      OptionT(lift(Get[K, V](k)))

    /** Returns current keys */
    val keys: F[Vector[K]] =
      lift(Keys[K, V]())

    /** Moves/renames key */
    def move(src: K, dst: K): F[Unit] =
      get(src).flatMapF(delete(src) *> put(dst, _)).run.void

    /** Atomically updates the value associated with the given key with the
      * result of applying the given function to the current value, if defined.
      */
    def modify(k: K, f: V => V): F[Unit] =
      get(k) flatMapF { v =>
        compareAndPut(k, Some(v), f(v)).ifM(().point[F], modify(k, f))
      } getOrElse (())

    /** Associate the given value with the given key. */
    def put(k: K, v: V): F[Unit] =
      lift(Put(k, v))
  }

  object Ops {
    implicit def apply[K, V, S[_]](implicit S: KeyValueStore[K, V, ?] :<: S): Ops[K, V, S] =
      new Ops[K, V, S]
  }

  /** Returns an interpreter of `KeyValueStore[K, V, ?]` into `Task`, given a
    * `TaskRef[Map[K, V]]`.
    */
  def fromTaskRef[K, V](ref: TaskRef[Map[K, V]]): KeyValueStore[K, V, ?] ~> Task =
    new (KeyValueStore[K, V, ?] ~> Task) {
      val toST = toState[State[Map[K,V],?]](Lens.id[Map[K,V]])
      def apply[C](fa: KeyValueStore[K, V, C]): Task[C] =
        ref.modifyS(toST(fa).run)
    }

  /** Interpret `KeyValueStore[K, V, ?]` into `AtomicRef[Map[K, V], ?]`, plus Free.
    * Usage: `toAtomicRef[K, V]()`. */
  object toAtomicRef {
    def apply[K, V]: Aux[K, V] = new Aux[K, V]

    final class Aux[K, V] {
      type Ref[A] = AtomicRef[Map[K, V], A]

      val R = AtomicRef.Ops[Map[K, V], Ref]

      def apply(): KeyValueStore[K, V, ?] ~> Free[Ref, ?] =
        new (KeyValueStore[K, V, ?] ~> Free[Ref, ?]) {
          def apply[A](m: KeyValueStore[K, V, A]) = m match {
            case Keys() =>
              R.get.map(_.keys.toVector)

            case Get(path) =>
              R.get.map(_.get(path))

            case Put(path, cfg) =>
              R.modify(_ + (path -> cfg)).void

            case CompareAndPut(path, expect, update) =>
              R.modifyS(m =>
                if (m.get(path) == expect) (m + (path -> update), true)
                else (m, false))

            case Delete(path) =>
              R.modify(_ - path).void
          }
        }
    }
  }

  /** Returns an interpreter of `KeyValueStore[K, V, ?]` into `F[S, ?]`,
    * given a `Lens[S, Map[K, V]]` and `MonadState[F, S]`.
    *
    * NB: Uses partial application of `F[_, _]` for better type inference, usage:
    *   `toState[F](lens)`
    */
  object toState {
    def apply[F[_]]: Aux[F] =
      new Aux[F]

    final class Aux[F[_]] {
      def apply[K, V, S](l: Lens[S, Map[K, V]])(implicit F: MonadState[F, S])
                        : KeyValueStore[K, V, ?] ~> F =
        new(KeyValueStore[K, V, ?] ~> F) {
          def apply[A](fa: KeyValueStore[K, V, A]): F[A] = fa match {
            case CompareAndPut(k, expect, update) =>
              lookup(k) flatMap { cur =>
                if (cur == expect)
                  modify(_ + (k -> update)).as(true)
                else
                  F.point(false)
              }

            case Delete(key) =>
              modify(_ - key)

            case Keys() =>
              F.gets(s => l.get(s).keys.toVector)

            case Get(key) =>
              lookup(key)

            case Put(key, value) =>
              modify(_ + (key -> value))
          }

          def lookup(k: K): F[Option[V]] =
            F.gets(s => l.get(s).get(k))

          def modify(f: Map[K, V] => Map[K, V]): F[Unit] =
            F.modify(l.modify(f))
        }
    }
  }
}
