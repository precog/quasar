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
import quasar.fp.free._

import monocle.Lens
import scalaz.{Lens => _, _}
import scalaz.concurrent.Task
import scalaz.syntax.monad._
import scalaz.syntax.id._
import scalaz.syntax.std.boolean._

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

  final case class CompareAndAlter[K, V](k: K, expect: Option[V], update: Option[V])
    extends KeyValueStore[K, V, Boolean]

  final case class Delete[K, V](k: K)
    extends KeyValueStore[K, V, Unit]

  final class Ops[K, V, S[_]](implicit S: KeyValueStore[K, V, ?] :<: S)
    extends LiftedOps[KeyValueStore[K, V, ?], S] {

    /** Similar to `alterS`, but returns the updated value. */
    def alter(k: K, f: Option[V] => Option[V]): OptionT[F, V] =
      OptionT(alterS(k, v => f(v).squared))

    /** Atomically associates the given key with the first part of the result
      * of applying the given function to the value currently associated with
      * the key, returning the second part of the result.
      */
    def alterS[A](k: K, f: Option[V] => (Option[V], A)): F[A] =
      for {
        cur       <- get(k).run
        (nxt, a0) =  f(cur)
        updated   <- compareAndAlter(k, cur, nxt)
        a         <- if (updated) a0.point[F] else alterS(k, f)
      } yield a

    /** Returns whether a value is associated with the given key. */
    def contains(k: K): F[Boolean] =
      get(k).isDefined

    /** Alters the state of the key in the store if the current state of the key
      * matches `expect`.
      *
      * @param expect the expected state of the key, `None` means it is not associated with a value.
      * @param update the new state of the key, `None` means it should be deleted.
      * @return whether the state of the key was modified.
      */
    def compareAndAlter(k: K, expect: Option[V], update: Option[V]): F[Boolean] =
      lift(CompareAndAlter(k, expect, update))

    /** Associate `update` with the given key if the current value at the key
      * is `expect`, passing `None` for `expect` indicates that they key is
      * expected not to be associated with a value. Returns whether the value
      * was updated.
      */
    def compareAndPut(k: K, expect: Option[V], update: V): F[Boolean] =
      lift(CompareAndAlter(k, expect, Some(update)))

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
    def apply[K, V, S[_]](implicit S: KeyValueStore[K, V, ?] :<: S): Ops[K, V, S] =
      new Ops[K, V, S]
  }

  /** Interpret `KeyValueStore[K, V, ?]` using `TaskRef[Map[K, V]]`. */
  def fromTaskRef[K, V](ref: TaskRef[Map[K, V]]): KeyValueStore[K, V, ?] ~> Task =
    foldMapNT(AtomicRef.fromTaskRef(ref)) compose toAtomicRef

  /** Interpret `KeyValueStore[K, V, ?]` using `AtomicRef[Map[K, V], ?]`. */
  def toAtomicRef[K, V]: KeyValueStore[K, V, ?] ~> Free[AtomicRef[Map[K, V], ?], ?] = {
    type Ref[A] = AtomicRef[Map[K, V], A]
    val R = AtomicRef.Ops[Map[K, V], Ref]
    val toST = toState[State[Map[K, V], ?]](Lens.id[Map[K, V]])

    new (KeyValueStore[K, V, ?] ~> Free[Ref, ?]) {
      def apply[A](fa: KeyValueStore[K, V, A]) =
        R.modifyS(toST(fa).run)
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
            case CompareAndAlter(k, expect, update) =>
              lookup(k) flatMap { cur =>
                if (cur == expect)
                  modify(m => update.fold(m - k)(m.updated(k, _))).as(true)
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

  /** Adds write-through caching to KeyValueStore. It is "full" as _all_ reads
    * are serviced from the cache (i.e. cache misses must imply a miss in the
    * backing store as well).
    *
    * The following assumptions must be true in order to use this cache:
    *
    * 1. The cache and backing store agree before servicing any requests.
    * 2. This is the only effect modifying the backing store.
    */
  def fullWriteThrough[S[_], K, V](
    implicit
    S0: KeyValueStore[K, V, ?] :<: S,
    S1: Cache[KeyValueStore[K, V, ?], ?] :<: S
  ): KeyValueStore[K, V, ?] ~> Free[S, ?] = {
    val kvs = Ops[K, V, S]
    val cached = mapSNT(Cache.cached[S, KeyValueStore[K, V, ?]])

    new (KeyValueStore[K, V, ?] ~> Free[S, ?]) {
      def apply[A](kv: KeyValueStore[K, V, A]) = kv match {
        case CompareAndAlter(k, exp, upd) =>
          cachedCAA(k, exp, upd)

        case Delete(k) =>
          for {
            cv <- cached(kvs.get(k).run)
            _  <- kvs.delete(k)
            _  <- alterOrCanonicalize(k, cv, None)
          } yield ()

        case Keys() =>
          cached(kvs.keys)

        case Get(k) =>
          cached(kvs.get(k).run)

        case Put(k, v) =>
          for {
            cv <- cached(kvs.get(k).run)
            _  <- kvs.put(k, v)
            _  <- alterOrCanonicalize(k, cv, Some(v))
          } yield ()
      }

      def cachedCAA(k: K, exp: Option[V], upd: Option[V]) =
        kvs.compareAndAlter(k, exp, upd) >>= { succeeded =>
          alterOrCanonicalize(k, exp, upd)
            .whenM(succeeded)
            .as(succeeded)
        }

      // compareAndAlter the cache with the given arguments, retrying with the
      // latest canonical value until sucessful.
      def alterOrCanonicalize(k: K, expCache: Option[V], newCache: Option[V]): Free[S, Unit] = {
        def canonicalize = for {
          c0 <- cached(kvs.get(k).run)
          c1 <- kvs.get(k).run
          _  <- alterOrCanonicalize(k, c0, c1)
        } yield ()

        cached(kvs.compareAndAlter(k, expCache, newCache)) >>= (_ unlessM canonicalize)
      }
    }
  }
}
