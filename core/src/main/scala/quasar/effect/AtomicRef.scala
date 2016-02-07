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
import quasar.SKI._
import quasar.fp.TaskRef
import quasar.fp.free

import monocle.Lens
import scalaz.{Lens => _, _}
import scalaz.concurrent.Task
import scalaz.syntax.applicative._
import scalaz.syntax.id._

/** A reference to a value that may be updated atomically.
  *
  * @tparam V the type of value referenced
  */
sealed trait AtomicRef[V, A]

object AtomicRef {
  /** NB: Attempted to define this as `Get[V]() extends AtomicRef[V, V]` but
    *     when pattern matching `(x: AtomicRef[A, B]) match { case Get() => }`
    *     scalac doesn't recognize that A =:= B.
    */
  final case class Get[V, A](f: V => A) extends AtomicRef[V, A]
  final case class Set[V](v: V) extends AtomicRef[V, Unit]
  final case class CompareAndSet[V](expect: V, update: V) extends AtomicRef[V, Boolean]

  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  final class Ops[V, S[_]: Functor](implicit S: AtomicRefF[V, ?] :<: S)
    extends LiftedOps[AtomicRef[V, ?], S] {

    /** Set the value of the ref to `update` if the current value is `expect`,
      * returns whether the value was updated.
      */
    def compareAndSet(expect: V, update: V): F[Boolean] =
      lift(CompareAndSet(expect, update))

    /** Returns the current value of the ref. */
    def get: F[V] =
      lift(Get(ι))

    /** Atomically updates the ref with the result of applying the given
      * function to the current value, returning the updated value.
      */
    def modify(f: V => V): F[V] =
      modifyS(v => f(v).squared)

    /** Atomically updates the ref with the first part of the result of applying
      * the given function to the current value, returning the second part.
      */
    def modifyS[A](f: V => (V, A)): F[A] =
      for {
        cur       <- get
        (nxt, a0) =  f(cur)
        updated   <- compareAndSet(cur, nxt)
        a         <- if (updated) a0.point[F] else modifyS(f)
      } yield a

    /** Sets the value of the ref to the given value. */
    def set(value: V): F[Unit] =
      lift(Set(value))
  }

  object Ops {
    def apply[V, S[_]: Functor](implicit S: AtomicRefF[V, ?] :<: S): Ops[V, S] =
      new Ops[V, S]
  }

  def fromTaskRef[A](tr: TaskRef[A]): AtomicRef[A, ?] ~> Task =
    new (AtomicRef[A, ?] ~> Task) {
      def apply[B](fb: AtomicRef[A, B]) = fb match {
        case Get(f) =>
          tr.read map f

        case Set(a) =>
          tr.write(a)

        case CompareAndSet(expect, update) =>
          tr.compareAndSet(expect, update)
      }
    }

  def toState[F[_, _], S](implicit F: MonadState[F, S])
                         : AtomicRef[S, ?] ~> F[S, ?] =
    new (AtomicRef[S, ?] ~> F[S, ?]) {
      def apply[A](fa: AtomicRef[S, A]) = fa match {
        case Get(f) =>
          F.gets(f)

        case Set(s) =>
          F.put(s)

        case CompareAndSet(expect, update) =>
          F.bind(F.get) { s =>
            if (s == expect)
              F.put(update).as(true)
            else
              F.point(false)
          }
      }
    }

  /** Decorate AtomicRef operations by running an effect after each successful
    * update. Usage: `onSet[V](effect)`
    */
  object onSet {
    def apply[V]: Aux[V] = new Aux[V]

    final class Aux[V] {
      type Ref[A] = AtomicRef[V, A]
      type RefF[A] = Coyoneda[Ref, A]

      def apply[S[_]: Functor, F[_]: Applicative]
          (f: V => F[Unit])
          (implicit
            S0: F :<: S,
            S1: RefF :<: S
          ): AtomicRef[V, ?] ~> Free[S, ?] = {
        val R = Ops[V, S]

        new (AtomicRef[V, ?] ~> Free[S, ?]) {
          def apply[A](r: AtomicRef[V, A]) = r match {
            case Get(f) =>
              R.get.map(f)

            case Set(value) =>
              R.set(value) *> free.lift(f(value)).into[S]

            case CompareAndSet(expect, update) =>
              for {
                upd <- R.compareAndSet(expect, update)
                _   <- free.lift {
                          if (upd) f(update) else ().point[F]
                        }.into[S]
              } yield upd
          }
        }
      }
    }
  }

  /** Given a lens A -> B, lifts AtomicRef[B, ?] into any effect type
    * providing AtomicRef[A, ?]. Usage: `zoom(aLens).into[S]`.
    */
  object zoom {
    def apply[A, B](lens: Lens[A, B]) = new Aux(lens)

    final class Aux[A, B](lens: Lens[A, B]) {
      type RefA[C] = AtomicRef[A, C]
      type RefAF[C] = Coyoneda[RefA, C]

      def into[S[_]: Functor](implicit S0: RefAF :<: S)
          : AtomicRef[B, ?] ~> Free[S, ?] = {

        val R = AtomicRef.Ops[A, S]

        new (AtomicRef[B, ?] ~> Free[S, ?]) {
          def apply[C](r: AtomicRef[B, C]) = r match {
            case Get(f) =>
              R.get.map(v => f(lens.get(v)))

            case Set(v) =>
              R.modify(u => lens.set(v)(u)).void

            case CompareAndSet(expect, update) =>
              R.modifyS(v =>
                if (lens.get(v) == expect) (lens.set(update)(v), true)
                else (v, false))
          }
        }
      }
    }
  }
}
