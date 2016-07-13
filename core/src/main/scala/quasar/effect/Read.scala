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
import quasar.fp._, free._

import scalaz._, concurrent.Task

/** Provides the ability to obtain a value of type `R` from the environment.
  *
  * @tparam R the type of value to be read.
  */
sealed abstract class Read[R, A]

object Read {
  final case class Ask[R, A](f: R => A) extends Read[R, A]

  final class Ops[R, S[_]](implicit S: Read[R, ?] :<: S)
    extends LiftedOps[Read[R, ?], S] { self =>

    /** Request a value from the environment. */
    def ask: F[R] =
    lift(Ask(r => r))

    /** Request and modify a value from the environment. */
    def asks[A](f: R => A): F[A] =
    ask map f

    /** Evaluate a computation in a modified environment. */
    def local(f: R => R): F ~> F = {
      val g: Read[R, ?] ~> F = injectFT compose contramapR(f)

      val s: S ~> F =
        new (S ~> F) {
          def apply[A](sa: S[A]) =
            S.prj(sa) match {
              case Some(read) => g.apply(read)
              case None       => Free.liftF(sa)
            }
        }

      flatMapSNT(s)
    }

    /** Evaluate a computation using the given environment. */
    def scope(r: R): F ~> F =
    local(_ => r)

    implicit val monadReader: MonadReader[F, R] =
      new MonadReader[F, R] {
        def ask = self.ask
        def local[A](f: R => R)(fa: F[A]) = self.local(f)(fa)
        def point[A](a: => A) = Free.pure(a)
        def bind[A, B](fa: F[A])(f: A => F[B]) = fa flatMap f
      }
  }

  object Ops {
    implicit def apply[R, S[_]](implicit S: Read[R, ?] :<: S): Ops[R, S] =
      new Ops[R, S]
  }

  def contramapR[Q, R](f: Q => R): Read[R, ?] ~> Read[Q, ?] =
    new (Read[R, ?] ~> Read[Q, ?]) {
      def apply[A](ra: Read[R, A]) = ra match {
        case Ask(g) => Ask(g compose f)
      }
    }

  def constant[F[_], R](r: R)(implicit F: Applicative[F]): Read[R, ?] ~> F =
    new (Read[R, ?] ~> F) {
      private val fr = F.point(r)
      def apply[A](ra: Read[R, A]) = ra match {
        case Ask(f) => F.map(fr)(f)
      }
    }

  def fromTaskRef[R](tr: TaskRef[R]): Read[R, ?] ~> Task =
    new (Read[R, ?] ~> Task) {
      def apply[A](ra: Read[R, A]) = ra match {
        case Ask(f) => tr.read.map(f)
      }
    }

  def toReader[F[_], R](implicit F: MonadReader[F, R]): Read[R, ?] ~> F =
    new (Read[R, ?] ~> F) {
      def apply[A](ra: Read[R, A]) = ra match {
        case Ask(f) => F.asks(f)
      }
    }

  def toState[F[_], R](implicit F: MonadState[F, R]): Read[R, ?] ~> F =
    new (Read[R, ?] ~> F) {
      def apply[A](ra: Read[R, A]) = ra match {
        case Ask(f) => F.gets(f)
      }
    }
}
