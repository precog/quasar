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

package quasar.qscript

import slamdata.Predef._

import quasar.RenderTree

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns.interpretM
import scalaz._, Scalaz._

sealed trait RecFreeS[F[_], A] extends Product with Serializable {
  import RecFreeS._

  @SuppressWarnings(Array("org.wartremover.warts.Recursion")) // this is like a very strange foldMap...
  def linearize: Free[F, A] = this match {
    case Suspend(fa) =>
      Free.liftF(fa)
    case Fix(form, rec) =>
      rec.flatMapSuspension(λ[RecFreeS[F, ?] ~> Free[F, ?]](_.linearize)) >> form.linearize
  }
}

object RecFreeS {
  final case class Suspend[F[_], A](fa: F[A]) extends RecFreeS[F, A]
  final case class Fix[F[_], A](form: RecFreeS[F, A], rec: Free[RecFreeS[F, ?], Hole]) extends RecFreeS[F, A]

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def recInterpretM[M[_]: Monad, F[_]: Traverse, A](fm: RecFreeS[F, A])(susint: AlgebraM[M, F, A], fixint: A => M[(A, A => M[A])]): M[A] =
    fm match {
      case Suspend(fa) => susint(fa)
      case Fix(form, rec) =>
        recInterpretM(form)(susint, fixint) >>= (fixint(_)) >>= {
          case (hole, cont) =>
            rec.cataM[M, A](interpretM(_ => hole.point[M], recInterpretM(_)(susint, fixint))) >>= (cont(_))
        }
    }

  def recInterpret[F[_]: Traverse, A](fm: RecFreeS[F, A])(susint: Algebra[F, A], fixint: A => (A, A => A)): A =
    recInterpretM[Id, F, A](fm)(susint, fixint)

  def fromFree[F[_], A](f: Free[F, A]): Free[RecFreeS[F, ?], A] =
    f.mapSuspension(λ[F ~> RecFreeS[F, ?]](Suspend(_)))

  def letIn[F[_], A](form: Free[RecFreeS[F, ?], A], body: Free[RecFreeS[F, ?], Hole]): Free[RecFreeS[F, ?], A] =
    form.mapSuspension(λ[RecFreeS[F, ?] ~> RecFreeS[F, ?]](f => Fix(f, body)))

  def linearize[F[_], A](f: Free[RecFreeS[F, ?], A]): Free[F, A] =
    f.flatMapSuspension(λ[RecFreeS[F, ?] ~> Free[F, ?]](_.linearize))

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def mapS[F[_], S[_], A](rc: RecFreeS[F, A])(t: F ~> S): RecFreeS[S, A] = rc match {
    case Suspend(fa) =>
      Suspend(t(fa))
    case Fix(form, rec) =>
      Fix(
        RecFreeS.mapS(form)(t),
        rec.mapSuspension(λ[RecFreeS[F, ?] ~> RecFreeS[S, ?]](RecFreeS.mapS(_)(t))))
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def traverse[F[_]: Traverse]: Traverse[RecFreeS[F, ?]] = new Traverse[RecFreeS[F, ?]] {
    def traverseImpl[G[_]: Applicative, A, B](fa: RecFreeS[F, A])(f: A => G[B]): G[RecFreeS[F, B]] = fa match {
      case Suspend(fa0) => Traverse[F].traverseImpl[G, A, B](fa0)(f) map (Suspend(_))
      case Fix(form, rec) => traverseImpl[G, A, B](form)(f) map (Fix(_, rec))
    }
  }

  // FIXME: Display the bound form and body separately
  implicit def renderTree[F[_], A](implicit F: RenderTree[F[A]], FR: RenderTree[Free[F, A]]): RenderTree[RecFreeS[F, A]] =
    RenderTree.make(rf => FR.render(rf.linearize))

  implicit def show[F[_], A](implicit SF: Show[Free[F, A]]): Show[Free[RecFreeS[F, ?], A]] =
    Show.show(_.linearize.show)

  implicit def equal[F[_], A](implicit SF: Equal[Free[F, A]]): Equal[Free[RecFreeS[F, ?], A]] =
    Equal.equalBy(_.linearize)

  implicit final class LinearizeOps[F[_], A](val self: Free[RecFreeS[F, ?], A]) extends AnyVal {
    def linearize: Free[F, A] = RecFreeS.linearize(self)
  }

  implicit final class RecOps[F[_], A](val self: Free[F, A]) extends AnyVal {
    def asRec: Free[RecFreeS[F, ?], A] = RecFreeS.fromFree(self)
  }
}
