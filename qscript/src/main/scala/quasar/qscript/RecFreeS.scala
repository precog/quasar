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

import quasar.{RenderTree, NonTerminal}
import quasar.fp.ski.κ

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns.{interpretM, interpret, CoEnv}
import scalaz._, Scalaz._

sealed trait RecFreeS[F[_], A] extends Product with Serializable {
  import RecFreeS._

  @SuppressWarnings(Array("org.wartremover.warts.Recursion")) // this is like a very strange foldMap...
  def linearize: Free[F, A] = this match {
    case Leaf(a) =>
      Free.point(a)
    case Suspend(fa) =>
      Free.liftF(fa)
    case Fix(form, rec) =>
      rec.flatMapSuspension(λ[RecFreeS[F, ?] ~> Free[F, ?]](_.linearize)) >> form.linearize
  }
}

object RecFreeS {
  final case class Leaf[F[_], A](a: A) extends RecFreeS[F, A]
  final case class Suspend[F[_], A](fa: F[A]) extends RecFreeS[F, A]
  final case class Fix[F[_], A](form: RecFreeS[F, A], rec: Free[RecFreeS[F, ?], Hole]) extends RecFreeS[F, A]

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def recInterpretM[M[_]: Monad, F[_]: Traverse, A](susint: AlgebraM[M, F, A], fixint: A => M[(A, A => M[A])]): AlgebraM[M, RecFreeS[F, ?], A] =
    fm => fm match {
      case Leaf(a) => a.point[M]
      case Suspend(fa) => susint(fa)
      case Fix(form, rec) =>
        recInterpretM[M, F, A](susint, fixint).apply(form) >>= (fixint(_)) >>= {
          case (hole, cont) =>
            rec.cataM[M, A](
              interpretM[M, RecFreeS[F, ?], Hole, A](
                κ(hole.point[M]),
                recInterpretM[M, F, A](susint, fixint).apply(_))) >>= (cont(_))
        }
    }

  def recInterpret[F[_]: Traverse, A](susint: Algebra[F, A], fixint: A => (A, A => A)): Algebra[RecFreeS[F, ?], A] =
    fm => recInterpretM[Id, F, A](susint, fixint).apply(fm)

  def fromFree[F[_], A](f: Free[F, A]): Free[RecFreeS[F, ?], A] =
    f.mapSuspension(λ[F ~> RecFreeS[F, ?]](Suspend(_)))

  def letIn[F[_]: Traverse, A](form: Free[RecFreeS[F, ?], A], body: Free[RecFreeS[F, ?], Hole]): Free[RecFreeS[F, ?], A] =
    form.resume match {
      case \/-(leaf) => Free.roll(Fix(Leaf(leaf.point[Free[RecFreeS[F, ?], ?]]), body))
      case -\/(step) => Free.roll(Fix(step, body))
    }

  def linearize[F[_], A](f: Free[RecFreeS[F, ?], A]): Free[F, A] =
    f.flatMapSuspension(λ[RecFreeS[F, ?] ~> Free[F, ?]](_.linearize))

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def mapS[F[_], S[_], A](rc: RecFreeS[F, A])(t: F ~> S): RecFreeS[S, A] = rc match {
    case Leaf(a) =>
      Leaf(a)
    case Suspend(fa) =>
      Suspend(t(fa))
    case Fix(form, rec) =>
      Fix(
        RecFreeS.mapS(form)(t),
        rec.mapSuspension(λ[RecFreeS[F, ?] ~> RecFreeS[S, ?]](RecFreeS.mapS(_)(t))))
  }

  def roll[F[_], A](rc: F[Free[RecFreeS[F, ?], A]]): Free[RecFreeS[F, ?], A] =
    Free.roll(RecFreeS.Suspend(rc))

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def traverse[F[_]: Traverse]: Traverse[RecFreeS[F, ?]] = new Traverse[RecFreeS[F, ?]] {
    def traverseImpl[G[_]: Applicative, A, B](fa: RecFreeS[F, A])(f: A => G[B]): G[RecFreeS[F, B]] = fa match {
      case Leaf(a) => f(a) map (Leaf(_))
      case Suspend(fa0) => Traverse[F].traverseImpl[G, A, B](fa0)(f) map (Suspend(_))
      case Fix(form, rec) => traverseImpl[G, A, B](form)(f) map (Fix(_, rec))
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def recRenderTree[F[_]: Traverse](implicit FR: Delay[RenderTree, F]): Delay[RenderTree, RecFreeS[F, ?]] =
    Delay.fromNT(λ[RenderTree ~> (RenderTree ∘ RecFreeS[F, ?])#λ](rt =>
      RenderTree.make {
        case Leaf(a) => rt.render(a)
        case Suspend(fa) => FR.apply(rt).render(fa)
        case Fix(form, body) =>
          NonTerminal(
            List("Let"),
            none,
            List(
              recRenderTree[F].apply(rt).render(form),
              RenderTree.free[RecFreeS[F, ?]].apply(RenderTree[Hole]).render(body)))
      }))

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def show[F[_]: Traverse](implicit S: Delay[Show, F]): Delay[Show, RecFreeS[F, ?]] =
    Delay.fromNT(λ[Show ~> (Show ∘ RecFreeS[F, ?])#λ](sh =>
      Show.show {
        case Leaf(a) => sh.show(a)
        case Suspend(fa) => S.apply(sh).show(fa)
        case Fix(form, body) => {
          def mkLet(f: Cord, b: Cord): Cord =
            Cord("Let(") ++ f ++ Cord(" = ") ++ b ++ Cord(")")

          val alg: Algebra[CoEnv[Hole, RecFreeS[F, ?], ?], Cord] =
            interpret[RecFreeS[F, ?], Hole, Cord](_.show, RecFreeS.recInterpret(_.show, frm => (Cord("SrcHole"), c => mkLet(frm, c))))

          mkLet(RecFreeS.show[F].apply(sh).show(form), body.cata(alg))
        }
      }))

  implicit def equal[F[_], A](implicit SF: Equal[Free[F, A]]): Equal[Free[RecFreeS[F, ?], A]] =
    Equal.equalBy(_.linearize)

  implicit final class LinearizeOps[F[_], A](val self: Free[RecFreeS[F, ?], A]) extends AnyVal {
    def linearize: Free[F, A] = RecFreeS.linearize(self)
  }

  implicit final class RecOps[F[_], A](val self: Free[F, A]) extends AnyVal {
    def asRec: Free[RecFreeS[F, ?], A] = RecFreeS.fromFree(self)
  }
}
