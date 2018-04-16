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

import quasar._

import matryoshka._
import monocle.macros.Lenses
import scalaz._, Scalaz._

sealed abstract class MapFuncDerived[T[_[_]], A]

sealed abstract class NullaryDerived[T[_[_]], A] extends MapFuncDerived[T, A]

sealed abstract class UnaryDerived[T[_[_]], A] extends MapFuncDerived[T, A] {
  def a1: A
}
sealed abstract class BinaryDerived[T[_[_]], A] extends MapFuncDerived[T, A] {
  def a1: A
  def a2: A
}
sealed abstract class TernaryDerived[T[_[_]], A] extends MapFuncDerived[T, A] {
  def a1: A
  def a2: A
  def a3: A
}

object MapFuncDerived {
  import MapFuncsDerived._

  implicit def traverse[T[_[_]]]: Traverse[MapFuncDerived[T, ?]] =
    new Traverse[MapFuncDerived[T, ?]] {
      def traverseImpl[G[_], A, B](
        fa: MapFuncDerived[T, A])(
        f: A => G[B])(
        implicit G: Applicative[G]):
          G[MapFuncDerived[T, B]] = fa match {
            // unary
            case Abs(a1) => f(a1) ∘ (Abs(_))
            case Ceil(a1) => f(a1) ∘ (Ceil(_))
            case Floor(a1) => f(a1) ∘ (Floor(_))
            case Trunc(a1) => f(a1) ∘ (Trunc(_))
            case Round(a1) => f(a1) ∘ (Round(_))
            case Typecheck(a1, typ)  => f(a1) ∘ (Typecheck(_, typ))

            // binary
            case FloorScale(a1, a2) => (f(a1) |@| f(a2))(FloorScale(_, _))
            case CeilScale(a1, a2)  => (f(a1) |@| f(a2))(CeilScale(_, _))
            case RoundScale(a1, a2) => (f(a1) |@| f(a2))(RoundScale(_, _))
          }
    }

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  implicit def equal[T[_[_]]: EqualT, A]: Delay[Equal, MapFuncDerived[T, ?]] =
    new Delay[Equal, MapFuncDerived[T, ?]] {
      def apply[A](in: Equal[A]): Equal[MapFuncDerived[T, A]] = Equal.equal {
        // unary
        case (Abs(a1), Abs(a2)) => in.equal(a1, a2)
        case (Ceil(a1), Ceil(a2)) => in.equal(a1, a2)
        case (Floor(a1), Floor(a2)) => in.equal(a1, a2)
        case (Trunc(a1), Trunc(a2)) => in.equal(a1, a2)
        case (Round(a1), Round(a2)) => in.equal(a1, a2)
        case (Typecheck(a1, typ1), Typecheck(a2, typ2)) => in.equal(a1, a2) && typ1 === typ2

        // binary
        case (FloorScale(a11, a12), FloorScale(a21, a22)) => in.equal(a11, a21) && in.equal(a12, a22)
        case (CeilScale(a11, a12), CeilScale(a21, a22))   => in.equal(a11, a21) && in.equal(a12, a22)
        case (RoundScale(a11, a12), RoundScale(a21, a22)) => in.equal(a11, a21) && in.equal(a12, a22)

        case (_, _) => false
      }
    }

  implicit def show[T[_[_]]: ShowT]: Delay[Show, MapFuncDerived[T, ?]] =
    new Delay[Show, MapFuncDerived[T, ?]] {
      def apply[A](sh: Show[A]): Show[MapFuncDerived[T, A]] = {
        def shz(label: String, a: A*) =
          Cord(label) ++ Cord("(") ++ a.map(sh.show).toList.intercalate(Cord(", ")) ++ Cord(")")

        Show.show {
          // unary
          case Abs(a1)   => shz("Abs", a1)
          case Ceil(a1)  => shz("Ceil", a1)
          case Floor(a1) => shz("Floor", a1)
          case Trunc(a1) => shz("Trunc", a1)
          case Round(a1) => shz("Round", a1)
          case Typecheck(a1, typ) => Cord("TypeCheck") ++ Cord("(") ++ sh.show(a1) ++ Cord(":") ++ typ.show ++ Cord(")")

          // binary
          case FloorScale(a1, a2) => shz("FloorScale", a1, a2)
          case CeilScale(a1, a2)  => shz("CeilScale", a1, a2)
          case RoundScale(a1, a2) => shz("RoundScale", a1, a2)
        }
      }
    }

  // TODO: replace this with some kind of pretty-printing based on a syntax for
  // MapFunc + EJson.
  implicit def renderTree[T[_[_]]: ShowT]: Delay[RenderTree, MapFuncDerived[T, ?]] =
    new Delay[RenderTree, MapFuncDerived[T, ?]] {
      val nt = "MapFuncDerived" :: Nil

      @SuppressWarnings(Array("org.wartremover.warts.ToString"))
      def apply[A](r: RenderTree[A]): RenderTree[MapFuncDerived[T, A]] = {
        def nAry(typ: String, as: A*): RenderedTree =
          NonTerminal(typ :: nt, None, as.toList.map(r.render(_)))

        RenderTree.make {
          // unary
          case Abs(a1) => nAry("Abs", a1)
          case Ceil(a1) => nAry("Ceil", a1)
          case Floor(a1) => nAry("Floor", a1)
          case Trunc(a1) => nAry("Trunc", a1)
          case Round(a1) => nAry("Round", a1)
          case Typecheck(a1, typ) => NonTerminal("Typecheck" :: nt, typ.shows.some, List(r.render(a1)))

          // binary
          case FloorScale(a1, a2) => nAry("FloorScale", a1, a2)
          case CeilScale(a1, a2)  => nAry("CeilScale", a1, a2)
          case RoundScale(a1, a2) => nAry("RoundScale", a1, a2)
        }
      }
    }
}

object MapFuncsDerived {
  @Lenses final case class Abs[T[_[_]], A](a1: A) extends UnaryDerived[T, A]
  @Lenses final case class Ceil[T[_[_]], A](a1: A) extends UnaryDerived[T, A]
  @Lenses final case class Floor[T[_[_]], A](a1: A) extends UnaryDerived[T, A]
  @Lenses final case class Trunc[T[_[_]], A](a1: A) extends UnaryDerived[T, A]
  @Lenses final case class Round[T[_[_]], A](a1: A) extends UnaryDerived[T, A]
  @Lenses final case class Typecheck[T[_[_]], A](a1: A, typ: Type) extends UnaryDerived[T, A]

  @Lenses final case class FloorScale[T[_[_]], A](a1: A, a2: A) extends BinaryDerived[T, A]
  @Lenses final case class CeilScale[T[_[_]], A](a1: A, a2: A)  extends BinaryDerived[T, A]
  @Lenses final case class RoundScale[T[_[_]], A](a1: A, a2: A) extends BinaryDerived[T, A]
}
