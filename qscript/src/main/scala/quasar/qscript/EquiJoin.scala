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
import quasar.{NonTerminal, RenderTree, RenderTreeT}, RenderTree.ops._
import quasar.common.JoinType
import quasar.contrib.matryoshka._
import quasar.fp._

import matryoshka._
import matryoshka.data._
import monocle.macros.Lenses
import scalaz._, Scalaz._

/** This is an optional component of QScript that can be used instead of
  * ThetaJoin. It’s easier to implement, but more restricted (where ThetaJoin
  * has an arbitrary predicate to determine if a pair of records should be
  * combined, EquiJoin has an expression on each side that is compared with
  * simple equality).
  */
@Lenses final case class EquiJoin[T[_[_]], A](
  src: A,
  lBranch: FreeQS[T],
  rBranch: FreeQS[T],
  key: List[(FreeMap[T], FreeMap[T])],
  f: JoinType,
  // TODO: This could potentially also index into the key.
  combine: JoinFunc[T])

object EquiJoin {
  implicit def equal[T[_[_]]: BirecursiveT: EqualT]:
      Delay[Equal, EquiJoin[T, ?]] =
    new Delay[Equal, EquiJoin[T, ?]] {
      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (EquiJoin(a1, l1, r1, k1, f1, c1),
                EquiJoin(a2, l2, r2, k2, f2, c2)) =>
            eq.equal(a1, a2) &&
            l1 ≟ l2 &&
            r1 ≟ r2 &&
            k1 ≟ k2 &&
            f1 ≟ f2 &&
            c1 ≟ c2
        }
    }

  implicit def show[T[_[_]]: ShowT]: Delay[Show, EquiJoin[T, ?]] =
    new Delay[Show, EquiJoin[T, ?]] {
      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def apply[A](showA: Show[A]): Show[EquiJoin[T, A]] = Show.show {
        case EquiJoin(src, lBr, rBr, key, f, combine) =>
          Cord("EquiJoin(") ++
          showA.show(src) ++ Cord(",") ++
          lBr.show ++ Cord(",") ++
          rBr.show ++ Cord(",") ++
          key.show ++ Cord(",") ++
          f.show ++ Cord(",") ++
          combine.show ++ Cord(")")
      }
    }

  implicit def renderTree[T[_[_]]: RenderTreeT: ShowT]: Delay[RenderTree, EquiJoin[T, ?]] =
    new Delay[RenderTree, EquiJoin[T, ?]] {
      val nt = List("EquiJoin")
      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def apply[A](r: RenderTree[A]): RenderTree[EquiJoin[T, A]] = RenderTree.make {
          case EquiJoin(src, lBr, rBr, key, tpe, combine) =>
            NonTerminal(nt, None, List(
              r.render(src),
              lBr.render,
              rBr.render,
              key.render,
              tpe.render,
              combine.render))
        }
      }

  implicit def traverse[T[_[_]]]: Traverse[EquiJoin[T, ?]] =
    new Traverse[EquiJoin[T, ?]] {
      def traverseImpl[G[_]: Applicative, A, B](
        fa: EquiJoin[T, A])(
        f: A => G[B]) =
        f(fa.src) ∘
          (EquiJoin(_, fa.lBranch, fa.rBranch, fa.key, fa.f, fa.combine))
    }
}
