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

package quasar.qscript

import quasar.Predef._
import quasar.{NonTerminal, RenderTree, RenderTreeT}, RenderTree.ops._
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
  lKey: FreeMap[T],
  rKey: FreeMap[T],
  f: JoinType,
  combine: JoinFunc[T])

object EquiJoin {
  implicit def equal[T[_[_]]: EqualT]:
      Delay[Equal, EquiJoin[T, ?]] =
    new Delay[Equal, EquiJoin[T, ?]] {
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (EquiJoin(a1, l1, r1, lk1, rk1, f1, c1),
                EquiJoin(a2, l2, r2, lk2, rk2, f2, c2)) =>
            eq.equal(a1, a2) &&
            l1 ≟ l2 &&
            r1 ≟ r2 &&
            lk1 ≟ lk2 &&
            rk1 ≟ rk2 &&
            f1 ≟ f2 &&
            c1 ≟ c2
        }
    }

  implicit def show[T[_[_]]: ShowT]: Delay[Show, EquiJoin[T, ?]] =
    new Delay[Show, EquiJoin[T, ?]] {
      def apply[A](showA: Show[A]): Show[EquiJoin[T, A]] = Show.show {
        case EquiJoin(src, lBr, rBr, lkey, rkey, f, combine) =>
          Cord("EquiJoin(") ++
          showA.show(src) ++ Cord(",") ++
          lBr.show ++ Cord(",") ++
          rBr.show ++ Cord(",") ++
          lkey.show ++ Cord(",") ++
          rkey.show ++ Cord(",") ++
          f.show ++ Cord(",") ++
          combine.show ++ Cord(")")
      }
    }

  implicit def renderTree[T[_[_]]: RenderTreeT: ShowT]: Delay[RenderTree, EquiJoin[T, ?]] =
    new Delay[RenderTree, EquiJoin[T, ?]] {
      val nt = List("EquiJoin")
      def apply[A](r: RenderTree[A]): RenderTree[EquiJoin[T, A]] = RenderTree.make {
          case EquiJoin(src, lBr, rBr, lKey, rKey, tpe, combine) =>
            NonTerminal(nt, None, List(
              r.render(src),
              lBr.render,
              rBr.render,
              lKey.render,
              rKey.render,
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
          (EquiJoin(_, fa.lBranch, fa.rBranch, fa.lKey, fa.rKey, fa.f, fa.combine))
    }

  implicit def mergeable[T[_[_]]: BirecursiveT: EqualT: ShowT]
      : Mergeable.Aux[T, EquiJoin[T, ?]] =
    new Mergeable[EquiJoin[T, ?]] {
      type IT[F[_]] = T[F]

      // TODO: merge two joins with different combine funcs
      def mergeSrcs(
        left: FreeMap[IT],
        right: FreeMap[IT],
        p1: EquiJoin[IT, ExternallyManaged],
        p2: EquiJoin[IT, ExternallyManaged]) =
        (p1, p2) match {
          case (EquiJoin(s1, l1, r1, lk1, rk1, f1, c1),
                EquiJoin(_, l2, r2, lk2, rk2, f2, c2)) =>
            val left1 = rebaseBranch(l1, left)
            val right1 = rebaseBranch(r1, left)
            val left2 = rebaseBranch(l2, right)
            val right2 = rebaseBranch(r2, right)

            (left1 ≟ left2 && right1 ≟ right2 && lk1 ≟ lk2 && rk1 ≟ rk2 && f1 ≟ f2).option {
              val (merged, left, right) = concat(c1, c2)
              SrcMerge(EquiJoin(s1, left1, right1, lk1, rk1, f1, merged), left, right)
            }
        }
    }
}
