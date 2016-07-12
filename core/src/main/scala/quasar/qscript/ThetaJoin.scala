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
import quasar.fp._

import matryoshka._
import monocle.macros.Lenses
import scalaz._, Scalaz._

/** Applies a function across two datasets, in the cases where the JoinFunc
  * evaluates to true. The branches represent the divergent operations applied
  * to some common src. Each branch references the src exactly once. (Since no
  * constructor has more than one recursive component, it’s guaranteed that
  * neither side references the src _more_ than once.)
  *
  * This case represents a full θJoin, but we could have an algebra that
  * rewites it as
  *     Filter(_, EquiJoin(...))
  * to simplify behavior for the backend.
  */
@Lenses final case class ThetaJoin[T[_[_]], A](
  src: A,
  lBranch: FreeQS[T],
  rBranch: FreeQS[T],
  on: JoinFunc[T],
  f: JoinType,
  combine: JoinFunc[T])

object ThetaJoin {
  implicit def equal[T[_[_]]: EqualT]: Delay[Equal, ThetaJoin[T, ?]] =
    new Delay[Equal, ThetaJoin[T, ?]] {
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (ThetaJoin(a1, l1, r1, o1, f1, c1), ThetaJoin(a2, l2, r2, o2, f2, c2)) =>
            eq.equal(a1, a2) && l1 ≟ l2 && r1 ≟ r2 && o1 ≟ o2 && f1 ≟ f2 && c1 ≟ c2
        }
    }

  implicit def traverse[T[_[_]]]: Traverse[ThetaJoin[T, ?]] =
    new Traverse[ThetaJoin[T, ?]] {
      def traverseImpl[G[_]: Applicative, A, B](
        fa: ThetaJoin[T, A])(
        f: A => G[B]) =
        f(fa.src) ∘ (ThetaJoin(_, fa.lBranch, fa.rBranch, fa.on, fa.f, fa.combine))
    }


  implicit def show[T[_[_]]: ShowT]: Delay[Show, ThetaJoin[T, ?]] =
    new Delay[Show, ThetaJoin[T, ?]] {
      def apply[A](showA: Show[A]): Show[ThetaJoin[T, A]] = Show.show {
        case ThetaJoin(src, lBr, rBr, on, f, combine) =>
          Cord("ThetaJoin(") ++
          showA.show(src) ++ Cord(",") ++
          lBr.show ++ Cord(",") ++
          rBr.show ++ Cord(",") ++
          on.show ++ Cord(",") ++
          f.show ++ Cord(",") ++
          combine.show ++ Cord(")")
      }
    }

  implicit def mergeable[T[_[_]]: EqualT]: Mergeable.Aux[T, ThetaJoin[T, Unit]] =
    new Mergeable[ThetaJoin[T, Unit]] {
      type IT[F[_]] = T[F]

      def mergeSrcs(
        left: FreeMap[IT],
        right: FreeMap[IT],
        p1: ThetaJoin[IT, Unit],
        p2: ThetaJoin[IT, Unit]) =
        OptionT(state(
          if (p1 ≟ p2)
            SrcMerge[ThetaJoin[IT, Unit], FreeMap[IT]](p1, left, right).some
          else
            None))
    }

  implicit def bucketable[T[_[_]]]: Bucketable.Aux[T, ThetaJoin[T, ?]] =
    new Bucketable[ThetaJoin[T, ?]] {
      type IT[G[_]] = T[G]

      def digForBucket[G[_]](tj: ThetaJoin[T, IT[G]]) = IndexedStateT.stateT(tj)
    }

  implicit def normalizable[T[_[_]]: Recursive: Corecursive: EqualT]:
      Normalizable[ThetaJoin[T, ?]] =
    new Normalizable[ThetaJoin[T, ?]] {
      def normalize = new (ThetaJoin[T, ?] ~> ThetaJoin[T, ?]) {
        def apply[A](tj: ThetaJoin[T, A]) =
          ThetaJoin(
            tj.src,
            tj.lBranch.mapSuspension(Normalizable[QScriptInternal[T, ?]].normalize),
            tj.rBranch.mapSuspension(Normalizable[QScriptInternal[T, ?]].normalize),
            normalizeMapFunc(tj.on),
            tj.f,
            normalizeMapFunc(tj.combine))
      }
    }
}
