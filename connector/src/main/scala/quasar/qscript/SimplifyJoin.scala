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
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns.CoEnv
import scalaz._, Scalaz._

/** Replaces [[ThetaJoin]] with [[EquiJoin]], which is often more feasible for
  * connectors to implement. It potentially adds a [[Filter]] iff there are
  * conditions in the [[ThetaJoin]] that can not be handled by an [[EquiJoin]].
  */
trait SimplifyJoin[F[_]] {
  type IT[F[_]]
  type G[A]

  def simplifyJoin[H[_]: Functor](GtoH: G ~> H): F[IT[H]] => H[IT[H]]
}

private final case class EquiJoinKey[T[_[_]]]
  (left: FreeMap[T], right: FreeMap[T])

private final case class SimplifiedJoinCondition[T[_[_]]]
  (keys: List[EquiJoinKey[T]], filter: Option[JoinFunc[T]])

object SimplifyJoin {
  type Aux[T[_[_]], F[_], H[_]] = SimplifyJoin[F] {
    type IT[F[_]] = T[F]
    type G[A] = H[A]
  }

  def apply[T[_[_]], F[_], G[_]](implicit ev: SimplifyJoin.Aux[T, F, G]) = ev

  def applyToBranch[T[_[_]]: BirecursiveT](branch: FreeQS[T])
      : FreeQS[T] =
    branch
      .convertTo[T[CoEnv[Hole, QScriptTotal[T, ?], ?]]]
      .transCata[T[CoEnv[Hole, QScriptTotal[T, ?], ?]]](liftCo(SimplifyJoin[T, QScriptTotal[T, ?], QScriptTotal[T, ?]].simplifyJoin(coenvPrism[QScriptTotal[T, ?], Hole].reverseGet)))
      .convertTo[FreeQS[T]]

  implicit def thetaJoin[T[_[_]]: BirecursiveT, F[_]]
    (implicit EJ: EquiJoin[T, ?] :<: F, QC: QScriptCore[T, ?] :<: F)
      : SimplifyJoin.Aux[T, ThetaJoin[T, ?], F] =
    new SimplifyJoin[ThetaJoin[T, ?]] {
      import MapFunc._
      import MapFuncs._

      type IT[F[_]] = T[F]
      type G[A] = F[A]

      def simplifyJoin[H[_]: Functor](GtoH: G ~> H): ThetaJoin[T, T[H]] => H[T[H]] =
        tj => {
          // TODO: This can potentially rewrite conditions to try to get left
          //       and right references on distinct sides.
          def alignCondition(l: JoinFunc[T], r: JoinFunc[T]): Option[EquiJoinKey[T]] =
            if (l.element(LeftSide) && r.element(RightSide) &&
              !l.element(RightSide) && !r.element(LeftSide))
              EquiJoinKey(l.as[Hole](SrcHole), r.as[Hole](SrcHole)).some
            else if (l.element(RightSide) && r.element(LeftSide) &&
              !l.element(LeftSide) && !r.element(RightSide))
              EquiJoinKey(r.as[Hole](SrcHole), l.as[Hole](SrcHole)).some
            else None

          def separateConditions(fm: JoinFunc[T]): SimplifiedJoinCondition[T] =
            fm.resume match {
              case -\/(And(a, b)) =>
                val (fir, sec) = (separateConditions(a), separateConditions(b))
                SimplifiedJoinCondition(
                  fir.keys ++ sec.keys,
                  fir.filter.fold(
                    sec.filter)(
                    f => sec.filter.fold(f.some)(s => Free.roll(And[T, JoinFunc[T]](f, s)).some)))
              case -\/(Eq(l, r)) =>
                alignCondition(l, r).fold(
                  SimplifiedJoinCondition(Nil, fm.some))(
                  pair => SimplifiedJoinCondition(List(pair), None))
              case _ => SimplifiedJoinCondition(Nil, fm.some)
            }

          def mergeSides(jf: JoinFunc[T]): FreeMap[T] =
            jf >>= {
              case LeftSide  => Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(0)))
              case RightSide => Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(1)))
            }

          val SimplifiedJoinCondition(keys, filter) = separateConditions(tj.on)
          GtoH(QC.inj(Map(filter.foldLeft(
            GtoH(EJ.inj(EquiJoin(
              tj.src,
              applyToBranch(tj.lBranch),
              applyToBranch(tj.rBranch),
              ConcatArraysN(keys.map(k => Free.roll(MakeArray[T, FreeMap[T]](k.left)))).embed,
              ConcatArraysN(keys.map(k => Free.roll(MakeArray[T, FreeMap[T]](k.right)))).embed,
              tj.f,
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(Free.point(LeftSide))),
                Free.roll(MakeArray(Free.point(RightSide)))))))).embed)(
            (ej, filt) => GtoH(QC.inj(Filter(ej, mergeSides(filt)))).embed),
            mergeSides(tj.combine))))
        }
    }

  implicit def qscriptCore[T[_[_]]: BirecursiveT, F[_]]
    (implicit QC: QScriptCore[T, ?] :<: F)
      : SimplifyJoin.Aux[T, QScriptCore[T, ?], F] =
    new SimplifyJoin[QScriptCore[T, ?]] {
      type IT[F[_]] = T [F]
      type G[A] = F[A]
      def simplifyJoin[H[_]: Functor](GtoH: G ~> H)
          : QScriptCore[T, T[H]] => H[T[H]] = fa => GtoH(QC.inj(fa match {
            case Union(src, lb, rb) =>
              Union(src, applyToBranch(lb), applyToBranch(rb))
            case Subset(src, lb, sel, rb) =>
              Subset(src, applyToBranch(lb), sel, applyToBranch(rb))
            case _ => fa
          }))
    }

  implicit def equiJoin[T[_[_]]: BirecursiveT, F[_]]
    (implicit EJ: EquiJoin[T, ?] :<: F)
      : SimplifyJoin.Aux[T, EquiJoin[T, ?], F] =
    new SimplifyJoin[EquiJoin[T, ?]] {
      type IT[F[_]] = T [F]
      type G[A] = F[A]
      def simplifyJoin[H[_]: Functor](GtoH: G ~> H): EquiJoin[T, T[H]] => H[T[H]] =
        ej => GtoH(EJ.inj(EquiJoin(
          ej.src,
          applyToBranch(ej.lBranch),
          applyToBranch(ej.rBranch),
          ej.lKey,
          ej.rKey,
          ej.f,
          ej.combine)))
    }

  implicit def coproduct[T[_[_]], F[_], I[_], J[_]]
    (implicit I: SimplifyJoin.Aux[T, I, F], J: SimplifyJoin.Aux[T, J, F])
      : SimplifyJoin.Aux[T, Coproduct[I, J, ?], F] =
    new SimplifyJoin[Coproduct[I, J, ?]] {
      type IT[F[_]] = T[F]
      type G[A] = F[A]
      def simplifyJoin[H[_]: Functor](GtoH: G ~> H)
          : Coproduct[I, J, T[H]] => H[T[H]] =
        _.run.fold(I.simplifyJoin(GtoH), J.simplifyJoin(GtoH))
    }

  def default[T[_[_]], F[_], I[_]](implicit F: F :<: I)
      : SimplifyJoin.Aux[T, F, I] =
    new SimplifyJoin[F] {
      type IT[F[_]] = T[F]
      type G[A] = I[A]

      def simplifyJoin[H[_]: Functor](GtoH: G ~> H): F[T[H]] => H[T[H]] =
        fa => GtoH(F.inj(fa))
    }

  implicit def deadEnd[T[_[_]], F[_]](implicit DE: Const[DeadEnd, ?] :<: F)
      : SimplifyJoin.Aux[T, Const[DeadEnd, ?], F] =
    default

  implicit def read[T[_[_]], F[_]](implicit R: Const[Read, ?] :<: F)
      : SimplifyJoin.Aux[T, Const[Read, ?], F] =
    default

  implicit def shiftedRead[T[_[_]], F[_]]
    (implicit SR: Const[ShiftedRead, ?] :<: F)
      : SimplifyJoin.Aux[T, Const[ShiftedRead, ?], F] =
    default

  implicit def projectBucket[T[_[_]], F[_]]
    (implicit PB: ProjectBucket[T, ?] :<: F)
      : SimplifyJoin.Aux[T, ProjectBucket[T, ?], F] =
    default
}
