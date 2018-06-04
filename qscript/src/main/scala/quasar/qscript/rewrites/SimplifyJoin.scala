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

package quasar.qscript.rewrites

import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript.RecFreeS._
import quasar.qscript._
import slamdata.Predef.{Map => _, _}

import matryoshka.{Hole => _, _}
import matryoshka.implicits._
import scalaz._, Scalaz._
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::

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

  val LeftK = "left"
  val RightK = "right"

  def apply[T[_[_]], F[_], G[_]](implicit ev: SimplifyJoin.Aux[T, F, G]) = ev

  def applyToBranch[T[_[_]]: BirecursiveT](branch: FreeQS[T]): FreeQS[T] = {
    val modify: T[CoEnvQS[T, ?]] => T[CoEnvQS[T, ?]] =
      _.transCata[T[CoEnvQS[T, ?]]](
        liftCo(SimplifyJoin[T, QScriptTotal[T, ?], QScriptTotal[T, ?]].simplifyJoin(coenvPrism.reverseGet)))

    applyCoEnvFrom[T, QScriptTotal[T, ?], Hole](modify).apply(branch)
  }

  implicit def thetaJoin[T[_[_]]: BirecursiveT, F[a] <: ACopK[a]]
    (implicit EJ: EquiJoin[T, ?] :<<: F, QC: QScriptCore[T, ?] :<<: F)
      : SimplifyJoin.Aux[T, ThetaJoin[T, ?], F] =
    new SimplifyJoin[ThetaJoin[T, ?]] {
      import MapFuncsCore._

      type IT[F[_]] = T[F]
      type G[A] = F[A]

      val func = construction.Func[T]

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

          @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
          def separateConditions(fm: JoinFunc[T]): SimplifiedJoinCondition[T] =
            fm.resume match {
              // TODO: Use `MapFunc.flattenAnd` instead of this case.
              case -\/(MFC(And(a, b))) =>
                val (fir, sec) = (separateConditions(a), separateConditions(b))
                SimplifiedJoinCondition(
                  fir.keys ++ sec.keys,
                  fir.filter.fold(sec.filter)(f => sec.filter.fold(f)(func.And(f, _)).some))
              case -\/(MFC(Eq(l, r))) =>
                alignCondition(l, r).fold(
                  SimplifiedJoinCondition(Nil, fm.some))(
                  pair => SimplifiedJoinCondition(List(pair), None))
              case _ => SimplifiedJoinCondition(Nil, fm.some)
            }

          def mergeSides(jf: JoinFunc[T]): FreeMap[T] =
            jf >>= {
              case LeftSide  => func.ProjectKeyS(func.Hole, LeftK)
              case RightSide => func.ProjectKeyS(func.Hole, RightK)
            }

          val SimplifiedJoinCondition(keys, filter) = separateConditions(tj.on)

          GtoH(QC.inj(Map(filter.foldLeft(
            GtoH(EJ.inj(EquiJoin(
              tj.src,
              applyToBranch(tj.lBranch),
              applyToBranch(tj.rBranch),
              keys.map(k => (k.left, k.right)),
              tj.f,
              func.StaticMapS(
                LeftK -> func.LeftSide,
                RightK -> func.RightSide)))).embed)(
            (ej, filt) => GtoH(QC.inj(Filter(ej, mergeSides(filt).asRec))).embed),
            mergeSides(tj.combine).asRec)))
        }
    }

  implicit def qscriptCore[T[_[_]]: BirecursiveT, F[a] <: ACopK[a]]
    (implicit QC: QScriptCore[T, ?] :<<: F)
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

  implicit def equiJoin[T[_[_]]: BirecursiveT, F[a] <: ACopK[a]]
    (implicit EJ: EquiJoin[T, ?] :<<: F)
      : SimplifyJoin.Aux[T, EquiJoin[T, ?], F] =
    new SimplifyJoin[EquiJoin[T, ?]] {
      type IT[F[_]] = T [F]
      type G[A] = F[A]
      def simplifyJoin[H[_]: Functor](GtoH: G ~> H): EquiJoin[T, T[H]] => H[T[H]] =
        ej => GtoH(EJ.inj(EquiJoin(
          ej.src,
          applyToBranch(ej.lBranch),
          applyToBranch(ej.rBranch),
          ej.key,
          ej.f,
          ej.combine)))
    }

  implicit def copk[T[_[_]], LL <: TListK, S[_]](implicit M: Materializer[T, LL, S]): SimplifyJoin.Aux[T, CopK[LL, ?], S] =
    M.materialize(offset = 0)

  sealed trait Materializer[T[_[_]], LL <: TListK, S[_]] {
    def materialize(offset: Int): SimplifyJoin.Aux[T, CopK[LL, ?], S]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[T[_[_]], F[_], S[_]](
      implicit
      S: SimplifyJoin.Aux[T, F, S]
    ): Materializer[T, F ::: TNilK, S] = new Materializer[T, F ::: TNilK, S] {
      override def materialize(offset: Int): SimplifyJoin.Aux[T, CopK[F ::: TNilK, ?], S] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new SimplifyJoin[CopK[F ::: TNilK, ?]] {
          type IT[F[_]] = T[F]
          type G[A] = S[A]

          def simplifyJoin[H[_]: Functor](GtoH: G ~> H): CopK[F ::: TNilK, T[H]] => H[T[H]] = {
            case I(fa) => S.simplifyJoin(GtoH).apply(fa)
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[T[_[_]], F[_], LL <: TListK, S[_]](
      implicit
      S: SimplifyJoin.Aux[T, F, S],
      LL: Materializer[T, LL, S]
    ): Materializer[T, F ::: LL, S] = new Materializer[T, F ::: LL, S] {
      override def materialize(offset: Int): SimplifyJoin.Aux[T, CopK[F ::: LL, ?], S] = {
        val I = mkInject[F, F ::: LL](offset)
        new SimplifyJoin[CopK[F ::: LL, ?]] {
          type IT[F[_]] = T[F]
          type G[A] = S[A]

          def simplifyJoin[H[_]: Functor](GtoH: G ~> H): CopK[F ::: LL, T[H]] => H[T[H]] = {
            case I(fa) => S.simplifyJoin(GtoH).apply(fa)
            case other => LL.materialize(offset + 1).simplifyJoin(GtoH).apply(other.asInstanceOf[CopK[LL, T[H]]])
          }
        }
      }
    }
  }

  def default[T[_[_]], F[_], I[a] <: ACopK[a]](implicit F: F :<<: I)
      : SimplifyJoin.Aux[T, F, I] =
    new SimplifyJoin[F] {
      type IT[F[_]] = T[F]
      type G[A] = I[A]

      def simplifyJoin[H[_]: Functor](GtoH: G ~> H): F[T[H]] => H[T[H]] =
        fa => GtoH(F.inj(fa))
    }

  implicit def deadEnd[T[_[_]], F[a] <: ACopK[a]](implicit DE: Const[DeadEnd, ?] :<<: F)
      : SimplifyJoin.Aux[T, Const[DeadEnd, ?], F] =
    default

  implicit def read[T[_[_]], F[a] <: ACopK[a], A](implicit R: Const[Read[A], ?] :<<: F)
      : SimplifyJoin.Aux[T, Const[Read[A], ?], F] =
    default

  implicit def shiftedRead[T[_[_]], F[a] <: ACopK[a], A]
    (implicit SR: Const[ShiftedRead[A], ?] :<<: F)
      : SimplifyJoin.Aux[T, Const[ShiftedRead[A], ?], F] =
    default

  implicit def projectBucket[T[_[_]], F[a] <: ACopK[a]]
    (implicit PB: ProjectBucket[T, ?] :<<: F)
      : SimplifyJoin.Aux[T, ProjectBucket[T, ?], F] =
    default
}
