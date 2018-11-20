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

import slamdata.Predef.{Map => _, _}
import quasar.RenderTreeT
import quasar.contrib.matryoshka._
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski._
import quasar.qscript._
import quasar.qscript.RecFreeS._
import quasar.qscript.MapFuncCore._
import quasar.qscript.MapFuncsCore._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::

/** Rewrites adjacent nodes. */
trait Coalesce[IN[_]] {
  type IT[F[_]]
  type OUT[A] <: ACopK[A]

  /** Coalesce for types containing QScriptCore. */
  protected[qscript] def coalesceQC[F[_]: Functor]
    (FToOut: PrismNT[F, OUT])
    (implicit QC: QScriptCore[IT, ?] :<<: OUT)
      : IN[IT[F]] => Option[IN[IT[F]]]

  def coalesceQCNormalize[F[_]: Functor]
    (FToOut: PrismNT[F, OUT])
    (implicit QC: QScriptCore[IT, ?] :<<: OUT, N: Normalizable[IN])
      : IN[IT[F]] => Option[IN[IT[F]]] =
    applyTransforms(coalesceQC[F](FToOut), N.normalizeF(_: IN[IT[F]]))
}

trait CoalesceInstances {
  def coalesce[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = new CoalesceT[T]

  implicit def qscriptCore[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, G[a] <: ACopK[a]]
    (implicit QC: QScriptCore[T, ?] :<<: G)
      : Coalesce.Aux[T, QScriptCore[T, ?], G] =
    coalesce[T].qscriptCore[G]

  implicit def thetaJoin[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, G[a] <: ACopK[a]]
    (implicit TJ: ThetaJoin[T, ?] :<<: G)
      : Coalesce.Aux[T, ThetaJoin[T, ?], G] =
    coalesce[T].thetaJoin[G]

  implicit def equiJoin[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, G[a] <: ACopK[a]]
    (implicit EJ: EquiJoin[T, ?] :<<: G)
      : Coalesce.Aux[T, EquiJoin[T, ?], G] =
    coalesce[T].equiJoin[G]

  implicit def copk[T[_[_]], LL <: TListK, I[a] <: ACopK[a]](implicit M: Materializer[T, LL, I]): Coalesce.Aux[T, CopK[LL, ?], I] =
    M.materialize(offset = 0)

  sealed trait Materializer[T[_[_]], LL <: TListK, I[a] <: ACopK[a]] {
    def materialize(offset: Int): Coalesce.Aux[T, CopK[LL, ?], I]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[T[_[_]], C[_], I[a] <: ACopK[a]](
      implicit
      C: Coalesce.Aux[T, C, I]
    ): Materializer[T, C ::: TNilK, I] = new Materializer[T, C ::: TNilK, I] {
      override def materialize(offset: Int): Coalesce.Aux[T, CopK[C ::: TNilK, ?], I] = {
        val I = mkInject[C, C ::: TNilK](offset)
        new Coalesce[CopK[C ::: TNilK, ?]] {
          type IT[F[_]] = T[F]
          type OUT[A] = I[A]

          def coalesceQC[F[_]: Functor](FToOut: PrismNT[F, OUT])(implicit QC: QScriptCore[IT, ?] :<<: OUT) = {
            case I(ca) => C.coalesceQC(FToOut).apply(ca).map(I(_))
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[T[_[_]], C[_], LL <: TListK, I[a] <: ACopK[a]](
      implicit
      C: Coalesce.Aux[T, C, I],
      LL: Materializer[T, LL, I]
    ): Materializer[T, C ::: LL, I] = new Materializer[T, C ::: LL, I] {
      override def materialize(offset: Int): Coalesce.Aux[T, CopK[C ::: LL, ?], I] = {
        val I = mkInject[C, C ::: LL](offset)
        new Coalesce[CopK[C ::: LL, ?]] {
          type IT[F[_]] = T[F]
          type OUT[A] = I[A]

          def coalesceQC[F[_]: Functor](FToOut: PrismNT[F, OUT])(implicit QC: QScriptCore[IT, ?] :<<: OUT) = {
            case I(ca) => C.coalesceQC(FToOut).apply(ca).map(I(_))
            case other => LL.materialize(offset + 1).coalesceQC(FToOut)
              .apply(other.asInstanceOf[CopK[LL, T[F]]]).asInstanceOf[Option[CopK[C ::: LL, T[F]]]]
          }
        }
      }
    }
  }

  def default[T[_[_]], IN[_], G[a] <: ACopK[a]]: Coalesce.Aux[T, IN, G] =
    new Coalesce[IN] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      def coalesceQC[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore[IT, ?] :<<: OUT) =
        κ(None)
    }

  implicit def read[T[_[_]], OUT[a] <: ACopK[a], A]: Coalesce.Aux[T, Const[Read[A], ?], OUT] =
    default

  implicit def interpretedRead[T[_[_]], OUT[a] <: ACopK[a], A]: Coalesce.Aux[T, Const[InterpretedRead[A], ?], OUT] =
    default
}

class CoalesceT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] extends TTypes[T] {
  private def CoalesceTotal = Coalesce[T, QScriptTotal, QScriptTotal]

  private def freeTotal(branch: FreeQS)(
    coalesce: QScriptTotal[T[CoEnvQS]] => Option[QScriptTotal[T[CoEnvQS]]]):
      FreeQS = {
    val modify: T[CoEnvQS] => T[CoEnvQS] =
      _.cata((co: CoEnvQS[T[CoEnvQS]]) =>
        co.run.fold(
          κ(co),
          in => CoEnv[Hole, QScriptTotal, T[CoEnvQS]](
            repeatedly(coalesce)(in).right)).embed)

    applyCoEnvFrom[T, QScriptTotal, Hole](modify).apply(branch)
  }

  private def freeQC(branch: FreeQS): FreeQS =
    freeTotal(branch)(CoalesceTotal.coalesceQCNormalize(coenvPrism[QScriptTotal, Hole]))

  private def ifNeq(f: FreeQS => FreeQS): FreeQS => Option[FreeQS] =
    branch => {
      val coalesced = f(branch)
      (branch ≠ coalesced).option(coalesced)
    }

  private def makeBranched[A, B]
    (lOrig: A, rOrig: A)
    (op: A => Option[A])
    (f: (A, A) => B)
      : Option[B] =
    (op(lOrig), op(rOrig)) match {
      case (None, None) => None
      case (l,    r)    => f(l.getOrElse(lOrig), r.getOrElse(rOrig)).some
    }

  def qscriptCore[G[a] <: ACopK[a]](implicit QC: QScriptCore :<<: G): Coalesce.Aux[T, QScriptCore, G] =
    new Coalesce[QScriptCore] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      def fmIsCondUndef(jf: JoinFunc): Boolean = {
        jf.resumeTwice.fold({
          case MFC(MapFuncsCore.Cond(_, _, -\/(MFC(MapFuncsCore.Undefined())))) => true
          case _ => false
        }, _ => false)
      }

      def coalesceQC[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore :<<: OUT) = {
        case Map(Embed(src), mf) => FToOut.get(src) >>= QC.prj.apply >>= (s =>
          if (mf.length ≟ 0 && (s match { case Unreferenced() => false; case _ => true }))
            Map(
              FToOut.reverseGet(QC.inj(Unreferenced[T, T[F]]())).embed,
              mf).some
          else s match {
            case Map(srcInner, mfInner) =>
              Map(srcInner, mf >> mfInner).some
            case LeftShift(srcInner, struct, id, stpe, undef, repair) =>
              LeftShift(srcInner, struct, id, stpe, undef, mf.linearize >> repair).some
            case Reduce(srcInner, bucket, funcs, repair) =>
              Reduce(srcInner, bucket, funcs, mf.linearize >> repair).some
            case Subset(innerSrc, lb, sel, rb) =>
              Subset(innerSrc,
                Free.roll(CopK.Inject[QScriptCore, QScriptTotal].inj(Map(lb, mf))),
                sel,
                rb).some
            case Filter(Embed(innerSrc), cond) => FToOut.get(innerSrc) >>= QC.prj.apply >>= {
              case Map(doubleInner, mfInner) =>
                Map(
                  FToOut.reverseGet(QC.inj(Filter(
                    doubleInner,
                    cond >> mfInner))).embed,
                  mf >> mfInner).some
              case _ => None
            }
            case Sort(Embed(innerSrc), buckets, ordering) => FToOut.get(innerSrc) >>= QC.prj.apply >>= {
              case Map(doubleInner, mfInner) =>
                Map(
                  FToOut.reverseGet(QC.inj(Sort(
                    doubleInner,
                    buckets ∘ (_ >> mfInner.linearize),
                    ordering ∘ (_.leftMap(_ >> mfInner.linearize))))).embed,
                  mf >> mfInner).some
              case _ => None
            }
            case _ => None
          })
        case LeftShift(Embed(src), struct, id, stpe, undef, shiftRepair) =>
          FToOut.get(src) >>= QC.prj.apply >>= {
            case LeftShift(innerSrc, innerStruct, innerId, innerStpe, innerUndef, innerRepair)
                if !shiftRepair.element(LeftSide) && !fmIsCondUndef(shiftRepair) && struct ≠ HoleR =>
              LeftShift(
                FToOut.reverseGet(QC.inj(LeftShift(
                  innerSrc,
                  innerStruct,
                  innerId,
                  innerStpe,
                  innerUndef,
                  struct.linearize >> innerRepair))).embed,
                HoleR,
                id,
                stpe,
                OnUndefined.omit,
                shiftRepair).some
            // TODO: Should be able to apply this where there _is_ a `LeftSide`
            //       reference, but currently that breaks merging.
            case Map(innerSrc, mf) if !shiftRepair.element(LeftSide) =>
              LeftShift(innerSrc, struct >> mf, id, stpe, OnUndefined.omit, shiftRepair).some
            case _ => None
          }
        case Filter(Embed(src), cond) => FToOut.get(src) >>= QC.prj.apply >>= {
          case Filter(srcInner, condInner) =>
            Filter(srcInner, RecFreeS.roll[MapFunc, Hole](MFC(And(condInner, cond)))).some
          case _ => None
        }
        case Subset(src, from, sel, count) =>
          makeBranched(from, count)(ifNeq(freeQC))(Subset(src, _, sel, _))
        case Union(src, from, count) =>
          makeBranched(from, count)(ifNeq(freeQC))(Union(src, _, _))
        case _ => None
      }
    }

  def thetaJoin[G[a] <: ACopK[a]](implicit TJ: ThetaJoin :<<: G): Coalesce.Aux[T, ThetaJoin, G] =
    new Coalesce[ThetaJoin] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      def coalesceQC[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore :<<: OUT) =
        tj => makeBranched(
          tj.lBranch, tj.rBranch)(
          ifNeq(freeQC))(
          ThetaJoin(tj.src, _, _, tj.on, tj.f, tj.combine))
    }

  def equiJoin[G[a] <: ACopK[a]](implicit EJ: EquiJoin :<<: G): Coalesce.Aux[T, EquiJoin, G] =
    new Coalesce[EquiJoin] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      def coalesceQC[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore :<<: OUT) =
        (ej: EquiJoin[IT[F]]) => {
          val branched: Option[EquiJoin[T[F]]] = makeBranched(
            ej.lBranch, ej.rBranch)(
            ifNeq(freeQC))(
            EquiJoin(ej.src, _, _, ej.key, ej.f, ej.combine))

          val qct = CopK.Inject[QScriptCore, QScriptTotal]

          def coalesceBranchMaps(ej: EquiJoin[T[F]]): Option[EquiJoin[T[F]]] = {
            def coalesceSide(branch: FreeQS, key: List[FreeMap], side: JoinSide):
                Option[(FreeQS, List[FreeMap], JoinFunc)] =
              branch.project.run.map(qct.prj.apply) match {
                case \/-(Some(Map(innerSrc, mf))) => (innerSrc, key ∘ (_ >> mf.linearize), mf.linearize.as(side)).some
                case _ => none
              }

            (coalesceSide(ej.lBranch, ej.key ∘ (_._1), LeftSide) |@| coalesceSide(ej.rBranch, ej.key ∘ (_._2), RightSide)) {
              case ((lSrc, lKey, lComb), (rSrc, rKey, rComb)) =>
                val combine = ej.combine >>= {
                  case LeftSide => lComb
                  case RightSide => rComb
                }
                EquiJoin(ej.src, lSrc, rSrc, lKey zip rKey, ej.f, combine)
            }
          }
          coalesceBranchMaps(branched.getOrElse(ej)) orElse branched
        }
    }
}

object Coalesce extends CoalesceInstances {
  type Aux[T[_[_]], IN[_], F[_]] = Coalesce[IN] {
    type IT[F[_]] = T[F]
    type OUT[A] = F[A]
  }

  def apply[T[_[_]], IN[_], OUT[_]](implicit ev: Coalesce.Aux[T, IN, OUT]) = ev
}
