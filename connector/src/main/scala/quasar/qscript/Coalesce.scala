/*
 * Copyright 2014–2017 SlamData Inc.
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
import quasar.contrib.pathy.{ADir, AFile}
import quasar.fp._
import quasar.fp.ski._
import quasar.qscript.MapFunc._
import quasar.qscript.MapFuncs._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

/** Rewrites adjacent nodes. */
trait Coalesce[IN[_]] {
  type IT[F[_]]
  type OUT[A]

  /** Coalesce for types containing QScriptCore. */
  def coalesceQC[F[_]: Functor]
    (FToOut: PrismNT[F, OUT])
    (implicit QC: QScriptCore[IT, ?] :<: OUT)
      : IN[IT[F]] => Option[IN[IT[F]]]

  /** Coalesce for types containing ShiftedRead. */
  def coalesceSR[F[_]: Functor, A]
    (FToOut: PrismNT[F, OUT])
    (implicit QC: QScriptCore[IT, ?] :<: OUT, SR: Const[ShiftedRead[A], ?] :<: OUT)
      : IN[IT[F]] => Option[IN[IT[F]]]

  /** Coalesce for types containing EquiJoin. */
  def coalesceEJ[F[_]: Functor]
    (FToOut: F ~> λ[α => Option[OUT[α]]])
    (implicit EJ: EquiJoin[IT, ?] :<: OUT)
      : IN[IT[F]] => Option[OUT[IT[F]]]

  /** Coalesce for types containing ThetaJoin. */
  def coalesceTJ[F[_]: Functor]
    (FToOut: F ~> λ[α => Option[OUT[α]]])
    (implicit TJ: ThetaJoin[IT, ?] :<: OUT)
      : IN[IT[F]] => Option[OUT[IT[F]]]
}

trait CoalesceInstances {
  def coalesce[T[_[_]]: BirecursiveT: EqualT: ShowT] = new CoalesceT[T]

  implicit def qscriptCore[T[_[_]]: BirecursiveT: EqualT: ShowT, G[_]]
    (implicit QC: QScriptCore[T, ?] :<: G)
      : Coalesce.Aux[T, QScriptCore[T, ?], G] =
    coalesce[T].qscriptCore[G]

  implicit def projectBucket[T[_[_]]: BirecursiveT: EqualT: ShowT, F[_]]
      : Coalesce.Aux[T, ProjectBucket[T, ?], F] =
    coalesce[T].projectBucket[F]

  implicit def thetaJoin[T[_[_]]: BirecursiveT: EqualT: ShowT, G[_]]
    (implicit TJ: ThetaJoin[T, ?] :<: G)
      : Coalesce.Aux[T, ThetaJoin[T, ?], G] =
    coalesce[T].thetaJoin[G]

  implicit def equiJoin[T[_[_]]: BirecursiveT: EqualT: ShowT, G[_]]
    (implicit EJ: EquiJoin[T, ?] :<: G)
      : Coalesce.Aux[T, EquiJoin[T, ?], G] =
    coalesce[T].equiJoin[G]

  implicit def coproduct[T[_[_]], F[_], G[_], H[_]]
    (implicit F: Coalesce.Aux[T, F, H], G: Coalesce.Aux[T, G, H])
      : Coalesce.Aux[T, Coproduct[F, G, ?], H] =
    new Coalesce[Coproduct[F, G, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = H[A]

      def coalesceQC[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore[IT, ?] :<: OUT) =
        _.run.bitraverse(F.coalesceQC(FToOut), G.coalesceQC(FToOut)) ∘ (Coproduct(_))

      def coalesceSR[F[_]: Functor, A]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore[IT, ?] :<: OUT, SR: Const[ShiftedRead[A], ?] :<: OUT) =
        _.run.bitraverse(F.coalesceSR(FToOut), G.coalesceSR(FToOut)) ∘ (Coproduct(_))

      def coalesceEJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit EJ: EquiJoin[IT, ?] :<: OUT) =
        _.run.fold(F.coalesceEJ(FToOut), G.coalesceEJ(FToOut))

      def coalesceTJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit TJ: ThetaJoin[IT, ?] :<: OUT) =
        _.run.fold(F.coalesceTJ(FToOut), G.coalesceTJ(FToOut))
    }

  def default[T[_[_]], IN[_], G[_]]: Coalesce.Aux[T, IN, G] =
    new Coalesce[IN] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      def coalesceQC[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore[IT, ?] :<: OUT) =
        κ(None)

      def coalesceSR[F[_]: Functor, A]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore[IT, ?] :<: OUT, SR: Const[ShiftedRead[A], ?] :<: OUT) =
        κ(None)

      def coalesceEJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit EJ: EquiJoin[IT, ?] :<: OUT) =
        κ(None)

      def coalesceTJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit TJ: ThetaJoin[IT, ?] :<: OUT) =
        κ(None)
    }

  implicit def deadEnd[T[_[_]], OUT[_]]: Coalesce.Aux[T, Const[DeadEnd, ?], OUT] =
    default

  implicit def read[T[_[_]], OUT[_], A]: Coalesce.Aux[T, Const[Read[A], ?], OUT] =
    default

  implicit def shiftedRead[T[_[_]], OUT[_], A]: Coalesce.Aux[T, Const[ShiftedRead[A], ?], OUT] =
    default
}

class CoalesceT[T[_[_]]: BirecursiveT: EqualT: ShowT] extends TTypes[T] {
  private def CoalesceTotal = Coalesce[T, QScriptTotal, QScriptTotal]

  private type QST = QScriptTotal[T[CoEnv[Hole, QScriptTotal, ?]]]
  private type CoEnvQST[A] = CoEnv[Hole, QScriptTotal, A]

  private def freeTotal(branch: FreeQS)(coalesce: QST => Option[QST]): FreeQS =
    branch
      .convertTo[T[CoEnv[Hole, QScriptTotal, ?]]]
      .cata((co: CoEnv[Hole, QScriptTotal, T[CoEnv[Hole, QScriptTotal, ?]]]) =>
        co.run.fold(
          κ(co),
          in => CoEnv[Hole, QScriptTotal, T[CoEnv[Hole, QScriptTotal, ?]]](repeatedly(coalesce)(in).right)).embed)
      .convertTo[FreeQS]

  private def freeQC(branch: FreeQS): FreeQS =
    freeTotal(branch)(CoalesceTotal.coalesceQC(coenvPrism[QScriptTotal, Hole]))

  private def freeSR(branch: FreeQS): FreeQS = {
    def freeSR0[A](b: FreeQS)(implicit SR: Const[ShiftedRead[A], ?] :<: QScriptTotal): FreeQS =
      freeTotal(b)(CoalesceTotal.coalesceSR[CoEnv[Hole, QScriptTotal, ?], A](coenvPrism[QScriptTotal, Hole]))

    freeSR0[AFile](freeSR0[ADir](branch))
  }

  private def freeEJ(branch: FreeQS): FreeQS =
    freeTotal(branch)(CoalesceTotal.coalesceEJ(coenvPrism[QScriptTotal, Hole].get))

  private def freeTJ(branch: FreeQS): FreeQS =
    freeTotal(branch)(CoalesceTotal.coalesceTJ(coenvPrism[QScriptTotal, Hole].get))

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

  private def eliminateRightSideProj(elem: FreeMap): Option[FreeMap] = {
    val oneRef = Free.roll[MapFunc, Hole](ProjectIndex(HoleF, IntLit(1)))
    val rightCount: Int = elem.elgotPara(count(HoleF))

    // all `RightSide` access is through `oneRef`
    (elem.elgotPara(count(oneRef)) ≟ rightCount).option(
      elem.transApoT(substitute(oneRef, HoleF)))
  }

  def qscriptCore[G[_]](implicit QC: QScriptCore :<: G): Coalesce.Aux[T, QScriptCore, G] =
    new Coalesce[QScriptCore] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      // TODO: I feel like this must be some standard fold.
      def sequenceReduce(rf: ReduceFunc[(IdStatus, JoinFunc)])
          : Option[(IdStatus, ReduceFunc[JoinFunc])] =
        rf match {
          case ReduceFuncs.Count(a)           => (a._1, ReduceFuncs.Count(a._2)).some
          case ReduceFuncs.Sum(a)             => (a._1, ReduceFuncs.Sum(a._2)).some
          case ReduceFuncs.Min(a)             => (a._1, ReduceFuncs.Min(a._2)).some
          case ReduceFuncs.Max(a)             => (a._1, ReduceFuncs.Max(a._2)).some
          case ReduceFuncs.Avg(a)             => (a._1, ReduceFuncs.Avg(a._2)).some
          case ReduceFuncs.Arbitrary(a)       => (a._1, ReduceFuncs.Arbitrary(a._2)).some
          case ReduceFuncs.First(a)           => (a._1, ReduceFuncs.First(a._2)).some
          case ReduceFuncs.Last(a)            => (a._1, ReduceFuncs.Last(a._2)).some
          case ReduceFuncs.UnshiftArray(a)    => (a._1, ReduceFuncs.UnshiftArray(a._2)).some
          case ReduceFuncs.UnshiftMap(a1, a2) =>
            (a1._1 ≟ a2._1).option((a1._1, ReduceFuncs.UnshiftMap(a1._2, a2._2)))
        }

      def rightOnly(replacement: FreeMap): JoinFunc => Option[FreeMap] =
        _.traverseM[Option, Hole] {
          case LeftSide  => None
          case RightSide => replacement.some
        }

      val nm = new NormalizableT[T]

      val rewrite = new Rewrite[T]

      def coalesceQC[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore :<: OUT) = {
        case Map(Embed(src), mf) => FToOut.get(src) >>= QC.prj >>= (s =>
          if (mf.length ≟ 0 && (s match { case Unreferenced() => false; case _ => true }))
            Map(
              FToOut.reverseGet(QC.inj(Unreferenced[T, T[F]]())).embed,
              mf).some
          else s match {
            case Map(srcInner, mfInner) =>
              Map(srcInner, nm.freeMF(mf >> mfInner)).some
            case LeftShift(srcInner, struct, id, repair) =>
              LeftShift(srcInner, struct, id, nm.freeMF(mf >> repair)).some
            case Reduce(srcInner, bucket, funcs, repair) =>
              Reduce(srcInner, bucket, funcs, nm.freeMF(mf >> repair)).some
            case Subset(innerSrc, lb, sel, rb) =>
              Subset(innerSrc,
                Free.roll(Inject[QScriptCore, QScriptTotal].inj(Map(lb, mf))),
                sel,
                rb).some
            case Filter(Embed(innerSrc), cond) => FToOut.get(innerSrc) >>= QC.prj >>= {
              case Map(doubleInner, mfInner) =>
                Map(
                  FToOut.reverseGet(QC.inj(Filter(
                    doubleInner,
                    nm.freeMF(cond >> mfInner)))).embed,
                  nm.freeMF(mf >> mfInner)).some
              case _ => None
            }
            case Sort(Embed(innerSrc), buckets, ordering) => FToOut.get(innerSrc) >>= QC.prj >>= {
              case Map(doubleInner, mfInner) =>
                Map(
                  FToOut.reverseGet(QC.inj(Sort(
                    doubleInner,
                    nm.freeMF(buckets >> mfInner),
                    ordering ∘ (_.leftMap(o => nm.freeMF(o >> mfInner)))))).embed,
                  nm.freeMF(mf >> mfInner)).some
              case _ => None
            }
            case _ => None
          })
        case LeftShift(Embed(src), struct, id, shiftRepair) =>
          FToOut.get(src) >>= QC.prj >>= {
            case LeftShift(innerSrc, innerStruct, innerId, innerRepair)
                if !shiftRepair.element(LeftSide) && struct ≠ HoleF =>
              LeftShift(
                FToOut.reverseGet(QC.inj(LeftShift(
                  innerSrc,
                  innerStruct,
                  innerId,
                  nm.freeMF(struct >> innerRepair)))).embed,
                HoleF,
                id,
                shiftRepair).some
            case Map(innerSrc, mf) if !shiftRepair.element(LeftSide) =>
              LeftShift(innerSrc, nm.freeMF(struct >> mf), id, shiftRepair).some
            case Reduce(srcInner, _, List(ReduceFuncs.UnshiftArray(elem)), redRepair)
                if nm.freeMF(struct >> redRepair) ≟ Free.point(ReduceIndex(0.some)) =>
              rightOnly(elem)(shiftRepair) ∘ (Map(srcInner, _))
            case Reduce(srcInner, _, List(ReduceFuncs.UnshiftMap(k, elem)), redRepair)
                if nm.freeMF(struct >> redRepair) ≟ Free.point(ReduceIndex(0.some)) =>
              rightOnly(id match {
                case IncludeId =>
                  Free.roll(ConcatArrays[T, FreeMap](
                    Free.roll(MakeArray(k)),
                    Free.roll(MakeArray(elem))))
                case _ => elem
              })(shiftRepair) ∘ (Map(srcInner, _))
            case _ => None
          }
        case Reduce(Embed(src), bucket, reducers, redRepair) =>
          FToOut.get(src) >>= QC.prj >>= {
            case LeftShift(innerSrc, struct, id, shiftRepair)
                if shiftRepair =/= RightSideF =>
              (rightOnly(HoleF)(nm.freeMF(bucket >> shiftRepair)) ⊛
                reducers.traverse(_.traverse(mf => rightOnly(HoleF)(nm.freeMF(mf >> shiftRepair)))))((sb, sr) =>
                Reduce(
                  FToOut.reverseGet(QC.inj(LeftShift(innerSrc, struct, id, RightSideF))).embed,
                  sb,
                  sr,
                  redRepair))
            case LeftShift(innerSrc, struct, id, shiftRepair) =>
              (rewrite.rewriteShift(id, nm.freeMF(bucket >> shiftRepair)) ⊛
                reducers.traverse(_.traverse(mf =>
                  rewrite.rewriteShift(id, nm.freeMF(mf >> shiftRepair)))))((b, r) =>
                r.foldRightM[Option, (IdStatus, (JoinFunc, List[ReduceFunc[JoinFunc]]))]((b._1, (b._2, Nil)))((elem, acc) => {
                  sequenceReduce(elem) >>= (e =>
                    (e._1 ≟ acc._1).option(
                      (acc._1, (acc._2._1, e._2 :: acc._2._2))))
                })).join >>= {
                case (newId, (bucket, reducers)) =>
                  (rightOnly(HoleF)(bucket) ⊛
                    (reducers.traverse(_.traverse(rightOnly(HoleF)))))((sb, sr) =>
                    Reduce(
                      FToOut.reverseGet(QC.inj(LeftShift(innerSrc, struct, newId, RightSideF))).embed,
                      sb,
                      sr,
                      redRepair))
              }
            case Map(innerSrc, mf) =>
              Reduce(
                innerSrc,
                nm.freeMF(bucket >> mf),
                reducers.map(_.map(red => nm.freeMF(red >> mf))),
                redRepair).some
            case _ => None
          }
        case Filter(Embed(src), cond) => FToOut.get(src) >>= QC.prj >>= {
          case Filter(srcInner, condInner) =>
            Filter(srcInner, Free.roll[MapFunc, Hole](And(condInner, cond))).some
          case _ => None
        }
        case Subset(src, from, sel, count) =>
          makeBranched(from, count)(ifNeq(freeQC))(Subset(src, _, sel, _))
        case Union(src, from, count) =>
          makeBranched(from, count)(ifNeq(freeQC))(Union(src, _, _))
        case _ => None
      }

      def coalesceSR[F[_]: Functor, A]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore :<: OUT, SR: Const[ShiftedRead[A], ?] :<: OUT) = {
        case Map(Embed(src), mf) =>
          ((FToOut.get(src) >>= SR.prj) ⊛ eliminateRightSideProj(mf))((const, newMF) =>
            Map(
              FToOut.reverseGet(SR.inj(Const[ShiftedRead[A], T[F]](ShiftedRead(const.getConst.path, ExcludeId)))).embed,
              newMF)) <+>
          (((FToOut.get(src) >>= QC.prj) match {
            case Some(Filter(Embed(innerSrc), cond)) =>
              ((FToOut.get(innerSrc) >>= SR.prj) ⊛ eliminateRightSideProj(cond))((const, newCond) =>
                Filter(
                  FToOut.reverseGet(SR.inj(Const[ShiftedRead[A], T[F]](ShiftedRead(const.getConst.path, ExcludeId)))).embed,
                  newCond))
            case _ => None
          }) ⊛ eliminateRightSideProj(mf))((newFilter, newMF) =>
            Map(
              FToOut.reverseGet(QC.inj(newFilter)).embed,
              newMF))
        case Reduce(Embed(src), bucket, reducers, repair) =>
          ((FToOut.get(src) >>= SR.prj) ⊛ eliminateRightSideProj(bucket) ⊛ reducers.traverse(_.traverse(eliminateRightSideProj)))(
            (const, newBuck, newRed) =>
            Reduce(
              FToOut.reverseGet(SR.inj(Const[ShiftedRead[A], T[F]](ShiftedRead(const.getConst.path, ExcludeId)))).embed,
              newBuck,
              newRed,
              repair))
        case Subset(src, from, sel, count) =>
          makeBranched(from, count)(ifNeq(freeSR))(Subset(src, _, sel, _))
        case Union(src, from, count) =>
          makeBranched(from, count)(ifNeq(freeSR))(Union(src, _, _))
        case _ => None
      }

      def coalesceEJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit EJ: EquiJoin :<: OUT) = {
        case Map(Embed(src), mf) =>
          (FToOut(src) >>= EJ.prj).map(
            ej => EJ.inj(EquiJoin.combine.modify(mf >> (_: JoinFunc))(ej)))
        case Subset(src, from, sel, count) =>
          makeBranched(from, count)(ifNeq(freeEJ))((l, r) => QC.inj(Subset(src, l, sel, r)))
        case Union(src, from, count) =>
          makeBranched(from, count)(ifNeq(freeEJ))((l, r) => QC.inj(Union(src, l, r)))
        case _ => None
      }

      def coalesceTJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit TJ: ThetaJoin :<: OUT) = {
        case Map(Embed(src), mf) =>
          (FToOut(src) >>= TJ.prj).map(
            tj => TJ.inj(ThetaJoin.combine.modify(mf >> (_: JoinFunc))(tj)))
        case Subset(src, from, sel, count) =>
          makeBranched(from, count)(ifNeq(freeTJ))((l, r) => QC.inj(Subset(src, l, sel, r)))
        case Union(src, from, count) =>
          makeBranched(from, count)(ifNeq(freeTJ))((l, r) => QC.inj(Union(src, l, r)))
        case _ => None
      }
    }

  def projectBucket[F[_]]: Coalesce.Aux[T, ProjectBucket, F] =
    new Coalesce[ProjectBucket] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def coalesceQC[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore :<: OUT) = {
        case BucketField(Embed(src), value, field) => FToOut.get(src) >>= QC.prj >>= {
          case Map(srcInner, mf) =>
            BucketField(srcInner, value >> mf, field >> mf).some
          case _ => None
        }
        case BucketIndex(Embed(src), value, index) => FToOut.get(src) >>= QC.prj >>= {
          case Map(srcInner, mf) =>
            BucketIndex(srcInner, value >> mf, index >> mf).some
          case _ => None
        }
      }

      def coalesceSR[F[_]: Functor, A]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore :<: OUT, SR: Const[ShiftedRead[A], ?] :<: OUT) =
        κ(None)

      def coalesceEJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit EJ: EquiJoin :<: OUT) =
        κ(None)

      def coalesceTJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit TJ: ThetaJoin :<: OUT) =
        κ(None)
    }

  def thetaJoin[G[_]](implicit TJ: ThetaJoin :<: G): Coalesce.Aux[T, ThetaJoin, G] =
    new Coalesce[ThetaJoin] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      def coalesceQC[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore :<: OUT) =
        tj => makeBranched(
          tj.lBranch, tj.rBranch)(
          ifNeq(freeQC))(
          ThetaJoin(tj.src, _, _, tj.on, tj.f, tj.combine))

      def coalesceSR[F[_]: Functor, A]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore :<: OUT, SR: Const[ShiftedRead[A], ?] :<: OUT) =
        tj => makeBranched(
          tj.lBranch, tj.rBranch)(
          ifNeq(freeSR))(
          ThetaJoin(tj.src, _, _, tj.on, tj.f, tj.combine))

      def coalesceEJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit EJ: EquiJoin :<: OUT) =
        tj => makeBranched(
          tj.lBranch, tj.rBranch)(
          ifNeq(freeEJ))((l, r) =>
          TJ.inj(ThetaJoin(tj.src, l, r, tj.on, tj.f, tj.combine)))

      def coalesceTJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit TJ: ThetaJoin :<: OUT) =
        tj => makeBranched(
          tj.lBranch, tj.rBranch)(
          ifNeq(freeTJ))((l, r) =>
          TJ.inj(ThetaJoin(tj.src, l, r, tj.on, tj.f, tj.combine)))
    }

  def equiJoin[G[_]](implicit EJ: EquiJoin :<: G): Coalesce.Aux[T, EquiJoin, G] =
    new Coalesce[EquiJoin] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      def coalesceQC[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore :<: OUT) =
        (ej: EquiJoin[IT[F]]) => {
          val branched: Option[EquiJoin[T[F]]] = makeBranched(
            ej.lBranch, ej.rBranch)(
            ifNeq(freeQC))(
            EquiJoin(ej.src, _, _, ej.lKey, ej.rKey, ej.f, ej.combine))

          val qct = Inject[QScriptCore, QScriptTotal]

          def coalesceBranchMaps(ej: EquiJoin[T[F]]): Option[EquiJoin[T[F]]] =
            (ej.lBranch.project.run.map(qct.prj), ej.rBranch.project.run.map(qct.prj)) match {
              case (\/-(Some(Map(innerLSrc, lmf))), \/-(Some(Map(innerRSrc, rmf)))) =>
                val newLKey = ej.lKey >> lmf
                val newRKey = ej.rKey >> rmf
                val newCombine = ej.combine >>= {
                  case LeftSide  => lmf.as[JoinSide](LeftSide)
                  case RightSide => rmf.as[JoinSide](RightSide)
                }
                EquiJoin(ej.src, innerLSrc, innerRSrc, newLKey, newRKey, ej.f, newCombine).some
              case _ => none
            }

          coalesceBranchMaps(branched.getOrElse(ej)) orElse branched
        }

      def coalesceSR[F[_]: Functor, A]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore :<: OUT, SR: Const[ShiftedRead[A], ?] :<: OUT) =
        ej => makeBranched(
          ej.lBranch, ej.rBranch)(
          ifNeq(freeSR))(
          EquiJoin(ej.src, _, _, ej.lKey, ej.rKey, ej.f, ej.combine))

      def coalesceEJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit EJ: EquiJoin :<: OUT) =
        ej => makeBranched(
          ej.lBranch, ej.rBranch)(
          ifNeq(freeEJ))((l, r) =>
          EJ.inj(EquiJoin(ej.src, l, r, ej.lKey, ej.rKey, ej.f, ej.combine)))

      def coalesceTJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit TJ: ThetaJoin :<: OUT) =
        ej => makeBranched(
          ej.lBranch, ej.rBranch)(
          ifNeq(freeTJ))((l, r) =>
          EJ.inj(EquiJoin(ej.src, l, r, ej.lKey, ej.rKey, ej.f, ej.combine)))
    }
}

object Coalesce extends CoalesceInstances {
  type Aux[T[_[_]], IN[_], F[_]] = Coalesce[IN] {
    type IT[F[_]] = T[F]
    type OUT[A] = F[A]
  }

  def apply[T[_[_]], IN[_], OUT[_]](implicit ev: Coalesce.Aux[T, IN, OUT]) = ev
}
