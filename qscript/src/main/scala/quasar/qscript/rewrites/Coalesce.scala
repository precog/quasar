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
import quasar.contrib.pathy.{ADir, AFile}
import quasar.contrib.matryoshka._
import quasar.ejson.implicits._
import quasar.fp._
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

/** Rewrites adjacent nodes. */
trait Coalesce[IN[_]] {
  type IT[F[_]]
  type OUT[A]

  /** Coalesce for types containing QScriptCore. */
  protected[qscript] def coalesceQC[F[_]: Functor]
    (FToOut: PrismNT[F, OUT])
    (implicit QC: QScriptCore[IT, ?] :<: OUT)
      : IN[IT[F]] => Option[IN[IT[F]]]

  def coalesceQCNormalize[F[_]: Functor]
    (FToOut: PrismNT[F, OUT])
    (implicit QC: QScriptCore[IT, ?] :<: OUT, N: Normalizable[IN])
      : IN[IT[F]] => Option[IN[IT[F]]] =
    applyTransforms(coalesceQC[F](FToOut), N.normalizeF(_: IN[IT[F]]))

  /** Coalesce for types containing ShiftedRead. */
  protected[qscript] def coalesceSR[F[_]: Functor, A]
    (FToOut: PrismNT[F, OUT])
    (implicit QC: QScriptCore[IT, ?] :<: OUT, SR: Const[ShiftedRead[A], ?] :<: OUT)
      : IN[IT[F]] => Option[IN[IT[F]]]

  def coalesceSRNormalize[F[_]: Functor, A]
    (FToOut: PrismNT[F, OUT])
    (implicit QC: QScriptCore[IT, ?] :<: OUT, SR: Const[ShiftedRead[A], ?] :<: OUT, N: Normalizable[IN])
      : IN[IT[F]] => Option[IN[IT[F]]] =
    applyTransforms(coalesceSR[F, A](FToOut), N.normalizeF(_: IN[IT[F]]))

  /** Coalesce for types containing EquiJoin. */
  protected[qscript] def coalesceEJ[F[_]: Functor]
    (FToOut: F ~> λ[α => Option[OUT[α]]])
    (implicit EJ: EquiJoin[IT, ?] :<: OUT)
      : IN[IT[F]] => Option[OUT[IT[F]]]

  def coalesceEJNormalize[F[_]: Functor]
    (FToOut: F ~> λ[α => Option[OUT[α]]])
    (implicit EJ: EquiJoin[IT, ?] :<: OUT, N: Normalizable[OUT])
      : IN[IT[F]] => Option[OUT[IT[F]]] = in => {
    coalesceEJ[F](FToOut).apply(in).flatMap(N.normalizeF(_: OUT[IT[F]]))
  }

  /** Coalesce for types containing ThetaJoin. */
  protected[qscript] def coalesceTJ[F[_]: Functor]
    (FToOut: F ~> λ[α => Option[OUT[α]]])
    (implicit TJ: ThetaJoin[IT, ?] :<: OUT)
      : IN[IT[F]] => Option[OUT[IT[F]]]

  def coalesceTJNormalize[F[_]: Functor]
    (FToOut: F ~> λ[α => Option[OUT[α]]])
    (implicit TJ: ThetaJoin[IT, ?] :<: OUT, N: Normalizable[OUT])
      : IN[IT[F]] => Option[OUT[IT[F]]] = in => {
    coalesceTJ[F](FToOut).apply(in).flatMap(N.normalizeF(_: OUT[IT[F]]))
  }
}

trait CoalesceInstances {
  def coalesce[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = new CoalesceT[T]

  implicit def qscriptCore[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, G[_]]
    (implicit QC: QScriptCore[T, ?] :<: G)
      : Coalesce.Aux[T, QScriptCore[T, ?], G] =
    coalesce[T].qscriptCore[G]

  implicit def projectBucket[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, F[_]]
      : Coalesce.Aux[T, ProjectBucket[T, ?], F] =
    coalesce[T].projectBucket[F]

  implicit def thetaJoin[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, G[_]]
    (implicit TJ: ThetaJoin[T, ?] :<: G)
      : Coalesce.Aux[T, ThetaJoin[T, ?], G] =
    coalesce[T].thetaJoin[G]

  implicit def equiJoin[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, G[_]]
    (implicit EJ: EquiJoin[T, ?] :<: G)
      : Coalesce.Aux[T, EquiJoin[T, ?], G] =
    coalesce[T].equiJoin[G]

  implicit def coproduct[T[_[_]], G[_], H[_], I[_]]
    (implicit G: Coalesce.Aux[T, G, I],
              H: Coalesce.Aux[T, H, I])
      : Coalesce.Aux[T, Coproduct[G, H, ?], I] =
    new Coalesce[Coproduct[G, H, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = I[A]

      def coalesceQC[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore[IT, ?] :<: OUT) =
        _.run.bitraverse(G.coalesceQC(FToOut), H.coalesceQC(FToOut)) ∘ (Coproduct(_))

      def coalesceSR[F[_]: Functor, A]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore[IT, ?] :<: OUT, SR: Const[ShiftedRead[A], ?] :<: OUT) =
        _.run.bitraverse(G.coalesceSR(FToOut), H.coalesceSR(FToOut)) ∘ (Coproduct(_))

      def coalesceEJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit EJ: EquiJoin[IT, ?] :<: OUT) =
        _.run.fold(G.coalesceEJ(FToOut), H.coalesceEJ(FToOut))

      def coalesceTJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit TJ: ThetaJoin[IT, ?] :<: OUT) =
        _.run.fold(G.coalesceTJ(FToOut), H.coalesceTJ(FToOut))
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

  private def freeSR(branch: FreeQS): FreeQS = {
    def freeSR0[A](b: FreeQS)(implicit SR: Const[ShiftedRead[A], ?] :<: QScriptTotal): FreeQS =
      freeTotal(b)(CoalesceTotal.coalesceSRNormalize[CoEnvQS, A](coenvPrism[QScriptTotal, Hole]))

    freeSR0[AFile](freeSR0[ADir](branch))
  }

  private def freeEJ(branch: FreeQS): FreeQS =
    freeTotal(branch)(CoalesceTotal.coalesceEJNormalize(coenvPrism[QScriptTotal, Hole].get))

  private def freeTJ(branch: FreeQS): FreeQS =
    freeTotal(branch)(CoalesceTotal.coalesceTJNormalize(coenvPrism[QScriptTotal, Hole].get))

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

  private def eliminateRightSideProj[A: Equal](func: FreeMapA[A], a: A): Option[FreeMapA[A]] = {
    val target = Free.point[MapFunc, A](a)
    val oneRef = Free.roll[MapFunc, A](MFC(ProjectIndex(target, IntLit(1))))
    val rightCount: Int = func.elgotPara[Int](count(target))

    // all `RightSide` access is through `oneRef`
    (func.elgotPara[Int](count(oneRef)) ≟ rightCount).option(
      func.transApoT(substitute(oneRef, target)))
  }

  private def eliminateRightSideProjUnary(fm: FreeMap): Option[FreeMap] =
    eliminateRightSideProj(fm, SrcHole)

  def qscriptCore[G[_]](implicit QC: QScriptCore :<: G): Coalesce.Aux[T, QScriptCore, G] =
    new Coalesce[QScriptCore] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]


      def sequenceBucket[A: Equal, B](b: List[(A, B)]): Option[(Option[A], List[B])] =
        b.foldRightM[Option, (Option[A], List[B])](
          (None, Nil)) {
          case ((a, b), (as, bs)) =>
            as.fold[Option[Option[A]]](Some(a.some))(oldA => (oldA ≟ a).option(as)) strengthR (b :: bs)
        }

      // TODO: I feel like this must be some standard fold.
      def sequenceReduce[A: Equal, B](rf: ReduceFunc[(A, B)])
          : Option[(A, ReduceFunc[B])] =
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

      val rewrite = new Rewrite[T]
      val nm = new NormalizableT[T]

      // TODO: Use this. It seems like a valid normalization, but it breaks
      //       autojoins. Maybe it can be applied as part of optimization, or as
      //       a connector-specific transformation.
      private def extractFilterFromQC[F[_]: Functor]
        (FToOut: OUT ~> F)
        (implicit QC: QScriptCore :<: OUT)
          : QScriptCore[IT[F]] => Option[QScriptCore[IT[F]]] = {
        case LeftShift(src, struct, id, stpe, undef, repair) =>
          MapFuncCore.extractFilter(struct.linearize)(_.some) ∘ { case (f, m, _) =>
            LeftShift(FToOut(QC.inj(Filter(src, f.asRec))).embed, m.asRec, id, stpe, undef, repair)
          }
        case Map(src, mf) =>
          MapFuncCore.extractFilter(mf.linearize)(_.some) ∘ { case (f, m, _) =>
            Map(FToOut(QC.inj(Filter(src, f.asRec))).embed, m.asRec)
          }
        case _ => none
      }

      def fmIsCondUndef(jf: JoinFunc): Boolean = jf.resumeTwice.fold(_.run.fold ({
        case MapFuncsCore.Cond(_, _, -\/(Coproduct(-\/(MapFuncsCore.Undefined())))) => true
        case _ => false
      }, _ => false), _ => false)

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
              Map(srcInner, mf >> mfInner).some
            case LeftShift(srcInner, struct, id, stpe, undef, repair) =>
              LeftShift(srcInner, struct, id, stpe, undef, mf.linearize >> repair).some
            case Reduce(srcInner, bucket, funcs, repair) =>
              Reduce(srcInner, bucket, funcs, mf.linearize >> repair).some
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
                    cond >> mfInner))).embed,
                  mf >> mfInner).some
              case _ => None
            }
            case Sort(Embed(innerSrc), buckets, ordering) => FToOut.get(innerSrc) >>= QC.prj >>= {
              case Map(doubleInner, mfInner) =>
                Map(
                  FToOut.reverseGet(QC.inj(Sort(
                    doubleInner,
                    buckets ∘ (_ >> mfInner.linearize),
                    ordering ∘ (_.leftMap(_ >> mfInner.linearize))))).embed,
                  mf >> mfInner).some
              case _ => None
            }
/* FIXME: Investigate why this causes the 'convert union' test to fail.
            case Union(innerSrc, lBranch, rBranch) =>
              Union(
                innerSrc,
                Free.roll(Inject[QScriptCore, QScriptTotal].inj(Map(lBranch, mf))),
                Free.roll(Inject[QScriptCore, QScriptTotal].inj(Map(rBranch, mf)))).some
*/
            case _ => None
          })
        case LeftShift(Embed(src), struct, id, stpe, undef, shiftRepair) =>
          FToOut.get(src) >>= QC.prj >>= {
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
            case Reduce(srcInner, _, List(ReduceFuncs.UnshiftArray(elem)), redRepair)
                if nm.freeMF(struct.linearize >> redRepair) ≟ Free.point(ReduceIndex(0.right)) =>
              rightOnly(elem)(shiftRepair) ∘ (RecFreeS.fromFree(_)) ∘ (Map(srcInner, _))
            case Reduce(srcInner, _, List(ReduceFuncs.UnshiftMap(k, elem)), redRepair)
                if nm.freeMF(struct.linearize >> redRepair) ≟ Free.point(ReduceIndex(0.right)) =>
              rightOnly(id match {
                case ExcludeId => elem
                case IdOnly    => k
                case IncludeId => StaticArray(List(k, elem))
              })(shiftRepair) ∘ (RecFreeS.fromFree(_)) ∘ (Map(srcInner, _))
            case _ => None
          }
        case Reduce(Embed(src), bucket, reducers, redRepair) =>
          FToOut.get(src) >>= QC.prj >>= {
            case LeftShift(innerSrc, struct, id, stpe, _, shiftRepair)
                if shiftRepair =/= RightSideF =>
              (bucket.traverse(b => rightOnly(HoleF)(nm.freeMF(b >> shiftRepair))) ⊛
                reducers.traverse(_.traverse(mf => rightOnly(HoleF)(nm.freeMF(mf >> shiftRepair)))))((sb, sr) =>
                Reduce(
                  FToOut.reverseGet(QC.inj(LeftShift(innerSrc, struct, id, stpe, OnUndefined.omit, RightSideF))).embed,
                  sb,
                  sr,
                  redRepair))
            case LeftShift(innerSrc, struct, id, stpe, _, shiftRepair) =>
              (bucket.traverse(b => rewrite.rewriteShift(id, nm.freeMF(b >> shiftRepair))).flatMap(sequenceBucket[IdStatus, JoinFunc]) ⊛
                reducers.traverse(_.traverse(mf =>
                  rewrite.rewriteShift(id, nm.freeMF(mf >> shiftRepair)))).flatMap(_.traverse(sequenceReduce[IdStatus, JoinFunc]) >>= sequenceBucket[IdStatus, ReduceFunc[JoinFunc]])) {
                case ((bId, bucket), (rId, reducers)) =>
                  val newId = bId.fold(rId.getOrElse(ExcludeId).some)(b => rId.fold(b.some)(r => (b ≟ r).option(b)))
                  newId strengthR ((bucket, reducers))
              }.join >>= {
                case (newId, (bucket, reducers)) =>
                  (bucket.traverse(rightOnly(HoleF)) ⊛
                    (reducers.traverse(_.traverse(rightOnly(HoleF)))))((sb, sr) =>
                    Reduce(
                      FToOut.reverseGet(QC.inj(LeftShift(innerSrc, struct, newId, stpe, OnUndefined.omit, RightSideF))).embed,
                      sb,
                      sr,
                      redRepair))
              }
            case Map(innerSrc, mf) =>
              Reduce(
                innerSrc,
                bucket ∘ (_ >> mf.linearize),
                reducers.map(_.map(_ >> mf.linearize)),
                redRepair).some
            case Sort(innerSrc, _, _)
                if !reducers.exists(ReduceFunc.isOrderDependent) =>
              Reduce(innerSrc, bucket, reducers, redRepair).some
            case _ => None
          }
        case Filter(Embed(src), cond) => FToOut.get(src) >>= QC.prj >>= {
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

      def coalesceSR[F[_]: Functor, A]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore :<: OUT, SR: Const[ShiftedRead[A], ?] :<: OUT) = {

        case Filter(Embed(src), mf) =>
          (FToOut.get(src) >>= QC.prj) match {
            case Some(Map(Embed(innerSrc), innermf)) =>
              (FToOut.get(innerSrc) >>= SR.prj) match {
                case Some(sr) =>
                  Map(
                    FToOut.reverseGet(QC.inj(
                      Filter(
                        FToOut.reverseGet(SR.inj(sr)).embed,
                        mf >> innermf))).embed,
                    innermf).some
                case _ => None
              }
            case _ => None
          }

        case Map(Embed(src), mf) =>
          ((FToOut.get(src) >>= SR.prj) ⊛ eliminateRightSideProjUnary(mf.linearize))((const, newMF) =>
            Map(
              FToOut.reverseGet(SR.inj(Const[ShiftedRead[A], T[F]](ShiftedRead(const.getConst.path, ExcludeId)))).embed,
              RecFreeS.fromFree(newMF))) <+>
          (((FToOut.get(src) >>= QC.prj) match {

            case Some(Filter(Embed(innerSrc), cond)) =>
              ((FToOut.get(innerSrc) >>= SR.prj) ⊛ eliminateRightSideProjUnary(cond.linearize))((const, newCond) =>
                Filter(
                  FToOut.reverseGet(SR.inj(Const[ShiftedRead[A], T[F]](ShiftedRead(const.getConst.path, ExcludeId)))).embed,
                  newCond.asRec))

            case Some(Sort(Embed(innerSrc), bucket, ord)) =>
              ((FToOut.get(innerSrc) >>= SR.prj) ⊛
                bucket.traverse(eliminateRightSideProjUnary) ⊛
                ord.traverse(Bitraverse[(?, ?)].leftTraverse.traverse(_)(eliminateRightSideProjUnary)))(
                (const, newBucket, newOrd) =>
                Sort(
                  FToOut.reverseGet(SR.inj(Const[ShiftedRead[A], T[F]](ShiftedRead(const.getConst.path, ExcludeId)))).embed,
                  newBucket,
                  newOrd))

            case _ => None
          }) ⊛ eliminateRightSideProjUnary(mf.linearize))((newFilter, newMF) =>
            Map(
              FToOut.reverseGet(QC.inj(newFilter)).embed,
              RecFreeS.fromFree(newMF)))

        case Reduce(Embed(src), bucket, reducers, repair) =>
          ((FToOut.get(src) >>= SR.prj) ⊛ bucket.traverse(eliminateRightSideProjUnary) ⊛ reducers.traverse(_.traverse(eliminateRightSideProjUnary)))(
            (const, newBuck, newRed) =>
            Reduce(
              FToOut.reverseGet(SR.inj(Const[ShiftedRead[A], T[F]](ShiftedRead(const.getConst.path, ExcludeId)))).embed,
              newBuck,
              newRed,
              repair))

        case LeftShift(Embed(src), struct, idStatus, shiftType, undef, repair) =>
          ((FToOut.get(src) >>= SR.prj) ⊛ eliminateRightSideProjUnary(struct.linearize) ⊛ eliminateRightSideProj(repair, LeftSide))(
            (sr, newStruct, newRepair) =>
            LeftShift(
              FToOut.reverseGet(SR.inj(Const[ShiftedRead[A], T[F]](ShiftedRead(sr.getConst.path, ExcludeId)))).embed,
              RecFreeS.fromFree(newStruct),
              idStatus,
              shiftType,
              undef,
              newRepair))

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
            ej => EJ.inj(EquiJoin.combine.modify(mf.linearize >> (_: JoinFunc))(ej)))
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
            tj => TJ.inj(ThetaJoin.combine.modify(mf.linearize >> (_: JoinFunc))(tj)))
        case Filter(Embed(src), cond) =>
          (FToOut(src) >>= TJ.prj).map(
            tj => TJ.inj(ThetaJoin.on[IT, IT[F]].modify(on => Free.roll(MFC(And(on, cond.linearize >> tj.combine))))(tj)))
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
        case BucketKey(Embed(src), value, key) => FToOut.get(src) >>= QC.prj >>= {
          case Map(srcInner, mf) =>
            BucketKey(srcInner, value >> mf.linearize, key >> mf.linearize).some
          case _ => None
        }
        case BucketIndex(Embed(src), value, index) => FToOut.get(src) >>= QC.prj >>= {
          case Map(srcInner, mf) =>
            BucketIndex(srcInner, value >> mf.linearize, index >> mf.linearize).some
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
            EquiJoin(ej.src, _, _, ej.key, ej.f, ej.combine))

          val qct = Inject[QScriptCore, QScriptTotal]

          def coalesceBranchMaps(ej: EquiJoin[T[F]]): Option[EquiJoin[T[F]]] = {
            def coalesceSide(branch: FreeQS, key: List[FreeMap], side: JoinSide):
                Option[(FreeQS, List[FreeMap], JoinFunc)] =
              branch.project.run.map(qct.prj) match {
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

      def coalesceSR[F[_]: Functor, A]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore :<: OUT, SR: Const[ShiftedRead[A], ?] :<: OUT) =
        ej => makeBranched(
          ej.lBranch, ej.rBranch)(
          ifNeq(freeSR))(
          EquiJoin(ej.src, _, _, ej.key, ej.f, ej.combine))

      def coalesceEJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit EJ: EquiJoin :<: OUT) =
        ej => makeBranched(
          ej.lBranch, ej.rBranch)(
          ifNeq(freeEJ))((l, r) =>
          EJ.inj(EquiJoin(ej.src, l, r, ej.key, ej.f, ej.combine)))

      def coalesceTJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit TJ: ThetaJoin :<: OUT) =
        ej => makeBranched(
          ej.lBranch, ej.rBranch)(
          ifNeq(freeTJ))((l, r) =>
          EJ.inj(EquiJoin(ej.src, l, r, ej.key, ej.f, ej.combine)))
    }
}

object Coalesce extends CoalesceInstances {
  type Aux[T[_[_]], IN[_], F[_]] = Coalesce[IN] {
    type IT[F[_]] = T[F]
    type OUT[A] = F[A]
  }

  def apply[T[_[_]], IN[_], OUT[_]](implicit ev: Coalesce.Aux[T, IN, OUT]) = ev
}
