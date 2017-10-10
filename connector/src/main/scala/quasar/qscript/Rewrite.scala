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

import slamdata.Predef._
import quasar.{RenderTree, RenderTreeT}
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.{ADir, AFile}
import quasar.contrib.scalaz.bitraverse._
import quasar.fp._
import quasar.fp.ski._
import quasar.fs.MonadFsErr
import quasar.qscript.MapFuncCore._
import quasar.qscript.MapFuncsCore._

import scala.collection.immutable.{Map => ScalaMap}

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.{:+: => _, Divide => _, _},
  Inject.{ reflexiveInjectInstance => _, _ },
  BijectionT._,
  Leibniz._,
  Scalaz._

class Rewrite[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] extends TTypes[T] {
  def rebuildArray[A: Show: RenderTree](funcs: List[FreeMapA[A]]): FreeMapA[A] = funcs match {
    case Nil    => Free.roll(MFC(EmptyArray[T, FreeMapA[A]]))
    case h :: t =>
      t.foldLeft(
        Free.roll(MFC(MakeArray[T, FreeMapA[A]](h))))(
        (acc, e) => Free.roll(MFC(ConcatArrays(acc, Free.roll(MFC(MakeArray(e)))))))
  }

  def rewriteShift(idStatus: IdStatus, repair: JoinFunc)
      : Option[(IdStatus, JoinFunc)] =
    (idStatus ≟ IncludeId).option[Option[(IdStatus, JoinFunc)]] {
      def makeRef(idx: Int): JoinFunc =
        Free.roll[MapFunc, JoinSide](MFC(ProjectIndex(RightSideF, IntLit(idx))))

      val zeroRef: JoinFunc = makeRef(0)
      val oneRef: JoinFunc = makeRef(1)
      val rightCount: Int = repair.elgotPara(count(RightSideF))

      if (repair.elgotPara(count(oneRef)) ≟ rightCount)
        // all `RightSide` access is through `oneRef`
        (ExcludeId, repair.transApoT(substitute[JoinFunc](oneRef, RightSideF))).some
      else if (repair.elgotPara(count(zeroRef)) ≟ rightCount)
        // all `RightSide` access is through `zeroRef`
        (IdOnly, repair.transApoT(substitute[JoinFunc](zeroRef, RightSideF))).some
      else
        None
    }.join

  // TODO: make this simply a transform itself, rather than a full traversal.
  def shiftRead[F[_]: Functor, G[_]: Traverse]
    (implicit QC: QScriptCore :<: G,
              TJ: ThetaJoin :<: G,
              SD: Const[ShiftedRead[ADir], ?] :<: G,
              SF: Const[ShiftedRead[AFile], ?] :<: G,
              GI: Injectable.Aux[G, QScriptTotal],
              S: ShiftRead.Aux[T, F, G],
              C: Coalesce.Aux[T, G, G],
              N: Normalizable[G])
      : T[F] => T[G] = {
    _.codyna(
      normalizeTJ[G] >>>
      liftFG(injectRepeatedly(C.coalesceSRNormalize[G, ADir](idPrism))) >>>
      liftFG(injectRepeatedly(C.coalesceSRNormalize[G, AFile](idPrism))) >>>
      (_.embed),
      ((_: T[F]).project) >>> (S.shiftRead(idPrism.reverseGet)(_)))
  }

  def shiftReadDir[F[_]: Functor, G[_]: Traverse](
    implicit
    QC: QScriptCore :<: G,
    TJ: ThetaJoin :<: G,
    SD: Const[ShiftedRead[ADir], ?] :<: G,
    GI: Injectable.Aux[G, QScriptTotal],
    S: ShiftReadDir.Aux[T, F, G],
    C: Coalesce.Aux[T, G, G],
    N: Normalizable[G]
  ): T[F] => T[G] =
    _.codyna(
      normalizeTJ[G] >>>
      liftFG(injectRepeatedly(C.coalesceSRNormalize[G, ADir](idPrism))) >>>
      (_.embed),
      ((_: T[F]).project) >>> (S.shiftReadDir(idPrism.reverseGet)(_)))

  def simplifyJoinOnShiftRead[F[_]: Functor, G[_]: Traverse, H[_]: Functor]
    (implicit QC: QScriptCore :<: G,
              TJ: ThetaJoin :<: G,
              SD: Const[ShiftedRead[ADir], ?] :<: G,
              SF: Const[ShiftedRead[AFile], ?] :<: G,
              GI: Injectable.Aux[G, QScriptTotal],
              S: ShiftRead.Aux[T, F, G],
              J: SimplifyJoin.Aux[T, G, H],
              C: Coalesce.Aux[T, G, G],
              N: Normalizable[G])
      : T[F] => T[H] =
    shiftRead[F, G].apply(_).transCata[T[H]](J.simplifyJoin(idPrism.reverseGet))


  // TODO: These optimizations should give rise to various property tests:
  //       • elideNopMap ⇒ no `Map(???, HoleF)`
  //       • normalize ⇒ a whole bunch, based on MapFuncsCore
  //       • elideNopJoin ⇒ no `ThetaJoin(???, HoleF, HoleF, LeftSide === RightSide, ???, ???)`
  //       • coalesceMaps ⇒ no `Map(Map(???, ???), ???)`
  //       • coalesceMapJoin ⇒ no `Map(ThetaJoin(???, …), ???)`

  def elideNopQC[F[_]: Functor]: QScriptCore[T[F]] => Option[F[T[F]]] = {
    case Filter(Embed(src), BoolLit(true)) => some(src)
    case Map(Embed(src), mf) if mf ≟ HoleF => some(src)
    case _                                 => none
  }

  type Remap[A] = JoinFunc => Option[FreeMapA[A]]
  type Combine[F[_], A, B] = FreeMapA[A] => Option[F[B]]

  class BranchUnification[F[_], A, B] private (val remap: Remap[A], val combine: Combine[F, A, B])

  object BranchUnification {
    def apply[F[_], A, B](remap: Remap[A])(combine: Combine[F, A, B]): BranchUnification[F, A, B] =
      new BranchUnification[F, A, B](remap, combine)
  }

  def NoneBranch[F[_], A, B] =
    BranchUnification[F, A, B]((_: JoinFunc) => None)((_: FreeMapA[A]) => None)

  def unifySimpleBranchesJoinSide[F[_], A]
    (src: A, left: FreeQS, right: FreeQS)
    (rebase: FreeQS => A => Option[A])
    (implicit
      QC: QScriptCore :<: F,
      FI: Injectable.Aux[F, QScriptTotal])
      : BranchUnification[F, JoinSide, A] = {

    // Unify (Map, LeftShift)
    def unifyMapRightSideShift(
      struct: FreeMap,
      status: IdStatus,
      repair: JoinFunc,
      shiftSrc: FreeMap,
      mapFn: FreeMap
    ): BranchUnification[F, JoinSide, A] =
      BranchUnification { jf =>
        (jf >>= {
          case LeftSide => mapFn >> LeftSideF  // references `src`
          case RightSide  => repair >>= {
            case LeftSide  => shiftSrc >> LeftSideF
            case RightSide => RightSideF
          }
        }).some
      } (func => QC.inj(LeftShift(src, struct >> shiftSrc, status, func)).some)

    // Unify (LeftShift, Map)
    def unifyMapLeftSideShift(
      struct: FreeMap,
      status: IdStatus,
      repair: JoinFunc,
      shiftSrc: FreeMap,
      mapFn: FreeMap
    ): BranchUnification[F, JoinSide, A] =
      BranchUnification { jf =>
        (jf >>= {
          case LeftSide  => repair >>= {
            case LeftSide  => shiftSrc >> LeftSideF
            case RightSide => RightSideF
          }
          case RightSide => mapFn >> LeftSideF  // references `src`
        }).some
      } (func => QC.inj(LeftShift(src, struct >> shiftSrc, status, func)).some)

    (left.resumeTwice, right.resumeTwice) match {
      // left side is the data while the right side shifts the same data
      case (\/-(SrcHole), -\/(r)) => FI.project(r) >>= QC.prj match {
        case Some(LeftShift(lshiftSrc, struct, status, repair)) =>
          lshiftSrc match {
            case \/-(SrcHole) =>
              unifyMapRightSideShift(struct, status, repair, HoleF, HoleF)

            case -\/(values) => FI.project(values) >>= QC.prj match {
              case Some(Map(mapSrc, srcFn)) if mapSrc ≟ HoleQS =>
                unifyMapRightSideShift(struct, status, repair, srcFn, HoleF)

              case _ => NoneBranch[F, JoinSide, A]
            }
          }
        case _ => NoneBranch
      }

      // right side is the data while the left side shifts the same data
      case (-\/(l), \/-(SrcHole)) => FI.project(l) >>= QC.prj match {
        case Some(LeftShift(lshiftSrc, struct, status, repair)) =>
          lshiftSrc match {
            case \/-(SrcHole) =>
              unifyMapLeftSideShift(struct, status, repair, HoleF, HoleF)

            case -\/(values) => FI.project(values) >>= QC.prj match {
              case Some(Map(mapSrc, srcFn)) if mapSrc ≟ HoleQS =>
                unifyMapLeftSideShift(struct, status, repair, srcFn, HoleF)

              case _ => NoneBranch[F, JoinSide, A]
            }
          }
        case _ => NoneBranch
      }

      case (-\/(l), -\/(r)) => (l, r).uTraverse(m => FI.project(m) >>= QC.prj) collect {
        // left side maps over the data while the right side shifts the same data
        case (Map(\/-(SrcHole), mapFn), LeftShift(lshiftSrc, struct, status, repair)) =>
          lshiftSrc match {
            case \/-(SrcHole) =>
              unifyMapRightSideShift(struct, status, repair, HoleF, mapFn)

            case -\/(values) => FI.project(values) >>= QC.prj match {
              case Some(Map(mapSrc, srcFn)) if mapSrc ≟ HoleQS =>
                unifyMapRightSideShift(struct, status, repair, srcFn, mapFn)

              case _ => NoneBranch[F, JoinSide, A]
            }
          }

        // right side maps over the data while the left side shifts the same data
        case (LeftShift(lshiftSrc, struct, status, repair), Map(\/-(SrcHole), mapFn)) =>
          lshiftSrc match {
            case \/-(SrcHole) =>
              unifyMapLeftSideShift(struct, status, repair, HoleF, mapFn)

            case -\/(values) => FI.project(values) >>= QC.prj match {
              case Some(Map(mapSrc, srcFn)) if mapSrc ≟ HoleQS =>
                unifyMapLeftSideShift(struct, status, repair, srcFn, mapFn)

              case _ => NoneBranch[F, JoinSide, A]
            }
          }
      } getOrElse NoneBranch

      case _ => NoneBranch
    }
  }

  def unifySimpleBranchesHole[F[_], A]
    (src: A, left: FreeQS, right: FreeQS)
    (rebase: FreeQS => A => Option[A])
    (implicit
      QC: QScriptCore :<: F,
      FI: Injectable.Aux[F, QScriptTotal])
      : BranchUnification[F, Hole, A] = {
    val UnrefedSrc: QScriptTotal[FreeQS] =
      Inject[QScriptCore, QScriptTotal] inj Unreferenced[T, FreeQS]()

    (left.resumeTwice, right.resumeTwice) match {
      case (-\/(m1), -\/(m2)) =>
        (FI.project(m1) >>= QC.prj, FI.project(m2) >>= QC.prj) match {
          // both sides only map over the same data
          case (Some(Map(\/-(SrcHole), mf1)), Some(Map(\/-(SrcHole), mf2))) =>
            BranchUnification { jf =>
              (jf >>= {
                case LeftSide  => mf1
                case RightSide => mf2
              }).some
            } (func => QC.inj(Map(src, func)).some)
          // neither side references the src
          case (Some(Map(-\/(src1), mf1)), Some(Map(-\/(src2), mf2)))
              if src1 ≟ UnrefedSrc && src2 ≟ UnrefedSrc =>
            BranchUnification { jf =>
              (jf >>= {
                case LeftSide  => mf1
                case RightSide => mf2
              }).some
            } (func => QC.inj(Map(src, func)).some)
          // only the right side references the source
          case (Some(Map(-\/(src1), mf1)), _) if src1 ≟ UnrefedSrc =>
            BranchUnification { jf =>
              (jf >>= {
                case LeftSide  => mf1
                case RightSide => HoleF
              }).some
            } (func => rebase(right)(src).map(tf => QC.inj(Map(tf, func))))
          case (Some(Unreferenced()), _) =>
            BranchUnification { jf =>
              jf.traverseM[Option, Hole] {
                case LeftSide  => None
                case RightSide => HoleF.some
              }
            } (func => rebase(right)(src).map(tf => QC.inj(Map(tf, func))))
          // only the left side references the source
          case (_, Some(Map(-\/(src2), mf2))) if src2 ≟ UnrefedSrc =>
            BranchUnification { jf =>
              (jf >>= {
                case LeftSide  => HoleF
                case RightSide => mf2
              }).some
            } (func => rebase(left)(src).map(tf => QC.inj(Map(tf, func))))
          case (_, Some(Unreferenced())) =>
            BranchUnification { jf =>
              jf.traverseM[Option, Hole] {
                case LeftSide  => HoleF.some
                case RightSide => None
              }
            } (func => rebase(left)(src).map(tf => QC.inj(Map(tf, func))))
          case (_, _) => NoneBranch
        }
      // one side maps over the src while the other passes the src untouched
      case (-\/(m1), \/-(SrcHole)) => (FI.project(m1) >>= QC.prj) match {
        case Some(Map(\/-(SrcHole), mf1)) =>
          BranchUnification { jf =>
            (jf >>= {
              case LeftSide  => mf1
              case RightSide => HoleF
            }).some
          } (func => QC.inj(Map(src, func)).some)
        case Some(Unreferenced()) =>
          BranchUnification { jf =>
            jf.traverseM[Option, Hole] {
              case LeftSide  => None
              case RightSide => HoleF.some
            }
          } (func => QC.inj(Map(src, func)).some)
        case _ => NoneBranch
      }
      // the other side maps over the src while the one passes the src untouched
      case (\/-(SrcHole), -\/(m2)) => (FI.project(m2) >>= QC.prj) match {
        case Some(Map(\/-(SrcHole), mf2)) =>
          BranchUnification { jf: JoinFunc =>
            (jf >>= {
              case LeftSide  => HoleF
              case RightSide => mf2
            }).some
          } (func => QC.inj(Map(src, func)).some)
        case Some(Unreferenced()) =>
          BranchUnification { jf =>
            jf.traverseM[Option, Hole] {
              case LeftSide  => HoleF.some
              case RightSide => None
            }
          } (func => QC.inj(Map(src, func)).some)
        case _ => NoneBranch
      }
      // both sides are the src
      case (\/-(SrcHole), \/-(SrcHole)) =>
        BranchUnification(
          jf => (jf.as(SrcHole): FreeMap).some)(
          func => QC.inj(Map(src, func)).some)
      case (_, _) => NoneBranch
    }
  }

  def unifySimpleBranches[F[_], A]
    (src: A, left: FreeQS, right: FreeQS, func: JoinFunc)
    (rebase: FreeQS => A => Option[A])
    (implicit
      QC: QScriptCore :<: F,
      FI: Injectable.Aux[F, QScriptTotal])
      : Option[F[A]] = {
    val branchHole: BranchUnification[F, Hole, A] =
      unifySimpleBranchesHole(src, left, right)(rebase)(QC, FI)
    val branchSide: BranchUnification[F, JoinSide, A] =
      unifySimpleBranchesJoinSide(src, left, right)(rebase)(QC, FI)

    branchHole.remap(func).flatMap(branchHole.combine) orElse
      branchSide.remap(func).flatMap(branchSide.combine)
  }

  def unifySimpleBranchesCoEnv[F[_], A]
    (src: A, l: FreeQS, r: FreeQS, combine: JoinFunc)
    (rebase: FreeQS => A => Option[A])
    (implicit
      QC: QScriptCore :<: F,
      FI: Injectable.Aux[F, QScriptTotal])
      : Option[CoEnv[Hole, F, A]] =
    unifySimpleBranches(src, l, r, combine)(rebase)(QC, FI).map(fa => CoEnv(\/-(fa)))

  // FIXME: This really needs to ensure that the condition is that of an
  //        autojoin, otherwise it’ll elide things that are truly meaningful.
  def elideNopJoin[F[_], A]
    (rebase: FreeQS => A => Option[A])
    (implicit QC: QScriptCore :<: F, FI: Injectable.Aux[F, QScriptTotal])
      : ThetaJoin[A] => Option[F[A]] = {
    case ThetaJoin(s, l, r, _, _, combine) => unifySimpleBranches[F, A](s, l, r, combine)(rebase)(QC, FI)
    case _                                 => None
  }

  def compactLeftShift[F[_]: Functor]
      (QCToF: PrismNT[F, QScriptCore])
      : QScriptCore[T[F]] => Option[F[T[F]]] = {
    case qs @ LeftShift(Embed(src), struct, ExcludeId, joinFunc) =>
      (QCToF.get(src), struct.resume) match {
        // LeftShift(Map(_, MakeArray(_)), Hole, ExcludeId, _)
        case (Some(Map(innerSrc, fm)), \/-(SrcHole)) =>
          fm.resume match {
            case -\/(MFC(MakeArray(value))) =>
              QCToF(Map(innerSrc, joinFunc >>= {
                case LeftSide => fm
                case RightSide => value
              })).some
            case _ => None
          }
        // LeftShift(_, MakeArray(_), ExcludeId, _)
        case (_, -\/(MFC(MakeArray(value)))) =>
          QCToF(Map(src.embed, joinFunc >>= {
            case LeftSide => HoleF
            case RightSide => value
          })).some
        case (_, _) => None
      }
    case qs => None
  }

  val compactQC = λ[QScriptCore ~> (Option ∘ QScriptCore)#λ] {
    case LeftShift(src, struct, id, repair) =>
      rewriteShift(id, repair) ∘ (xy => LeftShift(src, struct, xy._1, xy._2))

    case Reduce(src, bucket, reducers, repair0) =>
      // `indices`: the indices into `reducers` that are used
      val Empty   = ReduceIndex(-1.right)
      val used    = repair0.map(_.idx).toList.unite.toSet
      val indices = reducers.indices filter used
      val repair  = repair0 map (r => r.copy(r.idx ∘ indices.indexOf))
      val done    = repair ≟ repair0 || (repair element Empty)

      !done option Reduce(src, bucket, (indices map reducers).toList, repair)

    case _ => None
  }

  private def findUniqueBuckets(buckets: List[FreeMap]): Option[List[FreeMap]] = {
    val uniqued = buckets.distinctE.toList
    (uniqued ≠ buckets).option(uniqued)
  }

  val uniqueBuckets = λ[QScriptCore ~> (Option ∘ QScriptCore)#λ] {
    case Reduce(src, bucket, reducers, repair) =>
      // FIXME: Update indexes into bucket.
      findUniqueBuckets(bucket).map(Reduce(src, _, reducers, repair))
    case Sort(src, bucket, order) =>
      findUniqueBuckets(bucket).map(Sort(src, _, order))
    case _ => None
  }

  val compactReductions = λ[QScriptCore ~> (Option ∘ QScriptCore)#λ] {
    case Reduce(src, bucket, reducers, repair) =>
      val (_, mapping, newReducers) =
        // (shift as duplicate reducers are found, new mapping of reducers, resulting reducers)
        reducers.zipWithIndex.foldLeft[(Int, ScalaMap[Int, Int], List[ReduceFunc[FreeMap]])](
          (0, scala.collection.immutable.Map[Int, Int](), Nil)) {
          case ((shift, mapping, lrf), (rf, origIndex)) =>
            val i = lrf.indexWhere(_ ≟ rf)
            (i ≟ -1).fold(
              // when the reducer is new, we apply the shift
              (shift, mapping + ((origIndex, origIndex - shift)), lrf :+ rf),
              // when the reducer already exists, we record a shift
              (shift + 1, mapping + ((origIndex, i)), lrf))
        }
      (newReducers ≠ reducers).option(
        Reduce(
          src,
          bucket,
          newReducers,
          repair.map(ri => ReduceIndex(ri.idx.map(i => mapping.applyOrElse(i, κ(i)))))))

    case _ => None
  }

  // TODO: add reordering
  // - Filter can be moved ahead of Sort
  // - Subset can have a normalized order _if_ their counts are constant
  //   (maybe in some additional cases)

  // The order of optimizations is roughly this:
  // - elide NOPs
  // - read conversion given to us by the filesystem
  // - convert any remaning projects to maps
  // - coalesce nodes
  // - normalize mapfunc
  private def applyNormalizations[F[_]: Traverse: Normalizable, G[_]: Traverse](
    prism: PrismNT[G, F],
    rebase: FreeQS => T[G] => Option[T[G]],
    normalizeJoins: F[T[G]] => Option[G[T[G]]])(
    implicit C: Coalesce.Aux[T, F, F],
             QC: QScriptCore :<: F,
             FI: Injectable.Aux[F, QScriptTotal]):
      F[T[G]] => G[T[G]] = {

    val qcPrism = PrismNT.inject[QScriptCore, F] compose prism

    ftf => repeatedly(applyTransforms(
      liftFFTrans(prism)(Normalizable[F].normalizeF(_: F[T[G]])),
      liftFFTrans(qcPrism)(compactQC(_: QScriptCore[T[G]])),
      liftFGTrans(qcPrism)(compactLeftShift[G](qcPrism)),
      liftFFTrans(qcPrism)(uniqueBuckets(_: QScriptCore[T[G]])),
      liftFFTrans(qcPrism)(compactReductions(_: QScriptCore[T[G]])),
      liftFFTrans(prism)(C.coalesceQC[G](prism)),
      liftFGTrans(prism)(normalizeJoins),
      liftFGTrans(qcPrism)(elideNopQC[G])
    ))(prism(ftf))
  }

  private def normalizeWithBijection[F[_]: Traverse: Normalizable, G[_]: Traverse, A](
    bij: Bijection[A, T[G]])(
    prism: PrismNT[G, F],
    rebase: FreeQS => T[G] => Option[T[G]],
    normalizeJoins: F[T[G]] => Option[G[T[G]]])(
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<: F,
             FI: Injectable.Aux[F, QScriptTotal]):
      F[A] => G[A] =
    fa => applyNormalizations[F, G](prism, rebase, normalizeJoins)
      .apply(fa ∘ bij.toK.run) ∘ bij.fromK.run

  private def normalizeEJBijection[F[_]: Traverse: Normalizable, G[_]: Traverse, A](
    bij: Bijection[A, T[G]])(
    prism: PrismNT[G, F],
    rebase: FreeQS => T[G] => Option[T[G]])(
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<: F,
             EJ: EquiJoin :<: F,
             FI: Injectable.Aux[F, QScriptTotal]):
      F[A] => G[A] = {

    val normEJ =
      liftFFTrans(prism)(C.coalesceEJ[G](prism.get))

    normalizeWithBijection[F, G, A](bij)(prism, rebase, normEJ compose (prism apply _))
  }

  def normalizeEJ[F[_]: Traverse: Normalizable](
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<: F,
             EJ: EquiJoin :<: F,
             FI: Injectable.Aux[F, QScriptTotal]):
      F[T[F]] => F[T[F]] =
    normalizeEJBijection[F, F, T[F]](bijectionId)(idPrism, rebaseT)

  def normalizeEJCoEnv[F[_]: Traverse: Normalizable](
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<: F,
             EJ: EquiJoin :<: F,
             FI: Injectable.Aux[F, QScriptTotal]):
      F[Free[F, Hole]] => CoEnv[Hole, F, Free[F, Hole]] =
    normalizeEJBijection[F, CoEnv[Hole, F, ?], Free[F, Hole]](coenvBijection)(coenvPrism, rebaseTCo)

  private def normalizeTJBijection[F[_]: Traverse: Normalizable, G[_]: Traverse, A](
    bij: Bijection[A, T[G]])(
    prism: PrismNT[G, F],
    rebase: FreeQS => T[G] => Option[T[G]])(
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<: F,
             TJ: ThetaJoin :<: F,
             FI: Injectable.Aux[F, QScriptTotal]):
      F[A] => G[A] = {

    val normTJ = applyTransforms(
      liftFFTrans(prism)(C.coalesceTJ[G](prism.get)),
      liftFFTrans(prism)((fa: F[T[G]]) => TJ.prj(fa).flatMap(elideNopJoin[F, T[G]](rebase))))

    normalizeWithBijection[F, G, A](bij)(prism, rebase, normTJ compose (prism apply _))
  }

  def normalizeTJ[F[_]: Traverse: Normalizable](
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<: F,
             TJ: ThetaJoin :<: F,
             FI: Injectable.Aux[F, QScriptTotal]):
      F[T[F]] => F[T[F]] =
    normalizeTJBijection[F, F, T[F]](bijectionId)(idPrism, rebaseT)

  def normalizeTJCoEnv[F[_]: Traverse: Normalizable](
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<: F,
             TJ: ThetaJoin :<: F,
             FI: Injectable.Aux[F, QScriptTotal]):
      F[Free[F, Hole]] => CoEnv[Hole, F, Free[F, Hole]] =
    normalizeTJBijection[F, CoEnv[Hole, F, ?], Free[F, Hole]](coenvBijection)(coenvPrism, rebaseTCo)

  /** A backend-or-mount-specific `f` is provided, that allows us to rewrite
    * [[Root]] (and projections, etc.) into [[Read]], so then we can handle
    * exposing only “true” joins and converting intra-data joins to map
    * operations.
    *
    * `f` takes QScript representing a _potential_ path to a file, converts
    * [[Root]] and its children to path, with the operations post-file remaining.
    */
  def pathify[M[_]: Monad: MonadFsErr, IN[_]: Traverse, OUT[_]: Traverse]
    (g: DiscoverPath.ListContents[M])
    (implicit
      FS: DiscoverPath.Aux[T, IN, OUT],
      RD:  Const[Read[ADir], ?] :<: OUT,
      RF: Const[Read[AFile], ?] :<: OUT,
      QC:           QScriptCore :<: OUT,
      FI: Injectable.Aux[OUT, QScriptTotal])
      : T[IN] => M[T[OUT]] =
    _.cataM(FS.discoverPath[M](g)) >>= DiscoverPath.unionAll[T, M, OUT](g)
}
