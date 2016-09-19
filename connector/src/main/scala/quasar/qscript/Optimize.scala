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
import quasar.fs.MonadFsErr
import quasar.qscript.MapFunc._
import quasar.qscript.MapFuncs._

import matryoshka._,
  Recursive.ops._,
  FunctorT.ops._,
  TraverseT.nonInheritedOps._
import matryoshka.patterns._
import scalaz.{:+: => _, Divide => _, _}, Scalaz._, Inject._, Leibniz._

// NB: Helper types for `simplifyJoin`.
private final case class EquiJoinKey[T[_[_]]]
  (left: FreeMap[T], right: FreeMap[T])
private final case class SimplifiedJoinCondition[T[_[_]]]
  (keys: List[EquiJoinKey[T]], filter: Option[JoinFunc[T]])

class Optimize[T[_[_]]: Recursive: Corecursive: EqualT: ShowT] {

  // TODO: These optimizations should give rise to various property tests:
  //       • elideNopMap ⇒ no `Map(???, HoleF)`
  //       • normalize ⇒ a whole bunch, based on MapFuncs
  //       • elideNopJoin ⇒ no `ThetaJoin(???, HoleF, HoleF, LeftSide === RightSide, ???, ???)`
  //       • coalesceMaps ⇒ no `Map(Map(???, ???), ???)`
  //       • coalesceMapJoin ⇒ no `Map(ThetaJoin(???, …), ???)`

  // TODO: Turn `elideNop` into a type class?
  // def elideNopFilter[F[_]: Functor](implicit QC: QScriptCore[T, ?] :<: F):
  //     QScriptCore[T, T[F]] => F[T[F]] = {
  //   case Filter(src, Patts.True) => src.project
  //   case qc                      => QC.inj(qc)
  // }

  def elideNopMap[F[_]: Functor](implicit QC: QScriptCore[T, ?] :<: F):
      QScriptCore[T, T[F]] => F[T[F]] = {
    case Map(src, mf) if mf ≟ HoleF => src.project
    case x                          => QC.inj(x)
  }

  def elideNopMapCo[F[_]: Functor, A](implicit QC: QScriptCore[T, ?] :<: F):
      QScriptCore[T, T[CoEnv[A, F, ?]]] => CoEnv[A, F, T[CoEnv[A, F, ?]]] = {
    case Map(src, mf) if mf ≟ HoleF => src.project
    case x                          => CoEnv(QC.inj(x).right)
  }

  // FIXME: This really needs to ensure that the condition is that of an
  //        autojoin, otherwise it’ll elide things that are truly meaningful.
  def elideNopJoin[F[_]](
    implicit TJ: ThetaJoin[T, ?] :<: F, QC: QScriptCore[T, ?] :<: F, FI: F :<: QScriptTotal[T, ?]) = λ[ThetaJoin[T, ?] ~> F] {
      case ThetaJoin(src, l, r, on, Inner, combine)
          if l ≟ Free.point(SrcHole) && r ≟ Free.point(SrcHole) && on ≟ EquiJF =>
        QC.inj(Map(src, combine.map(_ => SrcHole: Hole)))
      case x @ ThetaJoin(src, l, r, on, _, combine) if on ≟ BoolLit(true) =>
        (l.resume.leftMap(_.map(_.resume)), r.resume.leftMap(_.map(_.resume))) match {
          case (-\/(m1), -\/(m2)) => (FI.prj(m1) >>= QC.prj, FI.prj(m2) >>= QC.prj) match {
            case (Some(Map(\/-(SrcHole), mf1)), Some(Map(\/-(SrcHole), mf2))) =>
              QC.inj(Map(src, combine >>= {
                case LeftSide  => mf1
                case RightSide => mf2
              }))
            case (_, _) => TJ.inj(x)
          }
          case (-\/(m1), \/-(SrcHole)) => (FI.prj(m1) >>= QC.prj) match {
            case Some(Map(\/-(SrcHole), mf1)) =>
              QC.inj(Map(src, combine >>= {
                case LeftSide  => mf1
                case RightSide => HoleF
              }))
            case _ => TJ.inj(x)
          }
          case (\/-(SrcHole), -\/(m2)) => (FI.prj(m2) >>= QC.prj) match {
            case Some(Map(\/-(SrcHole), mf2)) =>
              QC.inj(Map(src, combine >>= {
                case LeftSide  => HoleF
                case RightSide => mf2
              }))
            case _ => TJ.inj(x)
          }
          case (_, _) => TJ.inj(x)
        }
      case x => TJ.inj(x)
    }

  def rebaseT[F[_]: Traverse](
    target: FreeQS[T])(
    src: T[F])(
    implicit FI: F :<: QScriptTotal[T, ?]):
      Option[T[F]] =
    freeCata[QScriptTotal[T, ?], T[QScriptTotal[T, ?]], T[QScriptTotal[T, ?]]](
      target.map(_ => src.transAna(FI)))(recover(_.embed)).transAnaM(FI.prj)

  def rebaseTCo[F[_]: Traverse](
    target: FreeQS[T])(
    srcCo: T[CoEnv[Hole, F, ?]])(
    implicit FI: F :<: QScriptTotal[T, ?]):
      Option[T[CoEnv[Hole, F, ?]]] =
    // TODO: with the right instances & types everywhere, this should look like
    //       target.transAnaM(_.htraverse(FI.prj)) ∘ (srcCo >> _)
    freeTransCataM[T, Option, QScriptTotal[T, ?], F, Hole, Hole](target)(
      coEnvHtraverse(_)(λ[QScriptTotal[T, ?] ~> (Option ∘ F)#λ](FI prj _))
    ).map(targ => (srcCo.fromCoEnv >> targ).toCoEnv[T])

  private def rebaseLeft[F[_], G[_]](
    rebase: FreeQS[T] => T[G] => Option[T[G]])(
    tj: ThetaJoin[T, T[G]],
    mf: FreeMap[T])(
    implicit TJ: ThetaJoin[T, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F):
      F[T[G]] =
    rebase(tj.lBranch)(tj.src).fold(TJ.inj(tj))(
      tf => QC.inj(Map(tf, tj.combine >>= {
        case LeftSide  => HoleF
        case RightSide => mf
      })))

  private def rebaseRight[F[_], G[_]](
    rebase: FreeQS[T] => T[G] => Option[T[G]])(
    tj: ThetaJoin[T, T[G]],
    mf: FreeMap[T])(
    implicit TJ: ThetaJoin[T, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F):
      F[T[G]] =
    rebase(tj.rBranch)(tj.src).fold(TJ.inj(tj))(
      tf => QC.inj(Map(tf, tj.combine >>= {
        case LeftSide  => mf
        case RightSide => HoleF
      })))

  private def matchBoth[F[_], G[_]](
    rebase: FreeQS[T] => T[G] => Option[T[G]])(
    tj: ThetaJoin[T, T[G]],
    left: FreeMap[T],
    right: FreeMap[T])(
    implicit TJ: ThetaJoin[T, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F):
      F[T[G]] =
    (left.resume, right.resume) match { // if both sides are Constant, we hit the first case
      case (-\/(Constant(_)), _) => rebaseRight[F, G](rebase)(tj, left)
      case (_, -\/(Constant(_))) => rebaseLeft[F, G](rebase)(tj, right)
      case (-\/(mf), _) if mf.length ≟ 0 => rebaseRight[F, G](rebase)(tj, left)
      case (_, -\/(mf)) if mf.length ≟ 0 => rebaseLeft[F, G](rebase)(tj, right)
      case (_, _) => TJ.inj(tj)
    }

  private def matchLeft[F[_], G[_]](
    rebase: FreeQS[T] => T[G] => Option[T[G]])(
    tj: ThetaJoin[T, T[G]],
    left: FreeMap[T])(
    implicit TJ: ThetaJoin[T, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F):
      F[T[G]] =
    left.resume match {
      case -\/(Constant(_)) => rebaseRight[F, G](rebase)(tj, left)
      case -\/(mf) if mf.length ≟ 0 => rebaseRight[F, G](rebase)(tj, left)
      case _ => TJ.inj(tj)
    }

  private def matchRight[F[_], G[_]](
    rebase: FreeQS[T] => T[G] => Option[T[G]])(
    tj: ThetaJoin[T, T[G]],
    right: FreeMap[T])(
    implicit TJ: ThetaJoin[T, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F):
      F[T[G]] =
    right.resume match {
      case -\/(Constant(_)) => rebaseLeft[F, G](rebase)(tj, right)
      case -\/(mf) if mf.length ≟ 0 => rebaseLeft[F, G](rebase)(tj, right)
      case _ => TJ.inj(tj)
    }

  def elideConstantJoin[F[_], G[_]](
    rebase: FreeQS[T] => T[G] => Option[T[G]])(
    implicit TJ: ThetaJoin[T, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F,
             FI: F :<: QScriptTotal[T, ?]):
      ThetaJoin[T, T[G]] => F[T[G]] = {
    case tj @ ThetaJoin(src, left, right, on, Inner, _) if on ≟ BoolLit(true) =>
      (left.resume.leftMap(_.map(_.resume)), right.resume.leftMap(_.map(_.resume))) match {
        case (-\/(m1), -\/(m2)) => (FI.prj(m1) >>= QC.prj, FI.prj(m2) >>= QC.prj) match {
          // both sides `SrcHole`
          case (Some(Map(\/-(SrcHole), mf1)), Some(Map(\/-(SrcHole), mf2))) =>
            matchBoth[F, G](rebase)(tj, mf1, mf2)
          // both sides size 0 (catches `Unreferenced` and `Root`)
          case (Some(Map(-\/(src1), mf1)), Some(Map(-\/(src2), mf2))) if (src1.length ≟ 0) && (src2.length ≟ 0) =>
            matchBoth[F, G](rebase)(tj, mf1, mf2)
          // left side `SrcHole`, right side size 0
          case (Some(Map(\/-(SrcHole), mf1)), Some(Map(-\/(src2), mf2))) if src2.length ≟ 0 =>
            matchBoth[F, G](rebase)(tj, mf1, mf2)
          // right side `SrcHole`, left side size 0
          case (Some(Map(-\/(src1), mf1)), Some(Map(\/-(SrcHole), mf2))) if src1.length ≟ 0 =>
            matchBoth[F, G](rebase)(tj, mf1, mf2)
          // left side `SrcHole`
          case (Some(Map(\/-(SrcHole), mf1)), _) =>
            matchLeft[F, G](rebase)(tj, mf1)
          // right side `SrcHole`
          case (_, Some(Map(\/-(SrcHole), mf2))) =>
            matchRight[F, G](rebase)(tj, mf2)
          // left side size 0
          case (Some(Map(-\/(src1), mf1)), _) if src1.length ≟ 0 =>
            matchLeft[F, G](rebase)(tj, mf1)
          // right side size 0
          case (_, Some(Map(-\/(src2), mf2))) if src2.length ≟ 0 =>
            matchRight[F, G](rebase)(tj, mf2)
          case (_, _)=> TJ.inj(tj)
        }
        case (_, _) => TJ.inj(tj)
      }
    case tj => TJ.inj(tj)
  }

  /** Replaces [[ThetaJoin]] with [[EquiJoin]], which is often more feasible for
    * connectors to implement. It potentially adds a [[Filter]] iff there are
    * conditions in the [[ThetaJoin]] that can not be handled by an
    * [[EquiJoin]].
    */
  def simplifyJoin[F[_]: Functor]
    (implicit EJ: EquiJoin[T, ?] :<: F, QC: QScriptCore[T, ?] :<: F):
      ThetaJoin[T, T[F]] => F[T[F]] =
    tj => {
      // TODO: This can potentially rewrite conditions to try to get left and right
      //       references on distinct sides.
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
      QC.inj(Map(filter.foldLeft(
        EJ.inj(EquiJoin(
          tj.src,
          tj.lBranch,
          tj.rBranch,
          ConcatArraysN(keys.map(k => Free.roll(MakeArray[T, FreeMap[T]](k.left)))),
          ConcatArraysN(keys.map(k => Free.roll(MakeArray[T, FreeMap[T]](k.right)))),
          tj.f,
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(Free.point(LeftSide))),
            Free.roll(MakeArray(Free.point(RightSide))))))).embed)(
        (ej, filt) => QC.inj(Filter(ej, mergeSides(filt))).embed),
        mergeSides(tj.combine)))
    }

  def coalesceQC[F[_]: Functor, G[_]: Functor]
    (GtoF: PrismNT[G, F])
    (implicit
      QC: QScriptCore[T, ?] :<: F,
      FI: F :<: QScriptTotal[T, ?])
      : QScriptCore[T, T[G]] => Option[QScriptCore[T, T[G]]] = {
    case Map(Embed(src), mf) => GtoF.get(src) >>= QC.prj >>= {
      case Map(srcInner, mfInner) => Map(srcInner, mf >> mfInner).some
      case Reduce(srcInner, bucket, funcs, repair) => Reduce(srcInner, bucket, funcs, mf >> repair).some
      case _ => None
    }
    // TODO: I think this can capture cases where the LS.struct and
    //       Reduce.repair are more complex. Or maybe that’s already simplified
    //       by normalization?
    case LeftShift(Embed(src), struct, shiftRepair) if struct ≟ HoleF =>
      GtoF.get(src) >>= QC.prj >>= {
        case Reduce(srcInner, _, List(ReduceFuncs.UnshiftArray(elem)), redRepair)
            if redRepair ≟ Free.point(ReduceIndex(0)) =>
          shiftRepair.traverseM {
            case LeftSide => None
            case RightSide => elem.some
          } ∘ (Map(srcInner, _))
        case Reduce(srcInner, _, List(ReduceFuncs.UnshiftMap(_, elem)), redRepair)
            if redRepair ≟ Free.point(ReduceIndex(0)) =>
          shiftRepair.traverseM {
            case LeftSide => None
            case RightSide => elem.some
          } ∘ (Map(srcInner, _))
        case _ => None
      }
    // TODO: For Take and Drop, we should be able to pull _most_ of a Reduce repair function to after T/D
    case Take(src, from, count) => // Pull more work to _after_ limiting the dataset
      from.resume.swap.toOption >>= FI.prj >>= QC.prj >>= {
        case Map(fromInner, mf) => Map(GtoF.reverseGet(QC.inj(Take(src, fromInner, count))).embed, mf).some
        case _ => None
      }
    case Drop(src, from, count) => // Pull more work to _after_ limiting the dataset
      from.resume.swap.toOption >>= FI.prj >>= QC.prj >>= {
        case Map(fromInner, mf) => Map(GtoF.reverseGet(QC.inj(Drop(src, fromInner, count))).embed, mf).some
        case _ => None
      }
    case Filter(Embed(src), cond) => GtoF.get(src) >>= QC.prj >>= {
      case Filter(srcInner, condInner) =>
        Filter(srcInner, Free.roll[MapFunc[T, ?], Hole](And(condInner, cond))).some
      case _ => None
    }
    case _ => None
  }

  def coalesceMapShift[F[_], G[_]: Functor](
    GtoF: G ~> λ[α => Option[F[α]]])(
    implicit QC: QScriptCore[T, ?] :<: F):
      QScriptCore[T, T[G]] => F[T[G]] = {
    case x @ Map(Embed(src), mf) => (GtoF(src) >>= QC.prj >>= {
      case LeftShift(srcInner, struct, repair) =>
        QC.inj(LeftShift(srcInner, struct, mf >> repair)).some
      case _ => None
    }).getOrElse(QC.inj(x))
    case x => QC.inj(x)
  }

  def coalesceMapJoin[F[_], G[_]: Functor](
    GtoF: G ~> λ[α => Option[F[α]]])(
    implicit QC: QScriptCore[T, ?] :<: F, TJ: ThetaJoin[T, ?] :<: F):
      QScriptCore[T, T[G]] => F[T[G]] = {
    case x @ Map(Embed(src), mf) =>
      (GtoF(src) >>= TJ.prj).fold(
        QC.inj(x))(
        tj => TJ.inj(ThetaJoin.combine.modify(mf >> (_: JoinFunc[T]))(tj)))
    case x => QC.inj(x)
  }

  def swapMapCount[F[_], G[_]: Functor]
    (GtoF: G ~> (Option ∘ F)#λ)
    (implicit QC: QScriptCore[T, ?] :<: F)
      : QScriptCore[T, T[G]] => QScriptCore[T, T[G]] = {
    case x @ Map(Embed(src), mf) =>
      (GtoF(src) >>= QC.prj).fold[QScriptCore[T, T[G]]] (
        x)(
        {
          case Drop(innerSrc, lb, rb) =>
            Drop(innerSrc,
              Free.roll(Inject[QScriptCore[T, ?], QScriptTotal[T, ?]].inj(Map(lb, mf))),
              rb)
          case Take(innerSrc, lb, rb) =>
            Take(innerSrc,
              Free.roll(Inject[QScriptCore[T, ?], QScriptTotal[T, ?]].inj(Map(lb, mf))),
              rb)
          case _ => x
        })
    case x => x
  }

  def simplifyQC[F[_]: Functor, G[_]: Functor](
    GtoF: PrismNT[G, F])(
    implicit QC: QScriptCore[T, ?] :<: F):
      QScriptCore[T, T[G]] => QScriptCore[T, T[G]] = {
    case Map(src, f) if f.length ≟ 0 =>
      Map(GtoF.reverseGet(QC.inj(Unreferenced[T, T[G]]())).embed, f)
    case x @ LeftShift(src, struct, repair) =>
      if (!repair.element(RightSide))
        Map(src, repair ∘ κ(SrcHole))
      else if (!repair.element(LeftSide))
        (GtoF.get(src.project) >>= QC.prj >>= {
          case Map(innerSrc, mf) =>
            LeftShift(innerSrc, struct >> mf, repair).some
          case _ => None
        }).getOrElse(x)
      else
        x
    case x => x
  }

  def compactLeftShift[F[_], G[_]: Functor](
    implicit QC: QScriptCore[T, ?] :<: F):
      QScriptCore[T, T[G]] => F[T[G]] = {
    case x @ LeftShift(src, struct, repair) => {
      def rewrite(
        src: T[G],
        repair0: JoinFunc[T],
        elem: FreeMap[T],
        dup: FreeMap[T] => Unary[T, FreeMap[T]]):
          F[T[G]] = {
        val repair: T[CoEnv[JoinSide, MapFunc[T, ?], ?]] =
          repair0.toCoEnv[T]

        val rightSide: JoinFunc[T] =
          Free.point[MapFunc[T, ?], JoinSide](RightSide)
        val rightSideCoEnv: T[CoEnv[JoinSide, MapFunc[T, ?], ?]] =
          rightSide.toCoEnv[T]

        def makeRef(idx: Int): T[CoEnv[JoinSide, MapFunc[T, ?], ?]] =
          Free.roll[MapFunc[T, ?], JoinSide](ProjectIndex(rightSide, IntLit(idx))).toCoEnv[T]

        val zeroRef: T[CoEnv[JoinSide, MapFunc[T, ?], ?]] = makeRef(0)
        val oneRef: T[CoEnv[JoinSide, MapFunc[T, ?], ?]] = makeRef(1)

        val rightCount: Int = repair.para(count(rightSideCoEnv))

        if (repair.para(count(zeroRef)) ≟ rightCount) {   // all `RightSide` access is through `zeroRef`
          val replacement: T[CoEnv[JoinSide, MapFunc[T, ?], ?]] =
            transApoT(repair)(substitute(zeroRef, rightSideCoEnv))
          QC.inj(LeftShift(src, Free.roll[MapFunc[T, ?], Hole](dup(elem)), replacement.fromCoEnv))
        } else if (repair.para(count(oneRef)) ≟ rightCount) {   // all `RightSide` access is through `oneRef`
          val replacement: T[CoEnv[JoinSide, MapFunc[T, ?], ?]] =
            transApoT(repair)(substitute(oneRef, rightSideCoEnv))
          QC.inj(LeftShift(src, elem, replacement.fromCoEnv))
        } else {
          QC.inj(x)
        }
      }
      struct.resume match {
        case -\/(ZipArrayIndices(elem)) => rewrite(src, repair, elem, fm => DupArrayIndices(fm))
        case -\/(ZipMapKeys(elem)) => rewrite(src, repair, elem, fm => DupMapKeys(fm))
        case _ => QC.inj(x)
      }
    }
    case x => QC.inj(x)
  }

  def compactReduction[F[_]: Functor]:
      QScriptCore[T, T[F]] => QScriptCore[T, T[F]] = {
    case x @ Reduce(src, bucket, reducers0, repair0) => {
      // `reducers`: the reduce funcs that are used
      // `indices`: the indices into `reducers0` that are used
      val (reducers, indices): (List[ReduceFunc[FreeMap[T]]], List[Int]) = {
        val used: Set[Int] = repair0.foldLeft(Set[Int]()) {
          case (acc, redIdx) => acc + redIdx.idx
        }
        reducers0.zipWithIndex.filter {
          case (_, idx) => used.contains(idx)
        }.unzip
      }

      // reset the indices in `repair0`
      val repair: Free[MapFunc[T, ?], Int] = repair0.map {
        case ReduceIndex(idx) => indices.indexOf(idx)
      }

      if (repair.element(-1))
        x
      else
        Reduce(src, bucket, reducers, repair.map(ReduceIndex(_)))
    }
    case x => x
  }

  // TODO: add reordering
  // - Filter can be moved ahead of Sort
  // - Take/Drop can have a normalized order _if_ their counts are constant
  //   (maybe in some additional cases)
  // - Take/Drop can be moved ahead of Map

  // The order of optimizations is roughly this:
  // - elide NOPs
  // - read conversion given to us by the filesystem
  // - convert any remaning projects to maps
  // - coalesce nodes
  // - normalize mapfunc
  def applyAll[F[_]: Traverse: Normalizable](
    implicit DE: Const[DeadEnd, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F,
             TJ: ThetaJoin[T, ?] :<: F,
             PB: ProjectBucket[T, ?] :<: F,
             FI: F :<: QScriptTotal[T, ?],
             show: Delay[Show, F]):
      F[T[F]] => F[T[F]] =
    (Normalizable[F].normalize(_: F[T[F]])) ⋙
      quasar.fp.free.injectedNT[F](elideNopJoin[F]) ⋙
      liftFG(elideConstantJoin[F, F](rebaseT[F])) ⋙
      liftFF(repeatedly(coalesceQC[F, F](idPrism))) ⋙
      liftFG(coalesceMapShift[F, F](idPrism.get)) ⋙
      liftFG(coalesceMapJoin[F, F](idPrism.get)) ⋙
      liftFF(simplifyQC[F, F](idPrism)) ⋙
      liftFF(swapMapCount[F, F](idPrism.get)) ⋙
      liftFG(compactLeftShift[F, F]) ⋙
      liftFF(compactReduction[F]) ⋙
      liftFG(elideNopMap[F])

  def applyToFreeQS[F[_]: Traverse: Normalizable](
    implicit DE: Const[DeadEnd, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F,
             TJ: ThetaJoin[T, ?] :<: F,
             PB: ProjectBucket[T, ?] :<: F,
             FI: F :<: QScriptTotal[T, ?]):
      F[T[CoEnv[Hole, F, ?]]] => CoEnv[Hole, F, T[CoEnv[Hole, F, ?]]] =
    (Normalizable[F].normalize(_: F[T[CoEnv[Hole, F, ?]]])) ⋙
      quasar.fp.free.injectedNT[F](elideNopJoin[F]) ⋙
      liftFG(elideConstantJoin[F, CoEnv[Hole, F, ?]](rebaseTCo[F])) ⋙
      liftFF(repeatedly(coalesceQC[F, CoEnv[Hole, F, ?]](coenvPrism))) ⋙
      liftFG(coalesceMapShift[F, CoEnv[Hole, F, ?]](coenvPrism.get)) ⋙
      liftFG(coalesceMapJoin[F, CoEnv[Hole, F, ?]](coenvPrism.get)) ⋙
      liftFF(simplifyQC[F, CoEnv[Hole, F, ?]](coenvPrism)) ⋙
      liftFF(swapMapCount[F, CoEnv[Hole, F, ?]](coenvPrism.get)) ⋙
      liftFG(compactLeftShift[F, CoEnv[Hole, F, ?]]) ⋙
      liftFF(compactReduction[CoEnv[Hole, F, ?]]) ⋙
      (fa => QC.prj(fa).fold(CoEnv(fa.right[Hole]))(elideNopMapCo[F, Hole]))  // TODO remove duplication with `elideNopMap`

  /** A backend-or-mount-specific `f` is provided, that allows us to rewrite
    * [[Root]] (and projections, etc.) into [[Read]], so then we can handle
    * exposing only “true” joins and converting intra-data joins to map
    * operations.
    *
    * `f` takes QScript representing a _potential_ path to a file, converts
    * [[Root]] and its children to path, with the operations post-file remaining.
    */
  def pathify[M[_]: MonadFsErr, F[_]: Traverse](
    ls: ConvertPath.ListContents[M])(
    implicit FS: StaticPath.Aux[T, F],
             F: Pathable[T, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F,
             FI: F :<: QScriptTotal[T, ?],
             CP: ConvertPath.Aux[T, Pathable[T, ?], F]):
      T[F] => M[T[QScriptTotal[T, ?]]] =
    _.cataM[M, T[QScriptTotal[T, ?]] \/ T[Pathable[T, ?]]](FS.pathifyƒ[M, F](ls))
      .flatMap(_.fold(_.point[M], FS.toRead[M, F, QScriptTotal[T, ?]](ls)))

  def eliminateProjections[M[_]: MonadFsErr, F[_]: Traverse](
    lsOpt: Option[ConvertPath.ListContents[M]])(
    implicit FS: StaticPath.Aux[T, F],
             F: Pathable[T, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F,
             FI: F :<: QScriptTotal[T, ?],
             CP: ConvertPath.Aux[T, Pathable[T, ?], F]):
      T[F] => M[T[QScriptTotal[T, ?]]] = {
    val simplifyProj = SimplifyProjection[QScriptTotal[T, ?], QScriptTotal[T, ?]].simplifyProjection
    qs => lsOpt.fold(qs.transAna(FI.inj).point[M])(pathify[M, F](_).apply(qs)) map (_.transAna(simplifyProj))
  }
}
