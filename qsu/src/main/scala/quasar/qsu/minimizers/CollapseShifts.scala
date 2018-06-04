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

package quasar.qsu
package minimizers

import slamdata.Predef._
import quasar.RenderTreeT
import quasar.contrib.matryoshka._
import quasar.effect.NameGenerator
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski.κ
import quasar.fs.Planner, Planner.PlannerErrorME
import quasar.qscript.{
  construction,
  Hole,
  HoleF,
  IdStatus,
  JoinSide,
  LeftSide,
  OnUndefined,
  RightSide,
  SrcHole,
  RecFreeS
}
import quasar.qscript.RecFreeS._
import quasar.qscript.rewrites.NormalizableT
import quasar.qsu.{QScriptUniform => QSU}, QSU.ShiftTarget

import matryoshka.{delayEqual, BirecursiveT, EqualT, ShowT}
import matryoshka.data.free._
import scalaz.{
  -\/,
  \/-,
  \/,
  Free,
  Monad,
  NonEmptyList => NEL,
  Scalaz,
}, Scalaz._


final class CollapseShifts[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] private () extends Minimizer[T] {
  import MinimizeAutoJoins._
  import QSUGraph.Extractors._

  private type ShiftGraph = QSU.LeftShift[T, QSUGraph] \/ QSU.MultiLeftShift[T, QSUGraph]

  private val func = construction.Func[T]
  private val recFunc = construction.RecFunc[T]
  private val N = new NormalizableT[T]

  private val ResultsField = "results"
  private val OriginalField = "original"

  private val LeftField = "left"
  private val RightField = "right"

  private val accessHoleLeftF =
    Access.valueHole[T[EJson]](SrcHole).left[Int].point[FreeMapA]

  def couldApplyTo(candidates: List[QSUGraph]): Boolean =
    candidates exists { case ConsecutiveUnbounded(_, _) => true; case _ => false }

  def extract[
      G[_]: Monad: NameGenerator: PlannerErrorME: RevIdxM: MinStateM[T, ?[_]]](
      qgraph: QSUGraph): Option[(QSUGraph, (QSUGraph, FreeMap) => G[QSUGraph])] = qgraph match {

    case ConsecutiveUnbounded(src, shifts) =>
      def rebuild(src: QSUGraph, fm: FreeMap): G[QSUGraph] = {
        val reversed = shifts.reverse

        val rest = reversed.tail

        val initM = reversed.head match {
          case -\/(QSU.LeftShift(_, struct, idStatus, onUndefined, repair, rot)) =>
            val struct2 = struct >> RecFreeS.fromFree(fm)

            val repair2 = repair flatMap {
              case ShiftTarget.AccessLeftTarget(Access.Value(_)) =>
                fm.map[ShiftTarget[T]](
                  κ(ShiftTarget.AccessLeftTarget[T](Access.valueHole(SrcHole))))

              case access @ ShiftTarget.AccessLeftTarget(_) =>
                (access: ShiftTarget[T]).pure[FreeMapA]

              case ShiftTarget.LeftTarget() =>
                scala.sys.error("ShiftTarget.LeftTarget in CollapseShifts")

              case ShiftTarget.RightTarget() =>
                RightTarget[T]
            }

            updateGraph[T, G](QSU.LeftShift(src.root, struct2, idStatus, onUndefined, repair2, rot)) map { rewritten =>
              rewritten :++ src
            }

          case \/-(QSU.MultiLeftShift(_, shifts, onUndefined, repair)) =>
            val shifts2 = shifts map {
              case (struct, idStatus, rot) =>
                (struct >> fm, idStatus, rot)
            }

            val repair2 = repair flatMap {
              case -\/(access) =>
                fm.map[QAccess[Hole] \/ Int](κ(-\/(access)))

              case \/-(idx) =>
                idx.right[QAccess[Hole]].point[FreeMapA]
            }

            updateGraph[T, G](QSU.MultiLeftShift(src.root, shifts2, onUndefined, repair2)) map { rewritten =>
              rewritten :++ src
            }
        }

        for {
          init <- initM

          back <- rest.foldLeftM[G, QSUGraph](init) {
            case (src, -\/(QSU.LeftShift(_, struct, idStatus, onUndefined, repair, rot))) =>
              updateGraph[T, G](QSU.LeftShift(src.root, struct, idStatus, onUndefined, repair, rot)) map { rewritten =>
                rewritten :++ src
              }

            case (src, \/-(QSU.MultiLeftShift(_, shifts, onUndefined, repair))) =>
              updateGraph[T, G](QSU.MultiLeftShift(src.root, shifts, onUndefined, repair)) map { rewritten =>
                rewritten :++ src
              }
          }
        } yield back
      }

      Some((src, rebuild _))

    // we're potentially defined for all sides so long as there's a left shift around
    case qgraph =>
      def rebuild(src: QSUGraph, fm: FreeMap): G[QSUGraph] = {
        if (fm === HoleF[T]) {
          src.point[G]
        } else {
          // this case should never happen
          updateGraph[T, G](QSU.Map(src.root, fm.asRec)) map { rewritten =>
            rewritten :++ src
          }
        }
      }

      Some((qgraph, rebuild _))
  }

  /*
   * Builds some intermediate structure:
   *
   * - Results are wrapped in { "0": ..., "1": ..., etc }, matching the index of
   *   the inbound candidate and thus also the reference within fm.  The root will
   *   be overwritten with a Map that applies fm to ProjectKeyS(Hole, i.toString).
   *   This wrapping is done before any of the coalescences are considered, and the
   *   candidate shifts are corrected to ensure validity and consistency in light of
   *   this wrapping.
   *
   * - In the event of a flat coalescence with multiple shifts (e.g. `a[*][*] + b`),
   *   the original upstream value (Hole of the upper struct) will be wrapped in
   *   { "original": ... }, while the result at each stage will be wrapped in
   *   { "results": ... }, with both maps concatenated together in the repair.
   *
   * - In the event of a multi-coalesce (e.g. a[*] + b[*]), the coalescence is handled
   *   pair-wise with the left and right sides wrapped in { "left": ... } and
   *   { "right": ... }, respectively.  This allows each side to continue the shift chain
   *   in the event that each side has several stages.  It also allows access to a
   *   terminal value in the event that the chains are uneven (e.g. a[*][*] + b[*]).
   *
   * These wrappings compose together, but the second two should be entirely invisible
   * in the final output.  In other words, the final output should simply be wrapped in
   * the index object, enabling the final Map node to overwrite at root.  The second
   * and third wrappings are simply for bookkeeping purposes within each coalescence.
   */
  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  def apply[
      G[_]: Monad: NameGenerator: PlannerErrorME: RevIdxM: MinStateM[T, ?[_]]](
      qgraph: QSUGraph,
      src: QSUGraph,
      candidates: List[QSUGraph],
      fm: FreeMapA[Int]): G[Option[(QSUGraph, QSUGraph)]] = {

    val ConsecutiveBounded = ConsecutiveLeftShifts(_.root === src.root)

    def mergeIndexMaps(
        parent: QSUGraph,
        leftHole: FreeMap,
        leftIndices: Set[Int],
        rightHole: FreeMap,
        rightIndices: Set[Int]): G[QSUGraph] = {

      val leftMaps = leftIndices.toList map { i =>
        func.MakeMapS(i.toString, func.ProjectKeyS(leftHole, i.toString))
      }

      val leftFM = leftMaps reduceOption { (left, right) =>
        func.ConcatMaps(left, right)
      } getOrElse leftHole

      val rightMaps = rightIndices.toList map { i =>
        func.MakeMapS(i.toString, func.ProjectKeyS(rightHole, i.toString))
      }

      val rightFM = rightMaps reduceOption { (left, right) =>
        func.ConcatMaps(left, right)
      } getOrElse rightHole

      updateGraph[T, G](QSU.Map(parent.root, recFunc.ConcatMaps(leftFM.asRec, rightFM.asRec))) map { rewritten =>
        rewritten :++ parent
      }
    }

    def coalesceUneven(shifts: NEL[ShiftGraph], qgraph: QSUGraph): G[QSUGraph] = {
      val origFM = qgraph match {
        case Map(_, fm) => fm
        case _ => recFunc.Hole
      }

      val reversed = shifts.reverse

      val initPattern = reversed.head match {
        case -\/(QSU.LeftShift(_, struct, idStatus, _, repair, rot)) =>
          val repair2 = func.StaticMapS(
            OriginalField -> AccessLeftTarget[T](Access.valueHole[T[EJson]](_)),
            ResultsField -> repair)

          QSU.LeftShift[T, Symbol](src.root, struct, idStatus, OnUndefined.Emit, repair2, rot)

        case \/-(QSU.MultiLeftShift(_, shifts, _, repair)) =>
          val repair2 = func.StaticMapS(
            OriginalField -> accessHoleLeftF,
            ResultsField -> repair)

          QSU.MultiLeftShift[T, Symbol](src.root, shifts, OnUndefined.Emit, repair2)
      }

      for {
        init2 <- updateGraph[T, G](initPattern) map (pat => pat :++ src)

        reconstructed <- reversed.tail.foldLeftM[G, QSUGraph](init2) {
          case (src, -\/(QSU.LeftShift(_, struct, idStatus, _, repair, rot))) =>
            val struct2 = struct >> recFunc.ProjectKeyS(recFunc.Hole, ResultsField)

            val repair2 = repair flatMap {
              case ShiftTarget.AccessLeftTarget(Access.Value(_)) =>
                func.ProjectKeyS(
                  AccessLeftTarget[T](Access.valueHole[T[EJson]](_)),
                  ResultsField)

              case ShiftTarget.AccessLeftTarget(access) =>
                AccessLeftTarget[T](access.as(_))

              case ShiftTarget.LeftTarget() =>
                scala.sys.error("ShiftTarget.LeftTarget in CollapseShifts")

              case ShiftTarget.RightTarget() =>
                RightTarget[T]
            }

            val repair3 = func.StaticMapS(
              OriginalField ->
                func.ProjectKeyS(
                  AccessLeftTarget[T](Access.valueHole[T[EJson]](_)),
                  OriginalField),
              ResultsField -> repair2)

            updateGraph[T, G](QSU.LeftShift[T, Symbol](src.root, struct2, idStatus, OnUndefined.Emit, repair3, rot)) map { rewritten =>
              rewritten :++ src
            }

          case (src, \/-(QSU.MultiLeftShift(_, shifts, _, repair))) =>
            val shifts2 = shifts map {
              case (struct, idStatus, rot) =>
                val struct2 = struct >> func.ProjectKeyS(func.Hole, ResultsField)

                (struct2, idStatus, rot)
            }

            val repair2 = repair flatMap {
              case -\/(access) =>
                func.ProjectKeyS(access.left[Int].point[FreeMapA], ResultsField)

              case \/-(idx) => idx.right[QAccess[Hole]].point[FreeMapA]
            }

            val repair3 = func.StaticMapS(
              OriginalField ->
                func.ProjectKeyS(accessHoleLeftF, OriginalField),
              ResultsField -> repair2)

            updateGraph[T, G](QSU.MultiLeftShift[T, Symbol](src.root, shifts2, OnUndefined.Emit, repair3)) map { rewritten =>
              rewritten :++ src
            }
        }

        rewritten = reconstructed match {
          case reconstructed @ LeftShift(src, struct, idStatus, onUndefined, repair, rot) =>
            val origLifted = origFM >> recFunc.ProjectKeyS(repair.asRec, OriginalField)
            val repair2 = func.ConcatMaps(func.ProjectKeyS(repair, ResultsField), origLifted.linearize)

            reconstructed.overwriteAtRoot(
              QSU.LeftShift(src.root, struct, idStatus, onUndefined, N.freeMF(repair2), rot))

          case reconstructed @ MultiLeftShift(src, shifts, onUndefined, repair) =>
            val origLifted = origFM >> recFunc.ProjectKeyS(repair.asRec, OriginalField)
            val repair2 = func.ConcatMaps(func.ProjectKeyS(repair, ResultsField), origLifted.linearize)

            reconstructed.overwriteAtRoot(
              QSU.MultiLeftShift(src.root, shifts, onUndefined, repair2))

          case reconstructed => reconstructed
        }
      } yield rewritten
    }

    def coalesceZip(
        left: List[ShiftGraph],
        leftIndices: Set[Int],
        right: List[ShiftGraph],
        rightIndices: Set[Int],
        parent: Option[QSUGraph]): G[QSUGraph] = {

      val hasParent = parent.isDefined

      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def continue(
          fakeParent: QSUGraph,
          tailL: List[ShiftGraph],
          tailR: List[ShiftGraph])(
          pat: Symbol => QSU[T, Symbol]): G[QSUGraph] = {

        val realParent = parent.getOrElse(fakeParent)
        val cont = updateGraph[T, G](pat(realParent.root)) map { rewritten =>
          Some(rewritten :++ realParent)
        }

        cont.flatMap(coalesceZip(tailL, leftIndices, tailR, rightIndices, _))
      }

      def name(side: JoinSide) = side.fold(LeftField, RightField)

      def fixSingleStruct(struct: FreeMap, side: JoinSide): FreeMap = {
        if (hasParent)
          struct >> func.ProjectKeyS(func.Hole, name(side))
        else
          struct
      }

      def fixSingleRepairForMulti(
          repair: FreeMapA[ShiftTarget[T]],
          offset: Int,
          side: JoinSide): FreeMapA[QAccess[Hole] \/ Int] = {

        repair flatMap {
          case ShiftTarget.LeftTarget() =>
            scala.sys.error("ShiftTarget.LeftTarget in CollapseShifts")

          case ShiftTarget.AccessLeftTarget(access) =>
            val hole = Free.pure[MapFunc, QAccess[Hole] \/ Int](access.left[Int])

            if (hasParent)
              func.ProjectKeyS(hole, name(side))
            else
              hole

          case ShiftTarget.RightTarget() =>
            Free.pure(offset.right[QAccess[Hole]])
        }
      }

      def fixSingleRepairForSingle(
          repair: FreeMapA[ShiftTarget[T]],
          side: JoinSide): FreeMapA[ShiftTarget[T]] = {

        repair flatMap {
          case target @ (ShiftTarget.LeftTarget() | ShiftTarget.RightTarget()) =>
            Free.pure(target)

          case access @ ShiftTarget.AccessLeftTarget(_) =>
            val hole = Free.pure[MapFunc, ShiftTarget[T]](access)

            if (hasParent)
              func.ProjectKeyS(hole, name(side))
            else
              hole
        }
      }

      def fixMultiShifts(
          shifts: List[(FreeMap, IdStatus, QSU.Rotation)],
          side: JoinSide): List[(FreeMap, IdStatus, QSU.Rotation)] = {

        shifts map {
          case (struct, idStatus, rot) =>
            val struct2 = if (hasParent)
              struct >> func.ProjectKeyS(func.Hole, name(side))
            else
              struct

            (struct2, idStatus, rot)
        }
      }

      def fixMultiRepair(
          repair: FreeMapA[QAccess[Hole] \/ Int],
          offset: Int,
          side: JoinSide): FreeMapA[QAccess[Hole] \/ Int] = {

        repair flatMap {
          case -\/(access) =>
            if (hasParent)
              func.ProjectKeyS(Free.pure(access.left[Int]), name(side))
            else
              Free.pure(access.left[Int])

          case \/-(i) =>
            Free.pure((i + offset).right[QAccess[Hole]])
        }
      }

      (left, right) match {
        case
          (
            -\/(QSU.LeftShift(fakeParent, structL, idStatusL, _, repairL, rotL)) :: tailL,
            -\/(QSU.LeftShift(_, structR, idStatusR, _, repairR, rotR)) :: tailR) =>

          val structLAdj = fixSingleStruct(structL.linearize, LeftSide)
          val repairLAdj = fixSingleRepairForMulti(repairL, 0, LeftSide)

          val structRAdj = fixSingleStruct(structR.linearize, RightSide)
          val repairRAdj = fixSingleRepairForMulti(repairR, 1, RightSide)

          val repair =
            func.StaticMapS(
              LeftField -> repairLAdj,
              RightField -> repairRAdj)

          continue(fakeParent, tailL, tailR) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              (structLAdj, idStatusL, rotL) :: (structRAdj, idStatusR, rotR) :: Nil,
              OnUndefined.Emit,
              repair)
          }

        case
          (
            \/-(QSU.MultiLeftShift(fakeParent, shiftsL, _, repairL)) :: tailL,
            -\/(QSU.LeftShift(_, structR, idStatusR, _, repairR, rotR)) :: tailR) =>

          val offset = shiftsL.length

          val shiftsLAdj = fixMultiShifts(shiftsL, LeftSide)
          val repairLAdj = fixMultiRepair(repairL, 0, LeftSide)

          val structRAdj = fixSingleStruct(structR.linearize, RightSide)
          val repairRAdj = fixSingleRepairForMulti(repairR, offset, RightSide)

          val repair =
            func.StaticMapS(
              LeftField -> repairLAdj,
              RightField -> repairRAdj)

          continue(fakeParent, tailL, tailR) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              shiftsLAdj ::: (structRAdj, idStatusR, rotR) :: Nil,
              OnUndefined.Emit,
              repair)
          }

        case
          (
            -\/(QSU.LeftShift(fakeParent, structL, idStatusL, _, repairL, rotL)) :: tailL,
            \/-(QSU.MultiLeftShift(_, shiftsR, _, repairR)) :: tailR) =>

          val structLAdj = fixSingleStruct(structL.linearize, LeftSide)
          val repairLAdj = fixSingleRepairForMulti(repairL, 0, LeftSide)

          val shiftsRAdj = fixMultiShifts(shiftsR, RightSide)
          val repairRAdj = fixMultiRepair(repairR, 1, RightSide)

          val repair =
            func.StaticMapS(
              LeftField -> repairLAdj,
              RightField -> repairRAdj)

          continue(fakeParent, tailL, tailR) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              (structL.linearize, idStatusL, rotL) :: shiftsRAdj,
              OnUndefined.Emit,
              repair)
          }

        case
          (
            \/-(QSU.MultiLeftShift(fakeParent, shiftsL, _, repairL)) :: tailL,
            \/-(QSU.MultiLeftShift(_, shiftsR, _, repairR)) :: tailR) =>

          val offset = shiftsL.length

          val shiftsLAdj = fixMultiShifts(shiftsL, LeftSide)
          val repairLAdj = fixMultiRepair(repairL, 0, LeftSide)

          val shiftsRAdj = fixMultiShifts(shiftsR, RightSide)
          val repairRAdj = fixMultiRepair(repairR, offset, RightSide)

          val repair =
            func.StaticMapS(
              LeftField -> repairLAdj,
              RightField -> repairRAdj)

          continue(fakeParent, tailL, tailR) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              shiftsLAdj ::: shiftsRAdj,
              OnUndefined.Emit,
              repair)
          }

        case
          (
            -\/(QSU.LeftShift(fakeParent, structL, idStatusL, _, repairL, rotL)) :: tailL,
            Nil) =>

          val structLAdj = fixSingleStruct(structL.linearize, LeftSide)
          val repairLAdj = fixSingleRepairForSingle(repairL, LeftSide)

          val repair = func.StaticMapS(
            LeftField -> repairLAdj,
            RightField ->
              (if (hasParent)
                func.ProjectKeyS(AccessLeftTarget[T](Access.value(_)), RightField)
              else
                AccessLeftTarget[T](Access.value(_))))

          continue(fakeParent, tailL, Nil) { sym =>
            QSU.LeftShift[T, Symbol](
              sym,
              RecFreeS.fromFree(structLAdj),
              idStatusL,
              OnUndefined.Emit,
              repair,
              rotL)
          }

        case
          (
            \/-(QSU.MultiLeftShift(fakeParent, shiftsL, _, repairL)) :: tailL,
            Nil) =>

          val shiftsLAdj = fixMultiShifts(shiftsL, LeftSide)
          val repairLAdj = fixMultiRepair(repairL, 0, LeftSide)

          val repair = func.StaticMapS(
            LeftField -> repairLAdj,
            RightField ->
              (if (hasParent)
                func.ProjectKeyS(AccessHole[T].map(_.left[Int]), RightField)
              else
                AccessHole[T].map(_.left[Int])))

          continue(fakeParent, tailL, Nil) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              shiftsLAdj,
              OnUndefined.Emit,
              repair)
          }

        case
          (
            Nil,
            -\/(QSU.LeftShift(fakeParent, structR, idStatusR, _, repairR, rotR)) :: tailR) =>

          val structRAdj = fixSingleStruct(structR.linearize, RightSide)
          val repairRAdj = fixSingleRepairForSingle(repairR, RightSide)

          val repair = func.StaticMapS(
            LeftField ->
              (if (hasParent)
                func.ProjectKeyS(AccessLeftTarget[T](Access.value(_)), LeftField)
              else
                AccessLeftTarget[T](Access.value(_))),
            RightField -> repairRAdj)

          continue(fakeParent, Nil, tailR) { sym =>
            QSU.LeftShift[T, Symbol](
              sym,
              RecFreeS.fromFree(structRAdj),
              idStatusR,
              OnUndefined.Emit,
              repair,
              rotR)
          }

        case
          (
            Nil,
            \/-(QSU.MultiLeftShift(fakeParent, shiftsR, _, repairR)) :: tailR) =>

          val shiftsRAdj = fixMultiShifts(shiftsR, LeftSide)
          val repairRAdj = fixMultiRepair(repairR, 0, LeftSide)

          val repair = func.StaticMapS(
            LeftField ->
              (if (hasParent)
                func.ProjectKeyS(AccessHole[T].map(_.left[Int]), LeftField)
              else
                AccessHole[T].map(_.left[Int])),
            RightField -> repairRAdj)

          continue(fakeParent, Nil, tailR) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              shiftsRAdj,
              OnUndefined.Emit,
              repair)
          }

        case (Nil, Nil) =>
          // if parent is None here, it means we invoked on empty lists
          mergeIndexMaps(
            parent.getOrElse(???),
            func.ProjectKeyS(func.Hole, "left"),
            leftIndices,
            func.ProjectKeyS(func.Hole, "right"),
            rightIndices)
      }
    }

    def wrapCandidate(candidateGraph: QSUGraph, index: Int): G[(QSUGraph, Set[Int])] =
      candidateGraph match {
        case (qgraph @ ConsecutiveBounded(_, shifts)) =>
          // qgraph must beLike(shifts.head)

          // shifts.head is the LAST shift in the chain
          val back = shifts.head match {
            case -\/(QSU.LeftShift(parent, struct, idStatus, onUndefined, repair, rotation)) =>
              val pat = QSU.LeftShift(
                parent.root,
                struct,
                idStatus,
                onUndefined,
                func.MakeMapS(index.toString, repair),
                rotation)

              qgraph.overwriteAtRoot(pat).point[G]

            case \/-(QSU.MultiLeftShift(parent, shifts, onUndefined, repair)) =>
              val pat = QSU.MultiLeftShift(
                parent.root,
                shifts,
                onUndefined,
                func.MakeMapS(index.toString, repair))

              qgraph.overwriteAtRoot(pat).point[G]
          }

          back.map(g => (g, Set(index)))

        case (qgraph @ Map(parent, fm)) =>
          val back = qgraph.overwriteAtRoot(QSU.Map(parent.root, recFunc.MakeMapS(index.toString, fm))).point[G]
          back.map(g => (g, Set(index)))

        case qgraph =>
          val back = updateGraph[T, G](QSU.Map(qgraph.root, recFunc.MakeMapS(index.toString, recFunc.Hole))) map { rewritten =>
            rewritten :++ qgraph
          }

          back.map(g => (g, Set(index)))
      }

    def inlineMap(g: QSUGraph): Option[QSUGraph] = g match {
      case Map(LeftShift(src, struct, idStatus, onUndefined, repair, rot), fm) =>
        val repair2 = fm.linearize >> repair
        g.overwriteAtRoot(QSU.LeftShift(src.root, struct, idStatus, onUndefined, N.freeMF(repair2), rot)).some
      case Map(MultiLeftShift(src, shifts, onUndefined, repair), fm) =>
        val repair2 = fm.linearize >> repair
        g.overwriteAtRoot(QSU.MultiLeftShift(src.root, shifts, onUndefined, repair2)).some
      case _ => none
    }

    for {
      // converts all candidates to produce final results wrapped in their relevant indices
      wrapped <- candidates.zipWithIndex traverse { case (g, i) => wrapCandidate(g, i) }

      coalescedPair <- wrapped.tail.foldLeftM[G, (QSUGraph, Set[Int])](wrapped.head) {
        case (g0 @ (ConsecutiveBounded(_, shifts1), leftIndices), g1 @ (ConsecutiveBounded(_, shifts2), rightIndices)) =>
          val back = coalesceZip(shifts1.toList.reverse, leftIndices, shifts2.toList.reverse, rightIndices, None)

          back.map(g => (inlineMap(g).getOrElse(g), leftIndices ++ rightIndices))

        case ((qgraph, leftIndices), (ConsecutiveBounded(_, shifts), rightIndices)) =>
          val back = coalesceUneven(shifts, qgraph)

          back.map(g => (inlineMap(g).getOrElse(g), leftIndices ++ rightIndices))

        case ((ConsecutiveBounded(_, shifts), leftIndices), (qgraph, rightIndices)) =>
          val back = coalesceUneven(shifts, qgraph)

          back.map(g => (inlineMap(g).getOrElse(g), leftIndices ++ rightIndices))

        // these two graphs have to be maps on the same thing
        // if they aren't, we're in trouble
        case ((Map(parent1, left), leftIndices), (Map(parent2, right), rightIndices)) =>
          scala.Predef.assert(parent1.root === parent2.root)

          val back = mergeIndexMaps(
            parent1,
            left.linearize,
            leftIndices,
            right.linearize,
            rightIndices)

          back.map(g => (g, leftIndices ++ rightIndices))
      }

      (coalesced, _) = coalescedPair

      // we build the map node to overwrite the original autojoin (qgraph)
      back = qgraph.overwriteAtRoot(
        QSU.Map(
          coalesced.root,
          fm.flatMap(i => func.ProjectKeyS(func.Hole, i.toString)).asRec)) :++ coalesced
    } yield Some((coalesced, back))
  }

  private val ConsecutiveUnbounded = ConsecutiveLeftShifts(κ(false))

  // TODO support freemappable and Cond-able regions between shifts
  private def ConsecutiveLeftShifts(stop: QSUGraph => Boolean): Extractor = new Extractor {

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    lazy val Self = ConsecutiveLeftShifts(stop)

    def unapply(qgraph: QSUGraph)
        : Option[(QSUGraph, NEL[ShiftGraph])] = qgraph match {

      case qgraph if stop(qgraph) =>
        None

      case LeftShift(Read(_), _, _, _, _, _) =>
        None

      case
        LeftShift(
          MappableRegion.MaximalUnary(oparent @ Self(parent, inners), fm),
          struct,
          idStatus,
          onUndefined,
          repair,
          rot) =>

        val struct2 = struct >> fm.asRec

        val repair2 = repair flatMap {
          case alt @ ShiftTarget.AccessLeftTarget(Access.Value(_)) =>
            fm.map(_ => alt: ShiftTarget[T])

          case t => Free.pure[MapFunc, ShiftTarget[T]](t)
        }

        Some((parent, -\/(QSU.LeftShift[T, QSUGraph](oparent, struct2, idStatus, onUndefined, repair2, rot)) <:: inners))

      case LeftShift(parent, struct, idStatus, onUndefined, repair, rot) =>
        Some((parent, NEL(-\/(QSU.LeftShift[T, QSUGraph](parent, struct, idStatus, onUndefined, repair, rot)))))

      case
        MultiLeftShift(
          MappableRegion.MaximalUnary(oparent @ Self(parent, inners), fm),
          shifts,
          onUndefined,
          repair) =>

        val shifts2 = shifts map {
          case (struct, idStatus, rot) =>
            (struct >> fm, idStatus, rot)
        }

        val repair2 = repair flatMap {
          case l @ -\/(Access.Value(_)) =>
            fm.map(_ => l: (QAccess[Hole] \/ Int))

          case r => Free.pure[MapFunc, QAccess[Hole] \/ Int](r)
        }

        Some((parent, \/-(QSU.MultiLeftShift[T, QSUGraph](oparent, shifts2, onUndefined, repair2)) <:: inners))

      case MultiLeftShift(parent, shifts, onUndefined, repair) =>
        Some((parent, NEL(\/-(QSU.MultiLeftShift[T, QSUGraph](parent, shifts, onUndefined, repair)))))

      case Map(Self(parent, inners), fm) => Some((parent, inners))

      case _ =>
        None
    }
  }

  private trait Extractor {
    def unapply(qgraph: QSUGraph) : Option[(QSUGraph, NEL[ShiftGraph])]
  }
}

object CollapseShifts {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: CollapseShifts[T] =
    new CollapseShifts[T]
}
