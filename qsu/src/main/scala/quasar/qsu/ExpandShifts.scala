/*
 * Copyright 2014–2019 SlamData Inc.
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

import slamdata.Predef._

import quasar.IdStatus
import quasar.common.effect.NameGenerator
import quasar.contrib.scalaz._
import quasar.ejson.{EJson, Fixed}
import quasar.qscript.{
  MonadPlannerErr,
  OnUndefined,
  construction
}
import quasar.qscript.RecFreeS._
import quasar.qsu.{QScriptUniform => QSU}, QSU.Rotation
import quasar.qsu.QSUGraph.Extractors._
import quasar.qsu.ApplyProvenance.AuthenticatedQSU
import quasar.contrib.scalaz._

import matryoshka.{Hole => _, _}
import monocle.syntax.fields._
import scalaz._
import scalaz.Scalaz._
import StateT.stateTMonadState

sealed abstract class ExpandShifts[T[_[_]]: BirecursiveT: EqualT: ShowT] extends MraPhase[T] {

  implicit def PEqual: Equal[P]

  private val func = construction.Func[T]
  private val json = Fixed[T[EJson]]

  private type QAuthS[F[_]] = MonadState_[F, QAuth[P]]
  private type S = (QAuth[P], RevIdx)

  private implicit def qauthState[F[_]: Monad]: MonadState_[StateT[F, S, ?], QAuth[P]] =
    MonadState_.zoom[StateT[F, S, ?]](_1[S, QAuth[P]])

  private implicit def revIdxState[F[_]: Monad]: MonadState_[StateT[F, S, ?], RevIdx] =
    MonadState_.zoom[StateT[F, S, ?]](_2[S, RevIdx])

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def apply[F[_]: Monad: NameGenerator: MonadPlannerErr](aqsu: AuthenticatedQSU[T, P])
      : F[AuthenticatedQSU[T, P]] =
    aqsu.graph.rewriteM[StateT[F, S, ?]](expandShifts[StateT[F, S, ?]])
      .run((aqsu.auth, aqsu.graph.generateRevIndex))
      .map { case ((auth, _), graph) => AuthenticatedQSU(graph, auth) }

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  def expandShifts[G[_]: Monad: NameGenerator: RevIdxM: MonadPlannerErr: QAuthS]
      : PartialFunction[QSUGraph, G[QSUGraph]] = {
    case mls @ MultiLeftShift(source, shifts, _, repair) =>
      shifts.toNel.fold(source.pure[G]) { shifts1 =>
        val indexed = shifts.zipWithIndex

        val shiftedG = for {
          headShift <- buildShift[G](source.root, source, indexed.head)
          expandedShifts <- indexed.tail.foldLeftM(headShift)(buildShift[G](source.root, _, _))
        } yield expandedShifts

        val mapper = repair.flatMap {
          case -\/(_) => PrjOriginal
          case \/-(i) => func.ProjectKeyS(func.Hole, i.toString)
        }

        shiftedG map { shifted =>
          mls.overwriteAtRoot(QSU.Map[T, Symbol](shifted.root, mapper.asRec)) :++ shifted
        }
      }
  }

  ////

  private val OriginalKey = "original"
  private val NamePrefix = "esh"
  private val SrcVal = AccessLeftTarget[T](Access.value(_))
  private val PrjOriginal = func.ProjectKeyS(func.Hole, OriginalKey)

  private def buildShift[G[_]: Monad: NameGenerator: RevIdxM: MonadPlannerErr: QAuthS](
      commonRoot: Symbol,
      src: QSUGraph,
      shift: ((FreeMap, IdStatus, Rotation), Int))
      : G[QSUGraph] = {

    val ((struct, status, rotation), idx) = shift

    val repair =
      func.ConcatMaps(
        if (idx === 0) func.MakeMapS(OriginalKey, SrcVal) else SrcVal,
        func.MakeMapS(idx.toString, RightTarget))

    val adjustedStruct =
      if (idx === 0) struct else struct >> PrjOriginal

    // Use `src.root` to ensure we get a fresh name, if we use `commonRoot` it will
    // find the original shift in the reverse index.
    //
    // Eventually, we'll probably want the original shift as, if its provenance was
    // correct we'd just use it. As-is, it contains existentials due to uncollapsed
    // mappable regions prior to provenance computation.
    val tempShiftPat =
      QSU.LeftShift[T, Symbol](
        src.root, struct.asRec, status, OnUndefined.Emit, RightTarget, rotation)

    for {
      tempShift <- QSUGraph.withName[T, G](NamePrefix)(tempShiftPat)
      commonShift = tempShift.overwriteAtRoot(tempShiftPat.copy(source = commonRoot))

      // compute provenance for this shift on the common source.
      newDims <- ApplyProvenance.computeDims[T, G](qprov, commonShift)

      _ <- MonadState_[G, QAuth[P]].modify(_.addDims(commonShift.root, newDims))

      newShift = commonShift overwriteAtRoot {
        tempShiftPat.copy(
          struct = adjustedStruct.asRec,
          repair = repair)
      }
    } yield newShift :++ src
  }
}

object ExpandShifts {
  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT,
      F[_]: Monad: NameGenerator: MonadPlannerErr](
      qp: QProv[T])(qgraph: AuthenticatedQSU[T, qp.P])(
      implicit P: Equal[qp.P])
      : F[AuthenticatedQSU[T, qp.P]] = {

    val es = new ExpandShifts[T] {
      val qprov: qp.type = qp
      val PEqual = P
    }

    taggedInternalError("ExpandShifts", es[F](qgraph))
  }
}
