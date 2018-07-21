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

import slamdata.Predef._

import quasar.common.effect.NameGenerator
import quasar.contrib.scalaz._
import quasar.ejson.{EJson, Fixed}
import quasar.qscript.{
  Hole,
  MonadPlannerErr,
  OnUndefined,
  RecFreeS,
  SrcHole,
  construction
}
import quasar.qscript.RecFreeS._
import quasar.qsu.{QScriptUniform => QSU}
import QSU.Rotation
import quasar.qsu.QSUGraph.Extractors._
import quasar.qsu.ApplyProvenance.AuthenticatedQSU
import quasar.contrib.scalaz._

import matryoshka.{Hole => _, _}
import scalaz._
import scalaz.Scalaz._
import StateT.stateTMonadState

final class ExpandShifts[T[_[_]]: BirecursiveT: EqualT: ShowT] extends QSUTTypes[T] {
  val func = construction.Func[T]
  val json = Fixed[T[EJson]]
  val hole: Hole = SrcHole

  private val prov = new QProv[T]
  private type P = prov.P
  private type AuthT[F[_], A] = StateT[F, QAuth, A]

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def apply[F[_]: Monad: NameGenerator: MonadPlannerErr](aqsu: AuthenticatedQSU[T]): F[AuthenticatedQSU[T]] = {
    type G[A] = AuthT[StateT[F, RevIdx, ?], A]

    for {
      pair <- aqsu.graph.rewriteM[G](expandShifts[G]).run(aqsu.auth).eval(aqsu.graph.generateRevIndex)
      (auth, graph) = pair
    } yield AuthenticatedQSU(graph, auth)
  }

  val originalKey = "original"
  val namePrefix = "esh"

  def rotationsCompatible(rotation1: Rotation, rotation2: Rotation): Boolean = rotation1 match {
    case Rotation.FlattenArray | Rotation.ShiftArray =>
      rotation2 === Rotation.FlattenArray || rotation2 === Rotation.ShiftArray
    case Rotation.FlattenMap | Rotation.ShiftMap =>
      rotation2 === Rotation.FlattenMap || rotation2 === Rotation.ShiftMap
  }

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  def expandShifts[
    G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]: MonadPlannerErr: MonadState_[?[_], QAuth]
    ]
      : PartialFunction[QSUGraph, G[QSUGraph]] = {
    case mls @ MultiLeftShift(source, shifts, _, repair) =>
      val mapper = repair.flatMap {
        case -\/(_) => func.ProjectKeyS(func.Hole, originalKey)
        case \/-(i) => func.ProjectKeyS(func.Hole, i.toString)
      }
      val sortedShifts = IList.fromList(shifts).sortBy(_._3).toList
      val shiftedG = sortedShifts match {
        case (struct, idStatus, rotation) :: ss =>
          val firstRepair: FreeMapA[QScriptUniform.ShiftTarget[T]] =
            func.StaticMapS(
              "original" -> AccessLeftTarget[T](Access.valueHole(_)),
              "0" -> RightTarget
            )
          val firstShiftPat: QScriptUniform[Symbol] =
            QSU.LeftShift[T, Symbol](source.root, RecFreeS.fromFree(struct), idStatus, OnUndefined.Emit, firstRepair, rotation)
          for {
            firstShift <- QSUGraph.withName[T, G](namePrefix)(firstShiftPat)
            _ <- ApplyProvenance.computeProvenance[T, G](firstShift)

            shiftAndRotation <- ss.zipWithIndex.foldLeftM[G, (QSUGraph, Rotation)]((firstShift :++ mls, rotation)) {
              case ((shiftAbove, rotationAbove), ((newStruct, newIdStatus, newRotation), idx)) =>
                val keysAbove = ("original" :: (0 to idx).map(_.toString).toList)
                val staticAbove = func.StaticMapFS(keysAbove: _*)(func.ProjectKeyS(AccessLeftTarget[T](Access.valueHole(_)), _), s => s)

                val repair = func.ConcatMaps(staticAbove, func.MakeMapS((idx + 1).toString, RightTarget))
                val struct = newStruct >> func.ProjectKeyS(func.Hole, originalKey)
                val newShiftPat =
                  QSU.LeftShift[T, Symbol](shiftAbove.root, RecFreeS.fromFree(struct), newIdStatus, OnUndefined.Emit, repair, newRotation)
                for {
                  newShift <- QSUGraph.withName[T, G](namePrefix)(newShiftPat)
                  _ <- ApplyProvenance.computeProvenance[T, G](newShift)
                  identityCondition =
                  if (rotationsCompatible(rotationAbove, newRotation))
                    func.Cond(
                      func.Or(
                        func.Eq(
                          AccessLeftTarget[T](Access.id(IdAccess.identity[T[EJson]](shiftAbove.root), _)),
                          AccessLeftTarget[T](Access.id(IdAccess.identity[T[EJson]](newShift.root), _))),
                        func.IfUndefined(
                          AccessLeftTarget[T](Access.id(IdAccess.identity[T[EJson]](newShift.root), _)),
                          func.Constant(json.bool(true)))),
                      repair,
                      func.Undefined)
                  else repair
                  newShiftNewRepairPat =
                    newShiftPat.copy(repair = identityCondition)
                  newShiftNewRepair = newShift.overwriteAtRoot(newShiftNewRepairPat)
                } yield (newShiftNewRepair :++ shiftAbove, newRotation)
            }
            (nestedShifts, _) = shiftAndRotation
          } yield nestedShifts
        case Nil => source.pure[G]
      }
      for {
        shifted <- shiftedG
        map = QSU.Map[T, Symbol](shifted.root, mapper.asRec)
      } yield mls.overwriteAtRoot(map) :++ shifted
  }
}

object ExpandShifts {
  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT,
      F[_]: Monad: NameGenerator: MonadPlannerErr](
      qgraph: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] =
    new ExpandShifts[T].apply[F](qgraph)
}
