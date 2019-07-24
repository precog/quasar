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

import slamdata.Predef.{Map => _, _}

import quasar.{Qspec, TreeMatchers}
import quasar.IdStatus.{ExcludeId, IdOnly}
import quasar.contrib.iota._
import quasar.contrib.matryoshka._
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.{construction, Hole, OnUndefined, PlannerError, SrcHole}
import quasar.qsu.{QScriptUniform => QSU}
import quasar.qsu.mra.ProvImpl

import matryoshka._
import matryoshka.data._
import org.specs2.matcher.{Expectable, MatchResult, Matcher}
import pathy.Path._
import scalaz.{\/, EitherT, Need, StateT}
import scalaz.syntax.applicative._
import scalaz.syntax.either._
import scalaz.syntax.show._

import shims.{eqToScalaz, orderToCats, showToCats, showToScalaz}

import Fix._
import QSU.Rotation
import QSUGraph.Extractors._

object ExpandShiftsSpec extends Qspec with QSUTTypes[Fix] with TreeMatchers {
  type QSU[A] = QScriptUniform[A]

  val qsu = QScriptUniform.DslT[Fix]
  val func = construction.Func[Fix]
  val json = Fixed[Fix[EJson]]
  val recFunc = construction.RecFunc[Fix]
  val qprov = ProvImpl[Fix[EJson], IdAccess, IdType]

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]

  val hole: Hole = SrcHole

  def index(i: Int): FreeMapA[Access[Hole] \/ Int] =
    i.right[Access[Hole]].pure[FreeMapA]

  "MLS expansion" should {
    "convert singly nested LeftShift/ThetaJoin" in {
      val dataset = qsu.leftShift(
        qsu.read(rootDir </> file("dataset"), ExcludeId),
        recFunc.Hole,
        ExcludeId,
        OnUndefined.Omit,
        RightTarget[Fix],
        Rotation.ShiftArray)

      val multiShift = QSUGraph.fromTree(qsu.multiLeftShift(
        dataset,
        List(
          (func.ProjectKeyS(func.Hole, "foo"), ExcludeId, Rotation.ShiftArray),
          (func.ProjectKeyS(func.Hole, "bar"), ExcludeId, Rotation.ShiftArray)
        ),
        OnUndefined.Omit,
        func.Add(index(0), index(1))
      ))

      multiShift must expandTo {
        case qg@Map(
          LeftShift(
            LeftShift(
              LeftShift(
                Read(afile, ExcludeId),
                shiftedReadStruct,
                ExcludeId,
                OnUndefined.Omit,
                shiftedReadRepair,
                Rotation.ShiftArray
              ),
              projectFoo,
              ExcludeId,
              OnUndefined.Emit,
              innerRepair,
              Rotation.ShiftArray
            ),
            projectBar,
            ExcludeId,
            OnUndefined.Emit,
            outerRepair,
            Rotation.ShiftArray
          ),
          fm
        ) =>
          fm.linearize must beTreeEqual(
            func.Add(
              func.ProjectKeyS(func.Hole, "0"),
              func.ProjectKeyS(func.Hole, "1")))

          projectBar.linearize must beTreeEqual(
            func.ProjectKeyS(func.ProjectKeyS(func.Hole, "original"), "bar")
          )

          projectFoo.linearize must beTreeEqual(
            func.ProjectKeyS(func.Hole, "foo")
          )

          shiftedReadStruct.linearize must beTreeEqual(
            func.Hole
          )

          shiftedReadRepair must beTreeEqual(
            RightTarget[Fix]
          )

          innerRepair must beTreeEqual(
            func.StaticMapS(
              "original" -> AccessLeftTarget[Fix](Access.value(_)),
              "0" -> RightTarget[Fix])
          )

          outerRepair must beTreeEqual(
            func.ConcatMaps(
              AccessLeftTarget[Fix](Access.value(_)),
              func.MakeMapS("1", RightTarget[Fix])))
      }
    }

    "convert singly nested LeftShift/ThetaJoin with onUndefined = OnUndefined.Emit" in {
      val dataset = qsu.leftShift(
        qsu.read(rootDir </> file("dataset"), ExcludeId),
        recFunc.Hole,
        ExcludeId,
        OnUndefined.Omit,
        RightTarget[Fix],
        Rotation.ShiftArray)

      val multiShift = QSUGraph.fromTree(qsu.multiLeftShift(
        dataset,
        List(
          (func.ProjectKeyS(func.Hole, "foo"), ExcludeId, Rotation.ShiftArray),
          (func.ProjectKeyS(func.Hole, "bar"), ExcludeId, Rotation.ShiftArray)
        ),
        OnUndefined.Emit,
        func.Add(index(0), index(1))
      ))

      multiShift must expandTo {
        case qg@Map(
          LeftShift(
            LeftShift(
              LeftShift(
                Read(afile, ExcludeId),
                shiftedReadStruct,
                ExcludeId,
                OnUndefined.Omit,
                shiftedReadRepair,
                Rotation.ShiftArray
              ),
              projectFoo,
              ExcludeId,
              OnUndefined.Emit,
              innerRepair,
              Rotation.ShiftArray
            ),
            projectBar,
            ExcludeId,
            OnUndefined.Emit,
            outerRepair,
            Rotation.ShiftArray
          ),
          fm
        ) => ok
      }
    }

    "convert doubly nested LeftShift/ThetaJoin" in {
      val dataset = qsu.leftShift(
        qsu.read(rootDir </> file("dataset"), ExcludeId),
        recFunc.Hole,
        ExcludeId,
        OnUndefined.Omit,
        RightTarget[Fix],
        Rotation.ShiftArray)

      val multiShift = QSUGraph.fromTree(qsu.multiLeftShift(
        dataset,
        List(
          (func.ProjectKeyS(func.Hole, "foo"), ExcludeId, Rotation.ShiftArray),
          (func.ProjectKeyS(func.Hole, "bar"), ExcludeId, Rotation.ShiftArray),
          (func.ProjectKeyS(func.Hole, "baz"), ExcludeId, Rotation.ShiftArray)
        ),
        OnUndefined.Omit,
        func.Subtract(func.Add(index(0), index(1)), index(2))
      ))

      multiShift must expandTo {
        case qg @ Map(
          LeftShift(
            LeftShift(
              LeftShift(
                LeftShift(
                  Read(afile, ExcludeId),
                  shiftedReadStruct,
                  ExcludeId,
                  OnUndefined.Omit,
                  shiftedReadRepair,
                  Rotation.ShiftArray
                ),
                projectFoo,
                ExcludeId,
                OnUndefined.Emit,
                innermostRepair,
                Rotation.ShiftArray
              ),
              projectBar,
              ExcludeId,
              OnUndefined.Emit,
              innerRepair,
              Rotation.ShiftArray
            ),
            projectBaz,
            ExcludeId,
            OnUndefined.Emit,
            outerRepair,
            Rotation.ShiftArray
          ),
          fm
        ) =>
          fm.linearize must beTreeEqual(
            func.Subtract(
              func.Add(
                func.ProjectKeyS(func.Hole, "0"),
                func.ProjectKeyS(func.Hole, "1")),
              func.ProjectKeyS(func.Hole, "2")))

          projectBaz.linearize must beTreeEqual(
            func.ProjectKeyS(func.ProjectKeyS(func.Hole, "original"), "baz")
          )

          projectBar.linearize must beTreeEqual(
            func.ProjectKeyS(func.ProjectKeyS(func.Hole, "original"), "bar")
          )

          projectFoo.linearize must beTreeEqual(
            func.ProjectKeyS(func.Hole, "foo")
          )

          shiftedReadStruct.linearize must beTreeEqual(
            func.Hole
          )

          shiftedReadRepair must beTreeEqual(
            RightTarget[Fix]
          )

          innermostRepair must beTreeEqual(
            func.StaticMapS(
              "original" -> AccessLeftTarget[Fix](Access.value(_)),
              "0" -> RightTarget[Fix])
          )

          innerRepair must beTreeEqual(
            func.ConcatMaps(
              AccessLeftTarget[Fix](Access.value(_)),
              func.MakeMapS("1", RightTarget[Fix])))

          outerRepair must beTreeEqual(
            func.ConcatMaps(
              AccessLeftTarget[Fix](Access.value(_)),
              func.MakeMapS("2", RightTarget[Fix])))
      }
    }

    "join shifts of the same focus" in {
      val dataset = qsu.leftShift(
        qsu.read(rootDir </> file("dataset"), ExcludeId),
        recFunc.Hole,
        ExcludeId,
        OnUndefined.Omit,
        RightTarget[Fix],
        Rotation.ShiftArray)

      val multiShift = QSUGraph.fromTree(qsu.multiLeftShift(
        dataset,
        List(
          (func.ProjectKeyS(func.Hole, "foo"), IdOnly, Rotation.ShiftArray),
          (func.ProjectKeyS(func.Hole, "foo"), ExcludeId, Rotation.ShiftArray)
        ),
        OnUndefined.Emit,
        func.StaticMapS(
          "id" -> index(0),
          "val" -> index(1))
      ))

      multiShift must expandTo {
        case qg@Map(
          LeftShift(
            LeftShift(
              LeftShift(
                Read(afile, ExcludeId),
                shiftedReadStruct,
                ExcludeId,
                OnUndefined.Omit,
                shiftedReadRepair,
                Rotation.ShiftArray
              ),
              projectFoo1,
              IdOnly,
              OnUndefined.Emit,
              innerRepair,
              Rotation.ShiftArray
            ),
            projectFoo2,
            ExcludeId,
            OnUndefined.Emit,
            outerRepair,
            Rotation.ShiftArray
          ),
          fm
        ) =>
          projectFoo1.linearize must beTreeEqual(
            func.ProjectKeyS(func.Hole, "foo"))

          projectFoo2.linearize must beTreeEqual(
            func.ProjectKeyS(func.ProjectKeyS(func.Hole, "original"), "foo"))

          innerRepair must beTreeEqual(
            func.StaticMapS(
              "original" -> AccessLeftTarget[Fix](Access.value(_)),
              "0" -> RightTarget[Fix]))

          outerRepair must beTreeEqual(
             func.Cond(
               func.Or(
                 func.Eq(
                   AccessLeftTarget[Fix](Access.id(IdAccess.identity('esh0), _)),
                   AccessLeftTarget[Fix](Access.id(IdAccess.identity('esh1), _))),
                 func.IfUndefined(
                   AccessLeftTarget[Fix](Access.id(IdAccess.identity('esh1), _)),
                   func.Constant(json.bool(true)))),
               func.ConcatMaps(
                 AccessLeftTarget[Fix](Access.value(_)),
                 func.MakeMapS("1", RightTarget[Fix])),
               func.Undefined))
      }
    }.pendingUntilFixed
  }

  def expandTo(pf: PartialFunction[QSUGraph, MatchResult[_]]): Matcher[QSUGraph] =
    new Matcher[QSUGraph] {
      def apply[S <: QSUGraph](s: Expectable[S]): MatchResult[S] = {
        val actual =
          ApplyProvenance[Fix, F](qprov, s.value)
            .flatMap(ExpandShifts[Fix, F](qprov))
            .run.eval(0).value

        val mapped = actual.toOption.flatMap(aq => pf.lift(aq.graph)) map { r =>
          result(
            r.isSuccess,
            s.description + " is correct: " + r.message,
            s.description + " is incorrect: " + r.message,
            s)
        }

        // TODO Show[QSUGraph[Fix]]
        mapped.getOrElse(
          failure(s"${actual.shows} did not match expected pattern", s))
      }
    }
}
