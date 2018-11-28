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

import quasar._
import quasar.api.resource.ResourcePath
import quasar.fp._
import quasar.contrib.iota._
import quasar.contrib.iota.SubInject
import quasar.qscript._

import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._

import iotaz.{CopK, TNilK}
import iotaz.TListK.:::

object RewriteSpec extends quasar.Qspec with QScriptHelpers {
  import IdStatus.ExcludeId

  val rewrite = new Rewrite[Fix]

  def normalizeTExpr(expr: Fix[QS]): Fix[QS] =
    expr.transCata[Fix[QS]](rewrite.normalizeT)

  def normalizeExpr(expr: Fix[QS]): Fix[QST] =
    rewrite.normalize[QST].apply(expr)

  type QSI[A] = CopK[QScriptCore ::: ThetaJoin ::: TNilK, A]

  implicit val qsc: Injectable[QScriptCore, QSI] = Injectable.inject[QScriptCore, QSI]
  implicit val tj: Injectable[ThetaJoin, QSI] = Injectable.inject[ThetaJoin, QSI]

  val qsidsl = construction.mkDefaults[Fix, QSI]

  implicit def qsiToQscriptTotal: Injectable[QSI, QST] = SubInject[QSI, QST]

  // TODO instead of calling `.toOption` on the `\/`
  // write an `Equal[PlannerError]` and test for specific errors too
  "rewriter" should {

    // select b[*] + c[*] from intArrays.data
    "normalize static projections in shift coalescing" in {
      import qsdsl._
      import qstdsl.{fix => fixt, func => funct, recFunc => recFunct}

      val educated =
        fix.Map(
          fix.Map(
            fix.LeftShift(
              fix.LeftShift(
                fix.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("intArrays")), ExcludeId),
                recFunc.ProjectKeyS(recFunc.Hole, "b"),
                ExcludeId,
                ShiftType.Array,
                OnUndefined.Emit,
                func.ConcatMaps(
                  func.MakeMapS("original", func.LeftSide),
                  func.MakeMapS("0", func.RightSide))),
              recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "original"), "c"),
              ExcludeId,
              ShiftType.Array,
              OnUndefined.Emit,
              func.ConcatMaps(
                func.LeftSide,
                func.MakeMapS("1", func.RightSide))),
            recFunc.ConcatMaps(
              recFunc.MakeMapS("0", recFunc.ProjectKeyS(recFunc.Hole, "0")),
              recFunc.MakeMapS("1", recFunc.ProjectKeyS(recFunc.Hole, "1")))),
          recFunc.Add(
            recFunc.ProjectKeyS(recFunc.Hole, "0"),
            recFunc.ProjectKeyS(recFunc.Hole, "1")))

      val normalized =
        fixt.LeftShift(
          fixt.LeftShift(
            fixt.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("intArrays")), ExcludeId),
            recFunct.ProjectKeyS(recFunct.Hole, "b"),
            ExcludeId,
            ShiftType.Array,
            OnUndefined.Emit,
            funct.ConcatMaps(
              funct.MakeMapS("original", funct.LeftSide),
              funct.MakeMapS("0", funct.RightSide))),
          recFunct.ProjectKeyS(recFunct.ProjectKeyS(recFunct.Hole, "original"), "c"),
          ExcludeId,
          ShiftType.Array,
          OnUndefined.Emit,
          funct.Add(
            funct.ProjectKeyS(funct.LeftSide, "0"),
            funct.RightSide))

      normalizeExpr(educated) must equal(normalized)
    }

    // select (select a, b from zips).a + (select a, b from zips).b
    "normalize static projections in a contrived example" in {
      import qsdsl._
      import qstdsl.{fix => fixt, recFunc => recFunct}

      val educated =
        fix.Map(
          fix.Map(
            fix.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("zips")), ExcludeId),
            recFunc.ConcatMaps(
              recFunc.MakeMapS("a", recFunc.ProjectKeyS(recFunc.Hole, "a")),
              recFunc.MakeMapS("b", recFunc.ProjectKeyS(recFunc.Hole, "b")))),
          recFunc.Add(
            recFunc.ProjectKeyS(recFunc.Hole, "a"),
            recFunc.ProjectKeyS(recFunc.Hole, "b")))

      val normalized =
        fixt.Map(
          fixt.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("zips")), ExcludeId),
            recFunct.Add(
              recFunct.ProjectKeyS(recFunct.Hole, "a"),
              recFunct.ProjectKeyS(recFunct.Hole, "b")))

      normalizeExpr(educated) must equal(normalized)
    }

    "coalesce a Map into a subsequent LeftShift" in {
      import qsidsl._
      val exp: QSI[Fix[QSI]] =
        fix.LeftShift(
          fix.Map(
            fix.Unreferenced,
            recFunc.Constant(json.bool(true))),
          recFunc.Hole,
          ExcludeId,
          ShiftType.Array,
          OnUndefined.Omit,
          func.RightSide).unFix

      Coalesce[Fix, QSI, QSI].coalesceQC(idPrism).apply(exp) must
      equal(
        fix.LeftShift(
          fix.Unreferenced,
          recFunc.Constant(json.bool(true)),
          ExcludeId,
          ShiftType.Array,
          OnUndefined.Omit,
          func.RightSide).unFix.some)
    }
  }
}
