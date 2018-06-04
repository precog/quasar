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

import slamdata.Predef._
import quasar._
import quasar.common.JoinType
import quasar.contrib.pathy.{ADir, AFile}
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.contrib.iota.SubInject
import quasar.qscript._

import scala.Predef.implicitly
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._

import iotaz.{CopK, TNilK}
import iotaz.TListK.:::

class RewriteSpec extends quasar.Qspec with QScriptHelpers {
  val rewrite = new Rewrite[Fix]

  def normalizeFExpr(expr: Fix[QS]): Fix[QS] =
    expr.transCata[Fix[QS]](orOriginal(Normalizable[QS].normalizeF(_: QS[Fix[QS]])))

  def normalizeExpr(expr: Fix[QS]): Fix[QS] =
    expr.transCata[Fix[QS]](rewrite.normalizeTJ[QS])

  def simplifyJoinExpr(expr: Fix[QS]): Fix[QST] =
    expr.transCata[Fix[QST]](SimplifyJoin[Fix, QS, QST].simplifyJoin(idPrism.reverseGet))

  def compactLeftShiftExpr(expr: Fix[QS]): Fix[QS] =
    expr.transCata[Fix[QS]](liftFG[QScriptCore, QS, Fix[QS]](
      injectRepeatedly[QScriptCore, QS, Fix[QS]](
        rewrite.compactLeftShift[QS](PrismNT.injectCopK).apply(_: QScriptCore[Fix[QS]]))))

  def includeToExcludeExpr(expr: Fix[QST]): Fix[QST] =
    expr.transCata[Fix[QST]](
      (qst => repeatedly[QST[Fix[QST]]](Coalesce[Fix, QST, QST].coalesceSR[QST, ADir](idPrism))(qst)) >>>
      (qst => repeatedly[QST[Fix[QST]]](Coalesce[Fix, QST, QST].coalesceSR[QST, AFile](idPrism))(qst)))

  type QSI[A] = CopK[QScriptCore ::: ProjectBucket ::: ThetaJoin ::: Const[DeadEnd, ?] ::: TNilK, A]

  implicit val qsc: Injectable[QScriptCore, QSI] = Injectable.inject[QScriptCore, QSI]
  implicit val pb: Injectable[ProjectBucket, QSI] = Injectable.inject[ProjectBucket, QSI]
  implicit val tj: Injectable[ThetaJoin, QSI] = Injectable.inject[ThetaJoin, QSI]
  implicit val de: Injectable[Const[DeadEnd, ?], QSI] = Injectable.inject[Const[DeadEnd, ?], QSI]

  val qsidsl = construction.mkDefaults[Fix, QSI]

  val DEI = implicitly[Const[DeadEnd, ?] :<<: QSI]
  val QCI = implicitly[QScriptCore :<<: QSI]

  implicit def qsiToQscriptTotal: Injectable[QSI, QST] = SubInject[QSI, QST]

  // TODO instead of calling `.toOption` on the `\/`
  // write an `Equal[PlannerError]` and test for specific errors too
  "rewriter" should {
    // TODO re-enable when we can read from directories quasar#3095
    //"expand a directory read" in {
    //  import qstdsl._
    //  convert(lc.some, lpRead("/foo")).flatMap(
    //    _.transCataM(ExpandDirs[Fix, QS, QST].expandDirs(idPrism.reverseGet, lc)).toOption.run.copoint) must
    //  equal(
    //    fix.LeftShift(
    //      fix.Union(fix.Unreferenced,
    //        free.Map(free.Read[AFile](rootDir </> dir("foo") </> file("bar")), func.MakeMap(func.Constant(json.str("bar")), func.Hole)),
    //        free.Union(free.Unreferenced,
    //          free.Map(free.Read[AFile](rootDir </> dir("foo") </> file("car")), func.MakeMap(func.Constant(json.str("car")), func.Hole)),
    //          free.Union(free.Unreferenced,
    //            free.Map(free.Read[AFile](rootDir </> dir("foo") </> file("city")), func.MakeMap(func.Constant(json.str("city")), func.Hole)),
    //            free.Union(free.Unreferenced,
    //              free.Map(free.Read[AFile](rootDir </> dir("foo") </> file("person")), func.MakeMap(func.Constant(json.str("person")), func.Hole)),
    //              free.Map(free.Read[AFile](rootDir </> dir("foo") </> file("zips")), func.MakeMap(func.Constant(json.str("zips")), func.Hole)))))),
    //      func.Hole, ExcludeId, func.RightSide)
    //    .some)
    //}

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

    "coalesce a Filter into a preceding ThetaJoin" in {
      import qstdsl._
      val sampleFile = rootDir </> file("bar")

      val exp =
        fix.Filter(
          fix.ThetaJoin(
            fix.Unreferenced,
            free.ShiftedRead[AFile](sampleFile, IncludeId),
            free.ShiftedRead[AFile](sampleFile, IncludeId),
            func.And(
              func.Eq(func.ProjectKeyS(func.LeftSide, "l_id"), func.ProjectKeyS(func.RightSide, "r_id")),
              func.Eq(
                func.Add(
                  func.ProjectKeyS(func.LeftSide, "l_min"),
                  func.ProjectKeyS(func.LeftSide, "l_max")),
                func.Subtract(
                  func.ProjectKeyS(func.RightSide, "l_max"),
                  func.ProjectKeyS(func.RightSide, "l_min")))),
            JoinType.Inner,
            func.StaticMapS(
              "l" -> func.LeftSide,
              "r" -> func.RightSide)),
          recFunc.Lt(
            recFunc.ProjectKeyS(
              recFunc.ProjectKeyS(recFunc.Hole, "l"),
              "lat"),
            recFunc.ProjectKeyS(
              recFunc.ProjectKeyS(recFunc.Hole, "l"),
              "lon"))).unFix

      Coalesce[Fix, QST, QST].coalesceTJ(idPrism[QST].get).apply(exp).map(rewrite.normalizeTJ[QST]) must
      equal(
        fix.ThetaJoin(
          fix.Unreferenced,
          free.ShiftedRead[AFile](sampleFile, IncludeId),
          free.ShiftedRead[AFile](sampleFile, IncludeId),
          func.And(
            func.And(
              func.Eq(func.ProjectKeyS(func.LeftSide, "l_id"), func.ProjectKeyS(func.RightSide, "r_id")),
              func.Eq(
                func.Add(
                  func.ProjectKeyS(func.LeftSide, "l_min"),
                  func.ProjectKeyS(func.LeftSide, "l_max")),
                func.Subtract(
                  func.ProjectKeyS(func.RightSide, "l_max"),
                  func.ProjectKeyS(func.RightSide, "l_min")))),
            func.Lt(
              func.ProjectKeyS(func.LeftSide, "lat"),
              func.ProjectKeyS(func.LeftSide, "lon"))),
          JoinType.Inner,
          func.StaticMapS(
            "l" -> func.LeftSide,
            "r" -> func.RightSide)).unFix.some)

    }

    "fold a constant array value" in {
      import qsdsl._
      val value: Fix[EJson] =
        json.int(7)

      val exp: Fix[QS] =
        fix.Map(
          fix.Root,
          recFunc.MakeArray(recFunc.Constant(json.int(7))))

      val expected: Fix[QS] =
        fix.Map(
          fix.Root,
          recFunc.Constant(json.arr(List(value))))

      normalizeFExpr(exp) must equal(expected)
    }

    "elide a join with a constant on one side" in {
      import qsdsl._
      val exp: Fix[QS] =
        fix.ThetaJoin(
          fix.Root,
          free.LeftShift(
            free.Map(
              free.Root,
              recFunc.ProjectKey(recFunc.Hole, recFunc.Constant(json.str("city")))),
            recFunc.Hole,
            IncludeId,
            ShiftType.Array,
            OnUndefined.Omit,
            func.ConcatArrays(
              func.MakeArray(func.LeftSide),
              func.MakeArray(func.RightSide))),
          free.Map(
            free.Unreferenced,
            recFunc.Constant(json.str("name"))),
          func.Constant(json.bool(true)),
          JoinType.Inner,
          func.ProjectKey(
            func.ProjectIndexI(
              func.ProjectIndexI(func.LeftSide, 1),
              1),
            func.RightSide))

      // TODO: only require a single pass
      normalizeExpr(normalizeExpr(exp)) must equal(
        chainQS(
          fix.Root,
          fix.LeftShift(_,
            recFunc.ProjectKeyS(recFunc.Hole, "city"),
            ExcludeId,
            ShiftType.Array,
            OnUndefined.Omit,
            func.ProjectKeyS(func.RightSide, "name"))))
    }

    "fold a constant doubly-nested array value" in {
      import qsdsl._
      val value: Fix[EJson] =
        json.int(7)

      val exp: Fix[QS] =
        fix.Map(
          fix.Root,
          recFunc.MakeArray(recFunc.MakeArray(recFunc.Constant(json.int(7)))))

      val expected: Fix[QS] =
        fix.Map(
          fix.Root,
          recFunc.Constant(json.arr(List(json.arr(List(value))))))

      normalizeFExpr(exp) must equal(expected)
    }

    "elide a join in the branch of a join" in {
      import qsdsl._
      val exp: Fix[QS] =
        fix.ThetaJoin(
          fix.Root,
          free.Map(
            free.Unreferenced,
            recFunc.Constant(json.str("name"))),
          free.ThetaJoin(
            free.Root,
            free.LeftShift(
              free.Hole,
              recFunc.Hole,
              IncludeId,
              ShiftType.Array,
              OnUndefined.Omit,
              func.ConcatArrays(
                func.MakeArray(func.LeftSide),
                func.MakeArray(func.RightSide))),
            free.Map(
              free.Unreferenced,
              recFunc.Constant(json.str("name"))),
            func.Constant(json.bool(true)),
            JoinType.Inner,
            func.ConcatArrays(
              func.MakeArray(func.LeftSide),
              func.MakeArray(func.RightSide))),
          func.Constant(json.bool(true)),
          JoinType.Inner,
          func.ConcatArrays(
            func.MakeArray(func.LeftSide),
            func.MakeArray(func.RightSide)))

      // TODO: only require a single pass
      normalizeExpr(normalizeExpr(exp)) must
        equal(
          fix.LeftShift(
            fix.Root,
            recFunc.Hole,
            IncludeId,
            ShiftType.Array,
            OnUndefined.Omit,
            func.ConcatArrays(
              func.Constant(json.arr(List(json.str("name")))),
              func.MakeArray(
                func.ConcatArrays(
                  func.MakeArray(
                    func.ConcatArrays(
                      func.MakeArray(func.LeftSide),
                      func.MakeArray(func.RightSide))),
                  func.Constant(json.arr(List(json.str("name")))))))))
    }

    "fold nested boolean values" in {
      import qsdsl._
      val exp: Fix[QS] =
        fix.Map(
          fix.Root,
          recFunc.MakeArray(
            // !false && (false || !true)
            recFunc.And(
              recFunc.Not(recFunc.Constant(json.bool(false))),
              recFunc.Or(
                recFunc.Constant(json.bool(false)),
                recFunc.Not(recFunc.Constant(json.bool(true)))))))

      val expected: Fix[QS] =
        fix.Map(
          fix.Root,
          recFunc.Constant(json.arr(List(json.bool(false)))))

      normalizeFExpr(exp) must equal(expected)
    }

    "simplify a ThetaJoin" in {
      val exp: Fix[QS] = {
        import qsdsl._
        fix.ThetaJoin(
          fix.Unreferenced,
          free.Read[AFile](rootDir </> file("foo")),
          free.Read[AFile](rootDir </> file("bar")),
          func.And(func.And(
            // reversed equality
            func.Eq(
              func.ProjectKeyS(func.RightSide, "r_id"),
              func.ProjectKeyS(func.LeftSide, "l_id")),
            // more complicated expression, duplicated refs
            func.Eq(
              func.Add(
                func.ProjectKeyS(func.LeftSide, "l_min"),
                func.ProjectKeyS(func.LeftSide, "l_max")),
              func.Subtract(
                func.ProjectKeyS(func.RightSide, "l_max"),
                func.ProjectKeyS(func.RightSide, "l_min")))),
            // inequality
            func.Lt(
              func.ProjectKeyS(func.LeftSide, "l_lat"),
              func.ProjectKeyS(func.RightSide, "r_lat"))),
          JoinType.Inner,
          func.ConcatMaps(func.LeftSide, func.RightSide))
      }

      simplifyJoinExpr(exp) must equal {
        import qstdsl._
        fix.Map(
          fix.Filter(
            fix.EquiJoin(
              fix.Unreferenced,
              free.Read[AFile](rootDir </> file("foo")),
              free.Read[AFile](rootDir </> file("bar")),
              List(
                (func.ProjectKeyS(func.Hole, "l_id"),
                  func.ProjectKeyS(func.Hole, "r_id")),
                (func.Add(
                  func.ProjectKeyS(func.Hole, "l_min"),
                  func.ProjectKeyS(func.Hole, "l_max")),
                  func.Subtract(
                    func.ProjectKeyS(func.Hole, "l_max"),
                    func.ProjectKeyS(func.Hole, "l_min")))),
              JoinType.Inner,
              func.StaticMapS(
                SimplifyJoin.LeftK -> func.LeftSide,
                SimplifyJoin.RightK -> func.RightSide)),
            recFunc.Lt(
              recFunc.ProjectKeyS(
                recFunc.ProjectKeyS(recFunc.Hole, SimplifyJoin.LeftK),
                "l_lat"),
              recFunc.ProjectKeyS(
                recFunc.ProjectKeyS(recFunc.Hole, SimplifyJoin.RightK),
                "r_lat"))),
          recFunc.ConcatMaps(
            recFunc.ProjectKeyS(recFunc.Hole, SimplifyJoin.LeftK),
            recFunc.ProjectKeyS(recFunc.Hole, SimplifyJoin.RightK)))
      }
    }

    "transform a ShiftedRead with IncludeId to ExcludeId when possible" in {
      import qstdsl._
      val sampleFile = rootDir </> file("bar")

      val originalQScript =
        fix.Map(
          fix.ShiftedRead[AFile](sampleFile, IncludeId),
          recFunc.Add(
            recFunc.ProjectIndexI(recFunc.Hole, 1),
            recFunc.ProjectIndexI(recFunc.Hole, 1)))

      val expectedQScript =
        fix.Map(
          fix.ShiftedRead[AFile](sampleFile, ExcludeId),
          recFunc.Add(recFunc.Hole, recFunc.Hole))

      includeToExcludeExpr(originalQScript) must_= expectedQScript
    }

    "transform a ShiftedRead inside a LeftShift to ExcludeId when possible" in {
      import qstdsl._
      val sampleFile = rootDir </> file("bar")

      val originalQScript =
        fix.LeftShift(
          fix.ShiftedRead[AFile](sampleFile, IncludeId),
          recFunc.ProjectKeyS(recFunc.ProjectIndexI(recFunc.Hole, 1), "foo"),
          ExcludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          func.StaticMapS(
            "a" -> func.ProjectKeyS(func.ProjectIndexI(func.LeftSide, 1), "quux"),
            "b" -> func.RightSide))

      val expectedQScript =
        fix.LeftShift(
          fix.ShiftedRead[AFile](sampleFile, ExcludeId),
          recFunc.ProjectKeyS(recFunc.Hole, "foo"),
          ExcludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          func.StaticMapS(
            "a" -> func.ProjectKeyS(func.LeftSide, "quux"),
            "b" -> func.RightSide))

      includeToExcludeExpr(originalQScript) must_= expectedQScript
    }

    "transform a left shift with a static array as the source" in {
      import qsdsl._
      val original: Fix[QS] =
        fix.LeftShift(
          fix.Map(
            fix.Root,
            recFunc.MakeArray(recFunc.Add(recFunc.Hole, recFunc.Constant(json.int(3))))),
          recFunc.Hole,
          ExcludeId,
          ShiftType.Array,
          OnUndefined.Emit,
          func.StaticMapS(
            "right" -> func.RightSide,
            "left" -> func.LeftSide))

      val expected: Fix[QS] =
        fix.Map(
          fix.Root,
          recFunc.StaticMapS(
            "right" -> recFunc.Add(recFunc.Hole, recFunc.Constant(json.int(3))),
            "left" -> recFunc.MakeArray(recFunc.Add(recFunc.Hole, recFunc.Constant(json.int(3))))))

      compactLeftShiftExpr(original) must equal(expected)
    }

    "transform a left shift with a static array as the struct" in {
      import qsdsl._
      val original: Fix[QS] =
        fix.LeftShift(
          fix.Map(
            fix.Root,
            recFunc.Add(recFunc.Hole, recFunc.Constant(json.int(3)))),
          recFunc.MakeArray(recFunc.Subtract(recFunc.Hole, recFunc.Constant(json.int(5)))),
          ExcludeId,
          ShiftType.Array,
          OnUndefined.Emit,
          func.StaticMapS(
            "right" -> func.RightSide,
            "left" -> func.LeftSide))

      val expected: Fix[QS] =
        fix.Map(
          fix.Map(
            fix.Root,
            recFunc.Add(recFunc.Hole, recFunc.Constant(json.int(3)))),
          recFunc.StaticMapS(
            "right" -> recFunc.Subtract(recFunc.Hole, recFunc.Constant(json.int(5))),
            "left" -> recFunc.Hole))

      compactLeftShiftExpr(original) must equal(expected)
    }

    "extract filter from join condition" >> {
      "when guard is undefined in true branch" >> {
        import qsdsl._
        val original =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Read[AFile](rootDir </> file("foo")),
            free.Read[AFile](rootDir </> file("bar")),
            func.Guard(
              func.LeftSide,
              Type.AnyObject,
              func.Undefined,
              func.Eq(
                func.ProjectKey(func.RightSide, func.Constant(json.str("r_id"))),
                func.ProjectKey(func.LeftSide, func.Constant(json.str("l_id"))))),
            JoinType.Inner,
            func.ConcatMaps(
              func.Guard(
                func.LeftSide,
                Type.AnyObject,
                func.Undefined,
                func.LeftSide),
              func.RightSide))

        val expected =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Filter(
              free.Read[AFile](rootDir </> file("foo")),
              recFunc.Guard(
                recFunc.Hole,
                Type.AnyObject,
                recFunc.Constant(json.bool(false)),
                recFunc.Constant(json.bool(true)))),
            free.Read[AFile](rootDir </> file("bar")),
            func.Eq(
              func.ProjectKeyS(func.RightSide, "r_id"),
              func.ProjectKeyS(func.LeftSide, "l_id")),
            JoinType.Inner,
            func.ConcatMaps(func.LeftSide, func.RightSide))

        normalizeExpr(original) must equal(expected)
      }

      "when guard is undefined in false branch" >> {
        import qsdsl._
        val original =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Read[AFile](rootDir </> file("foo")),
            free.Read[AFile](rootDir </> file("bar")),
            func.Guard(
              func.LeftSide,
              Type.AnyObject,
              func.Eq(
                func.ProjectKeyS(func.RightSide, "r_id"),
                func.ProjectKeyS(func.LeftSide, "l_id")),
              func.Undefined),
            JoinType.Inner,
            func.ConcatMaps(
              func.Guard(
                func.LeftSide,
                Type.AnyObject,
                func.LeftSide,
                func.Undefined),
              func.RightSide))

        val expected =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Filter(
              free.Read[AFile](rootDir </> file("foo")),
              recFunc.Guard(
                recFunc.Hole,
                Type.AnyObject,
                recFunc.Constant(json.bool(true)),
                recFunc.Constant(json.bool(false)))),
            free.Read[AFile](rootDir </> file("bar")),
            func.Eq(
              func.ProjectKeyS(func.RightSide, "r_id"),
              func.ProjectKeyS(func.LeftSide, "l_id")),
            JoinType.Inner,
            func.ConcatMaps(func.LeftSide, func.RightSide))

        normalizeExpr(original) must equal(expected)
      }

      "when cond is undefined in true branch" >> {
        import qsdsl._
        val original =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Read[AFile](rootDir </> file("foo")),
            free.Read[AFile](rootDir </> file("bar")),
            func.Cond(
              func.Lt(func.ProjectKeyS(func.LeftSide, "x"), func.Constant(json.int(7))),
              func.Undefined,
              func.Eq(
                func.ProjectKeyS(func.RightSide, "r_id"),
                func.ProjectKeyS(func.LeftSide, "l_id"))),
            JoinType.Inner,
            func.ConcatMaps(
              func.Cond(
                func.Lt(func.ProjectKeyS(func.LeftSide, "x"), func.Constant(json.int(7))),
                func.Undefined,
                func.LeftSide),
              func.RightSide))

        val expected =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Filter(
              free.Read[AFile](rootDir </> file("foo")),
              recFunc.Not(recFunc.Lt(recFunc.ProjectKeyS(recFunc.Hole, "x"), recFunc.Constant(json.int(7))))),
            free.Read[AFile](rootDir </> file("bar")),
            func.Eq(
              func.ProjectKeyS(func.RightSide, "r_id"),
              func.ProjectKeyS(func.LeftSide, "l_id")),
            JoinType.Inner,
            func.ConcatMaps(func.LeftSide, func.RightSide))

        normalizeExpr(original) must equal(expected)
      }

      "when cond is undefined in false branch" >> {
        import qsdsl._
        val original =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Read[AFile](rootDir </> file("foo")),
            free.Read[AFile](rootDir </> file("bar")),
            func.Cond(
              func.Lt(func.ProjectKeyS(func.LeftSide, "x"), func.Constant(json.int(7))),
              func.Eq(
                func.ProjectKeyS(func.RightSide, "r_id"),
                func.ProjectKeyS(func.LeftSide, "l_id")),
              func.Undefined),
            JoinType.Inner,
            func.ConcatMaps(
              func.Cond(
                func.Lt(func.ProjectKeyS(func.LeftSide, "x"), func.Constant(json.int(7))),
                func.LeftSide,
                func.Undefined),
              func.RightSide))

        val expected =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Filter(
              free.Read[AFile](rootDir </> file("foo")),
              recFunc.Lt(recFunc.ProjectKeyS(recFunc.Hole, "x"), recFunc.Constant(json.int(7)))),
            free.Read[AFile](rootDir </> file("bar")),
            func.Eq(
              func.ProjectKeyS(func.RightSide, "r_id"),
              func.ProjectKeyS(func.LeftSide, "l_id")),
            JoinType.Inner,
            func.ConcatMaps(func.LeftSide, func.RightSide))

        normalizeExpr(original) must equal(expected)
      }
    }
  }
}
