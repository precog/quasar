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
import quasar._
import quasar.common.JoinType
import quasar.contrib.pathy.{ADir, AFile}
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fs._
import quasar.sql.CompilerHelpers

import scala.Predef.implicitly
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._

class QScriptRewriteSpec extends quasar.Qspec with CompilerHelpers with QScriptHelpers {
  val rewrite = new Rewrite[Fix]

  def normalizeFExpr(expr: Fix[QS]): Fix[QS] =
    expr.transCata[Fix[QS]](orOriginal(Normalizable[QS].normalizeF(_: QS[Fix[QS]])))

  def normalizeExpr(expr: Fix[QS]): Fix[QS] =
    expr.transCata[Fix[QS]](rewrite.normalizeTJ[QS])

  def simplifyJoinExpr(expr: Fix[QS]): Fix[QST] =
    expr.transCata[Fix[QST]](SimplifyJoin[Fix, QS, QST].simplifyJoin(idPrism.reverseGet))

  def compactLeftShiftExpr(expr: Fix[QS]): Fix[QS] =
    expr.transCata[Fix[QS]](liftFG(injectRepeatedly(
      rewrite.compactLeftShift[QS](PrismNT.inject).apply(_: QScriptCore[Fix[QS]]))))

  def includeToExcludeExpr(expr: Fix[QST]): Fix[QST] =
    expr.transCata[Fix[QST]](
      liftFG(repeatedly(Coalesce[Fix, QST, QST].coalesceSR[QST, ADir](idPrism))) >>>
      liftFG(repeatedly(Coalesce[Fix, QST, QST].coalesceSR[QST, AFile](idPrism))))

  type QSI[A] =
    (QScriptCore :\: ProjectBucket :\: ThetaJoin :/: Const[DeadEnd, ?])#M[A]

  implicit val qsc: Injectable.Aux[QScriptCore, QSI] = Injectable.inject[QScriptCore, QSI]
  implicit val pb: Injectable.Aux[ProjectBucket, QSI] = Injectable.inject[ProjectBucket, QSI]
  implicit val tj: Injectable.Aux[ThetaJoin, QSI] = Injectable.inject[ThetaJoin, QSI]
  implicit val de: Injectable.Aux[Const[DeadEnd, ?], QSI] = Injectable.inject[Const[DeadEnd, ?], QSI]

  val qsidsl = construction.mkDefaults[Fix, QSI]
  val qscdsl = construction.mkDefaults[Fix, QScriptCore]

  val DEI = implicitly[Const[DeadEnd, ?] :<: QSI]
  val QCI = implicitly[QScriptCore :<: QSI]

  implicit def qsiToQscriptTotal: Injectable.Aux[QSI, QST] =
    ::\::[QScriptCore](::\::[ProjectBucket](::/::[Fix, ThetaJoin, Const[DeadEnd, ?]]))

  // TODO instead of calling `.toOption` on the `\/`
  // write an `Equal[PlannerError]` and test for specific errors too
  "rewriter" should {
    "elide a no-op map in a constant boolean" in {
      import qsidsl._
      val query = lpf.constant(Data.Bool(true))
      val run: QSI[Fix[QSI]] => QSI[Fix[QSI]] =
        repeatedly((fa: QSI[Fix[QSI]]) => QCI.prj(fa) >>= rewrite.elideNopQC[QSI])

      QueryFile.convertAndNormalize[Fix, QSI](query)(run).toOption must
        equal(
          fix.Map(fix.Unreferenced, func.Constant(json.bool(true))).some)
    }

    "expand a directory read" in {
      import qstdsl._
      convert(lc.some, lpRead("/foo")).flatMap(
        _.transCataM(ExpandDirs[Fix, QS, QST].expandDirs(idPrism.reverseGet, lc)).toOption.run.copoint) must
      equal(
        fix.LeftShift(
          fix.Union(fix.Unreferenced,
            free.Map(free.Read[AFile](rootDir </> dir("foo") </> file("bar")), func.MakeMap(func.Constant(json.str("bar")), func.Hole)),
            free.Union(free.Unreferenced,
              free.Map(free.Read[AFile](rootDir </> dir("foo") </> file("car")), func.MakeMap(func.Constant(json.str("car")), func.Hole)),
              free.Union(free.Unreferenced,
                free.Map(free.Read[AFile](rootDir </> dir("foo") </> file("city")), func.MakeMap(func.Constant(json.str("city")), func.Hole)),
                free.Union(free.Unreferenced,
                  free.Map(free.Read[AFile](rootDir </> dir("foo") </> file("person")), func.MakeMap(func.Constant(json.str("person")), func.Hole)),
                  free.Map(free.Read[AFile](rootDir </> dir("foo") </> file("zips")), func.MakeMap(func.Constant(json.str("zips")), func.Hole)))))),
          func.Hole, ExcludeId, func.RightSide)
        .some)
    }

    "coalesce a Map into a subsequent LeftShift" in {
      import qscdsl._
      val exp: QScriptCore[Fix[QScriptCore]] =
        fix.LeftShift(
          fix.Map(
            fix.Unreferenced,
            func.Constant(json.bool(true))),
          func.Hole,
          ExcludeId,
          func.RightSide).unFix

      Coalesce[Fix, QScriptCore, QScriptCore].coalesceQC(idPrism).apply(exp) must
      equal(
        fix.LeftShift(
          fix.Unreferenced,
          func.Constant(json.bool(true)),
          ExcludeId,
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
              func.Eq(func.ProjectKey(func.LeftSide, func.Constant(json.str("l_id"))), func.ProjectKey(func.RightSide, func.Constant(json.str("r_id")))),
              func.Eq(
                func.Add(
                  func.ProjectKey(func.LeftSide, func.Constant(json.str("l_min"))),
                  func.ProjectKey(func.LeftSide, func.Constant(json.str("l_max")))),
                func.Subtract(
                  func.ProjectKey(func.RightSide, func.Constant(json.str("l_max"))),
                  func.ProjectKey(func.RightSide, func.Constant(json.str("l_min")))))),
            JoinType.Inner,
            func.ConcatMaps(
              func.MakeMap(func.Constant(json.str("l")), func.LeftSide),
              func.MakeMap(func.Constant(json.str("r")), func.RightSide))),
          func.Lt(
            func.ProjectKey(
              func.ProjectKey(func.Hole, func.Constant(json.str("l"))),
              func.Constant(json.str("lat"))),
            func.ProjectKey(
              func.ProjectKey(func.Hole, func.Constant(json.str("l"))),
              func.Constant(json.str("lon"))))).unFix

      Coalesce[Fix, QST, QST].coalesceTJ(idPrism[QST].get).apply(exp).map(rewrite.normalizeTJ[QST]) must
      equal(
        fix.ThetaJoin(
          fix.Unreferenced,
          free.ShiftedRead[AFile](sampleFile, IncludeId),
          free.ShiftedRead[AFile](sampleFile, IncludeId),
          func.And(
            func.And(
              func.Eq(func.ProjectKey(func.LeftSide, func.Constant(json.str("l_id"))), func.ProjectKey(func.RightSide, func.Constant(json.str("r_id")))),
              func.Eq(
                func.Add(
                  func.ProjectKey(func.LeftSide, func.Constant(json.str("l_min"))),
                  func.ProjectKey(func.LeftSide, func.Constant(json.str("l_max")))),
                func.Subtract(
                  func.ProjectKey(func.RightSide, func.Constant(json.str("l_max"))),
                  func.ProjectKey(func.RightSide, func.Constant(json.str("l_min")))))),
            func.Lt(
              func.ProjectKey(func.LeftSide, func.Constant(json.str("lat"))),
              func.ProjectKey(func.LeftSide, func.Constant(json.str("lon"))))),
          JoinType.Inner,
          func.ConcatMaps(
            func.MakeMap(func.Constant(json.str("l")), func.LeftSide),
            func.MakeMap(func.Constant(json.str("r")), func.RightSide))).unFix.some)

    }

    "fold a constant array value" in {
      import qsdsl._
      val value: Fix[EJson] =
        json.int(7)

      val exp: Fix[QS] =
        fix.Map(
          fix.Root,
          func.MakeArray(func.Constant(json.int(7))))

      val expected: Fix[QS] =
        fix.Map(
          fix.Root,
          func.Constant(json.arr(List(value))))

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
              func.ProjectKey(func.Hole, func.Constant(json.str("city")))),
            func.Hole,
            IncludeId,
            func.ConcatArrays(
              func.MakeArray(func.LeftSide),
              func.MakeArray(func.RightSide))),
          free.Map(
            free.Unreferenced,
            func.Constant(json.str("name"))),
          func.Constant(json.bool(true)),
          JoinType.Inner,
          func.ProjectKey(
            func.ProjectIndex(
              func.ProjectIndex(func.LeftSide, func.Constant(json.int(1))),
              func.Constant(json.int(1))),
            func.RightSide))

      // TODO: only require a single pass
      normalizeExpr(normalizeExpr(exp)) must equal(
        chainQS(
          fix.Root,
          fix.LeftShift(_,
            func.ProjectKey(func.Hole, func.Constant(json.str("city"))),
            ExcludeId,
            func.ProjectKey(func.RightSide, func.Constant(json.str("name"))))))
    }

    "fold a constant doubly-nested array value" in {
      import qsdsl._
      val value: Fix[EJson] =
        json.int(7)

      val exp: Fix[QS] =
        fix.Map(
          fix.Root,
          func.MakeArray(func.MakeArray(func.Constant(json.int(7)))))

      val expected: Fix[QS] =
        fix.Map(
          fix.Root,
          func.Constant(json.arr(List(json.arr(List(value))))))

      normalizeFExpr(exp) must equal(expected)
    }

    "elide a join in the branch of a join" in {
      import qsdsl._
      val exp: Fix[QS] =
        fix.ThetaJoin(
          fix.Root,
          free.Map(
            free.Unreferenced,
            func.Constant(json.str("name"))),
          free.ThetaJoin(
            free.Root,
            free.LeftShift(
              free.Hole,
              func.Hole,
              IncludeId,
              func.ConcatArrays(
                func.MakeArray(func.LeftSide),
                func.MakeArray(func.RightSide))),
            free.Map(
              free.Unreferenced,
              func.Constant(json.str("name"))),
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
            func.Hole,
            IncludeId,
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
          func.MakeArray(
            // !false && (false || !true)
            func.And(
              func.Not(func.Constant(json.bool(false))),
              func.Or(
                func.Constant(json.bool(false)),
                func.Not(func.Constant(json.bool(true)))))))

      val expected: Fix[QS] =
        fix.Map(
          fix.Root,
          func.Constant(json.arr(List(json.bool(false)))))

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
              func.ProjectKey(func.RightSide, func.Constant(json.str("r_id"))),
              func.ProjectKey(func.LeftSide, func.Constant(json.str("l_id")))),
            // more complicated expression, duplicated refs
            func.Eq(
              func.Add(
                func.ProjectKey(func.LeftSide, func.Constant(json.str("l_min"))),
                func.ProjectKey(func.LeftSide, func.Constant(json.str("l_max")))),
              func.Subtract(
                func.ProjectKey(func.RightSide, func.Constant(json.str("l_max"))),
                func.ProjectKey(func.RightSide, func.Constant(json.str("l_min")))))),
            // inequality
            func.Lt(
              func.ProjectKey(func.LeftSide, func.Constant(json.str("l_lat"))),
              func.ProjectKey(func.RightSide, func.Constant(json.str("r_lat"))))),
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
                (func.ProjectKey(func.Hole, func.Constant(json.str("l_id"))),
                  func.ProjectKey(func.Hole, func.Constant(json.str("r_id")))),
                (func.Add(
                  func.ProjectKey(func.Hole, func.Constant(json.str("l_min"))),
                  func.ProjectKey(func.Hole, func.Constant(json.str("l_max")))),
                  func.Subtract(
                    func.ProjectKey(func.Hole, func.Constant(json.str("l_max"))),
                    func.ProjectKey(func.Hole, func.Constant(json.str("l_min")))))),
              JoinType.Inner,
              func.ConcatArrays(
                func.MakeArray(func.LeftSide),
                func.MakeArray(func.RightSide))),
            func.Lt(
              func.ProjectKey(
                func.ProjectIndex(func.Hole, func.Constant(json.int(0))),
                func.Constant(json.str("l_lat"))),
              func.ProjectKey(
                func.ProjectIndex(func.Hole, func.Constant(json.int(1))),
                func.Constant(json.str("r_lat"))))),
          func.ConcatMaps(
            func.ProjectIndex(func.Hole, func.Constant(json.int(0))),
            func.ProjectIndex(func.Hole, func.Constant(json.int(1)))))
      }
    }

    "transform a ShiftedRead with IncludeId to ExcludeId when possible" in {
      import qstdsl._
      val sampleFile = rootDir </> file("bar")

      val originalQScript =
        fix.Map(
          fix.ShiftedRead[AFile](sampleFile, IncludeId),
          func.Add(
            func.ProjectIndex(func.Hole, func.Constant(json.int(1))),
            func.ProjectIndex(func.Hole, func.Constant(json.int(1)))))

      val expectedQScript =
        fix.Map(
          fix.ShiftedRead[AFile](sampleFile, ExcludeId),
          func.Add(func.Hole, func.Hole))

      includeToExcludeExpr(originalQScript) must_= expectedQScript
    }

    "transform a left shift with a static array as the source" in {
      import qsdsl._
      val original: Fix[QS] =
        fix.LeftShift(
          fix.Map(
            fix.Root,
            func.MakeArray(func.Add(func.Hole, func.Constant(json.int(3))))),
          func.Hole,
          ExcludeId,
          func.ConcatMaps(
            func.MakeMap(func.Constant(json.str("right")), func.RightSide),
            func.MakeMap(func.Constant(json.str("left")), func.LeftSide)))

      val expected: Fix[QS] =
        fix.Map(
          fix.Root,
          func.ConcatMaps(
            func.MakeMap(func.Constant(json.str("right")), func.Add(func.Hole, func.Constant(json.int(3)))),
            func.MakeMap(func.Constant(json.str("left")), func.MakeArray(func.Add(func.Hole, func.Constant(json.int(3)))))))

      compactLeftShiftExpr(original) must equal(expected)
    }

    "transform a left shift with a static array as the struct" in {
      import qsdsl._
      val original: Fix[QS] =
        fix.LeftShift(
          fix.Map(
            fix.Root,
            func.Add(func.Hole, func.Constant(json.int(3)))),
          func.MakeArray(func.Subtract(func.Hole, func.Constant(json.int(5)))),
          ExcludeId,
          func.ConcatMaps(
            func.MakeMap(func.Constant(json.str("right")), func.RightSide),
            func.MakeMap(func.Constant(json.str("left")), func.LeftSide)))

      val expected: Fix[QS] =
        fix.Map(
          fix.Map(
            fix.Root,
            func.Add(func.Hole, func.Constant(json.int(3)))),
          func.ConcatMaps(
            func.MakeMap(func.Constant(json.str("right")), func.Subtract(func.Hole, func.Constant(json.int(5)))),
            func.MakeMap(func.Constant(json.str("left")), func.Hole)))

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
              func.Guard(
                func.Hole,
                Type.AnyObject,
                func.Constant(json.bool(false)),
                func.Constant(json.bool(true)))),
            free.Read[AFile](rootDir </> file("bar")),
            func.Eq(
              func.ProjectKey(func.RightSide, func.Constant(json.str("r_id"))),
              func.ProjectKey(func.LeftSide, func.Constant(json.str("l_id")))),
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
                func.ProjectKey(func.RightSide, func.Constant(json.str("r_id"))),
                func.ProjectKey(func.LeftSide, func.Constant(json.str("l_id")))),
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
              func.Guard(
                func.Hole,
                Type.AnyObject,
                func.Constant(json.bool(true)),
                func.Constant(json.bool(false)))),
            free.Read[AFile](rootDir </> file("bar")),
            func.Eq(
              func.ProjectKey(func.RightSide, func.Constant(json.str("r_id"))),
              func.ProjectKey(func.LeftSide, func.Constant(json.str("l_id")))),
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
              func.Lt(func.ProjectKey(func.LeftSide, func.Constant(json.str("x"))), func.Constant(json.int(7))),
              func.Undefined,
              func.Eq(
                func.ProjectKey(func.RightSide, func.Constant(json.str("r_id"))),
                func.ProjectKey(func.LeftSide, func.Constant(json.str("l_id"))))),
            JoinType.Inner,
            func.ConcatMaps(
              func.Cond(
                func.Lt(func.ProjectKey(func.LeftSide, func.Constant(json.str("x"))), func.Constant(json.int(7))),
                func.Undefined,
                func.LeftSide),
              func.RightSide))

        val expected =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Filter(
              free.Read[AFile](rootDir </> file("foo")),
              func.Not(func.Lt(func.ProjectKey(func.Hole, func.Constant(json.str("x"))), func.Constant(json.int(7))))),
            free.Read[AFile](rootDir </> file("bar")),
            func.Eq(
              func.ProjectKey(func.RightSide, func.Constant(json.str("r_id"))),
              func.ProjectKey(func.LeftSide, func.Constant(json.str("l_id")))),
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
              func.Lt(func.ProjectKey(func.LeftSide, func.Constant(json.str("x"))), func.Constant(json.int(7))),
              func.Eq(
                func.ProjectKey(func.RightSide, func.Constant(json.str("r_id"))),
                func.ProjectKey(func.LeftSide, func.Constant(json.str("l_id")))),
              func.Undefined),
            JoinType.Inner,
            func.ConcatMaps(
              func.Cond(
                func.Lt(func.ProjectKey(func.LeftSide, func.Constant(json.str("x"))), func.Constant(json.int(7))),
                func.LeftSide,
                func.Undefined),
              func.RightSide))

        val expected =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Filter(
              free.Read[AFile](rootDir </> file("foo")),
              func.Lt(func.ProjectKey(func.Hole, func.Constant(json.str("x"))), func.Constant(json.int(7)))),
            free.Read[AFile](rootDir </> file("bar")),
            func.Eq(
              func.ProjectKey(func.RightSide, func.Constant(json.str("r_id"))),
              func.ProjectKey(func.LeftSide, func.Constant(json.str("l_id")))),
            JoinType.Inner,
            func.ConcatMaps(func.LeftSide, func.RightSide))

        normalizeExpr(original) must equal(expected)
      }
    }
  }
}
