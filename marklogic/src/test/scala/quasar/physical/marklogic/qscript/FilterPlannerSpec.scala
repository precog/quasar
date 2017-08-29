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

package quasar.physical.marklogic.qscript

import slamdata.Predef._

import quasar.contrib.pathy._
import quasar.ejson._
import quasar.physical.marklogic.DocType
import quasar.physical.marklogic.cts._
import quasar.physical.marklogic.xquery._
import quasar.qscript._
import quasar.qscript.{MapFuncsCore => MFCore}

import eu.timepit.refined.auto._
import matryoshka.data.Fix
import matryoshka.{Hole => _, _}
import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import xml.name._

import scalaz._, Scalaz._

final class FilterPlannerSpec extends quasar.Qspec {
  val comparisons = List(ComparisonOp.EQ , ComparisonOp.NE , ComparisonOp.LT , ComparisonOp.LE , ComparisonOp.GT , ComparisonOp.GE)

  def projectField(src: FreeMap[Fix], str: String): FreeMap[Fix] =
    Free.roll(MFC(MFCore.ProjectField(src, MFCore.StrLit(str))))

  def makeComp(op: ComparisonOp, src: FreeMap[Fix], str: String): FreeMap[Fix] = {
    val searchExpr = MFCore.StrLit[Fix, Hole](str)

    Free.roll(MFC(op match {
      case ComparisonOp.EQ => MFCore.Eq(src, searchExpr)
      case ComparisonOp.NE => MFCore.Neq(src,searchExpr)
      case ComparisonOp.LT => MFCore.Lt(src, searchExpr)
      case ComparisonOp.LE => MFCore.Lte(src,searchExpr)
      case ComparisonOp.GT => MFCore.Gt(src, searchExpr)
      case ComparisonOp.GE => MFCore.Gte(src,searchExpr)
    }))
  }

  case class ProjectTestCase(fm: FreeMap[Fix], path: ADir, op: ComparisonOp, expr: String)

  val genNestedProject: Gen[(FreeMap[Fix], ADir)] =
    for {
      dir0  <- arbitrary[ADir]
      first <- Gen.alphaStr
      path = rebaseA(rootDir[Sandboxed] </> dir(first))(dir0)
      nested = flatten(None, None, None, Some(_), Some(_), dir0).tail.unite.foldLeft(
        projectField(HoleF, first))((prj: FreeMap[Fix], nxt: String) => projectField(prj, nxt))
    } yield (nested, path)

  val genProjectTestCase: Gen[ProjectTestCase] =
    for {
      projection <- genNestedProject
      searchExpr <- Gen.alphaStr
      (nested, dir0) = projection
      op <- Gen.oneOf(comparisons)
    } yield ProjectTestCase(makeComp(op, nested, searchExpr), dir0, op, searchExpr)

  implicit val arbProjectTestCase: Arbitrary[ProjectTestCase] =
    Arbitrary(genProjectTestCase)

  type U = Fix[Query[Fix[EJson], ?]]

  def directoryQuery[Q](implicit Q: Birecursive.Aux[Q, Query[Fix[EJson], ?]]
  ): Q = Q.embed(Query.Directory[Fix[EJson], Q](IList("/some/ml/location"), MatchDepth.Children))

  def documentQuery[Q](implicit Q: Birecursive.Aux[Q, Query[Fix[EJson], ?]]
  ): Q = Q.embed(Query.Document[Fix[EJson], Q](IList("/some/ml/location")))

  def andQuery[Q](l: Q, r: Q)(implicit Q: Birecursive.Aux[Q, Query[Fix[EJson], ?]]
  ): Q = Q.embed(Query.And[Fix[EJson], Q](IList(l, r)))

  def pathRange[Q](path: String, op: ComparisonOp, value: Fix[EJson])(
    implicit Q: Birecursive.Aux[Q, Query[Fix[EJson], ?]]
  ): Q = Q.embed(Query.PathRange(IList(path), op, IList(value)))

  def elementRange[Q](element: QName, op: ComparisonOp, value: Fix[EJson])(
    implicit Q: Birecursive.Aux[Q, Query[Fix[EJson], ?]]
  ): Q = Q.embed(Query.ElementRange(IList(element), op, IList(value)))

  def jsonPropertyRange[Q](property: String, op: ComparisonOp, value: Fix[EJson])(
    implicit Q: Birecursive.Aux[Q, Query[Fix[EJson], ?]]
  ): Q = Q.embed(Query.JsonPropertyRange(IList(property), op, IList(value)))

  def src0[Q](implicit Q: Birecursive.Aux[Q, Query[Fix[EJson], ?]]): Search[Q] =
    Search(directoryQuery, IncludeId, IList())

  def str(value: String): Fix[EJson] = EJson.fromCommon(Str[Fix[EJson]](value))

  def qnameDirName(dir0: ADir): Option[QName] =
    dirName(dir0) >>= (d => QName.string.getOption(d.value))

  // The FMT type index is not relevant for these tests
  def planner(implicit ev: FilterPlanner[Fix, DocType.Json]
  ): FilterPlanner[Fix, DocType.Json] = ev

  "StarIndexPlanner" >> {
    "search expression includes * and projection path" >> prop { prj: ProjectTestCase =>
      val path = prettyPrint(rebaseA(rootDir[Sandboxed] </> dir("*"))(prj.path)).dropRight(1)

      val expectedSearch = IndexPlan(
        Search[U](
          andQuery[U](
            directoryQuery[U],
            pathRange[U](path, prj.op, str(prj.expr))),
          IncludeId,
          IList()), false)

      planner.StarIndexPlanner(src0[U], prj.fm) must beSome(expectedSearch)
    }
  }

  "PathIndexPlanner" >> {
    "plan includes the projection path" >> prop { prj: ProjectTestCase =>
      val path = prettyPrint(prj.path).dropRight(1)

      val expectedSearch = IndexPlan(
        Search[U](
          andQuery[U](
            directoryQuery[U],
            pathRange[U](path, prj.op, str(prj.expr))),
          IncludeId,
          IList()), false)

      planner.PathIndexPlanner(src0[U], prj.fm) must beSome(expectedSearch)
    }
  }

  "ElementIndexPlanner" >> {
    import axes.child
    import FilterPlanner.flattenDir

    "planXml" >> {
      def predPath(path0: ADir): XQuery =
        flattenDir(path0).map(child.elementNamed(_)).foldLeft(child.*)((path, segment) => path `/` segment)

      "plan with a star path and path predicate" >> prop { prj: ProjectTestCase =>
        val name: Option[QName] = qnameDirName(prj.path)

        val expectedSearch: Option[IndexPlan[U]] = name map ((elName: QName) =>
          IndexPlan(
            Search[U](
              andQuery[U](
                directoryQuery[U],
                elementRange[U](elName, prj.op, str(prj.expr))),
              IncludeId,
              IList(predPath(prj.path))), true))

        planner.ElementIndexPlanner.planXml(src0[U], prj.fm) must beEqualTo(expectedSearch)
      }

      "concatenate predicates if there's already one" >> prop { prj: ProjectTestCase =>
        val existingPredPath: XQuery = (child.elementNamed("aa") `/` child.elementNamed("bb"))

        val name: Option[QName] = qnameDirName(prj.path)
        val src1 = Search.pred.set(IList(existingPredPath))(src0[U])

        val expectedSearch: Option[IndexPlan[U]] = name map ((elName: QName) =>
          IndexPlan(
            Search[U](
              andQuery[U](
                directoryQuery[U],
                elementRange[U](elName, prj.op, str(prj.expr))),
              IncludeId,
              IList(existingPredPath, predPath(prj.path))), true))

        planner.ElementIndexPlanner.planXml(src1, prj.fm) must beEqualTo(expectedSearch)
      }
    }

    "planJson" >> {
      "add the path as a predicate to the search expression" >> prop { prj: ProjectTestCase =>
        val path = flattenDir(prj.path).map(child.nodeNamed(_)).foldLeft1Opt((path, segment) => path `/` segment)

        val expectedSearch: Option[IndexPlan[U]] = (path |@| dirName(prj.path))((pth, prop) =>
          IndexPlan(Search[U](
            andQuery[U](
              directoryQuery[U],
              jsonPropertyRange[U](prop.value, prj.op, str(prj.expr))),
            IncludeId,
            IList(pth)), true).some).join

        planner.ElementIndexPlanner.planJson(src0[U], prj.fm) must beEqualTo(expectedSearch)
      }
    }
  }

  "anyDocument" >> {
    "return true if cts.Document exists anywhere in the cts.Query AST" >> {
      import FilterPlanner.anyDocument

      anyDocument[Fix, U](
        andQuery[U](
          directoryQuery[U],
          andQuery[U](directoryQuery[U], documentQuery[U]))) must beEqualTo(true)

      anyDocument[Fix, U](directoryQuery[U]) must beEqualTo(false)
    }
  }
}
