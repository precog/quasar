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

package quasar.qscript.rewrites

import slamdata.Predef._
import quasar.IdStatus.ExcludeId
import quasar.api.resource.ResourcePath
import quasar.common.JoinType
import quasar.contrib.iota._
import quasar.ejson, ejson.{EJson, Fixed}
import quasar.fp._
import quasar.qscript._

import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._

object ThetaToEquiJoinSpec extends quasar.Qspec with TTypes[Fix] {

  type QS[A] = QScriptEducated[A]
  type QST[A] = QScriptTotal[A]

  val qsdsl = construction.mkDefaults[Fix, QS]
  val qstdsl = construction.mkDefaults[Fix, QST]
  val json = Fixed[Fix[EJson]]

  def simplifyJoinExpr(expr: Fix[QS]): Fix[QST] =
    expr.transCata[Fix[QST]](ThetaToEquiJoin[Fix, QS, QST].rewrite(idPrism.reverseGet))

  "simplify an outer ThetaJoin with a statically known condition" in {
    val exp: Fix[QS] = {
      import qsdsl._
      fix.ThetaJoin(
        fix.Unreferenced,
        free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("foo")), ExcludeId),
        free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("bar")), ExcludeId),
        func.Eq(
          func.Constant(json.int(0)),
          func.Constant(json.int(1))),
        JoinType.FullOuter,
        func.ConcatMaps(func.LeftSide, func.RightSide))
    }

    simplifyJoinExpr(exp) must equal {
      import qstdsl._
      fix.Map(
        fix.EquiJoin(
          fix.Unreferenced,
          free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("foo")), ExcludeId),
          free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("bar")), ExcludeId),
          List((
            func.Constant(json.int(0)),
            func.Constant(json.int(1)))),
          JoinType.FullOuter,
          func.StaticMapS(
            ThetaToEquiJoin.LeftK -> func.LeftSide,
            ThetaToEquiJoin.RightK -> func.RightSide)),
        recFunc.ConcatMaps(
          recFunc.ProjectKeyS(recFunc.Hole, ThetaToEquiJoin.LeftK),
          recFunc.ProjectKeyS(recFunc.Hole, ThetaToEquiJoin.RightK)))
    }
  }

  "simplify a ThetaJoin" in {
    val exp: Fix[QS] = {
      import qsdsl._
      fix.ThetaJoin(
        fix.Unreferenced,
        free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("foo")), ExcludeId),
        free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("bar")), ExcludeId),
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
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("foo")), ExcludeId),
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("bar")), ExcludeId),
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
              ThetaToEquiJoin.LeftK -> func.LeftSide,
              ThetaToEquiJoin.RightK -> func.RightSide)),
          recFunc.Lt(
            recFunc.ProjectKeyS(
              recFunc.ProjectKeyS(recFunc.Hole, ThetaToEquiJoin.LeftK),
              "l_lat"),
            recFunc.ProjectKeyS(
              recFunc.ProjectKeyS(recFunc.Hole, ThetaToEquiJoin.RightK),
              "r_lat"))),
        recFunc.ConcatMaps(
          recFunc.ProjectKeyS(recFunc.Hole, ThetaToEquiJoin.LeftK),
          recFunc.ProjectKeyS(recFunc.Hole, ThetaToEquiJoin.RightK)))
    }
  }
}
