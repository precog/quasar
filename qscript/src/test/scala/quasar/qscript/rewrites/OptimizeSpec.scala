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

import quasar.fp._
import quasar.qscript._

import matryoshka.data.Fix
import matryoshka.implicits._

class OptimizeSpec extends quasar.Qspec with QScriptHelpers {
  val optimize = new Optimize[Fix]

  def optimizeExpr(expr: Fix[QS]): Fix[QS] =
    expr.transCata[Fix[QS]](optimize.optimize[QS, QS](reflNT))

  import qsdsl._

  "optimizer" should {
    "move subset before map" in {
      val from =
        free.Map(
          free.Filter(free.Hole, recFunc.Constant(json.bool(true))),
          recFunc.ProjectKeyS(recFunc.Hole, "foo"))

      val count =
        free.Map(free.Hole, recFunc.ProjectIndexI(recFunc.Hole, 2))

      val input =
        fix.Subset(
          fix.Root,
          from,
          Take,
          count)

      val output =
        fix.Map(
          fix.Subset(
            fix.Root,
            free.Filter(free.Hole, recFunc.Constant(json.bool(true))),
            Take,
            count),
          recFunc.ProjectKeyS(recFunc.Hole, "foo"))

      optimizeExpr(input) must equal(output)
    }

    "move filter before union" in {
      val lBranch =
        free.Map(free.Hole, recFunc.ProjectKeyS(recFunc.Hole, "foo"))

      val rBranch =
        free.Map(free.Hole, recFunc.ProjectIndexI(recFunc.Hole, 2))

      val input =
        fix.Filter(
          fix.Union(
            fix.Root,
            lBranch,
            rBranch),
          recFunc.Eq(recFunc.Add(recFunc.Hole, recFunc.Constant(json.int(1))), recFunc.Constant(json.int(5))))

      val output =
        fix.Union(
          fix.Root,
          free.Filter(
            lBranch,
            recFunc.Eq(recFunc.Add(recFunc.Hole, recFunc.Constant(json.int(1))), recFunc.Constant(json.int(5)))),
          free.Filter(
            rBranch,
            recFunc.Eq(recFunc.Add(recFunc.Hole, recFunc.Constant(json.int(1))), recFunc.Constant(json.int(5)))))

      optimizeExpr(input) must equal(output)
    }
  }
}
