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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.sql._

import eu.timepit.refined.auto._

class PlannerLogSpec extends PlannerHelpers {

  import PlannerHelpers._

  "planner log" should {
    "include all phases when successful" in {
      planLog(sqlE"select city from zips").map(_.name) must_===
        Vector(
          "SQL AST", "Variables Substituted", "Absolutized", "Normalized Projections",
          "Sort Keys Projected", "Annotated Tree",
          "Logical Plan", "Optimized", "Typechecked", "Rewritten Joins",
          "QScript", "QScript (ShiftRead)", "QScript (Optimized)",
          "QScript Mongo", "QScript Mongo (Subset Before Map)", "QScript Mongo (Prefer Projection)",
          "Workflow Builder", "Workflow (raw)", "Workflow (crystallized)")
    }

    "log mapBeforeSort when it is applied" in {
      planLog(sqlE"select length(city) from zips order by city").map(_.name) must_===
        Vector(
          "SQL AST", "Variables Substituted", "Absolutized", "Normalized Projections",
          "Sort Keys Projected", "Annotated Tree",
          "Logical Plan", "Optimized", "Typechecked", "Rewritten Joins",
          "QScript", "QScript (ShiftRead)", "QScript (Optimized)",
          "QScript Mongo", "QScript Mongo (Subset Before Map)",
          "QScript Mongo (Prefer Projection)", "QScript Mongo (Map Before Sort)",
          "Workflow Builder", "Workflow (raw)", "Workflow (crystallized)")
    }

    "include correct phases with type error" in {
      planLog(sqlE"select 'a' + 0 from zips").map(_.name) must_===
        Vector(
          "SQL AST", "Variables Substituted", "Absolutized", "Annotated Tree", "Logical Plan", "Optimized")
    }.pendingUntilFixed("SD-1249")

    "include correct phases with planner error" in {
      planLog(sqlE"""select interval(bar) from zips""").map(_.name) must_===
        Vector(
          "SQL AST", "Variables Substituted", "Absolutized", "Normalized Projections",
          "Sort Keys Projected", "Annotated Tree",
          "Logical Plan", "Optimized", "Typechecked", "Rewritten Joins",
          "QScript", "QScript (ShiftRead)", "QScript (Optimized)",
          "QScript Mongo", "QScript Mongo (Subset Before Map)", "QScript Mongo (Prefer Projection)")
    }
  }

}
