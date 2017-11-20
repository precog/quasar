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
import quasar.fs._
import quasar.physical.mongodb.workflow._
import quasar.sql._

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.scalacheck._

class PlannerPropSpec extends PlannerWorkflowHelpers {

  import PlannerHelpers._

  def plan(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    PlannerHelpers.plan(query)

  "plan from query" should {
    "plan multiple reducing projections (all, distinct, orderBy)" >> Prop.forAll(select(distinct, maybeReducingExpr, Gen.option(filter), Gen.option(groupBySeveral), orderBySeveral)) { q =>
      plan(q.embed) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxAccumOps(wf, 2)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 1)
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        appropriateColumns(wf, q)
        rootPushes(wf) must_== Nil
      }
    }.set(maxSize = 3).pendingUntilFixed(notOnPar)  // FIXME: with more then a few keys in the order by, the planner gets *very* slow (see SD-658)

    "plan multiple reducing projections (all, distinct)" >> Prop.forAll(select(distinct, maybeReducingExpr, Gen.option(filter), Gen.option(groupBySeveral), noOrderBy)) { q =>
      plan(q.embed) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxAccumOps(wf, 2)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 1)
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        appropriateColumns(wf, q)
        rootPushes(wf) must_== Nil
      }
    }.set(maxSize = 10).pendingUntilFixed(notOnPar)

    "plan multiple reducing projections (all)" >> Prop.forAll(select(notDistinct, maybeReducingExpr, Gen.option(filter), Gen.option(groupBySeveral), noOrderBy)) { q =>
      plan(q.embed) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxAccumOps(wf, 1)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 1)
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        appropriateColumns0(wf, q)
        rootPushes(wf) must_== Nil
      }
    }.set(maxSize = 10).pendingUntilFixed(notOnPar)

    // NB: tighter constraint because we know there's no filter.
    "plan multiple reducing projections (no filter)" >> Prop.forAll(select(notDistinct, maybeReducingExpr, noFilter, Gen.option(groupBySeveral), noOrderBy)) { q =>
      plan(q.embed) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxAccumOps(wf, 1)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 0)
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        appropriateColumns0(wf, q)
        rootPushes(wf) must_== Nil
      }
    }.set(maxSize = 10).pendingUntilFixed(notOnPar)
  }

}
