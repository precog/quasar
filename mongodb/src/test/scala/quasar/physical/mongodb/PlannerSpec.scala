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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.contrib.specs2._
import quasar.fs._
import quasar.physical.mongodb.workflow._
import quasar.sql._

import scala.Either

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.matcher._
import scalaz._, Scalaz._

class PlannerSpec extends
    PlannerWorkflowHelpers with
    PendingWithActualTracking {

  // to write the new actuals:
  // override val mode = WriteMode

  import CollectionUtil._
  import PlannerHelpers._

  def plan(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    PlannerHelpers.plan(query)

  def trackPendingTemplate(
    name: String,
    plan: => Either[FileSystemError, Crystallized[WorkflowF]],
    planningMatcher: Matcher[Either[FileSystemError, Crystallized[WorkflowF]]],
    trackingMatcher: Matcher[Either[FileSystemError, Crystallized[WorkflowF]]]) = {
    name >> {
      lazy val plan0 = plan

      s"plan: $name" in {
        plan0 must planningMatcher
      }.pendingUntilFixed

      // s"track: $name" in {
      //   plan0 must trackingMatcher
      // }
    }
  }

  def trackPending(
    name: String,
    plan: => Either[FileSystemError, Crystallized[WorkflowF]],
    expectedOps: IList[MongoOp]) =
      trackPendingTemplate(name, plan,
        beRight.which(cwf => notBrokenWithOps(cwf.op, expectedOps, checkDanglingRefs = true)),
        beRight.which(cwf => trackActual(cwf, testFile(s"plan $name"))))

  def trackPendingTree(
    name: String,
    plan: => Either[FileSystemError, Crystallized[WorkflowF]],
    expectedOps: Tree[MongoOp]) =
      trackPendingTemplate(name, plan,
       beRight.which(cwf => notBrokenWithOpsTree(cwf.op, expectedOps)),
        beRight.which(cwf => trackActual(cwf, testFile(s"plan $name"))))

  def trackPendingErr(
    name: String,
    plan: => Either[FileSystemError, Crystallized[WorkflowF]],
    expectedOps: IList[MongoOp],
    errPattern: PartialFunction[FileSystemError, MatchResult[_]]) =
      trackPendingTemplate(name, plan,
        beRight.which(cwf => notBrokenWithOps(cwf.op, expectedOps)),
        beLeft(beLike(errPattern)))

  def trackPendingThrow(
    name: String,
    plan: => Either[FileSystemError, Crystallized[WorkflowF]],
    expectedOps: IList[MongoOp]) =
      trackPendingTemplate(name, plan,
        beRight.which(cwf => notBrokenWithOps(cwf.op, expectedOps)),
        throwA[scala.NotImplementedError])

  "plan from query string" should {

    "filter with both index and key projections" in {
      plan(sqlE"""select count(parents[0].sha) as count from slamengine_commits where parents[0].sha = "56d1caf5d082d1a6840090986e277d36d03f1859" """) must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, MatchOp, ProjectOp, MatchOp, GroupOp, ProjectOp)))
    }

    // TODO qz-3686
    /*trackPending(
      "having with multiple projections",
      plan(sqlE"select city, sum(pop) from extraSmallZips group by city having sum(pop) > 40000"),
      IList(ReadOp, GroupOp, MatchOp, ProjectOp)
    )*/

    "select partially-applied substring" in {
      plan3_2(sqlE"""select substring("abcdefghijklmnop", 5, trunc(pop / 10000)) from extraSmallZips""") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, ProjectOp)))
    }

    "sort wildcard on expression" in {
      plan(sqlE"select * from zips order by pop/10 desc") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, SimpleMapOp, ProjectOp, SortOp, ProjectOp)))
    }

    "sort with expression and alias" in {
      plan(sqlE"select pop/1000 as popInK from zips order by popInK") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, ProjectOp, SortOp)))
    }

    "sort with expression, alias, and filter" in {
      plan(sqlE"select pop/1000 as popInK from zips where pop >= 1000 order by popInK") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, MatchOp, ProjectOp, SortOp)))
    }

    "useful group by" in {
      plan(sqlE"""select city || ", " || state, sum(pop) from extraSmallZips group by city, state""") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, ProjectOp)))
    }

    trackPending(
      "group by simple expression",
      plan(sqlE"select city, sum(pop) from extraSmallZips group by lower(city)"),
      // should not use map-reduce and use much less pipeline ops
      // actual: [ReadOp,GroupOp,ProjectOp,MapOp,ReduceOp,ReadOp,ProjectOp,MatchOp,ProjectOp,MatchOp,GroupOp,ProjectOp,FoldLeftOp,MatchOp,UnwindOp,UnwindOp,SimpleMapOp]
      IList(ReadOp, GroupOp, UnwindOp, ProjectOp))

    "group by month" in {
      plan(sqlE"""select avg(epoch), date_part("month", `ts`) from days group by date_part("month", `ts`)""") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, ProjectOp)))
    }

    // FIXME: Needs an actual expectation and an IT
    // This gives wrong results in old mongo: returns multiple rows in case
    // count > 1. It should return 1 row per city.
    // see https://gist.github.com/rintcius/bff5b740a1252cafc976a31fc13dd7cf
    // Gives wrong results now as well: result of the case field is unexpected
    trackPending(
      "expr3 with grouping",
      plan(sqlE"select case when pop > 1000 then city else lower(city) end, count(*) from zips group by city"),
      IList()) //TODO

    "plan count and sum grouped by single field" in {
      plan(sqlE"select count(*) as cnt, sum(pop) as sm from zips group by state") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, ProjectOp)))
    }

    trackPending(
      "collect unaggregated fields into single doc when grouping",
      plan(sqlE"select city, state, sum(pop) from zips"),
      // should not use map-reduce and use much less pipeline ops
      // actual [ReadOp,GroupOp,ProjectOp,GroupOp,ProjectOp,ReduceOp,ReadOp,ProjectOp,GroupOp,ProjectOp,FoldLeftOp,MatchOp,UnwindOp,UnwindOp,SimpleMapOp]
      IList(ReadOp, ProjectOp, GroupOp, UnwindOp, ProjectOp))

    val unaggFieldWhenGrouping2ndCase = sqlE"select max(pop)/1000, pop from zips"

    "plan unaggregated field when grouping, second case - no root pushes" in {
      plan(unaggFieldWhenGrouping2ndCase) must
        beRight.which { cwf =>
          rootPushes(cwf.op) must_== Nil
        }
    }

    trackPending(
      "unaggregated field when grouping, second case",
      plan(unaggFieldWhenGrouping2ndCase),
      // should not use map-reduce and use much less pipeline ops
      // actual [ReadOp,GroupOp,ProjectOp,GroupOp,ProjectOp,ReduceOp,ReadOp,ProjectOp,GroupOp,ProjectOp,FoldLeftOp,MatchOp,UnwindOp,UnwindOp,SimpleMapOp]
      IList(ReadOp, GroupOp, UnwindOp, ProjectOp))

    trackPending(
      "double aggregation with another projection",
      plan(sqlE"select sum(avg(pop)), min(city) from zips group by state"),
      // should not use map-reduce and use much less pipeline ops
      // actual [ReadOp,GroupOp,ProjectOp,GroupOp,ProjectOp,ReduceOp,ReadOp,GroupOp,GroupOp,ProjectOp,GroupOp,ProjectOp,FoldLeftOp,MatchOp,UnwindOp,UnwindOp,SimpleMapOp]
      IList(ReadOp, GroupOp, GroupOp, UnwindOp))

    trackPending(
      "multiple expressions using same field",
      plan(sqlE"select pop, sum(pop), pop/1000 from zips"),
      // should not use map-reduce and use much less pipeline ops
      // actual: occurrences of consecutive $project ops: '1'
      IList(ReadOp, ProjectOp, GroupOp, UnwindOp, ProjectOp))

    "plan sum of expression in expression with another projection when grouped" in {
      plan(sqlE"select city, sum(pop-1)/1000 from zips group by city") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, ProjectOp)))
    }

    "length of min (JS on top of reduce)" in {
      plan3_2(sqlE"select state, length(min(city)) as shortest from zips group by state") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, ProjectOp, SimpleMapOp, ProjectOp)))
    }

    "plan js expr grouped by js expr" in {
      plan3_2(sqlE"select length(city) as len, count(*) as cnt from zips group by length(city)") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, SimpleMapOp, GroupOp, ProjectOp)))
    }

    "plan expressions with ~" in {
      plan(sqlE"""select foo ~ "bar.*", "abc" ~ "a|b", "baz" ~ regex, target ~ regex from a""") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, SimpleMapOp, ProjectOp)))
    }


    trackPending(
      "object flatten v34",
      plan3_4(sqlE"select geo{*} from usa_factbook", defaultStats, defaultIndexes, emptyDoc),
      // Actual: [ReadOp,ProjectOp,SimpleMapOp,ProjectOp]
      // FIXME: Inline the ProjectOp inside the SimpleMapOp as a SubMap
      IList(ReadOp, SimpleMapOp, ProjectOp))

    "object flatten" in {
      plan(sqlE"select geo{*} from usa_factbook") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, ProjectOp, UnwindOp, ProjectOp)))
    }

    "plan array concat with filter" in {
      plan(sqlE"""select loc || [ pop ] from zips where city = "BOULDER" """) must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, MatchOp, ProjectOp)))
    }

    "plan array flatten with unflattened field" in {
      plan(sqlE"SELECT `_id` as zip, loc as loc, loc[*] as coord FROM zips") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, ProjectOp, UnwindOp, ProjectOp)))
    }

    trackPending(
      "unify flattened fields",
      plan(sqlE"select loc[*] from zips where loc[*] < 0"),
      // actual: IList(ReadOp, ProjectOp, UnwindOp, ProjectOp, MatchOp, ProjectOp, UnwindOp, ProjectOp))
      //         The QScript contains two LeftShift (i.e flattens) which are not unified as they should,
      //         hence the two `UnwindOp`s
      IList(ReadOp, ProjectOp, UnwindOp, MatchOp, ProjectOp))

    trackPending(
      "group by flattened field",
      plan(sqlE"select substring(parents[*].sha, 0, 1), count(*) from slamengine_commits group by substring(parents[*].sha, 0, 1)"),
      IList(ReadOp, ProjectOp, UnwindOp, GroupOp, ProjectOp))

    trackPending(
      "sum flattened int arrays",
      plan(sqlE"select b[*] + c[*] from intArrays"),
      IList(ReadOp, ProjectOp, UnwindOp, ProjectOp, ProjectOp, UnwindOp, MatchOp, ProjectOp))

    "flatten array index" in {
      plan(sqlE"""select loc[*:] from extraSmallZips where city like "A%" """) must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, MatchOp, ProjectOp, UnwindOp, ProjectOp)))
    }

    "unify flattened fields with unflattened field" in {
      plan(sqlE"select `_id` as zip, loc[*] from zips order by loc[*]") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, ProjectOp, UnwindOp, ProjectOp, SortOp)))
    }

    trackPending(
      "unify flattened with double-flattened",
      plan(sqlE"""select * from user_comments where (comments[*].id LIKE "%Dr%" OR comments[*].replyTo[*] LIKE "%Dr%")"""),
      // Gives server-error, see https://gist.github.com/rintcius/b6c8292fc1d83d09abc69a482e9a04e2
      // should not use map-reduce
      // actual: IList(ReadOp, ProjectOp, SimpleMapOp, MatchOp, ProjectOp)
      IList(ReadOp, ProjectOp, UnwindOp, ProjectOp, UnwindOp, MatchOp, ProjectOp))

    "plan complex group by with sorting and limiting" in {
      plan(sqlE"SELECT city, SUM(pop) AS pop FROM zips GROUP BY city ORDER BY pop") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, ProjectOp, SortOp)))
    }

    "plan implicit group by with filter" in {
      plan(sqlE"""select avg(pop), min(city) from zips where state = "CO" """) must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, MatchOp, GroupOp, ProjectOp)))
    }

    "plan distinct as expression" in {
      plan(sqlE"select count(distinct(city)) from zips") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, GroupOp, ProjectOp)))
    }

    "plan distinct of expression as expression" in {
      plan(sqlE"select count(distinct substring(city, 0, 1)) from zips") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, GroupOp, ProjectOp)))
    }

    "plan distinct with unrelated order by" in {
      plan(sqlE"select distinct city from zips order by pop desc") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, ProjectOp, SortOp, GroupOp, SortOp, ProjectOp)))
    }

    trackPending(
      "distinct with sum and group",
      plan(sqlE"SELECT DISTINCT SUM(pop) AS totalPop, city, state FROM zips GROUP BY city"),
      // should not use map-reduce
      // the occurrences of consecutive $project ops: '1'
      IList(ReadOp, GroupOp, ProjectOp, UnwindOp, GroupOp, ProjectOp))

    trackPending(
      "distinct with sum, group, and orderBy",
      plan(sqlE"SELECT DISTINCT SUM(pop) AS totalPop, city, state FROM zips GROUP BY city ORDER BY totalPop DESC"),
      // should not use map-reduce
      // actual: the occurrences of consecutive $project ops: '1'
      IList(ReadOp, GroupOp, ProjectOp, UnwindOp, SortOp, GroupOp, ProjectOp, SortOp))

    "plan time_of_day (JS)" in {
      plan(sqlE"select time_of_day(ts) from days") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, ProjectOp)))
    }

    trackPendingTree(
      "non-equi join",
      plan(sqlE"select smallZips.city from zips join smallZips on zips.`_id` < smallZips.`_id`"),
      // duplicate typechecks. qz-3571
      projectOp.node(matchOp.node(projectOp.node(unwindOp.node(unwindOp.node(matchOp.node(stdFoldLeftJoinSubTree)))))))

    "plan simple inner equi-join (map-reduce)" in {
      plan(
        sqlE"select customers.last_name, orders.purchase_date from customers join orders on customers.customer_key = orders.customer_key") must
        beRight.which(cwf => notBrokenWithOpsTree(cwf.op,
          projectOp.node(unwindOp.node(unwindOp.node(matchOp.node(stdFoldLeftJoinSubTree))))))
    }

    trackPending(
      "simple inner equi-join with expression ($lookup)",
      plan(sqlE"select zips.city, smallZips.state from zips join smallZips on lower(zips.`_id`) = smallZips.`_id`"),
      // duplicate typechecks. qz-3571
      IList(ReadOp, ProjectOp, LookupOp, ProjectOp, UnwindOp, ProjectOp))

    "plan simple inner equi-join with pre-filtering ($lookup)" in {
      plan(sqlE"select zips.city, smallZips.state from zips join smallZips on zips.`_id` = smallZips.`_id` where smallZips.pop >= 10000") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, MatchOp, ProjectOp, LookupOp, UnwindOp, ProjectOp), checkDanglingRefs = false))
    }

    "plan simple outer equi-join with wildcard" in {
      plan(sqlE"select * from customers full join orders on customers.customer_key = orders.customer_key") must
        beRight.which(cwf => notBrokenWithOpsTree(cwf.op,
          //old mongo:
          //projectOp.node(simpleMapOp.node(unwindOp.node(unwindOp.node(projectOp.node(stdFoldLeftJoinSubTree)))))))
          simpleMapOp.node(unwindOp.node(unwindOp.node(projectOp.node(stdFoldLeftJoinSubTree))))))
    }

    "plan simple left equi-join (map-reduce)" in {
      plan(
        sqlE"select customers.last_name, orders.purchase_date from customers left join orders on customers.customer_key = orders.customer_key") must
        beRight.which(cwf => notBrokenWithOpsTree(cwf.op,
          projectOp.node(unwindOp.node(unwindOp.node(projectOp.node(matchOp.node(stdFoldLeftJoinSubTree)))))))
    }

    "plan simple left equi-join ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address from foo left join bar on foo.id = bar.foo_id",
        defaultStats,
        indexes(collection("db", "bar") -> BsonField.Name("foo_id")),
        emptyDoc) must beRight.which(wf =>
          notBrokenWithOps(wf.op, IList(ReadOp, MatchOp, ProjectOp, LookupOp, ProjectOp, UnwindOp, ProjectOp), false))
    }

    "plan simple right equi-join ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address from foo right join bar on foo.id = bar.foo_id",
        defaultStats,
        indexes(collection("db", "foo") -> BsonField.Name("id")),
        emptyDoc) must beRight.which(wf =>
          notBrokenWithOps(wf.op, IList(ReadOp, MatchOp, ProjectOp, LookupOp, ProjectOp, UnwindOp, ProjectOp), false))
    }

    trackPendingTree(
      "3-way right equi-join (map-reduce)",
      plan(sqlE"select customers.last_name, orders.purchase_date, ordered_items.qty from customers join orders on customers.customer_key = orders.customer_key right join ordered_items on orders.order_key = ordered_items.order_key"),
      // Failing because of typechecks on statically known structures (qz-3577)
      // should use less pipeline ops
      projectOp.node(unwindOp.node(unwindOp.node(matchOp.node(
        foldLeftJoinSubTree(
          readOp.leaf,
          reduceOp.node(mapOp.node(unwindOp.node(unwindOp.node(stdFoldLeftJoinSubTree))))))))))

    trackPending(
      "3-way equi-join ($lookup)",
      plan3_4(
        sqlE"select extraSmallZips.city, smallZips.state, zips.pop from extraSmallZips join smallZips on extraSmallZips.`_id` = smallZips.`_id` join zips on smallZips.`_id` = zips.`_id`",
        defaultStats,
        defaultIndexes,
        emptyDoc),
      // Failing because of typechecks on statically known structures (qz-3577)
      // actual [ReadOp,MatchOp,ProjectOp,LookupOp,UnwindOp,MatchOp,ProjectOp,MatchOp,ProjectOp,LookupOp,UnwindOp,SimpleMapOp,ProjectOp]
      IList(ReadOp, MatchOp, ProjectOp, LookupOp, UnwindOp, MatchOp, ProjectOp, LookupOp, UnwindOp, ProjectOp))

    trackPending(
      "count of $lookup",
      plan3_4(
        sqlE"select tp.`_id`, count(*) from `zips` as tp join `largeZips` as ti on tp.`_id` = ti.TestProgramId group by tp.`_id`",
        defaultStats,
        defaultIndexes,
        emptyDoc),
      // should not use map-reduce
      // actual [ReadOp,MatchOp,ProjectOp,LookupOp,UnwindOp,ProjectOp,SimpleMapOp,GroupOp]
      IList(ReadOp, MatchOp, ProjectOp, LookupOp, UnwindOp, GroupOp, ProjectOp))

    trackPendingTree(
      "join with multiple conditions",
      plan(sqlE"select l.sha as child, l.author.login as c_auth, r.sha as parent, r.author.login as p_auth from slamengine_commits as l join slamengine_commits as r on r.sha = l.parents[0].sha and l.author.login = r.author.login"),
      // should use less pipeline ops
      projectOp.node(unwindOp.node(unwindOp.node(matchOp.node(foldLeftJoinSubTree(simpleMapOp.node(readOp.leaf), readOp.leaf))))))

    trackPendingTree(
      "join with non-JS-able condition",
      plan(sqlE"select z1.city as city1, z1.loc, z2.city as city2, z2.pop from zips as z1 join zips as z2 on z1.loc[*] = z2.loc[*]"),
      projectOp.node(unwindOp.node(unwindOp.node(matchOp.node(foldLeftOp.node(
        projectOp.node(groupOp.node(unwindOp.node(projectOp.node(readOp.leaf)))),
        reduceOp.node(projectOp.node(groupOp.node(unwindOp.node(projectOp.node(readOp.leaf)))))))))))

    trackPendingTree(
      "simple cross",
      plan(sqlE"select extraSmallZips.city from smallZips, extraSmallZips where smallZips.pop < extraSmallZips.pop"),
      projectOp.node(matchOp.node(projectOp.node(unwindOp.node(unwindOp.node(matchOp.node(stdFoldLeftJoinSubTree)))))))

    "SD-1263 specific case of plan multiple reducing projections (all, distinct, orderBy)" in {
      val q = sqlE"select distinct loc || [pop - 1] as p1, pop - 1 as p2 from zips group by territory order by p2".project.asInstanceOf[Select[Fix[Sql]]]

      plan(q.embed) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        countAccumOps(wf) must_== 1
        countUnwindOps(wf) must_== 0
        countMatchOps(wf) must_== 0
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        appropriateColumns(wf, q)
        rootPushes(wf) must_== Nil
      }
    }
  }
}
