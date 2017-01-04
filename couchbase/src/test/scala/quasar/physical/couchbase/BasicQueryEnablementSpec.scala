/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.couchbase

import quasar.Predef._
import quasar.{Planner => _, _}
import quasar.common.PhaseResultT
import quasar.contrib.pathy.{ADir, PathSegment}
import quasar.effect.MonotonicSeq
import quasar.fp._, eitherT._
import quasar.fp.free._
import quasar.fp.ski.ι
import quasar.frontend.logicalplan.LogicalPlan
import quasar.physical.couchbase.fs.queryfile._
import quasar.physical.couchbase.planner._, Planner._
import quasar.qscript.{Map => _, Read => _, _}, MapFuncs._
import quasar.sql.CompilerHelpers

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.execute.Pending
import org.specs2.specification.core.Fragment
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

// NB: These tests are incredibly fragile and should be removed once the
// implementation is sufficient to support existing query integration tests.

// TODO: Roll with N1QL AST and RenderN1QL tests instead

class BasicQueryEnablementSpec
  extends Qspec
  with QScriptHelpers
  with CompilerHelpers {

  sequential

  def compileLogicalPlan(query: String): Fix[LogicalPlan] =
    compile(query).map(optimizer.optimize).fold(e => scala.sys.error(e.shows), ι)

  def lc[S[_]]: DiscoverPath.ListContents[Plan[S, ?]] =
    Kleisli[Id, ADir, Set[PathSegment]](listContents >>> (_ + FileName("beer-sample").right))
      .transform(λ[Id ~> Plan[S, ?]](_.η[Plan[S, ?]]))
      .run

  type Eff[A] = (MonotonicSeq :/: Task)#M[A]

  def n1qlFromSql2(sql2: String): String =
    (lpLcToN1ql[Fix, Eff](compileLogicalPlan(sql2), lc) >>= (RenderQuery.compact(_).liftPE))
      .run.run.map(_._2)
      .foldMap(MonotonicSeq.fromZero.unsafePerformSync :+: reflNT[Task])
      .unsafePerformSync
      .fold(e => scala.sys.error(e.shows), ι)

  def n1qlFromQS(qs: Fix[QST]): String =
    (qs.cataM(Planner[Fix, Free[MonotonicSeq, ?], QST].plan) >>= (n1ql =>
      EitherT(RenderQuery.compact(n1ql).η[Free[MonotonicSeq, ?]].liftM[PhaseResultT])
    )).run.run.map(_._2)
      .foldMap(MonotonicSeq.fromZero.unsafePerformSync)
      .unsafePerformSync
      .fold(e => scala.sys.error(e.shows), ι)

  def testSql2ToN1ql(sql2: String, n1ql: String): Fragment =
    sql2 in (n1qlFromSql2(sql2) must_= n1ql)

  def testSql2ToN1qlPending(sql2: String, p: Pending): Fragment =
    sql2 in p

  "SQL² to N1QL" should {
    testSql2ToN1ql(
      "select * from `beer-sample`",
      """select value v from (select value `_1` from (select value ifmissing(`_0`.['value'], `_0`) from `beer-sample` as `_0`) as `_1`) v""")

    testSql2ToN1ql(
      "select name from `beer-sample`",
      """select value v from (select value `_1`.['name'] from (select value ifmissing(`_0`.['value'], `_0`) from `beer-sample` as `_0`) as `_1`) v""")

    testSql2ToN1ql(
      "select name, type from `beer-sample`",
      """select value v from (select value {'name': `_1`.['name'], 'type': `_1`.['type']} from (select value ifmissing(`_0`.['value'], `_0`) from `beer-sample` as `_0`) as `_1`) v""")

    testSql2ToN1ql(
      "select name from `beer-sample` offset 1",
      """select value v from (select value `_7`.['name'] from (select value `_4` from (select (select value ifmissing(`_5`.['value'], `_5`) from `beer-sample` as `_5`) as `_1`, (select value 1 from (select value []) as `_6`) as `_2` from (select value []) as `_0`) as `_3` unnest `_1`[`_2`[0]:] as `_4`) as `_7`) v""")

    testSql2ToN1ql(
      "select count(*) from `beer-sample`",
      """select value v from (select value `_2` from (select count(`_1`) as `_2` from (select value ifmissing(`_0`.['value'], `_0`) from `beer-sample` as `_0`) as `_1` group by null) as `_3` where (`_2` is not null)) v""")

    testSql2ToN1ql(
      "select count(name) from `beer-sample`",
      """select value v from (select value `_2` from (select count(`_1`.['name']) as `_2` from (select value ifmissing(`_0`.['value'], `_0`) from `beer-sample` as `_0`) as `_1` group by null) as `_3` where (`_2` is not null)) v""")

    testSql2ToN1ql(
      "select geo.lat + geo.lon from `beer-sample`",
      """select value v from (select value (`_1`.['geo'].['lat'] + `_1`.['geo'].['lon']) from (select value ifmissing(`_0`.['value'], `_0`) from `beer-sample` as `_0`) as `_1`) v""")
  }

  "QScript to N1QL" should {

    "read followed by a map" in {
      // select (a + b) from foo
      val qs =
        chain[Fix[QST], QST](
          SRT.inj(Const(ShiftedRead(rootDir </> file("foo"), ExcludeId))),
          QCT.inj(qscript.Map((),
            Free.roll(Add(
              ProjectFieldR(HoleF, StrLit("a")),
              ProjectFieldR(HoleF, StrLit("b")))))))

      val n1ql = n1qlFromQS(qs)

      n1ql must_= """select value v from (select value (`_1`.['a'] + `_1`.['b']) from (select value ifmissing(`_0`.['value'], `_0`) from `foo` as `_0`) as `_1`) v"""
    }
  }

}
