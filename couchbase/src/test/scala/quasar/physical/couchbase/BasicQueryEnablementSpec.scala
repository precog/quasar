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

package quasar.physical.couchbase

import slamdata.Predef._
import quasar._
import quasar.compile.CompilerHelpers
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.effect.MonotonicSeq
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski.ι
import quasar.frontend.logicalplan.LogicalPlan
import quasar.fs.Planner.PlannerError
import quasar.qscript.{Map => _, Read => _, _}
import quasar.sql._

import scala.collection.JavaConverters._

import com.couchbase.client._, core._, java._, java.env._
import eu.timepit.refined.auto._
import matryoshka._, data._
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
  import common._, planner._

  sequential

  object CB extends Couchbase {
    override val QueryFileModule = new fs.queryfile with QueryFileModule {
      override def listContents(dir: ADir): Backend[Set[PathSegment]] =
        Set[PathSegment](FileName("beer").right, FileName("brewery")).η[Backend]
    }
  }

  val cbEnv = DefaultCouchbaseEnvironment.builder.build

  val docTypeKey = DocTypeKey("type")

  val cfg =
    Config(
      ClientContext(
        new CouchbaseBucket(
          cbEnv,
          new CouchbaseCore(cbEnv),
          "beer-sample",
          "",
          List[transcoder.Transcoder[_, _]]().asJava),
        docTypeKey,
        ListContentsView(docTypeKey)),
      CouchbaseCluster.create(cbEnv))

  def compileLogicalPlan(query: Fix[Sql]): Fix[LogicalPlan] =
    compile(query).map(optimizer.optimize).fold(e => scala.sys.error(e.shows), ι)

  def interp: CB.Eff ~> Task = fs.interp.unsafePerformSync

  def n1qlFromSql2(sql2: Fix[Sql]): String =
    (CB.lpToRepr(compileLogicalPlan(sql2)) ∘ (_.repr) >>= (CB.QueryFileModule.explain))
      .run.value.run(cfg)
      .foldMap(interp)
      .flatMap(_.fold(e => Task.fail(new RuntimeException(e.shows)), Task.now))
      .unsafePerformSync

  def n1qlFromQS(qs: Fix[QST]): String =
    qs.cataM(Planner[Fix, EitherT[Kleisli[Free[MonotonicSeq, ?], Context, ?], PlannerError, ?], QST].plan)
      .flatMapF(RenderQuery.compact(_).η[Kleisli[Free[MonotonicSeq, ?], Context, ?]])
      .run(Context(BucketName(cfg.ctx.bucket.name), cfg.ctx.docTypeKey))
      .foldMap(MonotonicSeq.from(0L).unsafePerformSync)
      .unsafePerformSync
      .valueOr(e => scala.sys.error(e.shows))

  def testSql2ToN1ql(sql2: Fix[Sql], n1ql: String): Fragment =
    pprint(sql2) in (n1qlFromSql2(sql2) must_= n1ql)

  def testSql2ToN1qlPending(sql2: String, p: Pending): Fragment =
    sql2 in p

  "SQL² to N1QL" should {
    testSql2ToN1ql(
      sqlE"select * from `beer`",
      """select v from (select value `_1` from (select value ifmissing(`_0`.['value'], `_0`) from `beer-sample` as `_0` where (`type` = 'beer')) as `_1`) v""")

    testSql2ToN1ql(
      sqlE"select name from `beer`",
      """select v from (select value `_1`.['name'] from (select value ifmissing(`_0`.['value'], `_0`) from `beer-sample` as `_0` where (`type` = 'beer')) as `_1`) v""")

    testSql2ToN1ql(
      sqlE"select name, type from `beer`",
      """select v from (select value {'name': `_1`.['name'], 'type': `_1`.['type']} from (select value ifmissing(`_0`.['value'], `_0`) from `beer-sample` as `_0` where (`type` = 'beer')) as `_1`) v""")

    testSql2ToN1ql(
      sqlE"select name from `beer` offset 1",
      """select v from (select value `_7`.['name'] from (select value `_4` from (select (select value ifmissing(`_5`.['value'], `_5`) from `beer-sample` as `_5` where (`type` = 'beer')) as `_1`, (select value 1 from (select value (select value [])) as `_6`) as `_2` from (select value []) as `_0`) as `_3` unnest `_1`[`_2`[0]:] as `_4`) as `_7`) v""")

    testSql2ToN1ql(
      sqlE"select count(*) from `beer`",
      """select v from (select value `_2` from (select count(`_1`) as `_2` from (select value ifmissing(`_0`.['value'], `_0`) from `beer-sample` as `_0` where (`type` = 'beer')) as `_1` group by null) as `_3` where (`_2` is not null)) v""")

    testSql2ToN1ql(
      sqlE"select count(name) from `beer`",
      """select v from (select value `_2` from (select count(`_1`.['name']) as `_2` from (select value ifmissing(`_0`.['value'], `_0`) from `beer-sample` as `_0` where (`type` = 'beer')) as `_1` group by null) as `_3` where (`_2` is not null)) v""")

    testSql2ToN1ql(
      sqlE"select geo.lat + geo.lon from `brewery`",
      """select v from (select value (`_1`.['geo'].['lat'] + `_1`.['geo'].['lon']) from (select value ifmissing(`_0`.['value'], `_0`) from `beer-sample` as `_0` where (`type` = 'brewery')) as `_1`) v""")
  }

  "QScript to N1QL" should {

    "read followed by a map" in {
      import qstdsl._
      // select (a + b) from foo
      val qs =
        fix.Map(
          fix.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
          recFunc.Add(
            recFunc.ProjectKeyS(recFunc.Hole, "a"),
            recFunc.ProjectKeyS(recFunc.Hole, "b")))

      val n1ql = n1qlFromQS(qs)

      n1ql must_= """select v from (select value (`_1`.['a'] + `_1`.['b']) from (select value ifmissing(`_0`.['value'], `_0`) from `beer-sample` as `_0` where (`type` = 'foo')) as `_1`) v"""
    }
  }

}
