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
import quasar._, RenderTree.ops._
import quasar.common.{Map => _, _}
import quasar.contrib.pathy._, Helpers._
import quasar.fp._
import quasar.fp.ski._
import quasar.fs._
import quasar.{jscore => js}
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP}
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.workflow._
import quasar.qscript.DiscoverPath
import quasar.sql , sql._

import java.io.{File => JFile}
import java.time.Instant
import scala.Either

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.bson.BsonDocument
import org.specs2.execute._
import org.specs2.matcher.{Matcher, Expectable}
import pathy.Path._
import scalaz._, Scalaz._

object PlannerHelpers {

  val notOnPar = "Not on par with old (LP-based) connector."

  import fixExprOp._

  val expr3_0Fp: ExprOp3_0F.fixpoint[Fix[ExprOp], ExprOp] =
    new ExprOp3_0F.fixpoint[Fix[ExprOp], ExprOp](_.embed)
  val expr3_2Fp: ExprOp3_2F.fixpoint[Fix[ExprOp], ExprOp] =
    new ExprOp3_2F.fixpoint[Fix[ExprOp], ExprOp](_.embed)
  val expr3_4Fp: ExprOp3_4F.fixpoint[Fix[ExprOp], ExprOp] =
    new ExprOp3_4F.fixpoint[Fix[ExprOp], ExprOp](_.embed)

  implicit def toBsonField(name: String) = BsonField.Name(name)
  implicit def toLeftShape(shape: Reshape[ExprOp]): Reshape.Shape[ExprOp] = -\/ (shape)
  implicit def toRightShape(exprOp: Fix[ExprOp]):   Reshape.Shape[ExprOp] =  \/-(exprOp)

  def isNumeric(field: BsonField): Selector =
    Selector.Or(
      Selector.Doc(field -> Selector.Type(BsonType.Int32)),
      Selector.Doc(field -> Selector.Type(BsonType.Int64)),
      Selector.Doc(field -> Selector.Type(BsonType.Dec)),
      Selector.Doc(field -> Selector.Type(BsonType.Text)),
      Selector.Or(
        Selector.Doc(field -> Selector.Type(BsonType.Date)),
        Selector.Doc(field -> Selector.Type(BsonType.Bool))))

  def divide(a1: Fix[ExprOp], a2: Fix[ExprOp]) =
    $cond($eq(a2, $literal(Bson.Int32(0))),
      $cond($eq(a1, $literal(Bson.Int32(0))),
        $literal(Bson.Dec(Double.NaN)),
        $cond($gt(a1, $literal(Bson.Int32(0))),
          $literal(Bson.Dec(Double.PositiveInfinity)),
          $literal(Bson.Dec(Double.NegativeInfinity)))),
      $divide(a1, a2))

  val underSigil: js.JsFn =
    js.JsFn(js.Name("__to_sigil"), js.obj(sigil.Quasar -> js.ident("__to_sigil")))

  type EitherWriter[A] =
    EitherT[Writer[Vector[PhaseResult], ?], FileSystemError, A]

  val resourcesDir: RDir =
    currentDir[Sandboxed] </> dir("mongodb") </> dir("src") </> dir("test") </>
    dir("resources") </> dir("planner")

  def toRFile(testName: String): RFile = resourcesDir </>
    file(testName.replaceAll(" ", "_")) <:> "txt"

  def testFile(testName: String): JFile = jFile(toRFile(testName))

  def emit[A: RenderTree](label: String, v: A): EitherWriter[A] =
    EitherT[Writer[PhaseResults, ?], FileSystemError, A](Writer(Vector(PhaseResult.tree(label, v)), v.right))

  case class equalToWorkflow(expected: Workflow, addDetails: Boolean)
      extends Matcher[Crystallized[WorkflowF]] {
    def apply[S <: Crystallized[WorkflowF]](s: Expectable[S]) = {

      val st = RenderTree[Crystallized[WorkflowF]].render(s.value)

      val diff: String = (st diff expected.render).shows

      val details =
        if (addDetails) FailureDetails(st.shows, expected.render.shows)
        else NoDetails

      result(expected == s.value.op,
             "\ntrees are equal:\n" + diff,
             "\ntrees are not equal:\n" + diff,
             s,
             details)
    }
  }

  val basePath = rootDir[Sandboxed] </> dir("db")

  val listContents: DiscoverPath.ListContents[EitherWriter] =
    dir => (
      if (dir ≟ rootDir)
        Set(
          DirName("db").left[FileName],
          DirName("db1").left,
          DirName("db2").left)
      else
        Set(
          FileName("foo").right[DirName],
          FileName("divide").right,
          FileName("nullsWithMissing").right,
          FileName("cars").right,
          FileName("cars2").right,
          FileName("extraSmallZips").right,
          FileName("smallZips").right,
          FileName("zips").right,
          FileName("zips2").right,
          FileName("largeZips").right,
          FileName("a").right,
          FileName("slamengine_commits").right,
          FileName("person").right,
          FileName("caloriesBurnedData").right,
          FileName("bar").right,
          FileName("baz").right,
          FileName("usa_factbook").right,
          FileName("user_comments").right,
          FileName("days").right,
          FileName("logs").right,
          FileName("usa_factbook").right,
          FileName("cities").right)).point[EitherWriter]

  val emptyDoc: Collection => OptionT[EitherWriter, BsonDocument] =
    _ => OptionT.some(new BsonDocument)

  implicit val monadTell: MonadTell[EitherT[PhaseResultT[Id, ?], FileSystemError, ?], PhaseResults] =
    EitherT.monadListen[WriterT[Id, Vector[PhaseResult], ?], PhaseResults, FileSystemError](
      WriterT.writerTMonadListen[Id, Vector[PhaseResult]])

  def compileSqlToLP[M[_]: Monad: MonadFsErr: PhaseResultTell](sql: Fix[Sql]): M[Fix[LP]] = {
    val (log, s) = queryPlan(sql, Variables.empty, basePath, 0L, None).run.run
    val lp = s.fold(
      e => scala.sys.error(e.shows),
      d => d.fold(e => scala.sys.error(e.shows), ι)
    )
    for {
      _ <- scala.Predef.implicitly[PhaseResultTell[M]].tell(log)
    } yield lp
  }

  def queryPlanner[M[_]: Monad: MonadFsErr: PhaseResultTell]
      (sql: Fix[Sql],
        model: MongoQueryModel,
        stats: Collection => Option[CollectionStatistics],
        indexes: Collection => Option[Set[Index]],
        lc: DiscoverPath.ListContents[M],
        anyDoc: Collection => OptionT[M, BsonDocument],
        execTime: Instant)
      : M[Crystallized[WorkflowF]] = {
    for {
      lp <- compileSqlToLP[M](sql)
      qs <- MongoDb.lpToQScript(lp, lc)
      repr <- MongoDb.doPlan[Fix, M](qs, fs.QueryContext(stats, indexes), model, anyDoc, execTime)
    } yield repr
  }

  val defaultStats: Collection => Option[CollectionStatistics] =
    κ(CollectionStatistics(10, 100, false).some)
  val defaultIndexes: Collection => Option[Set[Index]] =
    κ(Set(Index("_id_", NonEmptyList(BsonField.Name("_id") -> IndexType.Ascending), false)).some)

  def indexes(ps: (Collection, BsonField)*): Collection => Option[Set[Index]] =  {
    val map: Map[Collection, Set[Index]] = ps.toList.foldMap { case (c, f) => Map(c -> Set(Index(f.asText + "_", NonEmptyList(f -> IndexType.Ascending), false))) }
    c => map.get(c).orElse(defaultIndexes(c))
  }

  def plan0(
    query: Fix[Sql],
    model: MongoQueryModel,
    stats: Collection => Option[CollectionStatistics],
    indexes: Collection => Option[Set[Index]],
    anyDoc: Collection => OptionT[EitherWriter, BsonDocument]
  ): Either[FileSystemError, Crystallized[WorkflowF]] =
    queryPlanner(query, model, stats, indexes, listContents, anyDoc, Instant.now).run.value.toEither

  def plan2_6(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan0(query, MongoQueryModel.`2.6`, κ(None), κ(None), emptyDoc)

  def plan3_0(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan0(query, MongoQueryModel.`3.0`, κ(None), κ(None), emptyDoc)

  def plan3_2(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan0(query, MongoQueryModel.`3.2`, defaultStats, defaultIndexes, emptyDoc)

  def plan3_4(
    query: Fix[Sql],
    stats: Collection => Option[CollectionStatistics],
    indexes: Collection => Option[Set[Index]],
    anyDoc: Collection => OptionT[EitherWriter, BsonDocument]
  ): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan0(query, MongoQueryModel.`3.4`, stats, indexes, anyDoc)

  def plan(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan3_4(query, defaultStats, defaultIndexes, emptyDoc)

  def planAt(time: Instant, query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    queryPlanner(query, MongoQueryModel.`3.4`, defaultStats, defaultIndexes, listContents, emptyDoc, time).run.value.toEither

  def planLog(query: Fix[Sql]): Vector[PhaseResult] =
    queryPlanner(query, MongoQueryModel.`3.2`, defaultStats, defaultIndexes, listContents, emptyDoc, Instant.now).run.written

  def qplan0(
    qs: Fix[fs.MongoQScript[Fix, ?]],
    model: MongoQueryModel,
    stats: Collection => Option[CollectionStatistics],
    indexes: Collection => Option[Set[Index]],
    anyDoc: Collection => OptionT[EitherWriter, BsonDocument]
  ): Either[FileSystemError, Crystallized[WorkflowF]] =
    MongoDb.doPlan(qs, fs.QueryContext(stats, indexes), model, anyDoc, Instant.now).run.value.toEither

  def qplan(qs: Fix[fs.MongoQScript[Fix, ?]]): Either[FileSystemError, Crystallized[WorkflowF]] =
    qplan0(qs, MongoQueryModel.`3.4`, defaultStats, defaultIndexes, emptyDoc)

  def qtestFile(testName: String): JFile = jFile(toRFile("q " + testName))

}

trait PlannerHelpers extends
    org.specs2.mutable.Specification with
    org.specs2.ScalaCheck with
    CompilerHelpers with
    TreeMatchers {

  import PlannerHelpers._

  def beWorkflow(wf: Workflow) = beRight(equalToWorkflow(wf, addDetails = false))
  def beWorkflow0(wf: Workflow) = beRight(equalToWorkflow(wf, addDetails = true))

  def planLP(logical: Fix[LP]): Either[FileSystemError, Crystallized[WorkflowF]] = {
    (for {
     _  <- emit("Input", logical)
     simplified <- emit("Simplified", optimizer.simplify(logical))
     qs <- MongoDb.lpToQScript(simplified, listContents)
     phys <- MongoDb.doPlan[Fix, EitherWriter](qs, fs.QueryContext(defaultStats, defaultIndexes), MongoQueryModel.`3.2`, emptyDoc, Instant.now)
    } yield phys).run.value.toEither
  }
}
