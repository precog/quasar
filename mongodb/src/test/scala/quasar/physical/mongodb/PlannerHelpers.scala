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
import quasar._, RenderTree.ops._
import quasar.common.{Map => _, _}
import quasar.compile.{queryPlan, CompilerHelpers, SemanticErrsT}
import quasar.contrib.pathy._, Helpers._
import quasar.contrib.specs2._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski._
import quasar.fs._
import quasar.frontend.{logicalplan => lp}, lp.{JoinDir, LogicalPlan => LP}
import quasar.javascript._
import quasar.{jscore => js}
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.selector.Selector
import quasar.physical.mongodb.workflow._
import quasar.qscript.DiscoverPath
import quasar.sql._

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
import org.specs2.specification.core.Fragment
import pathy.Path._
import scalaz.{Success => _, _}, Scalaz._

object PlannerHelpers {
  import Grouped.grouped
  import Reshape.reshape
  import jscore._

  import fixExprOp._

  sealed trait TestStatus
  case object Ok extends TestStatus
  case class Pending(s: String) extends TestStatus

  case class QsSpec(
    sqlToQs: TestStatus,
    qsToWf: TestStatus,
    qscript: Fix[fs.MongoQScript[Fix, ?]]
  )
  case class PlanSpec(
    name: String,
    sqlToWf: TestStatus,
    sql: Fix[Sql],
    qspec: Option[QsSpec],
    workflow: Workflow
  )

  val notOnPar = "Not on par with old (LP-based) connector."
  val nonOptimalQs = "QScript not optimal"

  import fixExprOp._

  val fp34: ExprOp3_4F.fixpoint[Fix[ExprOp], ExprOp] =
    new ExprOp3_4F.fixpoint[Fix[ExprOp], ExprOp](_.embed)
  val fp36: ExprOp3_6F.fixpoint[Fix[ExprOp], ExprOp] =
    new ExprOp3_6F.fixpoint[Fix[ExprOp], ExprOp](_.embed)

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

  // Some useful debugging objects
  val rt  = RenderTree[Crystallized[WorkflowF]]
  val rtq = RenderTree[Fix[fs.MongoQScript[Fix, ?]]]
  val toMetalPlan: Crystallized[WorkflowF] => Option[String] = WorkflowExecutor.toJS(_).toOption

  val basePathDb = rootDir[Sandboxed] </> dir("db")

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
          FileName("zips").right,
          FileName("zips2").right,
          FileName("extraSmallZips").right,
          FileName("smallZips").right,
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

  def compileSqlToLP[M[_]: Monad: MonadFsErr: PhaseResultTell]
      (sql: Fix[Sql], basePath: ADir): M[Fix[LP]] = {
    val (log, s) = queryPlan[SemanticErrsT[PhaseResultW, ?], Fix, Fix[LP]](
      sql, Variables.empty, basePath, 0L, None).run.run
    val lp = s.valueOr(e => scala.sys.error(e.shows))
    for {
      _ <- PhaseResultTell[M].tell(log)
    } yield lp
  }

  def sqlToQscript[M[_]: Monad: MonadFsErr: PhaseResultTell]
      (sql: Fix[Sql],
        basePath: ADir,
        lc: DiscoverPath.ListContents[M])
      : M[Fix[fs.MongoQScript[Fix, ?]]] =
    for {
      lp <- compileSqlToLP[M](sql, basePath)
      qs <- MongoDb.lpToQScript(lp, lc)
    } yield qs

  def planSqlToQScript(query: Fix[Sql]): Either[FileSystemError, Fix[fs.MongoQScript[Fix, ?]]] =
    sqlToQscript(query, basePathDb, listContents).run.value.toEither

  def queryPlanner[M[_]: Monad: MonadFsErr: PhaseResultTell]
      (sql: Fix[Sql],
        basePath: ADir,
        model: MongoQueryModel,
        stats: Collection => Option[CollectionStatistics],
        indexes: Collection => Option[Set[Index]],
        lc: DiscoverPath.ListContents[M],
        anyDoc: Collection => OptionT[M, BsonDocument],
        execTime: Instant)
      : M[Crystallized[WorkflowF]] =
    for {
      qs <- sqlToQscript(sql, basePath, lc)
      repr <- MongoDb.doPlan[Fix, M](qs, fs.QueryContext(stats, indexes), model, anyDoc, execTime)
    } yield repr

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
    basePath: ADir,
    model: MongoQueryModel,
    stats: Collection => Option[CollectionStatistics],
    indexes: Collection => Option[Set[Index]],
    anyDoc: Collection => OptionT[EitherWriter, BsonDocument]
  ): Either[FileSystemError, Crystallized[WorkflowF]] =
    queryPlanner(query, basePath, model, stats, indexes, listContents, anyDoc, Instant.now).run.value.toEither

  def plan3_2(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan0(query, basePathDb, MongoQueryModel.`3.2`, defaultStats, defaultIndexes, emptyDoc)

  def plan3_4(
    query: Fix[Sql],
    stats: Collection => Option[CollectionStatistics],
    indexes: Collection => Option[Set[Index]],
    anyDoc: Collection => OptionT[EitherWriter, BsonDocument]
  ): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan0(query, basePathDb, MongoQueryModel.`3.4`, stats, indexes, anyDoc)

  def plan3_4_4(
    query: Fix[Sql],
    stats: Collection => Option[CollectionStatistics],
    indexes: Collection => Option[Set[Index]],
    anyDoc: Collection => OptionT[EitherWriter, BsonDocument]
  ): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan0(query, basePathDb, MongoQueryModel.`3.4.4`, stats, indexes, anyDoc)

  def plan3_6(
    query: Fix[Sql],
    stats: Collection => Option[CollectionStatistics],
    indexes: Collection => Option[Set[Index]],
    anyDoc: Collection => OptionT[EitherWriter, BsonDocument]
  ): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan0(query, basePathDb, MongoQueryModel.`3.6`, stats, indexes, anyDoc)

  def plan(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan3_6(query, defaultStats, defaultIndexes, emptyDoc)

  def planMetal(query: Fix[Sql]): Option[String] =
    plan(query).disjunction.toOption >>= toMetalPlan

  def planAt(time: Instant, query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    queryPlanner(query, basePathDb, MongoQueryModel.`3.4`, defaultStats, defaultIndexes, listContents, emptyDoc, time).run.value.toEither

  def planLog(query: Fix[Sql]): Vector[PhaseResult] =
    queryPlanner(query, basePathDb, MongoQueryModel.`3.2`, defaultStats, defaultIndexes, listContents, emptyDoc, Instant.now).run.written

  def qscriptPlan(
    qs: Fix[fs.MongoQScript[Fix, ?]],
    model: MongoQueryModel,
    stats: Collection => Option[CollectionStatistics],
    indexes: Collection => Option[Set[Index]],
    anyDoc: Collection => OptionT[EitherWriter, BsonDocument]
  ): Either[FileSystemError, Crystallized[WorkflowF]] =
    MongoDb.doPlan(qs, fs.QueryContext(stats, indexes), model, anyDoc, Instant.now).run.value.toEither

  def qplan0(
    qs: Fix[fs.MongoQScript[Fix, ?]],
    stats: Collection => Option[CollectionStatistics],
    indexes: Collection => Option[Set[Index]]
  ): Either[FileSystemError, Crystallized[WorkflowF]] =
    qscriptPlan(qs, MongoQueryModel.`3.4.4`, stats, indexes, emptyDoc)

  def qplan(qs: Fix[fs.MongoQScript[Fix, ?]]): Either[FileSystemError, Crystallized[WorkflowF]] =
    qscriptPlan(qs, MongoQueryModel.`3.4.4`, defaultStats, defaultIndexes, emptyDoc)

  def qtestFile(testName: String): JFile = jFile(toRFile("q " + testName))

  def joinStructure0(
    left: Workflow, leftName: String, leftBase: Fix[ExprOp], right: Workflow,
    leftKey: Reshape.Shape[ExprOp], rightKey: (String, Fix[ExprOp], Reshape.Shape[ExprOp]) \/ JsCore,
    fin: FixOp[WorkflowF],
    swapped: Boolean) = {

    val (leftLabel, rightLabel) =
      if (swapped) (JoinDir.Right.name, JoinDir.Left.name) else (JoinDir.Left.name, JoinDir.Right.name)
    def initialPipeOps(
      src: Workflow, name: String, base: Fix[ExprOp], key: Reshape.Shape[ExprOp], mainLabel: String, otherLabel: String):
        Workflow =
      chain[Workflow](
        src,
        $group(grouped(name -> $push(base)), key),
        $project(
          reshape(
            mainLabel  -> $field(name),
            otherLabel -> $literal(Bson.Arr(List())),
            "_id"      -> $include()),
          IncludeId))
    fin(
      $foldLeft(
        initialPipeOps(left, leftName, leftBase, leftKey, leftLabel, rightLabel),
        chain[Workflow](
          right,
          rightKey.fold(
            rk => initialPipeOps(_, rk._1, rk._2, rk._3, rightLabel, leftLabel),
            rk => $map($MapF.mapKeyVal(("key", "value"),
              rk.toJs,
              Js.AnonObjDecl(List(
                (leftLabel, Js.AnonElem(List())),
                (rightLabel, Js.AnonElem(List(Js.Ident("value"))))))),
              ListMap())),
          $reduce(
            Js.AnonFunDecl(List("key", "values"),
              List(
                Js.VarDef(List(
                  ("result", Js.AnonObjDecl(List(
                    (leftLabel, Js.AnonElem(List())),
                    (rightLabel, Js.AnonElem(List()))))))),
                Js.Call(Js.Select(Js.Ident("values"), "forEach"),
                  List(Js.AnonFunDecl(List("value"),
                    List(
                      Js.BinOp("=",
                        Js.Select(Js.Ident("result"), leftLabel),
                        Js.Call(
                          Js.Select(Js.Select(Js.Ident("result"), leftLabel), "concat"),
                          List(Js.Select(Js.Ident("value"), leftLabel)))),
                      Js.BinOp("=",
                        Js.Select(Js.Ident("result"), rightLabel),
                        Js.Call(
                          Js.Select(Js.Select(Js.Ident("result"), rightLabel), "concat"),
                          List(Js.Select(Js.Ident("value"), rightLabel)))))))),
                Js.Return(Js.Ident("result")))),
            ListMap()))))
  }

  def joinStructure(
      left: Workflow, leftName: String, leftBase: Fix[ExprOp], right: Workflow,
      leftKey: Reshape.Shape[ExprOp], rightKey: (String, Fix[ExprOp], Reshape.Shape[ExprOp]) \/ JsCore,
      fin: FixOp[WorkflowF],
      swapped: Boolean) =
    Crystallize[WorkflowF].crystallize(joinStructure0(left, leftName, leftBase, right, leftKey, rightKey, fin, swapped))


}

trait PlannerHelpers extends
    org.specs2.mutable.Specification with
    ScalazSpecs2Instances with
    org.specs2.ScalaCheck with
    CompilerHelpers with
    TreeMatchers with
    PendingWithActualTracking {

  import PlannerHelpers._

  val mode: Mode = TestMode

  def beWorkflow(wf: Workflow) = beRight(equalToWorkflow(wf, addDetails = false))
  def beWorkflow0(wf: Workflow) = beRight(equalToWorkflow(wf, addDetails = true))

  def trackActual(wf: Crystallized[WorkflowF], file: JFile): Result = {
    val st = RenderTree[Crystallized[WorkflowF]].render(wf)
    mode match {
      case TestMode =>
        st.shows must_== unsafeRead(file)
      case WriteMode =>
        unsafeWrite(file, st.shows)
        Success(s" Wrote file with new actual $file")
    }
  }

  def planLP(logical: Fix[LP]): Either[FileSystemError, Crystallized[WorkflowF]] = {
    (for {
     _  <- emit("Input", logical)
     simplified <- emit("Simplified", optimizer.simplify(logical))
     qs <- MongoDb.lpToQScript(simplified, listContents)
     phys <- MongoDb.doPlan[Fix, EitherWriter](qs, fs.QueryContext(defaultStats, defaultIndexes), MongoQueryModel.`3.2`, emptyDoc, Instant.now)
    } yield phys).run.value.toEither
  }

  def testPlanSpec(s: PlanSpec): Fragment = {
    val planName = s"plan ${s.name}"
    planName >> {
      s.qspec.map { qspec =>
        val f = qtestFile(planName)
        qspec.qsToWf match {
          case Ok =>
            "qscript -> wf" in {
              if (f.exists) {
                failure(s"$f exists but expectation status is Ok. Either change status or remove file.")
              }
              qplan(qspec.qscript) must beWorkflow(s.workflow)
            }
          case Pending(str) =>
            "qscript -> wf" in {
              qplan(qspec.qscript) must beWorkflow0(s.workflow)
            }.pendingWithActual(str, f)
        }
        qspec.sqlToQs match {
          case Ok =>
            "sql^2 -> qscript" >> {
              planSqlToQScript(s.sql) must beRight(beTreeEqual(qspec.qscript))
            }
          case Pending(str) =>
            "sql^2 -> qscript" >> {
              planSqlToQScript(s.sql) must beRight(beTreeEqual(qspec.qscript))
            }.pendingUntilFixed(str)
        }
      }
      val f = testFile(planName)
      s.sqlToWf match {
        case Ok =>
          "sql^2 -> wf" in {
            if (f.exists) {
              failure(s"$f exists but expectation status is Ok. Either change status or remove file.")
            }
            PlannerHelpers.plan(s.sql) must beWorkflow(s.workflow)
          }
        case Pending(str) =>
          "sql^2 -> wf" in {
            PlannerHelpers.plan(s.sql) must beWorkflow0(s.workflow)
          }.pendingWithActual(str, f)
      }
    }
  }


}
