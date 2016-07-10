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

package quasar.physical.mongodb.fs

import quasar.Predef._
import quasar.RenderTree.ops._
import quasar._
import quasar.fp._
import quasar.fs._
import quasar.javascript._
import quasar.physical.mongodb._, WorkflowExecutor.WorkflowCursor

import argonaut.JsonObject, JsonObject.{single => jSingle}
import argonaut.JsonIdentity._
import com.mongodb.async.client.MongoClient
import matryoshka.{Fix, Recursive}
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.stream.{Writer => _, _}
import scalaz.concurrent.Task

object queryfile {
  import QueryFile.ResultHandle

  type ResultCursor[C]     = List[Bson] \/ WorkflowCursor[C]
  type ResultMap[C]        = Map[ResultHandle, ResultCursor[C]]
  type EvalState[C]        = (Long, ResultMap[C])
  type QueryRT[F[_], C, A] = ReaderT[F, (Option[DefaultDb], TaskRef[EvalState[C]]), A]
  type MongoQuery[C, A]    = QueryRT[MongoDbIO, C, A]

  def interpret[C](execMongo: WorkflowExecutor[MongoDbIO, C])
                  (implicit C: DataCursor[MongoDbIO, C])
                  : QueryFile ~> MongoQuery[C, ?] = {

    new QueryFileInterpreter(execMongo)
  }

  def run[C, S[_]](
    client: MongoClient,
    defDb: Option[DefaultDb]
  )(implicit
    S0: Task :<: S,
    S1: MongoErr :<: S
  ): Task[MongoQuery[C, ?] ~> Free[S, ?]] = {
    type MQ[A] = MongoQuery[C, A]
    type F[A]  = Free[S, A]

    def runMQ(ref: TaskRef[EvalState[C]]): MQ ~> F =
      new (MQ ~> F) {
        def apply[A](mq: MQ[A]) =
          mq.run((defDb, ref)).runF(client)
      }

    TaskRef((0L, Map.empty: ResultMap[C])) map runMQ
  }
}

private final class QueryFileInterpreter[C](
  execMongo: WorkflowExecutor[MongoDbIO, C])(
  implicit C: DataCursor[MongoDbIO, C]
) extends (QueryFile ~> queryfile.MongoQuery[C, ?]) {

  import QueryFile._
  import Planner.{PlannerError => PPlannerError}
  import Workflow._
  import FileSystemError._, fsops._
  import Recursive.ops._
  import queryfile._

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import WriterT.writerTMonadListen

  type QRT[F[_], A] = QueryRT[F, C, A]
  type MQ[A]        = QRT[MongoDbIO, A]

  private val execJs = WorkflowExecutor.javaScript

  def apply[A](qf: QueryFile[A]) = qf match {
    case ExecutePlan(lp, out) => (for {
      dst  <- EitherT(Collection.fromFile(out)
                .leftMap(pathErr(_))
                .point[MongoLogWF])
      coll <- handlePlan(lp,
                execJs.execute(_, dst),
                execWorkflow(_, dst, _))
    } yield coll.asFile).run.run

    case EvaluatePlan(lp) => (for {
      dbName  <- liftMQ(defaultDbName)
      rcursor <- handlePlan(lp,
                   execJs.evaluate(_, dbName),
                   evalWorkflow(_, dbName, _))
      handle  <- liftMQ(recordCursor(rcursor))
    } yield handle).run.run

    case More(h) =>
      moreResults(h)
        .toRight(unknownResultHandle(h))
        .run

    case Close(h) =>
      OptionT[MQ, ResultCursor[C]](MongoQuery(resultsL(h) <:= none))
        .flatMapF(_.fold(κ(().point[MQ]), wc =>
          DataCursor[MongoDbIO, WorkflowCursor[C]]
            .close(wc)
            .liftM[QRT]))
        .run.void

    case Explain(lp) => (for {
      wf  <- convertPlanR(lp)(MongoDbPlanner plan lp)
      db  <- liftMQ(defaultDbName)
      (stmts, r) = execJs.evaluate(wf, db)
                     .leftMap(wfErrToFsErr(lp))
                     .run.run("tmp.gen").eval(0).run
      out =  Js.Stmts(stmts.toList).pprint(0)
      ep  <- EitherT.fromDisjunction[MongoLogWF](
               r.as(ExecutionPlan(MongoDBFsType, out)))
      _   <- logProgram(stmts).liftM[FileSystemErrT]
    } yield ep).run.run

    case ListContents(dir) =>
      (dirName(dir) match {
        case Some(_) =>
          collectionsInDir(dir)
            .map(_ foldMap (collectionPathSegment(dir) andThen (_.toSet)))
            .run

        case None if depth(dir) == 0 =>
          MongoDbIO.collections
            .map(collectionPathSegment(dir))
            .pipe(process1.stripNone)
            .runLog
            .map(_.toSet.right[FileSystemError])

        case None =>
          nonExistentParent[Set[PathSegment]](dir).run
      }).liftM[QRT]

    case FileExists(file) =>
      Collection.fromFile(file).fold(
        κ(false.point[MQ]),
        coll => MongoDbIO.collectionExists(coll).liftM[QRT])
  }

  ////

  private type PlanR[A]       = EitherT[Writer[PhaseResults, ?], PPlannerError, A]
  private type MongoLogWF[A]  = PhaseResultT[MQ, A]
  private type MongoLogWFR[A] = FileSystemErrT[MongoLogWF, A]

  private type JsR[A] =
    WorkflowExecErrT[ReaderT[SeqNameGeneratorT[JavaScriptLog,?],String,?], A]

  private val queryR =
    MonadReader[MQ, (Option[DefaultDb], TaskRef[EvalState[C]])]

  private def MongoQuery[A](f: TaskRef[EvalState[C]] => Task[A]): MQ[A] =
    queryR.ask flatMapK { case (_, ref) => MongoDbIO.liftTask(f(ref)) }

  private def MongoQuery[A](s: State[EvalState[C], A]): MQ[A] =
    MongoQuery(_ modifyS s.run)

  private val seqL: EvalState[C] @> Long =
    Lens.firstLens

  private val resultMapL: EvalState[C] @> ResultMap[C] =
    Lens.secondLens

  private def resultsL(h: ResultHandle): EvalState[C] @> Option[ResultCursor[C]] =
    Lens.mapVLens(h) <=< resultMapL

  private def freshHandle: MQ[ResultHandle] =
    MongoQuery(seqL <%= (_ + 1)) map (ResultHandle(_))

  private def recordCursor(c: ResultCursor[C]): MQ[ResultHandle] =
    freshHandle flatMap (h => MongoQuery(resultsL(h) := some(c)) as h)

  private def lookupCursor(h: ResultHandle): OptionT[MQ, ResultCursor[C]] =
    OptionT(MongoQuery(resultsL(h).st))

  private def defaultDbName: MQ[Option[String]] =
    queryR.asks(_._1.map(_.run))

  private def genPrefix: MQ[String] =
    MongoDbIO.liftTask(NameGenerator.salt)
      .map(salt => s"tmp.gen_${salt}")
      .liftM[QRT]

  private val liftMQ: MQ ~> MongoLogWFR =
    liftMT[MongoLogWF, FileSystemErrT] compose liftMT[MQ, PhaseResultT]

  private def convertPlanR(lp: Fix[LogicalPlan]): PlanR ~> MongoLogWFR =
    new (PlanR ~> MongoLogWFR) {
      def apply[A](pa: PlanR[A]) = {
        val r = pa.leftMap(planningFailed(lp, _)).run.run
        val f: MongoLogWF[FileSystemError \/ A] = WriterT(r.point[MQ])
        EitherT(f)
      }
    }

  private def handlePlan[A](
    lp: Fix[LogicalPlan],
    log: Crystallized => JsR[_],
    handle: (Crystallized, String) => WorkflowExecErrT[MQ, A]
  ): MongoLogWFR[A] = for {
    _      <- checkPathsExist(lp)
    wf     <- convertPlanR(lp)(MongoDbPlanner.plan(lp))
    prefix <- liftMQ(genPrefix)
    _      <- writeJsLog(lp, log(wf), prefix)
    a      <- EitherT[MongoLogWF, FileSystemError, A](
                handle(wf, prefix)
                  .leftMap(wfErrToFsErr(lp))
                  .run.mapK(_.attemptMongo.leftMap(err =>
                    executionFailed(lp,
                      s"MongoDB Error: ${err.getMessage}",
                      JsonObject.empty,
                      some(err)).left[A]
                    ).merge)
                  .liftM[PhaseResultT])
  } yield a

  private def execWorkflow(
    wf: Crystallized,
    dst: Collection,
    tmpPrefix: String
  ): WorkflowExecErrT[MQ, Collection] =
    EitherT[MQ, WorkflowExecutionError, Collection](
      execMongo.execute(wf, dst).run.run(tmpPrefix).eval(0).liftM[QRT])

  private def evalWorkflow(
    wf: Crystallized,
    defDb: Option[String],
    tmpPrefix: String
  ): WorkflowExecErrT[MQ, ResultCursor[C]] =
    EitherT[MQ, WorkflowExecutionError, ResultCursor[C]](
      execMongo.evaluate(wf, defDb).run.run(tmpPrefix).eval(0).liftM[QRT])

  private def writeJsLog(lp: Fix[LogicalPlan], jsr: JsR[_], tmpPrefix: String): MongoLogWFR[Unit] = {
    val (stmts, r) = jsr.run.run(tmpPrefix).eval(0).run
    EitherT(logProgram(stmts) as r.leftMap(wfErrToFsErr(lp))).void
  }

  private def logProgram(prog: JavaScriptPrg): MongoLogWF[Unit] =
    MonadTell[MongoLogWF, PhaseResults].tell(Vector(
      PhaseResult.Detail("MongoDB", Js.Stmts(prog.toList).pprint(0))))

  private def checkPathsExist(lp: Fix[LogicalPlan]): MongoLogWFR[Unit] = {
    // Documentation on `QueryFile` guarantees absolute paths, so calling `mkAbsolute`
    def checkFileExists(f: AFile): MongoFsM[Unit] = for {
      coll <- EitherT.fromDisjunction[MongoDbIO](Collection.fromFile(f))
                .leftMap(pathErr(_))
      _    <- EitherT(MongoDbIO.collectionExists(coll)
                .map(_ either (()) or pathErr(PathError.pathNotFound(f))))
    } yield ()

    EitherT[MongoLogWF, FileSystemError, Unit](
      (LogicalPlan.paths(lp)
        .traverse_(file => checkFileExists(mkAbsolute(rootDir, file))).run
        .liftM[QRT]: MQ[FileSystemError \/ Unit])
        .liftM[PhaseResultT])
  }

  private def moreResults(h: ResultHandle): OptionT[MQ, Vector[Data]] = {
    def pureNextChunk(bsons: List[Bson]) =
      if (bsons.isEmpty)
        Vector.empty[Data].point[MQ]
      else
        MongoQuery(resultsL(h) := some(List().left))
          .as(bsons.map(BsonCodec.toData).toVector)

    lookupCursor(h) flatMapF (_.fold(pureNextChunk, wc =>
      DataCursor[MongoDbIO, WorkflowCursor[C]]
        .nextChunk(wc)
        .liftM[QRT]))
  }

  import WorkflowExecutionError.{InvalidTask, InsertFailed, NoDatabase}

  private def wfErrToFsErr(lp: Fix[LogicalPlan]): WorkflowExecutionError => FileSystemError = {
    case InvalidTask(task, reason) =>
      executionFailed(lp,
        s"Invalid MongoDB workflow task: $reason",
        jSingle("workflowTask", task.render.asJson),
        none)

    case InsertFailed(bson, reason) =>
      executionFailed(lp,
        s"Unable to insert data into MongoDB: $reason",
        jSingle("data", bson.toString.asJson),
        none)

    case NoDatabase =>
      executionFailed_(lp,
        "Executing this plan on MongoDB requires temporary collections, but a database in which to store them could not be determined.")
  }
}
