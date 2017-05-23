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

package quasar.physical.mongodb.fs

import slamdata.Predef._
import quasar._, RenderTree.ops._
import quasar.common.{PhaseResult, PhaseResults, PhaseResultT}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.scalaz.kleisli._
import quasar.fp._
import quasar.fp.ski._
import quasar.fs._
import quasar.javascript._
import quasar.frontend.logicalplan.{LogicalPlan, LogicalPlanR}
import quasar.physical.mongodb._, WorkflowExecutor.WorkflowCursor
import quasar.physical.mongodb.planner.MongoDbPlanner

import argonaut.JsonObject, JsonObject.{single => jSingle}
import argonaut.JsonIdentity._
import com.mongodb.async.client.MongoClient
import matryoshka.data.Fix
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object queryfileTypes {
  import QueryFile.ResultHandle

  type ResultCursor[C]     = List[Bson] \/ WorkflowCursor[C]
  type ResultMap[C]        = Map[ResultHandle, ResultCursor[C]]
  type EvalState[C]        = (Long, ResultMap[C])
  type QueryRT[F[_], C, A] = ReaderT[F, (Option[DefaultDb], TaskRef[EvalState[C]]), A]
  type MongoQuery[C, A]    = QueryRT[MongoDbIO, C, A]
}

object queryfile {
  import queryfileTypes._

  def interpret[C]
    (execMongo: WorkflowExecutor[MongoDbIO, C])
    (implicit C: DataCursor[MongoDbIO, C])
      : QueryFile ~> MongoQuery[C, ?] =
    new QueryFileInterpreter(
      execMongo,
      (lp, qc) => EitherT(WriterT(MongoDbPlanner.plan(lp, qc).leftMap(FileSystemError.planningFailed(lp, _)).run.run.point[MongoDbIO])))

  def interpretQ[C]
    (execMongo: WorkflowExecutor[MongoDbIO, C])
    (implicit C: DataCursor[MongoDbIO, C])
      : QueryFile ~> MongoQuery[C, ?] =
    new QueryFileInterpreter(
      execMongo,
      MongoDbQScriptPlanner.plan[Fix, FileSystemErrT[PhaseResultT[MongoDbIO, ?], ?]])

  def run[C, S[_]](
    client: MongoClient,
    defDb: Option[DefaultDb]
  )(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
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

final case class QueryContext[M[_]](
  model: MongoQueryModel,
  statistics: Collection => Option[CollectionStatistics],
  indexes: Collection => Option[Set[Index]],
  listContents: qscript.DiscoverPath.ListContents[M])

object QueryContext {

  import queryfileTypes._
  import FileSystemError._

  type MongoLogWF[C, A]  = PhaseResultT[QueryRT[MongoDbIO, C, ?], A]
  type MongoLogWFR[C, A] = FileSystemErrT[MongoLogWF[C, ?], A]

  val lpr = new LogicalPlanR[Fix[LogicalPlan]]

  def collections(lp: Fix[LogicalPlan]): PathError \/ Set[Collection] =
    // NB: documentation on `QueryFile` guarantees absolute paths, so calling `mkAbsolute`
    lpr.paths(lp).toList
      .traverse(file => Collection.fromFile(mkAbsolute(rootDir, file)))
      .map(_.toSet)

  def queryContext[C](lp: Fix[LogicalPlan]):
      MongoLogWFR[C, QueryContext[FileSystemErrT[PhaseResultT[MongoDbIO, ?], ?]]] = {
    def lift[A](fa: FileSystemErrT[MongoDbIO, A]): MongoLogWFR[C, A] =
      EitherT[MongoLogWF[C, ?], FileSystemError, A](
        fa.run.liftM[QueryRT[?[_], C, ?]].liftM[PhaseResultT])

    def lookup[A](f: Collection => MongoDbIO[A]): EitherT[MongoDbIO, FileSystemError, Map[Collection, A]] =
      for {
        colls <- EitherT.fromDisjunction[MongoDbIO](collections(lp).leftMap(pathErr(_)))
        a     <- colls.toList.traverse(c => f(c).strengthL(c)).map(Map(_: _*)).liftM[FileSystemErrT]
      } yield a

    lift(
      (MongoDbIO.serverVersion.liftM[FileSystemErrT] |@|
        lookup(MongoDbIO.collectionStatistics) |@|
        lookup(MongoDbIO.indexes))((vers, stats, idxs) =>
        QueryContext(
          MongoQueryModel(vers),
          stats.get(_),
          idxs.get(_),
          dir => EitherT(WriterT(listContents(dir).run.map((Vector[PhaseResult](), _)))))))
  }
}

private final class QueryFileInterpreter[C](
  execMongo: WorkflowExecutor[MongoDbIO, C],
  plan: (Fix[LogicalPlan], QueryContext[FileSystemErrT[PhaseResultT[MongoDbIO, ?], ?]]) => FileSystemErrT[PhaseResultT[MongoDbIO, ?], workflow.Crystallized[workflow.WorkflowF]])(
  implicit C: DataCursor[MongoDbIO, C]
) extends (QueryFile ~> queryfileTypes.MongoQuery[C, ?]) {

  import QueryFile._
  import quasar.physical.mongodb.workflow._
  import FileSystemError._
  import queryfileTypes._
  import QueryContext._

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import WriterT.writerTMonadListen
 
  private val execJs = WorkflowExecutor.javaScript

  def apply[A](qf: QueryFile[A]) = qf match {
    case ExecutePlan(lp, out) => (for {
      dst  <- EitherT(Collection.fromFile(out)
                .leftMap(pathErr(_))
                .point[MongoLogWF[C, ?]])
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
      ctx <- QueryContext.queryContext(lp)
      wf  <- convertPlanR(lp)(plan(lp, ctx))
      db  <- liftMQ(defaultDbName)
      (stmts, r) = execJs.evaluate(wf, db)
                     .leftMap(wfErrToFsErr(lp))
                     .run.run(CollectionName("tmp.gen_"))
                     .eval(0).run
      out =  Js.Stmts(stmts.toList).pprint(0)
      // TODO: Extract from QScript once legacy planner goes away.
      ipt =  lpr.absolutePaths(lp)
      ep  <- EitherT.fromDisjunction[MongoLogWF[C, ?]](
               r.as(ExecutionPlan(FsType, out, ipt)))
      _   <- logProgram(stmts).liftM[FileSystemErrT]
    } yield ep).run.run

    case ListContents(dir) =>
      listContents(dir).run.liftM[QRT]

    case FileExists(file) =>
      Collection.fromFile(file).fold(
        κ(false.point[MQ]),
        coll => MongoDbIO.collectionExists(coll).liftM[QRT])
  }

  ////

  private type PlanR[A]       = EitherT[WriterT[MongoDbIO, PhaseResults, ?], FileSystemError, A]

  type QRT[F[_], A] = QueryRT[F, C, A]
  type MQ[A]        = QRT[MongoDbIO, A]

  private type JsR[A] =
    WorkflowExecErrT[ReaderT[StateT[JavaScriptLog, Long, ?], CollectionName, ?], A]

  private val queryR =
    MonadReader[MQ, (Option[DefaultDb], TaskRef[EvalState[C]])]

  // FIXME: Not sure how to distinguish these.
  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
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

  private def defaultDbName: MQ[Option[DatabaseName]] =
    queryR.asks(_._1.map(_.run))

  private def genPrefix: MQ[CollectionName] =
    MongoDbIO.liftTask(NameGenerator.salt)
      .map(salt => CollectionName(s"tmp.gen_${salt}_"))
      .liftM[QRT]

  private val liftMQ: MQ ~> MongoLogWFR[C, ?] =
    liftMT[MongoLogWF[C, ?], FileSystemErrT] compose liftMT[MQ, PhaseResultT]


  private def convertPlanR(lp: Fix[LogicalPlan]): PlanR ~> MongoLogWFR[C, ?] =
    new (PlanR ~> MongoLogWFR[C, ?]) {
      def apply[A](pa: PlanR[A]) = {
        val r = pa.run.run
        val f: MongoLogWF[C, FileSystemError \/ A] = WriterT(r.liftM[QueryRT[?[_], C, ?]])
        EitherT(f)
      }
    }

  private def handlePlan[A](
    lp: Fix[LogicalPlan],
    log: Crystallized[WorkflowF] => JsR[_],
    handle: (Crystallized[WorkflowF], CollectionName) => WorkflowExecErrT[MQ, A]
  ): MongoLogWFR[C, A] = for {
    _      <- checkPathsExist(lp)
    ctx    <- QueryContext.queryContext(lp)
    wf     <- convertPlanR(lp)(plan(lp, ctx))
    prefix <- liftMQ(genPrefix)
    _      <- writeJsLog(lp, log(wf), prefix)
    a      <- EitherT[MongoLogWF[C, ?], FileSystemError, A](
                handle(wf, prefix)
                  .leftMap(wfErrToFsErr(lp))
                  .run.mapK(_.attemptMongo.leftMap(err =>
                    executionFailed(lp,
                      s"MongoDB Error: ${err.cause.getMessage}",
                      JsonObject.empty,
                      some(err)).left[A]
                    ).merge)
                  .liftM[PhaseResultT])
  } yield a

  private def execWorkflow(
    wf: Crystallized[WorkflowF],
    dst: Collection,
    tmpPrefix: CollectionName
  ): WorkflowExecErrT[MQ, Collection] =
    EitherT[MQ, WorkflowExecutionError, Collection](
      execMongo.execute(wf, dst).run.run(tmpPrefix).eval(0).liftM[QRT])

  private def evalWorkflow(
    wf: Crystallized[WorkflowF],
    defDb: Option[DatabaseName],
    tmpPrefix: CollectionName
  ): WorkflowExecErrT[MQ, ResultCursor[C]] =
    EitherT[MQ, WorkflowExecutionError, ResultCursor[C]](
      execMongo.evaluate(wf, defDb).run.run(tmpPrefix).eval(0).liftM[QRT])

  private def writeJsLog(lp: Fix[LogicalPlan], jsr: JsR[_], tmpPrefix: CollectionName): MongoLogWFR[C, Unit] = {
    val (stmts, r) = jsr.run.run(tmpPrefix).eval(0).run
    EitherT(logProgram(stmts) as r.leftMap(wfErrToFsErr(lp))).void
  }

  private def logProgram(prog: JavaScriptPrg): MongoLogWF[C, Unit] =
    MonadTell[MongoLogWF[C, ?], PhaseResults].tell(Vector(
      PhaseResult.detail("MongoDB", Js.Stmts(prog.toList).pprint(0))))


  private def checkPathsExist(lp: Fix[LogicalPlan]): MongoLogWFR[C, Unit] = {
    val rez = for {
      colls <- EitherT.fromDisjunction[MongoDbIO](collections(lp).leftMap(pathErr(_)))
      _     <- colls.traverse_(c => EitherT(MongoDbIO.collectionExists(c)
                .map(_ either (()) or pathErr(PathError.pathNotFound(c.asFile)))))
    } yield ()
    EitherT[MongoLogWF[C, ?], FileSystemError, Unit](
      rez.run.liftM[QRT].liftM[PhaseResultT])
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
        jSingle("data", bson.shows.asJson),
        none)

    case NoDatabase =>
      executionFailed_(lp,
        "Executing this plan on MongoDB requires temporary collections, but a database in which to store them could not be determined.")
  }
}
