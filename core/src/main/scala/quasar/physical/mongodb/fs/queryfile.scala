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
import quasar._
import quasar.effect.Failure
import quasar.fp._
import quasar.fs._
import quasar.javascript._
import quasar.physical.mongodb._, WorkflowExecutor.WorkflowCursor

import com.mongodb.async.client.MongoClient
import matryoshka.{Fix, Recursive}
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.stream._
import scalaz.concurrent.Task

object queryfile {
  import QueryFile.ResultHandle

  type ResultCursor[C]     = List[Bson] \/ WorkflowCursor[C]
  type ResultMap[C]        = Map[ResultHandle, ResultCursor[C]]
  type EvalState[C]        = (Long, ResultMap[C])
  type QueryRT[F[_], C, A] = ReaderT[F, (Option[DefaultDb], TaskRef[EvalState[C]]), A]
  type QueryR[C, A]        = QueryRT[MongoDbIO, C, A]
  type MongoQuery[C, A]    = WorkflowExecErrT[QueryR[C, ?], A]

  def interpret[C](execMongo: WorkflowExecutor[MongoDbIO, C])
                  (implicit C: DataCursor[MongoDbIO, C])
                  : QueryFile ~> MongoQuery[C, ?] = {

    new QueryFileInterpreter(execMongo)
  }

  def run[C, S[_]: Functor](
    client: MongoClient,
    defDb: Option[DefaultDb]
  )(implicit
    S0: Task :<: S,
    S1: MongoErrF :<: S,
    S2: WorkflowExecErrF :<: S
  ): Task[MongoQuery[C, ?] ~> Free[S, ?]] = {
    type MQ[A] = MongoQuery[C, A]
    type F[A]  = Free[S, A]

    val wfErr = Failure.Ops[WorkflowExecutionError, S]

    def runMQ(ref: TaskRef[EvalState[C]]): MQ ~> F =
      new (MQ ~> F) {
        def apply[A](mq: MQ[A]) =
          wfErr.unattempt(mq.run.run((defDb, ref)).runF(client))
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

  type QRT[F[_], A] = QueryRT[F, C, A]
  type QR[A]        = QRT[MongoDbIO, A]
  type MQ[A]        = WorkflowExecErrT[QR, A]

  private val execJs = WorkflowExecutor.javaScript

  def apply[A](qf: QueryFile[A]) = qf match {
    case ExecutePlan(lp, out) =>
      EitherT[QR, WorkflowExecutionError, (PhaseResults, FileSystemError \/ AFile)](
        (for {
          _      <- checkPathsExist(lp)
          dst    <- EitherT(Collection.fromPath(out)
                              .leftMap(pathErr(_))
                              .point[MongoLogWF])
          wf     <- convertPlanR(lp)(MongoDbPlanner plan lp)
          prefix <- liftMQ(genPrefix)
          _      <- writeJsLog(execJs.execute(wf, dst), prefix)
          coll   <- liftMQ(execWorkflow(wf, dst, prefix))
        } yield coll.asFile).run.run.run)

    case EvaluatePlan(lp) =>
      EitherT[QR, WorkflowExecutionError, (PhaseResults, FileSystemError \/ ResultHandle)](
        (for {
          _       <- checkPathsExist(lp)
          wf      <- convertPlanR(lp)(MongoDbPlanner plan lp)
          prefix  <- liftMQ(genPrefix)
          dbName  <- liftMQ(defaultDbName)
          _       <- writeJsLog(execJs.evaluate(wf, dbName), prefix)
          rcursor <- liftMQ(evalWorkflow(wf, dbName, prefix))
          handle  <- liftMQ(recordCursor(rcursor))
        } yield handle).run.run.run)

    case More(h) =>
      moreResults(h)
        .toRight(unknownResultHandle(h))
        .run

    case Close(h) =>
      OptionT[MQ, ResultCursor[C]](MongoQuery(resultsL(h) <:= none))
        .flatMapF(_.fold(κ(().point[MQ]), wc =>
          DataCursor[MongoDbIO, WorkflowCursor[C]]
            .close(wc)
            .liftM[QRT]
            .liftM[WorkflowExecErrT]))
        .run.void

    case Explain(lp) =>
      EitherT[QR, WorkflowExecutionError, (PhaseResults, FileSystemError \/ ExecutionPlan)](
        (for {
          wf  <- convertPlanR(lp)(MongoDbPlanner plan lp)
          db  <- liftMQ(defaultDbName)
          res =  execJs.evaluate(wf, db).run.run("tmp.gen").eval(0).run
          (stmts, r) = res
          out =  Js.Stmts(stmts.toList).pprint(0)
          ep  <- liftMQ(EitherT.fromDisjunction(r as ExecutionPlan(MongoDBFsType, out)))
          _   <- logProgram(stmts)
        } yield ep).run.run.run)

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
      }).liftM[QRT].liftM[WorkflowExecErrT]

    case FileExists(file) =>
      Collection.fromPath(file).fold(
        κ(false.point[MQ]),
        coll => MongoDbIO.collectionExists(coll).liftM[QRT].liftM[WorkflowExecErrT])
  }

  ////

  private type PlanR[A]       = EitherT[(PhaseResults, ?), PPlannerError, A]
  private type MongoLogWF[A]  = PhaseResultT[MQ, A]
  private type MongoLogWFR[A] = FileSystemErrT[MongoLogWF, A]

  private type MQE[A, B] = EitherT[QR, A, B]
  private type W[A, B]   = WriterT[MQ, A, B]
  private type R[A, B]   = ReaderT[MongoDbIO, A, B]

  private val queryR =
    MonadReader[R, (Option[DefaultDb], TaskRef[EvalState[C]])]

  private def MongoQuery[A](f: TaskRef[EvalState[C]] => Task[A]): MQ[A] =
    queryR.ask
      .flatMapK { case (_, ref) => MongoDbIO.liftTask(f(ref)) }
      .liftM[WorkflowExecErrT]

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
    queryR.asks(_._1.map(_.run)).liftM[WorkflowExecErrT]

  private def genPrefix: MQ[String] =
    (MongoDbIO.liftTask(NameGenerator.salt)
      .liftM[QRT]: QR[String])
      .liftM[WorkflowExecErrT]
      .map(salt => s"tmp.gen_${salt}")

  private val liftMQ: MQ ~> MongoLogWFR =
    liftMT[MongoLogWF, FileSystemErrT] compose liftMT[MQ, PhaseResultT]

  private def convertPlanR(lp: Fix[LogicalPlan]): PlanR ~> MongoLogWFR =
    new (PlanR ~> MongoLogWFR) {
      def apply[A](pa: PlanR[A]) = {
        val r = pa.leftMap(plannerError(lp, _)).run
        val f: MongoLogWF[FileSystemError \/ A] = WriterT(r.point[MQ])
        EitherT(f)
      }
    }

  private def execWorkflow(
    wf: Crystallized,
    dst: Collection,
    tmpPrefix: String
  ): MQ[Collection] =
    EitherT[QR, WorkflowExecutionError, Collection](
      execMongo.execute(wf, dst).run.run(tmpPrefix).eval(0).liftM[QRT])

  private def evalWorkflow(
    wf: Crystallized,
    defDb: Option[String],
    tmpPrefix: String
  ): MQ[ResultCursor[C]] =
    EitherT[QR, WorkflowExecutionError, ResultCursor[C]](
      execMongo.evaluate(wf, defDb).run.run(tmpPrefix).eval(0).liftM[QRT])

  private type JsR[A] =
    WorkflowExecErrT[ReaderT[SeqNameGeneratorT[JavaScriptLog,?],String,?], A]

  private def writeJsLog(jsr: JsR[_], tmpPrefix: String): MongoLogWFR[Unit] = {
    val (stmts, r) = jsr.run.run(tmpPrefix).eval(0).run
    r.fold(err => liftMQ(err.raiseError[MQE, Unit]), κ(logProgram(stmts)))
  }

  private def logProgram(prog: JavaScriptPrg): MongoLogWFR[Unit] = {
    val phaseR: PhaseResult =
      PhaseResult.Detail("MongoDB", Js.Stmts(prog.toList).pprint(0))

    (MonadTell[W, PhaseResults].tell(Vector(phaseR)): MongoLogWF[Unit])
      .liftM[FileSystemErrT]
  }

  private def checkPathsExist(lp: Fix[LogicalPlan]): MongoLogWFR[Unit] = {
    // Documentation on `QueryFile` guarantees absolute paths, so calling `mkAbsolute`
    def checkPathExists(p: AFile): MongoFsM[Unit] = for {
      coll <- EitherT.fromDisjunction[MongoDbIO](Collection.fromPath(p))
                .leftMap(pathErr(_))
      _    <- EitherT(MongoDbIO.collectionExists(coll)
                .map(_ either (()) or pathErr(PathError.pathNotFound(p))))
    } yield ()

    EitherT[MongoLogWF, FileSystemError, Unit](
      (LogicalPlan.paths(lp).traverse_(path => checkPathExists(mkAbsolute(rootDir, path))).run
        .liftM[QRT]
        .liftM[WorkflowExecErrT]: MQ[FileSystemError \/ Unit])
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
        .liftM[QRT]
        .liftM[WorkflowExecErrT]))
  }
}
