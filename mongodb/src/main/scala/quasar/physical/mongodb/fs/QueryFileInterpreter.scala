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

package quasar.physical.mongodb.fs

import slamdata.Predef._
import quasar._, RenderTree.ops._
import quasar.common.{PhaseResult, PhaseResults, PhaseResultT}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.scalaz.kleisli._
import quasar.effect.NameGenerator
import quasar.fp._
import quasar.fp.ski._
import quasar.frontend.logicalplan.{constant, LogicalPlan}
import quasar.fs._
import quasar.javascript._
import quasar.physical.mongodb._, WorkflowExecutor.WorkflowCursor

import argonaut.JsonObject, JsonObject.{single => jSingle}
import argonaut.JsonIdentity._
import java.time.Instant
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

final class QueryFileInterpreter(execMongo: WorkflowExecutor[MongoDbIO, BsonCursor]) {

  import QueryFile._
  import quasar.physical.mongodb.workflow._
  import FileSystemError._
  import queryfileTypes._
  import QueryContext._

  private val execJs = WorkflowExecutor.javaScript

  def execPlan(wf: Crystallized[WorkflowF], out: AFile): MQPhErr[Unit] =
    (for {
      dst <- EitherT(Collection.fromFile(out)
                .leftMap(pathErr(_))
                .point[MongoLogWF[C, ?]])
      _   <- handlePlan(wf, execJs.execute(_, dst), execWorkflow(_, dst, _))
    } yield ()).run.run


  def evalPlan(wf: Crystallized[WorkflowF]): MQPhErr[ResultHandle] =
    (for {
      rcursor <- handlePlan(wf, execJs.evaluate(_), evalWorkflow(_, _))
      handle <- liftMQ(recordCursor(rcursor))
    } yield handle).run.run

  def explain(wf: Crystallized[WorkflowF]): MQPhErr[String] = {
    val (stmts, r) = execJs.evaluate(wf)
                       .leftMap(wfErrToFsErr(wf))
                       .run.run(CollectionName("tmp.gen_"))
                       .eval(0).run
    val out = Js.Stmts(stmts.toList).pprint(0)

    (for {
      exp <- EitherT.fromDisjunction[MongoLogWF[C, ?]](r.as(out))
      _   <- logProgram(stmts).liftM[FileSystemErrT]
    } yield exp).run.run
  }

  def more(h: ResultHandle): MQErr[Vector[Data]] =
    moreResults(h)
      .toRight(unknownResultHandle(h))
      .run

  def close(h: ResultHandle): MQ[Unit] =
    OptionT[MQ, ResultCursor[C]](MongoQuery(resultsL(h) <:= none))
      .flatMapF(_.fold(κ(().point[MQ]), wc =>
        DataCursor[MongoDbIO, WorkflowCursor[C]]
          .close(wc)
          .liftM[QRT]))
      .run.void

  def listContents0(dir: ADir): MQErr[Set[PathSegment]] =
    listContents(dir).run.liftM[QRT]

  def fileExists(file: AFile): MQ[Boolean] =
    Collection.fromFile(file).fold(
      κ(false.point[MQ]),
      coll => MongoDbIO.collectionExists(coll).liftM[QRT])

  def queryTime: MQ[Instant] =
    MongoDbIO.liftTask(Task.delay { Instant.now }).liftM[QRT]

  ////

  private type PlanR[A]       = EitherT[WriterT[MongoDbIO, PhaseResults, ?], FileSystemError, A]

  type C = BsonCursor
  implicit val DC: DataCursor[MongoDbIO, C] = bsoncursor.bsonCursorDataCursor

  private type JsR[A] =
    WorkflowExecErrT[ReaderT[StateT[JavaScriptLog, Long, ?], CollectionName, ?], A]

  private val queryR =
    MonadReader[MQ, TaskRef[EvalState[C]]]

  // FIXME: Not sure how to distinguish these.
  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  private def MongoQuery[A](f: TaskRef[EvalState[C]] => Task[A]): MQ[A] =
    queryR.ask flatMapK (ref => MongoDbIO.liftTask(f(ref)))

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

  private def genPrefix: MQ[CollectionName] =
    MongoDbIO.liftTask(NameGenerator.salt)
      .map(salt => CollectionName(s"tmp.gen_${salt}_"))
      .liftM[QRT]

  private val liftMQ: MQ ~> MongoLogWFR[C, ?] =
    liftMT[MongoLogWF[C, ?], FileSystemErrT] compose liftMT[MQ, PhaseResultT]

  def handlePlan[A](
    wf: Crystallized[WorkflowF],
    log: Crystallized[WorkflowF] => JsR[_],
    handle: (Crystallized[WorkflowF], CollectionName) => WorkflowExecErrT[MQ, A]
  ): MongoLogWFR[C, A] =
    for {
      prefix <- liftMQ(genPrefix)
      _ <- writeJsLog(wf, log(wf), prefix)
      a <- toMongoLogWFR(wf, handle(wf, prefix))
    } yield a

  private def toMongoLogWFR[A](wf: Crystallized[WorkflowF], wfErrMq: WorkflowExecErrT[MQ, A]): MongoLogWFR[C, A] =
    EitherT[MongoLogWF[C, ?], FileSystemError, A](
      wfErrMq.leftMap(wfErrToFsErr(wf))
        .run.mapK(_.attemptMongo.leftMap(err =>
          execFailed(
            wf,
            s"MongoDB Error: ${err.cause.getMessage}",
            JsonObject.empty,
            some(err)).left[A]
          ).merge)
        .liftM[PhaseResultT])

  def execWorkflow(
    wf: Crystallized[WorkflowF],
    dst: Collection,
    tmpPrefix: CollectionName
  ): WorkflowExecErrT[MQ, Unit] =
    EitherT[MQ, WorkflowExecutionError, Unit](
      execMongo.execute(wf, dst).run.run(tmpPrefix).eval(0).liftM[QRT])

  def evalWorkflow(
    wf: Crystallized[WorkflowF],
    tmpPrefix: CollectionName
  ): WorkflowExecErrT[MQ, ResultCursor[C]] =
    EitherT[MQ, WorkflowExecutionError, ResultCursor[C]](
      execMongo.evaluate(wf).run.run(tmpPrefix).eval(0).liftM[QRT])

  private def writeJsLog(wf: Crystallized[WorkflowF], jsr: JsR[_], tmpPrefix: CollectionName): MongoLogWFR[C, Unit] = {
    val (stmts, r) = jsr.run.run(tmpPrefix).eval(0).run
    EitherT(logProgram(stmts) as r.leftMap(wfErrToFsErr(wf))).void
  }

  private def logProgram(prog: JavaScriptPrg): MongoLogWF[C, Unit] =
    MonadTell[MongoLogWF[C, ?], PhaseResults].tell(Vector(
      PhaseResult.detail("MongoDB", Js.Stmts(prog.toList).pprint(0))))

  private def moreResults(h: ResultHandle): OptionT[MQ, Vector[Data]] = {
    val toData: Bson => Data =
      (BsonCodec.toData _) <<< sigil.Sigil[Bson].elideQuasarSigil

    def pureNextChunk(bsons: List[Bson]) =
      if (bsons.isEmpty)
        Vector.empty[Data].point[MQ]
      else
        MongoQuery(resultsL(h) := some(List().left))
          .as(bsons.map(toData).toVector)

    lookupCursor(h) flatMapF (_.fold(pureNextChunk, wc =>
      DataCursor[MongoDbIO, WorkflowCursor[C]]
        .nextChunk(wc)
        .liftM[QRT]))
  }

  import WorkflowExecutionError.{InvalidTask, InsertFailed, NoDatabase}

  private def execFailed(wf: Crystallized[WorkflowF], s: String, detail: JsonObject, cause: Option[PhysicalError])
      : FileSystemError =
    //FIXME executionFailed expects a LogicalPlan but this is not available anymore at this stage
    //so just supplying an empty LogicalPlan for now.
    //We can fix this properly after executionFailed has been refactored
    executionFailed(constant[Fix[LogicalPlan]](quasar.Data.NA).embed, s, detail, cause)

  private def execFailed_(wf: Crystallized[WorkflowF], s: String): FileSystemError =
    //FIXME executionFailed expects a LogicalPlan but this is not available anymore at this stage
    //so just supplying an empty LogicalPlan for now.
    //We can fix this properly after executionFailed has been refactored
    executionFailed_(constant[Fix[LogicalPlan]](quasar.Data.NA).embed, s)

  private def wfErrToFsErr(wf: Crystallized[WorkflowF]): WorkflowExecutionError => FileSystemError = {
    case InvalidTask(task, reason) =>
      execFailed(wf,
        s"Invalid MongoDB workflow task: $reason",
        jSingle("workflowTask", task.render.asJson),
        none)

    case InsertFailed(bson, reason) =>
      execFailed(wf,
        s"Unable to insert data into MongoDB: $reason",
        jSingle("data", bson.shows.asJson),
        none)

    case NoDatabase =>
      execFailed_(wf,
        "Executing this plan on MongoDB requires temporary collections, but a database in which to store them could not be determined.")
  }
}
