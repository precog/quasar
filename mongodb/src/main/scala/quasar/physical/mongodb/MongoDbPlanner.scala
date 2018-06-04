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

import quasar._
import quasar.common.{PhaseResult, PhaseResults, PhaseResultT, PhaseResultTell}
import quasar.contrib.scalaz._, eitherT._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fs.{FileSystemError, MonadFsErr, Planner => _}
import quasar.physical.mongodb.WorkflowBuilder._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner.{selector => _, _}
import quasar.physical.mongodb.planner.common._
import quasar.physical.mongodb.workflow._
import quasar.qscript._, RenderQScriptDSL._
import quasar.qscript.rewrites.{Coalesce => _, Optimize, PreferProjection, Rewrite}

import java.time.Instant
import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import org.bson.BsonDocument
import scalaz._, Scalaz._

object MongoDbPlanner {

  def toMongoQScript[
      T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      M[_]: Monad: MonadFsErr: PhaseResultTell]
      (anyDoc: Collection => OptionT[M, BsonDocument],
        qs: T[fs.MongoQScript[T, ?]])
      (implicit BR: Branches[T, fs.MongoQScript[T, ?]])
      : M[T[fs.MongoQScript[T, ?]]] = {

    type MQS[A] = fs.MongoQScript[T, A]
    type QST[A] = QScriptTotal[T, A]

    val O = new Optimize[T]
    val R = new Rewrite[T]

    def normalize(mqs: T[MQS]): M[T[MQS]] = {
      val mqs1 = mqs.transCata[T[MQS]](R.normalizeEJ[MQS])
      val mqs2 = BR.branches.modify(
          _.transCata[FreeQS[T]](liftCo(R.normalizeEJCoEnv[QScriptTotal[T, ?]]))
        )(mqs1.project).embed
      Trans.applyTrans(assumeReadType[T, MQS, M](Type.AnyObject), idPrism[MQS])(mqs2)
    }

    // TODO: All of these need to be applied through branches. We may also be able to compose
    //       them with normalization as the last step and run until fixpoint. Currently plans are
    //       too sensitive to the order in which these are applied.
    //       Some constraints:
    //       - elideQuasarSigil should only be applied once
    //       - elideQuasarSigil needs assumeReadType to be applied in order to
    //         work properly in all cases
    //       - R.normalizeEJ/R.normalizeEJCoEnv may change the structure such
    //         that assumeReadType can elide more guards
    //         E.g. Map(x, SrcHole) is normalized into x. assumeReadType does
    //         not recognize any Map as shape preserving, but it may recognize
    //         x being shape preserving (e.g. when x = ShiftedRead(y, ExcludeId))
    for {
      mongoQS1 <- Trans.applyTrans(assumeReadType[T, MQS, M](Type.AnyObject), idPrism[MQS])(qs)
      mongoQS2 <- mongoQS1.transCataM(elideQuasarSigil[T, MQS, M](anyDoc))
      mongoQS3 <- normalize(mongoQS2)
      _ <- PhaseResults.logPhase[M](
             PhaseResult.treeAndCode("QScript Mongo", mongoQS3))

      mongoQS4 =  mongoQS3.transCata[T[MQS]](
                    liftFFCopK[QScriptCore[T, ?], MQS, T[MQS]](
                      repeatedly(O.subsetBeforeMap[MQS, MQS](
                        reflNT[MQS]))))
      _ <- PhaseResults.logPhase[M](
             PhaseResult.treeAndCode("QScript Mongo (Subset Before Map)",
             mongoQS4))

      // TODO: Once field deletion is implemented for 3.4, this could be selectively applied, if necessary.
      mongoQS5 =  PreferProjection.preferProjection[MQS](mongoQS4)
      _ <- PhaseResults.logPhase[M](
             PhaseResult.treeAndCode("QScript Mongo (Prefer Projection)",
             mongoQS5))
    } yield mongoQS5
  }

  def buildWorkflow
    [T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      M[_]: Monad: PhaseResultTell: MonadFsErr: ExecTimeR,
      WF[_]: Functor: Coalesce: Crush,
      EX[_]: Traverse]
    (cfg: PlannerConfig[T, EX, WF, M])
    (qs: T[fs.MongoQScript[T, ?]])
    (implicit
      ev0: WorkflowOpCoreF :<: WF,
      ev1: ExprOpCoreF :<: EX,
      ev2: EX :<: ExprOp,
      ev3: RenderTree[Fix[WF]])
      : M[Fix[WF]] =
    for {
      wb <- qs.cataM[M, WorkflowBuilder[WF]](
              Planner[T, fs.MongoQScript[T, ?]].plan[M, WF, EX](cfg).apply(_) ∘
                (_.transCata[Fix[WorkflowBuilderF[WF, ?]]](
                  repeatedly(WorkflowBuilder.normalize[WF, Fix[WorkflowBuilderF[WF, ?]]]))))
      _ <- PhaseResults.logPhase[M](
             PhaseResult.tree("Workflow Builder", wb))

      wf <- liftM[M, Fix[WF]](WorkflowBuilder.build[WBM, WF](wb, cfg.queryModel))
      _ <- PhaseResults.logPhase[M](
             PhaseResult.tree("Workflow (raw)", wf))

    } yield wf

  def plan0
    [T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      M[_]: Monad: ExecTimeR,
      WF[_]: Traverse: Coalesce: Crush: Crystallize,
      EX[_]: Traverse]
    (anyDoc: Collection => OptionT[M, BsonDocument],
      cfg: PlannerConfig[T, EX, WF, M])
    (qs: T[fs.MongoQScript[T, ?]])
    (implicit
      ev0: WorkflowOpCoreF :<: WF,
      ev1: WorkflowBuilder.Ops[WF],
      ev2: ExprOpCoreF :<: EX,
      ev3: EX :<: ExprOp,
      ev4: RenderTree[Fix[WF]],
      ME:  MonadFsErr[M],
      MT:  PhaseResultTell[M])
      : M[Crystallized[WF]] = {

    def doBuildWorkflow[F[_]: Monad: ExecTimeR]
      (cfg0: PlannerConfig[T, EX, WF, F])
      (qs0: T[fs.MongoQScript[T, ?]])
      (implicit ME: MonadFsErr[F])
        : F[FileSystemError \/ (PhaseResults, Fix[WF])] = {
      val fh = cfg0.funcHandler andThen (_.liftM[PhaseResultT])
      // NB: buildWorkflow[T, FileSystemErrT[PhaseResultT[F, ?], ?], WF, EX]
      // gives the right return type F[(PhaseResults, FileSystemError \/ Fix[WF])]
      // but adding a second FileSystemErrT screws up error handling:
      // unimplemented MapFunc's in FuncHandler don't fall back to JsFuncHandler
      // anymore
      ME.attempt(buildWorkflow[T, PhaseResultT[F, ?], WF, EX](
        cfg0.copy(funcHandler = fh))(qs0).run)
    }

    def doMapBeforeSort(qs0: T[fs.MongoQScript[T, ?]]) =
      for {
        t <- Trans(mapBeforeSort[T, M], qs0)
        _ <- PhaseResults.logPhase[M](
                PhaseResult.tree("QScript Mongo (Map Before Sort)", t))
      } yield t

    for {
      qs0 <- toMongoQScript[T, M](anyDoc, qs)
      res0 <- doBuildWorkflow[M](cfg)(qs0)
      wf0 <- res0 match {
               case \/-((_, wf)) if (needsMapBeforeSort(wf)) =>
                 // TODO look into adding mapBeforeSort to WorkflowBuilder or Workflow stage
                 // instead, so that we can avoid having to rerun some transformations.
                 // See #3063
                 doMapBeforeSort(qs0) >>= buildWorkflow[T, M, WF, EX](cfg)
               case \/-((log, wf)) =>
                 MT.tell(log) *> wf.point[M]
               case -\/(err) =>
                 raiseErr[M, Fix[WF]](err)
             }
      wf1 = Crystallize[WF].crystallize(wf0)
      _ <- PhaseResults.logPhase[M](
             PhaseResult.tree("Workflow (crystallized)", wf1))
    } yield wf1
  }

  def planExecTime[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      M[_]: Monad: PhaseResultTell: MonadFsErr](
      qs: T[fs.MongoQScript[T, ?]],
      queryContext: fs.QueryContext,
      queryModel: MongoQueryModel,
      anyDoc: Collection => OptionT[M, BsonDocument],
      execTime: Instant)
      : M[Crystallized[WorkflowF]] = {
    val peek = anyDoc andThen (_.mapT(_.liftM[ReaderT[?[_], Instant, ?]]))
    plan[T, ReaderT[M, Instant, ?]](qs, queryContext, queryModel, peek).run(execTime)
  }

  /** Translate the QScript plan to an executable MongoDB "physical"
    * plan, taking into account the current runtime environment as captured by
    * the given context.
    *
    * Internally, the type of the plan being built constrains which operators
    * can be used, but the resulting plan uses the largest, common type so that
    * callers don't need to worry about it.
    *
    * @param anyDoc returns any document in the given `Collection`
    */
  def plan[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      M[_]: Monad: PhaseResultTell: MonadFsErr: ExecTimeR](
      qs: T[fs.MongoQScript[T, ?]],
      queryContext: fs.QueryContext,
      queryModel: MongoQueryModel,
      anyDoc: Collection => OptionT[M, BsonDocument])
      : M[Crystallized[WorkflowF]] = {
    import MongoQueryModel._

    val bsonVersion = toBsonVersion(queryModel)

    def joinHandler[WF[_]: Functor: Coalesce: Crush: Crystallize]
      (implicit ev0: Classify[WF], ev1: WorkflowOpCoreF :<: WF, ev2: RenderTree[WorkflowBuilder[WF]])
        : JoinHandler[WF, WBM] =
      JoinHandler.fallback[WF, WBM](
        JoinHandler.pipeline[WBM, WF](queryModel, queryContext.statistics, queryContext.indexes),
        JoinHandler.mapReduce[WBM, WF](queryModel))

    queryModel match {
      case `3.6` =>
        val cfg = PlannerConfig[T, Expr3_6, Workflow3_4F, M](
          joinHandler[Workflow3_4F],
          FuncHandler.handle3_6[MapFunc[T, ?], M](bsonVersion),
          StaticHandler.handle,
          queryModel,
          bsonVersion)
        plan0[T, M, Workflow3_4F, Expr3_6](anyDoc, cfg)(qs)

      case `3.4.4` =>
        val cfg = PlannerConfig[T, Expr3_4_4, Workflow3_4F, M](
          joinHandler[Workflow3_4F],
          FuncHandler.handle3_4_4[MapFunc[T, ?], M](bsonVersion),
          StaticHandler.handle,
          queryModel,
          bsonVersion)
        plan0[T, M, Workflow3_4F, Expr3_4_4](anyDoc, cfg)(qs)

      case `3.4` =>
        val cfg = PlannerConfig[T, Expr3_4, Workflow3_4F, M](
          joinHandler[Workflow3_4F],
          FuncHandler.handle3_4[MapFunc[T, ?], M](bsonVersion),
          StaticHandler.handle,
          queryModel,
          bsonVersion)
        plan0[T, M, Workflow3_4F, Expr3_4](anyDoc, cfg)(qs)

      case `3.2` =>
        val cfg = PlannerConfig[T, Expr3_2, Workflow3_2F, M](
          joinHandler[Workflow3_2F],
          FuncHandler.handle3_2[MapFunc[T, ?], M](bsonVersion),
          StaticHandler.handle,
          queryModel,
          bsonVersion)
        plan0[T, M, Workflow3_2F, Expr3_2](anyDoc, cfg)(qs).map(_.inject[WorkflowF])

    }
  }
}
