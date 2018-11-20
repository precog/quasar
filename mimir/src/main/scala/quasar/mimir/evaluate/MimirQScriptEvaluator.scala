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

package quasar.mimir.evaluate

import quasar._
import quasar.api.resource.ResourcePath
import quasar.common.PhaseResultTell
import quasar.connector.QScriptEvaluator
import quasar.contrib.cats.effect.liftio._
import quasar.contrib.iota._
import quasar.contrib.pathy._
import quasar.contrib.std.errorImpossible
import quasar.fp._
import quasar.fp.numeric._
import quasar.mimir
import quasar.mimir.MimirRepr
import quasar.mimir.MimirCake._
import quasar.mimir.evaluate.Config.{EvalConfigT, EvaluatorConfig}
import quasar.qscript._
import quasar.qscript.rewrites.{RewritePushdown, Unirewrite}
import quasar.yggdrasil.{Config => YggConfig, MonadFinalizers}

import scala.Predef.implicitly
import scala.concurrent.ExecutionContext

import cats.effect.{ContextShift, IO, LiftIO}
import iotaz.CopK
import iotaz.TListK.:::
import iotaz.TNilK
import matryoshka._
import matryoshka.implicits._
import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.either._
import shims._

final class MimirQScriptEvaluator[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: LiftIO: Monad: MonadPlannerErr: MonadFinalizers[?[_], IO]: PhaseResultTell] private (
    cake: Cake)(
    implicit cs: ContextShift[IO], ec: ExecutionContext)
    extends QScriptEvaluator[T, EvalConfigT[T, F, IO, ?], MimirRepr] {

  type MT[X[_], A] = EvalConfigT[T, X, IO, A]
  type M[A] = MT[F, A]

  type QSRewrite[U[_[_]]] =
    QScriptCore[U, ?]            :::
    EquiJoin[U, ?]               :::
    Const[Read[ResourcePath], ?] :::
    TNilK

  type QS[U[_[_]]] = Const[InterpretedRead[ResourcePath], ?] ::: QSRewrite[U]

  type Repr = MimirRepr

  implicit def QSMToQScriptTotal: Injectable[QSMRewrite, QScriptTotal[T, ?]] =
    SubInject[CopK[QSRewrite[T], ?], QScriptTotal[T, ?]]

  implicit def QSMRewriteToQSM: Injectable[QSMRewrite, QSM] =
    SubInject[CopK[QSRewrite[T], ?], CopK[QS[T], ?]]

  def RenderTQSMRewrite: RenderTree[T[QSMRewrite]] = {
    val toTotal: T[QSMRewrite] => T[QScriptTotal[T, ?]] =
      _.cata[T[QScriptTotal[T, ?]]](SubInject[CopK[QSRewrite[T], ?], QScriptTotal[T, ?]].inject(_).embed)

    RenderTree.contramap(toTotal)
  }

  def RenderTQSM: RenderTree[T[QSM]] = {
    val toTotal: T[QSM] => T[QScriptTotal[T, ?]] =
      _.cata[T[QScriptTotal[T, ?]]](SubInject[CopK[QS[T], ?], QScriptTotal[T, ?]].inject(_).embed)

    RenderTree.contramap(toTotal)
  }

  def QSMRewriteFunctor: Functor[QSMRewrite] = Functor[QSMRewrite]
  def QSMFunctor: Functor[QSM] = Functor[QSM]

  def UnirewriteT: Unirewrite[T, QSRewrite[T]] =
    implicitly[Unirewrite[T, QSRewrite[T]]]

  def optimize: M[QSMRewrite[T[QSM]] => QSM[T[QSM]]] =
    Kleisli.ask[F, EvaluatorConfig[T, IO]] map {
      _.pushdown match {
        case Pushdown.EnablePushdown => RewritePushdown[T, QSM, QSMRewrite, ResourcePath]
        case Pushdown.DisablePushdown => QSMRewriteToQSM.inject(_)  // no-op
      }
    }

  def execute(repr: Repr): M[Repr] =
    repr.map(_.compact(repr.P.trans.TransSpec1.Id).canonicalize(YggConfig.maxSliceRows).materialized)
      .asInstanceOf[Repr].point[M]    // stupid dependent types...

  def plan(cp: T[QSM]): M[Repr] = {
    def qScriptCorePlanner =
      new mimir.QScriptCorePlanner[T, M](cake)

    def equiJoinPlanner =
      new mimir.EquiJoinPlanner[T, M]

    def shiftedReadPlanner =
      new FederatedReadPlanner[T, F](cake)

    lazy val planQST: AlgebraM[M, QScriptTotal[T, ?], Repr] = {
      val QScriptCore = CopK.Inject[QScriptCore[T, ?], QScriptTotal[T, ?]]
      val EquiJoin = CopK.Inject[EquiJoin[T, ?], QScriptTotal[T, ?]]
      val Read = CopK.Inject[Const[Read[ResourcePath], ?], QScriptTotal[T, ?]]

      _ match {
        case QScriptCore(value) =>
          qScriptCorePlanner.plan(planQST)(value)

        case EquiJoin(value) =>
          equiJoinPlanner.plan(planQST)(value)

        case Read(value) =>
          val sr = value.getConst

          sr.path match {
            case ResourcePath.Leaf(file) =>
              shiftedReadPlanner.plan(Const(
                quasar.qscript.Read[AFile](file, sr.idStatus)).left)
            case ResourcePath.Root =>
              errorImpossible
          }

        case _ => errorImpossible
      }
    }

    def planQSM(in: QSM[Repr]): M[Repr] = {
      val QScriptCore = CopK.Inject[QScriptCore[T, ?], QSM]
      val EquiJoin = CopK.Inject[EquiJoin[T, ?], QSM]
      val Read = CopK.Inject[Const[Read[ResourcePath], ?], QSM]
      val InterpretedRead = CopK.Inject[Const[InterpretedRead[ResourcePath], ?], QSM]

      in match {
        case QScriptCore(value) =>
          qScriptCorePlanner.plan(planQST)(value)

        case EquiJoin(value) =>
          equiJoinPlanner.plan(planQST)(value)

        case Read(value) =>
          val sr = value.getConst

          sr.path match {
            case ResourcePath.Leaf(file) =>
              shiftedReadPlanner.plan(Const(
                quasar.qscript.Read[AFile](file, sr.idStatus)).left)
            case ResourcePath.Root =>
              errorImpossible
          }

        case InterpretedRead(value) =>
          val ir = value.getConst

          ir.path match {
            case ResourcePath.Leaf(file) =>
              shiftedReadPlanner.plan(Const(
                quasar.qscript.InterpretedRead[AFile](file, ir.instructions)).right)
            case ResourcePath.Root =>
              errorImpossible
          }

        case _ => errorImpossible
      }
    }

    cp.cataM[M, Repr](planQSM _)
  }
}

object MimirQScriptEvaluator {
  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: LiftIO: Monad: MonadPlannerErr: MonadFinalizers[?[_], IO]: PhaseResultTell](
      cake: Cake)(
      implicit cs: ContextShift[IO], ec: ExecutionContext)
      : QScriptEvaluator[T, EvalConfigT[T, F, IO, ?], MimirRepr] =
    new MimirQScriptEvaluator[T, F](cake)
}
