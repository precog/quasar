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

package quasar.regression

import quasar._
import quasar.api.resource.ResourcePath
import quasar.common.PhaseResultTell
import quasar.connector.QScriptEvaluator
import quasar.contrib.iota._
import quasar.fp._
import quasar.qscript._
import quasar.qscript.rewrites.{RewritePushdown, Unirewrite}

import scala.Predef.implicitly

import cats.effect.IO
import iotaz.CopK
import iotaz.TListK.:::
import iotaz.TNilK
import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import scalaz.{\/-, -\/, Const, Functor, Monad, Monoid}
import scalaz.syntax.applicative._
import scalaz.syntax.monoid._
import shims._

final class RegressionQScriptEvaluator[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: Monad: MonadPlannerErr: PhaseResultTell]
    extends QScriptEvaluator[T, F, QScriptCount] {

  type M[A] = IO[A]

  type QSRewrite[U[_[_]]] =
    QScriptCore[U, ?]            :::
    EquiJoin[U, ?]               :::
    Const[Read[ResourcePath], ?] :::
    TNilK

  type QS[U[_[_]]] = Const[InterpretedRead[ResourcePath], ?] ::: QSRewrite[U]

  type Repr = QScriptCount

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

  def UnirewriteT: Unirewrite[T, QSMRewrite] =
    implicitly[Unirewrite[T, QSMRewrite]]

  def optimize: F[QSMRewrite[T[QSM]] => QSM[T[QSM]]] =
    RewritePushdown[T, QSM, QSMRewrite, ResourcePath].point[F]

  def execute(repr: Repr): F[QScriptCount] = repr.point[F]

  def plan(cp: T[QSM]): F[Repr] = {
    val QScriptCore = CopK.Inject[QScriptCore[T, ?], QSM]
    val EquiJoin = CopK.Inject[EquiJoin[T, ?], QSM]
    val Read = CopK.Inject[Const[Read[ResourcePath], ?], QSM]
    val InterpretedRead = CopK.Inject[Const[InterpretedRead[ResourcePath], ?], QSM]

    def count: QSM[QScriptCount] => QScriptCount = {
      case InterpretedRead(_) => QScriptCount.incrementInterpretedRead

      case Read(_) => QScriptCount.incrementRead

      case EquiJoin(value) =>
        value.src |+| countBranch(value.lBranch) |+| countBranch(value.rBranch)

      case QScriptCore(value) => value match {
        case Map(src, _) => src
        case LeftShift(src, _, _, _, _, _) =>
          src |+| QScriptCount.incrementLeftShift
        case Reduce(src, _, _, _) => src
        case Sort(src, _, _) => src
        case Union(src, lBranch, rBranch) =>
          src |+| countBranch(lBranch) |+| countBranch(rBranch)
        case Filter(src, _) => src
        case Subset(src, from, _, count) =>
          src |+| countBranch(from) |+| countBranch(count)
        case Unreferenced() => Monoid[QScriptCount].zero
      }
    }

    def alg: ((T[QSM], QSM[QScriptCount])) => QScriptCount = {
      case (_, qsm) => count(qsm)
    }

    def algCoEnv: ((FreeQS[T], CoEnvQS[T, QScriptCount])) => QScriptCount = {
      case (_, coenv) => coenv.run match {
        case -\/(_) =>
          Monoid[QScriptCount].zero
        case \/-(qst) =>
          SubInject[CopK[QS[T], ?], QScriptTotal[T, ?]].project(qst)
            .map(count)
            .getOrElse(scala.sys.error("NOPE"))
      }
    }

    def countBranch(freeqs: FreeQS[T]): QScriptCount =
      freeqs.elgotPara[QScriptCount](algCoEnv)

    cp.elgotPara[QScriptCount](alg).point[F]
  }
}
