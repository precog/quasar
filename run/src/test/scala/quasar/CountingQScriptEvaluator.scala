/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.run

import quasar._
import quasar.api.resource.ResourcePath
import quasar.common.PhaseResultTell
import quasar.connector.QScriptEvaluator
import quasar.contrib.iota._
import quasar.contrib.std.errorImpossible
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript._

import iotaz.CopK
import iotaz.TListK.:::

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns.interpret

import scalaz.{Const, Functor, Monad}
import scalaz.syntax.applicative._
import scalaz.syntax.monoid._

import shims.monadToScalaz

abstract class CountingQScriptEvaluator[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: Monad: MonadPlannerErr: PhaseResultTell]
    extends QScriptEvaluator[T, F, QScriptCount] {

  type QS[U[_[_]]] = Const[InterpretedRead[ResourcePath], ?] ::: QScriptNormalizedList[U]

  type Repr = QScriptCount

  implicit def QSNormToQSM: Injectable[QScriptNormalized[T, ?], QSM] =
    SubInject[QScriptNormalized[T, ?], CopK[QS[T], ?]]

  def QSMFunctor: Functor[QSM] = Functor[QSM]

  def RenderTQSM: RenderTree[T[QSM]] = {
    val toTotal: T[QSM] => T[QScriptTotal[T, ?]] =
      _.cata[T[QScriptTotal[T, ?]]](SubInject[CopK[QS[T], ?], QScriptTotal[T, ?]].inject(_).embed)

    RenderTree.contramap(toTotal)
  }

  def execute(repr: Repr): F[QScriptCount] =
    repr.point[F]

  def plan(cp: T[QSM]): F[Repr] = {
    val QScriptCore = CopK.Inject[QScriptCore[T, ?], QSM]
    val EquiJoin = CopK.Inject[EquiJoin[T, ?], QSM]
    val Read = CopK.Inject[Const[Read[ResourcePath], ?], QSM]
    val InterpretedRead = CopK.Inject[Const[InterpretedRead[ResourcePath], ?], QSM]

    def count: QSM[QScriptCount] => QScriptCount = {
      case InterpretedRead(_) =>
        QScriptCount.incrementInterpretedRead

      case Read(_) =>
        QScriptCount.incrementRead

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

        case Unreferenced() => mzero[QScriptCount]
      }
    }

    def countT(qst: QScriptTotal[T, QScriptCount]): QScriptCount =
      SubInject[CopK[QS[T], ?], QScriptTotal[T, ?]]
        .project(qst)
        .fold(errorImpossible)(count)

    def countBranch(freeqs: FreeQS[T]): QScriptCount =
      freeqs.cata(interpret(κ(mzero[QScriptCount]), countT))

    cp.cata[QScriptCount](count).point[F]
  }
}
