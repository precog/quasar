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

package quasar.connector

import slamdata.Predef.Set
import quasar.RenderTreeT
import quasar.contrib.iota.:<<:
import quasar.contrib.pathy._
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript._
import quasar.qscript.rewrites._

import iotaz.{CopK, TListK}
import matryoshka.{BirecursiveT, EqualT, ShowT}
import matryoshka.implicits._
import scalaz.{Functor, Monad}
import scalaz.syntax.monad._

/** Provides for evaluating QScript to a result. */
abstract class QScriptEvaluator[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: Monad: MonadPlannerErr,
    R] {

  /** QScript used by this evaluator. */
  type QS[U[_[_]]] <: TListK
  type QSM[A] = CopK[QS[T], A]

  /** Executable representation. */
  type Repr

  def QSMFunctor: Functor[QSM]
  def QSMFromQScriptCore: QScriptCore[T, ?] :<<: QSM
  def QSMToQScriptTotal: Injectable[QSM, QScriptTotal[T, ?]]
  def UnirewriteT: Unirewrite[T, QS[T]]
  def UnicoalesceCap: Unicoalesce.Capture[T, QS[T]]

  /** Returns the result of executing the `Repr`. */
  def execute(repr: Repr): F[R]

  /** Returns a function that optimizes QScript for this evaluator. */
  def optimize: QSM[T[QSM]] => QSM[T[QSM]]

  /** Returns the executable representation of the given optimized QScript. */
  def plan(cp: T[QSM]): F[Repr]

  ////

  def evaluate(qsr: T[QScriptEducated[T, ?]]): F[R] =
    for {
      shifted <- Unirewrite[T, QS[T], F](new Rewrite[T], κ(Set[PathSegment]().point[F])).apply(qsr)

      optimized = shifted.transHylo(optimize, Unicoalesce.Capture[T, QS[T]].run)

      repr <- plan(optimized)

      result <- execute(repr)
    } yield result

  private final implicit def _QSMFunctor: Functor[QSM] = QSMFunctor
  private final implicit def _QSMFromQScriptCore: QScriptCore[T, ?] :<<: QSM = QSMFromQScriptCore
  private final implicit def _QSMToQScriptTotal: Injectable[QSM, QScriptTotal[T, ?]] = QSMToQScriptTotal
  private final implicit def _UnirewriteT: Unirewrite[T, QS[T]] = UnirewriteT
  private final implicit def _UnicoalesceCap: Unicoalesce.Capture[T, QS[T]] = UnicoalesceCap
}
