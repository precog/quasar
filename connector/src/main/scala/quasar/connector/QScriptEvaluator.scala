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

import quasar.{RenderTree, RenderTreeT}
import quasar.common.PhaseResultTell
import quasar.common.phase
import quasar.qscript._
import quasar.qscript.rewrites._
import quasar.contrib.iota._

import iotaz.{CopK, TListK}
import matryoshka.{BirecursiveT, EqualT, ShowT}
import matryoshka.implicits._
import scalaz.{Functor, Monad}
import scalaz.syntax.monad._

/** Provides for evaluating QScript to a result. */
abstract class QScriptEvaluator[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: Monad: MonadPlannerErr: PhaseResultTell,
    R] {

  /** QScript used by this evaluator in Unirewrite. */
  type QSRewrite[U[_[_]]] <: TListK
  type QSMRewrite[A] = CopK[QSRewrite[T], A]

  /** QScript used by this evaluator in planning. */
  type QS[U[_[_]]] <: TListK
  type QSM[A] = CopK[QS[T], A]

  /** Executable representation. */
  type Repr

  def QSMRewriteFunctor: Functor[QSMRewrite]
  def QSMFunctor: Functor[QSM]
  def UnirewriteT: Unirewrite[T, QSMRewrite]

  def RenderTQSMRewrite: RenderTree[T[QSMRewrite]]
  def RenderTQSM: RenderTree[T[QSM]]

  /** Returns the result of executing the `Repr`. */
  def execute(repr: Repr): F[R]

  /** Returns the executable representation of the provided QScript. */
  def plan(cp: T[QSM]): F[Repr]

  /** Rewrites the qscript for optimal evaluation. */
  def optimize: F[QSMRewrite[T[QSM]] => QSM[T[QSM]]]

  ////

  def evaluate(qsr: T[QScriptEducated[T, ?]]): F[R] =
    for {
      rewritten <- Unirewrite[T, QSMRewrite](new Rewrite[T]).apply(qsr).point[F]
      _ <- phase[F][T[QSMRewrite]]("QScript (Rewritten)", rewritten)

      optimized <- optimize
      interpreted <- phase[F][T[QSM]]("QScript (Optimized)", rewritten.transCata[T[QSM]](optimized))

      repr <- plan(interpreted)
      result <- execute(repr)
    } yield result

  private final implicit def _QSMRewriteFunctor: Functor[QSMRewrite] = QSMRewriteFunctor
  private final implicit def _QSMFunctor: Functor[QSM] = QSMFunctor
  private final implicit def _UnirewriteT: Unirewrite[T, QSMRewrite] = UnirewriteT
  private final implicit def _RenderTQSMRewrite: RenderTree[T[QSMRewrite]] = RenderTQSMRewrite
  private final implicit def _RenderTQSM: RenderTree[T[QSM]] = RenderTQSM
}
