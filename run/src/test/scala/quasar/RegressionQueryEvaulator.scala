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
import quasar.api.QueryEvaluator
import quasar.common.PhaseResultTell
import quasar.qscript._

import matryoshka._
import scalaz.Monad

final class RegressionQueryEvaluator[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: Monad: MonadPlannerErr: PhaseResultTell]
    extends QueryEvaluator[F, T[QScriptEducated[T, ?]], QScriptCount] {

  val regressionEvaluator = new RegressionQScriptEvaluator[T, F]

  def evaluate(qs: T[QScriptEducated[T, ?]]): F[QScriptCount] =
    regressionEvaluator.evaluate(qs)
}
