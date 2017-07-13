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

package quasar

import quasar.common.PhaseResultW
import quasar.effect.Failure
import quasar.frontend.SemanticErrsT

import scalaz._

package object connector {
  type CompileM[A] = SemanticErrsT[PhaseResultW, A]

  type EnvErr[A] = Failure[EnvironmentError, A]
  type EnvErrT[F[_], A] = EitherT[F, EnvironmentError, A]

  type PlannerErrT[F[_], A] = EitherT[F, Planner.PlannerError, A]
}
