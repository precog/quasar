/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.connector.datasource

import quasar.RenderTreeT
import quasar.common.PhaseResultTell
import quasar.connector.{PhysicalDatasource, QScriptEvaluator}
import quasar.qscript.{MonadPlannerErr, QScriptEducated}

import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.Monad

/** A Datasource capable of executing QScript. */
abstract class HeavyweightDatasource[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: Monad: MonadPlannerErr: PhaseResultTell,
    G[_],
    R]
    extends QScriptEvaluator[T, F, R]
    with PhysicalDatasource[F, G, T[QScriptEducated[T, ?]], R]
