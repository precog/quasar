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

package quasar.physical.marklogic.qscript

import slamdata.Predef.String
import quasar.fp.ski.κ
import quasar.physical.marklogic.cts.Query
import quasar.physical.marklogic.xquery._

import matryoshka._
import scalaz._

private[qscript] final class UnreachablePlanner[M[_]: MonadPlanErr, FMT, F[_]](
  name: String
) extends Planner[M, FMT, F] {

  def plan[Q, V](implicit Q: Birecursive.Aux[Q, Query[V, ?]]): AlgebraM[M, F, Search[Q] \/ XQuery] =
    κ(MonadPlanErr[M].raiseError(MarkLogicPlannerError.unreachable(name)))
}
