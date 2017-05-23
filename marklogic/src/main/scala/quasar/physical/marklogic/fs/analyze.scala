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

package quasar.physical.marklogic.fs

import quasar.common.PhaseResultTell
import quasar.fs._
import quasar.frontend.logicalplan.LogicalPlan
import quasar.fp.free._
import quasar.physical.marklogic.qscript.SearchOptions
import quasar.physical.marklogic.xcc._

import matryoshka.data.Fix
import scalaz._

object analyze {
  def interpreter[
    F[_]: Monad: PhaseResultTell: Xcc,
    S[_],
    FMT: SearchOptions](implicit
    s0: F :<: S, 
    Q: QueryFile.Ops[S]
  ): Analyze ~> Free[S, ?] = {
    type G[A] = queryfile.MLQScript[Fix, A]
    type Err[A] = FileSystemErrT[F, A]
    Analyze.defaultInterpreter[S, G, Fix[G]]((lp: Fix[LogicalPlan]) => queryfile.lpToQScript[Err, Fix, FMT](lp).mapT(b => lift(b).into[S]))

  }
}
