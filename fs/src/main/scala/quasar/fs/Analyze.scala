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

package quasar.fs

import slamdata.Predef._
import quasar.effect.LiftedOps
import quasar.frontend.logicalplan.LogicalPlan

import matryoshka.data.Fix
import scalaz._

sealed abstract class Analyze[A]

object Analyze {

  final case class QueryCost(lp: Fix[LogicalPlan]) extends Analyze[FileSystemError \/ Int]

  final class Ops[S[_]](implicit S: Analyze :<: S) extends LiftedOps[Analyze, S] {
    def queryCost(lp: Fix[LogicalPlan]): FileSystemErrT[Free[S, ?], Int] = EitherT(lift(QueryCost(lp)))
  }

  object Ops {
    implicit def apply[S[_]](implicit S: Analyze :<: S): Ops[S] =
      new Ops[S]
  }
}
