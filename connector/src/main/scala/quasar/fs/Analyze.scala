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

package quasar.fs

import slamdata.Predef._
import quasar.effect.LiftedOps
import quasar.frontend.logicalplan.LogicalPlan
import quasar.qscript.analysis._

import matryoshka.{Hole => _, _}
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

  def defaultInterpreter[S[_], F[_] : Traverse, T](toQS: Fix[LogicalPlan] => FileSystemErrT[Free[S, ?], T])(implicit
    R: Recursive.Aux[T, F],
    CA: Cardinality[F],
    CO: Cost[F],
    Q: QueryFile.Ops[S]
  ): Analyze ~> Free[S, ?] = new (Analyze ~> Free[S, ?]) {

    def apply[A](from: Analyze[A]) = from match {
      case Analyze.QueryCost(lp) => (for {
        qs <- toQS(lp)
        c  <- R.zygoM(qs)(CA.calculate(pathCard[S]), CO.evaluate(pathCard[S]))
      } yield c).run
    }
  }

}
