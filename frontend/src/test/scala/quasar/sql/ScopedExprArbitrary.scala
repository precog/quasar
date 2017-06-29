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

package quasar.sql

import slamdata.Predef._
import quasar.sql.StatementArbitrary._
import quasar.sql.ExprArbitrary._

import scala.Predef._

import org.scalacheck.Arbitrary
import matryoshka.data.Fix

trait ScopedExprArbitrary {

  implicit val scopedExprArbitrary: Arbitrary[ScopedExpr[Fix[Sql]]] =
    Arbitrary(for {
      expr <- Arbitrary.arbitrary[Fix[Sql]]
      scope <- Arbitrary.arbitrary[List[Statement[Fix[Sql]]]]
    } yield ScopedExpr(expr, scope))
}

object ScopedExprArbitrary extends ScopedExprArbitrary