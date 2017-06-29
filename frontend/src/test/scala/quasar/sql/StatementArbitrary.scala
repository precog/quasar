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
import quasar.contrib.pathy.DPath
import quasar.contrib.pathy.PathArbitrary._
import quasar.sql.ExprArbitrary._
import quasar.sql.CINameArbitrary._

import scala.Predef._

import matryoshka.data.Fix
import org.scalacheck._

trait StatementArbitrary {

  implicit val statementArbitrary: Arbitrary[Statement[Fix[Sql]]] =
    Arbitrary(Gen.oneOf(funcDeclGen, importGen))

  val funcDeclGen: Gen[FunctionDecl[Fix[Sql]]] =
    for {
      body <- Arbitrary.arbitrary[Fix[Sql]]
      name <- Arbitrary.arbitrary[CIName]
      args <- Arbitrary.arbitrary[List[CIName]]
    } yield FunctionDecl(name, args, body)

  val importGen: Gen[Import[Fix[Sql]]] =
    Arbitrary.arbitrary[DPath].map(Import(_))
}

object StatementArbitrary extends StatementArbitrary