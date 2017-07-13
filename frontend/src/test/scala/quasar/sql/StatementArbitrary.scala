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

import quasar.sql.ExprArbitrary._

import scala.Predef._

import matryoshka.data.Fix
import org.scalacheck._
import org.scalacheck.Shrink.shrink
import pathy.Path._
import pathy.scalacheck._

trait StatementArbitrary {

  implicit val statementArbitrary: Arbitrary[Statement[Fix[Sql]]] =
    Arbitrary(Gen.oneOf(funcDeclGen, importGen))

  implicit val statementShrink: Shrink[Statement[Fix[Sql]]] = Shrink {
    case func @ FunctionDecl(name, args, body) =>
      shrink(args).map(a => func.copy(args = a)) append
      shrink(body).map(b => func.copy(body = b))
    case Import(path) => shrink(path).map(Import[Fix[Sql]](_))
  }

  val funcDeclGen: Gen[FunctionDecl[Fix[Sql]]] =
    for {
      body <- Arbitrary.arbitrary[Fix[Sql]]
      name <- nonEmptyNameGen
      args <- Gen.listOf(nonEmptyNameGen)
    } yield FunctionDecl(name, args, body)

  val nonEmptyNameGen: Gen[CIName] = Gen.identifier.map(CIName(_))

  // TODO: Replace with `Arbitrary.arbitrary[DPath].map(Import(_))`
  // but this requires fixing some corner cases in the pretty printing
  // of import statements surrounding most probably pathy escaping
  // of path special characters
  val importGen: Gen[Import[Fix[Sql]]] =
    Gen.oneOf(
      Arbitrary.arbitrary[PathOf[Abs, Dir, Sandboxed, AlphaCharacters]].map(p => Import[Fix[Sql]](unsandbox(p.path))),
      Arbitrary.arbitrary[PathOf[Rel, Dir, Sandboxed, AlphaCharacters]].map(p => Import[Fix[Sql]](unsandbox(p.path))))
}

object StatementArbitrary extends StatementArbitrary