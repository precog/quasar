/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.sql.ExprArbitrary

import org.scalacheck.Arbitrary, Arbitrary.{arbitrary => arb}

trait VariablesArbitrary {
  implicit val arbitraryVarName: Arbitrary[VarName] =
    Arbitrary(arb[String].map(VarName(_)))

  implicit val arbitraryVarValue: Arbitrary[VarValue] =
    Arbitrary(ExprArbitrary.constExprGen.map(expr => VarValue(expr.toString)))

  implicit val arbitraryVariables: Arbitrary[Variables] =
    Arbitrary(arb[Map[VarName, VarValue]].map(Variables(_)))
}

object VariablesArbitrary extends VariablesArbitrary
