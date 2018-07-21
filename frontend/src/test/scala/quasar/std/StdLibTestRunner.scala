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

package quasar.std

import slamdata.Predef._
import quasar.common.data.Data
import quasar.frontend.logicalplan.{LogicalPlan => LP}
import qdata.time.DateTimeInterval

import java.time._

import matryoshka.data.Fix
import org.specs2.execute._
import org.scalacheck.Gen

/** The operations needed to execute the various StdLib tests for a backend. */
trait StdLibTestRunner {
  /** The result of comparing `expected` to the result of the executing the given
    * nullary function on the backend.
    */
  def nullary(
    prg: Fix[LP],
    expected: Data): Result

  /** The result of comparing `expected` to the result of the executing the given
    * unary function on the backend.
    */
  def unary(
    prg: Fix[LP] => Fix[LP],
    arg: Data,
    expected: Data): Result

  /** The result of comparing `expected` to the result of the executing the given
    * binary function on the backend.
    */
  def binary(
    prg: (Fix[LP], Fix[LP]) => Fix[LP],
    arg1: Data, arg2: Data,
    expected: Data): Result

  /** The result of comparing `expected` to the result of the executing the given
    * ternary function on the backend.
    */
  def ternary(
    prg: (Fix[LP], Fix[LP], Fix[LP]) => Fix[LP],
    arg1: Data, arg2: Data, arg3: Data,
    expected: Data): Result

  /** Defines the domain of values for `Data.Int` for which the implementation is
    * well-behaved.
    */
  def intDomain: Gen[BigInt]

  /** Defines the domain of values for `Data.Dec` for which the implementation is
    * well-behaved.
    */
  def decDomain: Gen[BigDecimal]

  /** Defines the domain of values for `Data.Str` for which the implementation is
    * well-behaved.
    */
  def stringDomain: Gen[String]

  /** Defines the domain of values for `Data.LocalDate` for which the implementation is
    * well-behaved.
    */
  def dateDomain: Gen[LocalDate]

  /** Defines the domain of values for `Data.LocalTime` for which the implementation is
    * well-behaved.
    */
  def timeDomain: Gen[LocalTime]

  /** Defines the domain of time zones for which the implementation is well-behaved.
    */
  def timezoneDomain: Gen[ZoneOffset]

  /** Defines the domain of datetime intervals for which the implementation is well-behaved.
    */
  def intervalDomain: Gen[DateTimeInterval]
}
