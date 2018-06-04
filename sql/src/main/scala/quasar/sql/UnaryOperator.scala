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

package quasar.sql

import slamdata.Predef._

import scalaz._

sealed abstract class UnaryOperator(val sql: String) extends Product with Serializable {
  def apply[A](expr: A): Sql[A] = unop(expr, this)
  val name = sql
}

final case object Not                 extends UnaryOperator("not")
final case object Exists              extends UnaryOperator("exists")
final case object Positive            extends UnaryOperator("+")
final case object Negative            extends UnaryOperator("-")
final case object Distinct            extends UnaryOperator("distinct")
final case object FlattenMapKeys      extends UnaryOperator("{*:}")
final case object FlattenMapValues    extends UnaryOperator("flatten_map")
final case object ShiftMapKeys        extends UnaryOperator("{_:}")
final case object ShiftMapValues      extends UnaryOperator("shift_map")
final case object FlattenArrayIndices extends UnaryOperator("[*:]")
final case object FlattenArrayValues  extends UnaryOperator("flatten_array")
final case object ShiftArrayIndices   extends UnaryOperator("[_:]")
final case object ShiftArrayValues    extends UnaryOperator("shift_array")
final case object UnshiftArray        extends UnaryOperator("[...]")

object UnaryOperator {
  implicit val equal: Equal[UnaryOperator] = Equal.equalA
  implicit val show: Show[UnaryOperator] = Show.show(_.sql)
}
