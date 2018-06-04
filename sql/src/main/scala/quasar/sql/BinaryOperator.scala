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

sealed abstract class BinaryOperator(val sql: String) extends Product with Serializable {
  def apply[A](lhs: A, rhs: A): Sql[A] = binop(lhs, rhs, this)
  val name = "(" + sql + ")"
}

final case object IfUndefined  extends BinaryOperator("??")
final case object Range        extends BinaryOperator("..")
final case object Or           extends BinaryOperator("or")
final case object And          extends BinaryOperator("and")
final case object Eq           extends BinaryOperator("=")
final case object Neq          extends BinaryOperator("<>")
final case object Ge           extends BinaryOperator(">=")
final case object Gt           extends BinaryOperator(">")
final case object Le           extends BinaryOperator("<=")
final case object Lt           extends BinaryOperator("<")
final case object Concat       extends BinaryOperator("||")
final case object Plus         extends BinaryOperator("+")
final case object Minus        extends BinaryOperator("-")
final case object Mult         extends BinaryOperator("*")
final case object Div          extends BinaryOperator("/")
final case object Mod          extends BinaryOperator("%")
final case object Pow          extends BinaryOperator("^")
final case object In           extends BinaryOperator("in")
final case object KeyDeref     extends BinaryOperator("{}")
final case object IndexDeref   extends BinaryOperator("[]")
final case object Limit        extends BinaryOperator("limit")
final case object Offset       extends BinaryOperator("offset")
final case object Sample       extends BinaryOperator("sample")
final case object Union        extends BinaryOperator("union")
final case object UnionAll     extends BinaryOperator("union all")
final case object Intersect    extends BinaryOperator("intersect")
final case object IntersectAll extends BinaryOperator("intersect all")
final case object Except       extends BinaryOperator("except")
final case object UnshiftMap   extends BinaryOperator("{...}")

object BinaryOperator {
  implicit val equal: Equal[BinaryOperator] = Equal.equalA
  implicit val show: Show[BinaryOperator] = Show.show(_.sql)
}
