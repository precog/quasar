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

package quasar.sql

import quasar.Predef._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._

package object fixpoint {
  def SelectR(
    isDistinct:   IsDistinct,
    projections:  List[Proj[Fix[Sql]]],
    relations:    Option[SqlRelation[Fix[Sql]]],
    filter:       Option[Fix[Sql]],
    groupBy:      Option[GroupBy[Fix[Sql]]],
    orderBy:      Option[OrderBy[Fix[Sql]]]):
      Fix[Sql] =
    select(isDistinct, projections, relations, filter, groupBy, orderBy).embed
  def VariR(symbol: String): Fix[Sql] = vari[Fix[Sql]](symbol).embed
  def SetLiteralR(exprs: List[Fix[Sql]]): Fix[Sql] = setLiteral(exprs).embed
  def ArrayLiteralR(exprs: List[Fix[Sql]]): Fix[Sql] = arrayLiteral(exprs).embed
  def MapLiteralR(exprs: List[(Fix[Sql], Fix[Sql])]): Fix[Sql] =
    mapLiteral(exprs).embed
  def SpliceR(prefix: Option[Fix[Sql]]): Fix[Sql] = splice(prefix).embed
  def BinopR(lhs: Fix[Sql], rhs: Fix[Sql], op: BinaryOperator): Fix[Sql] =
    binop(lhs, rhs, op).embed
  def UnopR(expr: Fix[Sql], op: UnaryOperator): Fix[Sql] = unop(expr, op).embed
  def IdentR(name: String): Fix[Sql] = ident[Fix[Sql]](name).embed
  def InvokeFunctionR(name: String, args: List[Fix[Sql]]): Fix[Sql] =
    invokeFunction(name, args).embed
  def MatchR(expr: Fix[Sql], cases: List[Case[Fix[Sql]]], default: Option[Fix[Sql]]):
      Fix[Sql] =
    matc(expr, cases, default).embed
  def SwitchR(cases: List[Case[Fix[Sql]]], default: Option[Fix[Sql]]): Fix[Sql] =
    switch(cases, default).embed
  def LetR(name: String, form: Fix[Sql], body: Fix[Sql]): Fix[Sql] =
    let(name, form, body).embed
  def IntLiteralR(v: Long): Fix[Sql] = intLiteral[Fix[Sql]](v).embed
  def FloatLiteralR(v: Double): Fix[Sql] = floatLiteral[Fix[Sql]](v).embed
  def StringLiteralR(v: String): Fix[Sql] = stringLiteral[Fix[Sql]](v).embed
  val NullLiteralR: Fix[Sql] = nullLiteral[Fix[Sql]]().embed
  def BoolLiteralR(value: Boolean): Fix[Sql] = boolLiteral[Fix[Sql]](value).embed
}
