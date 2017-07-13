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

package quasar.physical.mongodb

import slamdata.Predef.{ Eq => _, _ }
import quasar._
import quasar.javascript.Js
import quasar.jscore._

import java.time.Instant

final case class javascript[R](embed: JsCoreF[R] => R) {
  val js = jscore.fixpoint[R](embed)
  import js._

  /** Convert a `Bson.Date` to a JavaScript `Date`. */
  def toJsDate(value: Bson.Date): R =
    New(Name("Date"), List(
      Literal(Js.Str(Instant.ofEpochMilli(value.millis).toString))))

  /** Convert a `Bson.ObjectId` to a JavaScript `ObjectId`. */
  def toJsObjectId(value: Bson.ObjectId): R =
    New(Name("ObjectId"), List(Literal(Js.Str(value.str))))

  def isNull(expr: R): R =
    BinOp(Eq, Literal(Js.Null), expr)

  def isAnyNumber(expr: R): R =
    BinOp(Or, isDec(expr), isInt(expr))

  def isInt[A](expr: R): R =
    BinOp(Or,
      BinOp(Instance, expr, ident("NumberInt")),
      BinOp(Instance, expr, ident("NumberLong")))

  def isDec(expr: R): R =
    Call(ident("isNumber"), List(expr))

  def isString(expr: R): R =
    Call(ident("isString"), List(expr))

  def isObjectOrArray(expr: R): R =
    Call(ident("isObject"), List(expr))

  def isArray(expr: R): R =
    Call(select(ident("Array"), "isArray"), List(expr))

  def isObject(expr: R): R =
    BinOp(And,
      isObjectOrArray(expr),
      UnOp(Not, isArray(expr)))

  def isArrayOrString(expr: R): R =
    BinOp(Or,
      isArray(expr),
      isString(expr))

  def isBoolean(expr: R): R =
    BinOp(Eq, UnOp(TypeOf, expr), Literal(Js.Str("boolean")))

  def isTimestamp(expr: R): R =
    BinOp(Instance, expr, ident("Timestamp"))

  def isDate(expr: R): R =
    BinOp(Instance, expr, ident("Date"))

  def isBinary(expr: R): R =
    BinOp(Instance, expr, ident("Binary"))

  def isObjectId(expr: R): R =
    BinOp(Instance, expr, ident("ObjectId"))
}
