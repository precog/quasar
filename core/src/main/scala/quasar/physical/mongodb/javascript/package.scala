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

package quasar.physical.mongodb

import quasar.Predef._

import quasar._
import quasar.jscore._
import quasar.javascript.{Js}

package object javascript {
  /** Convert a `Bson.Date` to a JavaScript `Date`. */
  def toJsDate(value: Bson.Date): JsCore =
    New(Name("Date"), List(Literal(Js.Str(value.value.toString))))

  /** Convert a `Bson.ObjectId` to a JavaScript `ObjectId`. */
  def toJsObjectId(value: Bson.ObjectId): JsCore =
    New(Name("ObjectId"), List(Literal(Js.Str(value.str))))

  def isNull(expr: JsCore): JsCore =
    BinOp(Eq, Literal(Js.Null), expr)

  def isAnyNumber(expr: JsCore): JsCore =
    BinOp(Or, BinOp(Or,
      isDec(expr),
      BinOp(Instance, expr, ident("NumberInt"))),
      BinOp(Instance, expr, ident("NumberLong")))

  def isDec(expr: JsCore): JsCore =
    Call(ident("isNumber"), List(expr))

  def isString(expr: JsCore): JsCore =
    Call(ident("isString"), List(expr))

  def isObjectOrArray(expr: JsCore): JsCore =
    Call(ident("isObject"), List(expr))

  def isArray(expr: JsCore): JsCore =
    Call(Select(ident("Array"), "isArray"), List(expr))

  def isObject(expr: JsCore): JsCore =
    BinOp(And,
      isObjectOrArray(expr),
      UnOp(Not, isArray(expr)))

  def isBoolean(expr: JsCore): JsCore =
    BinOp(Eq, UnOp(TypeOf, expr), jscore.Literal(Js.Str("boolean")))

  def isTimestamp(expr: JsCore): JsCore =
    jscore.BinOp(jscore.Instance, expr, jscore.ident("Timestamp"))

  def isDate(expr: JsCore): JsCore =
    jscore.BinOp(jscore.Instance, expr, jscore.ident("Date"))

  def isBinary(expr: JsCore): JsCore =
    jscore.BinOp(jscore.Instance, expr, jscore.ident("Binary"))

  def isObjectId(expr: JsCore): JsCore =
    jscore.BinOp(jscore.Instance, expr, jscore.ident("ObjectId"))
}
