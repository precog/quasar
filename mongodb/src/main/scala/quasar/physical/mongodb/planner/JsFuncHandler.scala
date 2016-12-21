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

package quasar.physical.mongodb.planner

import quasar.Predef._
import quasar.javascript.Js
import quasar.jscore, jscore.{Name, JsCoreF}
import quasar.std.StdLib._
import quasar.qscript.MapFunc
import quasar.qscript.MapFuncs, MapFuncs._

import scalaz.{Free, Scalaz}, Scalaz._

object JsFuncHandler {
  def apply[T[_[_]], A](func: MapFunc[T, A]): Option[Free[JsCoreF, A]] = {
    implicit def hole(a: A): Free[JsCoreF, A] = Free.pure(a)

    val mjs = quasar.physical.mongodb.javascript[Free[JsCoreF, A]](Free.roll)
    import mjs._
    import mjs.js._

    // NB: Math.trunc is not present in MongoDB.
    def trunc(expr: Free[JsCoreF, A]): Free[JsCoreF, A] =
      Let(Name("x"), expr,
        BinOp(jscore.Sub,
          ident("x"),
          BinOp(jscore.Mod, ident("x"), Literal(Js.Num(1, false)))))

    func.some collect {
      case Add(a1, a2)      => BinOp(jscore.Add, a1, a2)
      case Multiply(a1, a2) => BinOp(jscore.Mult, a1, a2)
      case Power(a1, a2) =>
        Call(select(ident("Math"), "pow"), List(a1, a2))
      case Subtract(a1, a2) => BinOp(jscore.Sub, a1, a2)
      case Divide(a1, a2)   => BinOp(jscore.Div, a1, a2)
      case Modulo(a1, a2)   => BinOp(jscore.Mod, a1, a2)
      case Negate(a1)       => UnOp(jscore.Neg, a1)

      case MapFuncs.Eq(a1, a2)  => BinOp(jscore.Eq, a1, a2)
      case Neq(a1, a2) => BinOp(jscore.Neq, a1, a2)
      case Lt(a1, a2)  => BinOp(jscore.Lt, a1, a2)
      case Lte(a1, a2) => BinOp(jscore.Lte, a1, a2)
      case Gt(a1, a2)  => BinOp(jscore.Gt, a1, a2)
      case Gte(a1, a2) => BinOp(jscore.Gte, a1, a2)
      case Not(a1)     => UnOp(jscore.Not, a1)
      case And(a1, a2) => BinOp(jscore.And, a1, a2)
      case Or(a1, a2)  => BinOp(jscore.Or, a1, a2)
      case Between(value, min, max) =>
          BinOp(jscore.And,
            BinOp(jscore.Lte, min, value),
            BinOp(jscore.Lte, value, max))

      case ConcatArrays(a1, a2) => BinOp(jscore.Add, a1, a2)
      case Length(str) =>
        Call(ident("NumberLong"), List(select(hole(str), "length")))
      case Substring(field, start, len) =>
        If(BinOp(jscore.Lt, start, Literal(Js.Num(0, false))),
          Literal(Js.Str("")),
          If(BinOp(jscore.Lt, len, Literal(Js.Num(0, false))),
            Call(select(field, "substr"), List(start, select(field, "length"))),
            Call(select(field, "substr"), List(start, len))))
      case Search(field, pattern, insen) =>
          Call(
            select(
              New(Name("RegExp"), List(
                pattern,
                If(insen, Literal(Js.Str("im")), Literal(Js.Str("m"))))),
              "test"),
            List(field))
      case Null(str) =>
        If(
          BinOp(jscore.Eq, str, Literal(Js.Str("null"))),
          Literal(Js.Null),
          ident("undefined"))
      case Bool(str) =>
        If(
          BinOp(jscore.Eq, str, Literal(Js.Str("true"))),
          Literal(Js.Bool(true)),
          If(
            BinOp(jscore.Eq, str, Literal(Js.Str("false"))),
            Literal(Js.Bool(false)),
            ident("undefined")))
      case Integer(str) =>
        If(Call(select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.intRegex + "$")))), "test"), List(str)),
          Call(ident("NumberLong"), List(str)),
          ident("undefined"))
      case Decimal(str) =>
          If(Call(select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.floatRegex + "$")))), "test"), List(str)),
            Call(ident("parseFloat"), List(str)),
            ident("undefined"))
      case Date(str) =>
        If(Call(select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.dateRegex + "$")))), "test"), List(str)),
          Call(ident("ISODate"), List(str)),
          ident("undefined"))
      case Time(str) =>
        If(Call(select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.timeRegex + "$")))), "test"), List(str)),
          str,
          ident("undefined"))
      case Timestamp(str) =>
        If(Call(select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.timestampRegex + "$")))), "test"), List(str)),
          Call(ident("ISODate"), List(str)),
          ident("undefined"))
      // TODO: case Interval(str) =>
      case ToString(value) =>
        If(isInt(value),
          // NB: This is a terrible way to turn an int into a string, but the
          //     only one that doesn’t involve converting to a decimal and
          //     losing precision.
          Call(select(Call(ident("String"), List(value)), "replace"), List(
            Call(ident("RegExp"), List(
              Literal(Js.Str("[^-0-9]+")),
              Literal(Js.Str("g")))),
            Literal(Js.Str("")))),
          If(BinOp(jscore.Or, isTimestamp(value), isDate(value)),
            Call(select(value, "toISOString"), Nil),
            Call(ident("String"), List(value))))
      // TODO: case ToTimestamp(str) =>

      case TimeOfDay(date) =>
        def pad2(x: Free[JsCoreF, A]) =
          Let(Name("x"), x,
            If(
              BinOp(jscore.Lt, ident("x"), Literal(Js.Num(10, false))),
              BinOp(jscore.Add, Literal(Js.Str("0")), ident("x")),
              ident("x")))
        def pad3(x: Free[JsCoreF, A]) =
          Let(Name("x"), x,
            If(
              BinOp(jscore.Lt, ident("x"), Literal(Js.Num(10, false))),
              BinOp(jscore.Add, Literal(Js.Str("00")), ident("x")),
              If(
                BinOp(jscore.Lt, ident("x"), Literal(Js.Num(100, false))),
                BinOp(jscore.Add, Literal(Js.Str("0")), ident("x")),
                ident("x"))))
        Let(Name("t"), date,
          binop(jscore.Add,
            pad2(Call(select(ident("t"), "getUTCHours"), Nil)),
            Literal(Js.Str(":")),
            pad2(Call(select(ident("t"), "getUTCMinutes"), Nil)),
            Literal(Js.Str(":")),
            pad2(Call(select(ident("t"), "getUTCSeconds"), Nil)),
            Literal(Js.Str(".")),
            pad3(Call(select(ident("t"), "getUTCMilliseconds"), Nil))))

      case ExtractCentury(date) =>
        Call(select(ident("Math"), "ceil"), List(
          BinOp(jscore.Div,
            Call(select(date, "getUTCFullYear"), Nil),
            Literal(Js.Num(100, false)))))
      case ExtractDayOfMonth(date) => Call(select(date, "getUTCDate"), Nil)
      case ExtractDecade(date) =>
        trunc(
          BinOp(jscore.Div,
            Call(select(date, "getUTCFullYear"), Nil),
            Literal(Js.Num(10, false))))
      case ExtractDayOfWeek(date) =>
        Call(select(date, "getUTCDay"), Nil)
      // TODO: case ExtractDayOfYear(date) =>
      case ExtractEpoch(date) =>
        BinOp(jscore.Div,
          Call(select(date, "valueOf"), Nil),
          Literal(Js.Num(1000, false)))
      case ExtractHour(date) => Call(select(date, "getUTCHours"), Nil)
      case ExtractIsoDayOfWeek(date) =>
        Let(Name("x"), Call(select(date, "getUTCDay"), Nil),
          If(
            BinOp(jscore.Eq, ident("x"), Literal(Js.Num(0, false))),
            Literal(Js.Num(7, false)),
            ident("x")))
      // TODO: case ExtractIsoYear(date) =>
      case ExtractMicroseconds(date) =>
        BinOp(jscore.Mult,
          BinOp(jscore.Add,
            Call(select(date, "getUTCMilliseconds"), Nil),
            BinOp(jscore.Mult,
              Call(select(date, "getUTCSeconds"), Nil),
              Literal(Js.Num(1000, false)))),
          Literal(Js.Num(1000, false)))
      case ExtractMillennium(date) =>
        Call(select(ident("Math"), "ceil"), List(
          BinOp(jscore.Div,
            Call(select(date, "getUTCFullYear"), Nil),
            Literal(Js.Num(1000, false)))))
      case ExtractMilliseconds(date) =>
        BinOp(jscore.Add,
          Call(select(date, "getUTCMilliseconds"), Nil),
          BinOp(jscore.Mult,
            Call(select(date, "getUTCSeconds"), Nil),
            Literal(Js.Num(1000, false))))
      case ExtractMinute(date) =>
        Call(select(date, "getUTCMinutes"), Nil)
      case ExtractMonth(date) =>
        BinOp(jscore.Add,
          Call(select(date, "getUTCMonth"), Nil),
          Literal(Js.Num(1, false)))
      case ExtractQuarter(date) =>
        BinOp(jscore.Add,
          BinOp(jscore.BitOr,
            BinOp(jscore.Div,
              Call(select(date, "getUTCMonth"), Nil),
              Literal(Js.Num(3, false))),
            Literal(Js.Num(0, false))),
          Literal(Js.Num(1, false)))
      case ExtractSecond(date) =>
        BinOp(jscore.Add,
          Call(select(date, "getUTCSeconds"), Nil),
          BinOp(jscore.Div,
            Call(select(date, "getUTCMilliseconds"), Nil),
            Literal(Js.Num(1000, false))))
      // TODO: case ExtractWeek(date) =>
      case ExtractYear(date) => Call(select(date, "getUTCFullYear"), Nil)

      case Now() => Call(select(ident("Date"), "now"), Nil)

      case ProjectField(obj, field) => Access(obj, field)
      case ProjectIndex(arr, index) => Access(arr, index)
    }
  }
}
