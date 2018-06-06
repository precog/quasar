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

package quasar.physical.mongodb.planner

import slamdata.Predef._
import quasar.{Data, Type}, Type.{Coproduct => _, _}
import quasar.ejson.EJson
import quasar.fp._
import quasar.contrib.iota._
import quasar.contrib.iota.mkInject
import quasar.fp.ski._
import quasar.fs.MonadFsErr
import quasar.fs.{Planner => QPlanner}, QPlanner._
import quasar.javascript.Js
import quasar.jscore, jscore.{JsCore, Name}
import quasar.physical.mongodb.planner.common._
import quasar.qscript._
import quasar.std.StdLib._
import quasar.time.TemporalPart._

import scala.Predef.implicitly

import matryoshka._
import matryoshka.implicits._
import scalaz.{Divide => _, _}, Scalaz._
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::

trait JsFuncHandler[IN[_]] {
  def handle[M[_]: Monad: MonadFsErr: ExecTimeR]: AlgebraM[M, IN, JsCore]
}

object JsFuncHandler {

  // TODO: Use `JsonCodec.encode` and avoid failing.
  private def ejsonToJs[M[_]: Applicative: MonadFsErr, EJ: Show]
    (ej: EJ)(implicit EJ: Recursive.Aux[EJ, EJson])
      : M[JsCore] =
    ej.cata(Data.fromEJson).toJs.fold(
      raisePlannerError[M, JsCore](NonRepresentableEJson(ej.shows)))(
      _.point[M])

  implicit def mapFuncCore[T[_[_]]: BirecursiveT: ShowT, J, E]
      : JsFuncHandler[MapFuncCore[T, ?]] =
    new JsFuncHandler[MapFuncCore[T, ?]] {
      import jscore.{
        Add => _, In => _,
        Lt => _, Lte => _, Gt => _, Gte => _, Eq => _, Neq => _,
        And => _, Or => _, Not => _, TypeOf => _,
        _}

      val mjs = quasar.physical.mongodb.javascript[JsCore](_.embed)
      import mjs._

      import MapFuncsCore._

      def execTime[M[_]: Applicative](implicit ev: ExecTimeR[M]): M[JsCore] =
          ev.ask map (ts => Literal(Js.Str(ts.toString)))

      // NB: Math.trunc is not present in MongoDB.
      def trunc(expr: JsCore): JsCore =
        Let(Name("x"), expr,
          BinOp(jscore.Sub,
            ident("x"),
            BinOp(jscore.Mod, ident("x"), Literal(Js.Num(1, false)))))

      def dateZ(year: JsCore, month: JsCore, day: JsCore, hr: JsCore, min: JsCore, sec: JsCore, ms: JsCore): JsCore =
        New(Name("Date"), List(
          Call(Select(ident("Date"), "parse"), List(
            binop(jscore.Add,
              pad4(year), litStr("-"), pad2(month), litStr("-"), pad2(day), litStr("T"),
              pad2(hr), litStr(":"), pad2(min), litStr(":"), pad2(sec), litStr("."),
              pad3(ms), litStr("Z"))))))

      def year(date: JsCore): JsCore =
        Call(Select(date, "getUTCFullYear"), Nil)

      def month(date: JsCore): JsCore =
        BinOp(jscore.Add,
          Call(Select(date, "getUTCMonth"), Nil),
          litNum(1))

      def day(date: JsCore): JsCore =
        Call(Select(date, "getUTCDate"), Nil)

      def hour(date: JsCore): JsCore =
        Call(Select(date, "getUTCHours"), Nil)

      def minute(date: JsCore): JsCore =
        Call(Select(date, "getUTCMinutes"), Nil)

      def second(date: JsCore): JsCore =
        Call(Select(date, "getUTCSeconds"), Nil)

      def millisecond(date: JsCore): JsCore =
        Call(Select(date, "getUTCMilliseconds"), Nil)

      def dayOfWeek(date: JsCore): JsCore =
        Call(Select(date, "getUTCDay"), Nil)

      def quarter(date: JsCore): JsCore =
        BinOp(jscore.Add,
          Call(Select(ident("Math"), "floor"), List(
            BinOp(jscore.Div, Call(Select(date, "getUTCMonth"), Nil), litNum(3)))),
          litNum(1))

      def decade(date: JsCore): JsCore =
        trunc(BinOp(jscore.Div, year(date), litNum(10)))

      def century(date: JsCore): JsCore =
        Call(Select(ident("Math"), "ceil"), List(
          BinOp(jscore.Div, year(date), litNum(100))))

      def millennium(date: JsCore): JsCore =
        Call(Select(ident("Math"), "ceil"), List(
          BinOp(jscore.Div, year(date), litNum(1000))))

      def litStr(s: String): JsCore =
        Literal(Js.Str(s))

      def litNum(i: Int): JsCore =
        Literal(Js.Num(i.toDouble, false))

      def pad2(x: JsCore) =
        Let(Name("x"), x,
          If(
            BinOp(jscore.Lt, ident("x"), litNum(10)),
            BinOp(jscore.Add, litStr("0"), ident("x")),
            ident("x")))

      def pad3(x: JsCore) =
        Let(Name("x"), x,
          If(
            BinOp(jscore.Lt, ident("x"), litNum(10)),
            BinOp(jscore.Add, litStr("00"), ident("x")),
            If(
              BinOp(jscore.Lt, ident("x"), litNum(100)),
              BinOp(jscore.Add, litStr("0"), ident("x")),
              ident("x"))))

      def pad4(x: JsCore) =
        Let(Name("x"), x,
          If(
            BinOp(jscore.Lt, ident("x"), litNum(10)),
            BinOp(jscore.Add, litStr("000"), ident("x")),
            If(
              BinOp(jscore.Lt, ident("x"), litNum(100)),
              BinOp(jscore.Add, litStr("00"), ident("x")),
              If(
                BinOp(jscore.Lt, ident("x"), litNum(1000)),
                BinOp(jscore.Add, litStr("0"), ident("x")),
                ident("x")))))

      def handle[M[_]: Monad: MonadFsErr: ExecTimeR]: AlgebraM[M, MapFuncCore[T, ?], JsCore] = {
        case Constant(v1) => ejsonToJs[M, T[EJson]](v1)
        case JoinSideName(n) => raisePlannerError[M, JsCore](UnexpectedJoinSide(n))
        case Now() => execTime[M] map (ts => New(Name("ISODate"), List(ts)))
        case Interval(a1) => unimplemented[M, JsCore]("Interval JS")

        case ExtractCentury(date) =>
          century(date).point[M]

        case ExtractDayOfMonth(date) =>
          day(date).point[M]

        case ExtractDecade(date) =>
          decade(date).point[M]

        case ExtractDayOfWeek(date) =>
          dayOfWeek(date).point[M]

        case ExtractDayOfYear(date) =>
          Call(ident("NumberInt"), List(
            Call(Select(ident("Math"), "floor"), List(
              BinOp(jscore.Add,
                BinOp(jscore.Div,
                  BinOp(Sub,
                    date,
                    New(Name("Date"), List(
                      Call(Select(date, "getFullYear"), Nil),
                      litNum(0),
                      litNum(0)))),
                  litNum(86400000)),
                litNum(1)))))).point[M]

        case ExtractEpoch(date) =>
          BinOp(jscore.Div,
            Call(Select(date, "valueOf"), Nil),
            litNum(1000)).point[M]

        case ExtractHour(date) =>
          hour(date).point[M]

        case ExtractIsoDayOfWeek(date) =>
          Let(Name("x"), dayOfWeek(date),
            If(
              BinOp(jscore.Eq, ident("x"), litNum(0)),
              litNum(7),
              ident("x"))).point[M]

        case ExtractIsoYear(date) =>
          year(date).point[M]

        case ExtractMicrosecond(date) =>
          BinOp(jscore.Mult,
            BinOp(jscore.Add,
              millisecond(date),
              BinOp(jscore.Mult,
                second(date),
                litNum(1000))),
            litNum(1000)).point[M]

        case ExtractMillennium(date) =>
          millennium(date).point[M]

        case ExtractMillisecond(date) =>
          BinOp(jscore.Add,
            millisecond(date),
            BinOp(jscore.Mult,
              second(date),
              litNum(1000))).point[M]

        case ExtractMinute(date) =>
          minute(date).point[M]

        case ExtractMonth(date) =>
          month(date).point[M]

        case ExtractQuarter(date) =>
          Call(ident("NumberInt"), List(quarter(date))).point[M]

        case ExtractSecond(date) =>
          BinOp(jscore.Add,
            second(date),
            BinOp(jscore.Div, millisecond(date), litNum(1000))).point[M]

        case ExtractWeek(date) =>
          Call(ident("NumberInt"), List(
            Call(Select(ident("Math"), "floor"), List(
              BinOp(jscore.Add,
                BinOp(jscore.Div,
                  Let(Name("startOfYear"),
                    New(Name("Date"), List(
                      Call(Select(date, "getFullYear"), Nil),
                      litNum(0),
                      litNum(1))),
                    BinOp(jscore.Add,
                      BinOp(Div,
                        BinOp(Sub, date, ident("startOfYear")),
                        litNum(86400000)),
                      BinOp(jscore.Add,
                        Call(Select(ident("startOfYear"), "getDay"), Nil),
                        litNum(1)))),
                  litNum(7)),
                litNum(1)))))).point[M]

        case MakeMap(Embed(LiteralF(Js.Str(str))), a2) => Obj(ListMap(Name(str) -> a2)).point[M]
        // TODO: pull out the literal, and handle this case in other situations

        case MakeMap(a1, a2) => Obj(ListMap(Name("__Quasar_non_string_map") ->
          Arr(List(Arr(List(a1, a2)))))).point[M]

        case ConcatArrays(Embed(ArrF(a1)), Embed(ArrF(a2))) =>
          Arr(a1 |+| a2).point[M]

        case ConcatArrays(a1, a2) =>
          If(BinOp(jscore.Or, isArray(a1), isArray(a2)),
            Call(Select(a1, "concat"), List(a2)),
            BinOp(jscore.Add, a1, a2)).point[M]

        case ConcatMaps(Embed(ObjF(o1)), Embed(ObjF(o2))) =>
          Obj(o1 ++ o2).point[M]

        case ConcatMaps(a1, a2) => SpliceObjects(List(a1, a2)).point[M]

        case Guard(expr, typ, cont, fallback) =>
          val jsCheck: Type => Option[JsCore => JsCore] =
            generateTypeCheck[JsCore, JsCore](BinOp(jscore.Or, _, _)) {
              case Type.Null             => isNull
              case Type.Dec              => isDec
              case Type.Int
                 | Type.Int ⨿ Type.Dec
                 | Type.Int ⨿ Type.Dec ⨿ Type.Interval
                  => isAnyNumber
              case Type.Str              => isString
              case Type.Obj(_, _) ⨿ Type.FlexArr(_, _, _)
                  => isObjectOrArray
              case Type.Obj(_, _)        => isObject
              case Type.FlexArr(_, _, _) => isArray
              case Type.Binary           => isBinary
              case Type.Id               => isObjectId
              case Type.Bool             => isBoolean
              case Type.OffsetDateTime | Type.OffsetDate | Type.OffsetTime |
                  Type.LocalDateTime | Type.LocalDate | Type.LocalTime
                  => isDate
              case Type.Syntaxed         => isSyntaxed
            }
          jsCheck(typ).fold[M[JsCore]](
            raiseInternalError("uncheckable type"))(
            f => If(f(expr), cont, fallback).point[M])

        case Add(a1, a2)      => BinOp(jscore.Add, a1, a2).point[M]
        case Multiply(a1, a2) => BinOp(jscore.Mult, a1, a2).point[M]
        case Power(a1, a2) =>
          Call(Select(ident("Math"), "pow"), List(a1, a2)).point[M]
        case Subtract(a1, a2) => BinOp(jscore.Sub, a1, a2).point[M]
        case Divide(a1, a2)   => BinOp(jscore.Div, a1, a2).point[M]
        case Modulo(a1, a2)   => BinOp(jscore.Mod, a1, a2).point[M]
        case Negate(a1)       => UnOp(jscore.Neg, a1).point[M]
        case Undefined()      => ident("undefined").point[M]

        case MapFuncsCore.Eq(a1, a2)  => BinOp(jscore.Eq, a1, a2).point[M]
        case Neq(a1, a2) => BinOp(jscore.Neq, a1, a2).point[M]
        case Lt(a1, a2)  => BinOp(jscore.Lt, a1, a2).point[M]
        case Lte(a1, a2) => BinOp(jscore.Lte, a1, a2).point[M]
        case Gt(a1, a2)  => BinOp(jscore.Gt, a1, a2).point[M]
        case Gte(a1, a2) => BinOp(jscore.Gte, a1, a2).point[M]
        case Not(a1)     => UnOp(jscore.Not, a1).point[M]
        case And(a1, a2) => BinOp(jscore.And, a1, a2).point[M]
        case Or(a1, a2)  => BinOp(jscore.Or, a1, a2).point[M]

        case Between(value, min, max) =>
            BinOp(jscore.And,
              BinOp(jscore.Lte, min, value),
              BinOp(jscore.Lte, value, max)).point[M]

        case MakeArray(a1) => Arr(List(a1)).point[M]

        case Length(str) =>
          Call(ident("NumberLong"), List(Select(str, "length"))).point[M]

        case Substring(field, start, len) =>
          If(BinOp(jscore.Lt, start, litNum(0)),
            litStr(""),
            If(BinOp(jscore.Lt, len, litNum(0)),
              Call(Select(field, "substr"), List(start, Select(field, "length"))),
              Call(Select(field, "substr"), List(start, len)))).point[M]

        case MapFuncsCore.Split(a1, a2) =>
          Call(Select(a1, "split"), List(a2)).point[M]

        case Lower(a1) => Call(Select(a1, "toLowerCase"), Nil).point[M]

        case Upper(a1) => Call(Select(a1, "toUpperCase"), Nil).point[M]

        case Search(field, pattern, insen) =>
            Call(
              Select(
                New(Name("RegExp"), List(
                  pattern,
                  If(insen, litStr("im"), litStr("m")))),
                "test"),
              List(field)).point[M]

        case Within(a1, a2) =>
          BinOp(jscore.Neq,
            litNum(-1),
            Call(Select(a2, "indexOf"), List(a1))).point[M]

        case Null(str) =>
          If(
            BinOp(jscore.Eq, str, litStr("null")),
            Literal(Js.Null),
            ident("undefined")).point[M]

        case Bool(str) =>
          If(
            BinOp(jscore.Eq, str, litStr("true")),
            Literal(Js.Bool(true)),
            If(
              BinOp(jscore.Eq, str, litStr("false")),
              Literal(Js.Bool(false)),
              ident("undefined"))).point[M]

        case Integer(str) =>
          If(Call(Select(Call(ident("RegExp"), List(litStr("^" + string.intRegex + "$"))), "test"), List(str)),
            Call(ident("NumberLong"), List(str)),
            ident("undefined")).point[M]

        case Decimal(str) =>
            If(Call(Select(Call(ident("RegExp"), List(litStr("^" + string.floatRegex + "$"))), "test"), List(str)),
              Call(ident("parseFloat"), List(str)),
              ident("undefined")).point[M]

        case LocalDate(str) =>
          If(Call(Select(Call(ident("RegExp"), List(litStr("^" + string.dateRegex + "$"))), "test"), List(str)),
            Call(ident("ISODate"), List(str)),
            ident("undefined")).point[M]

        case LocalTime(str) =>
          If(Call(Select(Call(ident("RegExp"), List(litStr("^" + string.timeRegex + "$"))), "test"), List(str)),
            str,
            ident("undefined")).point[M]

        case LocalDateTime(str) =>
          If(Call(Select(Call(ident("RegExp"), List(litStr("^" + string.timestampRegex + "$"))), "test"), List(str)),
            Call(ident("ISODate"), List(str)),
            ident("undefined")).point[M]

        case OffsetDateTime(str) =>
          If(Call(Select(Call(ident("RegExp"), List(litStr("^" + string.timestampRegex + "$"))), "test"), List(str)),
            Call(ident("ISODate"), List(str)),
            ident("undefined")).point[M]

        case ToString(value) =>
          If(isInt(value),
            // NB: This is a terrible way to turn an int into a string, but the
            //     only one that doesn’t involve converting to a decimal and
            //     losing precision.
            Call(Select(Call(ident("String"), List(value)), "replace"), List(
              Call(ident("RegExp"), List(
                litStr("[^-0-9]+"),
                litStr("g"))),
              litStr(""))),
            If(isObjectId(value),
              Call(Select(value, "toString"), Nil),
              (If(binop(jscore.Or, isTimestamp(value), isDate(value)),
                Call(Select(value, "toISOString"), Nil),
                Call(ident("String"), List(value)))))).point[M]

        case ToTimestamp(a1) => New(Name("Date"), List(a1)).point[M]

        case TimeOfDay(date) =>
          Let(Name("t"), date,
            binop(jscore.Add,
              pad2(hour(ident("t"))),
              litStr(":"),
              pad2(minute(ident("t"))),
              litStr(":"),
              pad2(second(ident("t"))),
              litStr("."),
              pad3(millisecond(ident("t"))))).point[M]

        case ExtractYear(date) => year(date).point[M]

        case StartOfDay(date) =>
          dateZ(year(date), month(date), day(date), litNum(0), litNum(0), litNum(0), litNum(0)).point[M]

        case TemporalTrunc(Century, date) =>
          val yr =
            Call(Select(ident("Math"), "floor"), List(BinOp(jscore.Div, year(date), litNum(100))))
          dateZ(
            BinOp(jscore.Mult, yr, litNum(100)),
            litNum(1), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0)).point[M]

        case TemporalTrunc(Day, date) =>
          dateZ(year(date), month(date), day(date), litNum(0), litNum(0), litNum(0), litNum(0)).point[M]

        case TemporalTrunc(Decade, date) =>
          dateZ(
            BinOp(jscore.Mult, decade(date), litNum(10)),
            litNum(1), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0)).point[M]

        case TemporalTrunc(Hour, date) =>
          dateZ(year(date), month(date), day(date), hour(date), litNum(0), litNum(0), litNum(0)).point[M]

        case TemporalTrunc(Millennium, date) =>
          dateZ(
            BinOp(jscore.Mult,
              Call(Select(ident("Math"), "floor"), List(
                BinOp(jscore.Div, year(date), litNum(1000)))),
              litNum(1000)),
            litNum(1), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0)).point[M]

        case TemporalTrunc(Microsecond | Millisecond, date) =>
          dateZ(
            year(date), month(date), day(date),
            hour(date), minute(date), second(date), millisecond(date)).point[M]

        case TemporalTrunc(Minute, date) =>
          dateZ(year(date), month(date), day(date), hour(date), minute(date), litNum(0), litNum(0)).point[M]

        case TemporalTrunc(Month, date) =>
          dateZ(year(date), month(date), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0)).point[M]

        case TemporalTrunc(Quarter, date) =>
          dateZ(
            year(date),
            BinOp(jscore.Add,
              BinOp(jscore.Mult,
                BinOp(jscore.Sub, quarter(date), litNum(1)),
                litNum(3)),
              litNum(1)),
            litNum(1), litNum(0), litNum(0), litNum(0), litNum(0)).point[M]

        case TemporalTrunc(Second, date) =>
          dateZ(year(date), month(date), day(date), hour(date), minute(date), second(date), litNum(0)).point[M]

        case TemporalTrunc(Week, date) =>
          val d =
            New(Name("Date"), List(
              BinOp(jscore.Sub,
                Call(Select(date, "getTime"), Nil),
                BinOp(jscore.Mult,
                  litNum(24*60*60*1000),
                  BinOp(jscore.Mod,
                    BinOp(jscore.Add, dayOfWeek(date), litNum(6)),
                    litNum(7))
              ))))
          Let(Name("d"), d,
            dateZ(year(d), month(d), day(d), litNum(0), litNum(0), litNum(0), litNum(0))).point[M]

        case TemporalTrunc(Year, date) =>
          dateZ(year(date), litNum(1), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0)).point[M]

        case ProjectKey(obj, key) => Access(obj, key).point[M]

        case ProjectIndex(arr, index) => Access(arr, index).point[M]

        case DeleteKey(a1, a2) => Call(ident("remove"), List(a1, a2)).point[M]

        // TODO: This doesn't return the right values most of the time.
        case TypeOf(v) =>
          Let(Name("typ"), UnOp(jscore.TypeOf, v),
            If(BinOp(jscore.Eq, ident("typ"), Literal(Js.Str("object"))),
              If(BinOp(jscore.Eq, v, Literal(Js.Null)),
                Literal(Js.Str("null")),
                If(Call(Select(ident("Array"), "isArray"), List(v)),
                  Literal(Js.Str("array")),
                  Literal(Js.Str("map")))),
              If(BinOp(jscore.Eq, ident("typ"), Literal(Js.Str("string"))),
                Literal(Js.Str("array")),
                ident("typ")))).point[M]

        case IfUndefined(a1, a2) =>
          // TODO: Only evaluate `value` once.
          If(BinOp(jscore.Eq, a1, ident("undefined")), a2, a1).point[M]

        case ToId(a1) => New(Name("ObjectId"), List(a1)).point[M]

        case Cond(i, t, e) => If(i, t, e).point[M]

        // FIXME: Doesn't work for Char.
        case Range(start, end) =>
          Call(
            Select(
              Call(Select(ident("Array"), "apply"), List(
                Literal(Js.Null),
                Call(ident("Array"), List(BinOp(jscore.Sub, end, start))))),
              "map"),
            List(
              Fun(List(Name("element"), Name("index")),
                BinOp(jscore.Add, ident("index"), start)))).point[M]

        // TODO: Specify the function name for pattern match failures
        case _ => unimplemented[M, JsCore]("JS function")
      }
    }

  implicit def mapFuncDerived[T[_[_]]: CorecursiveT]
    (implicit core: JsFuncHandler[MapFuncCore[T, ?]])
      : JsFuncHandler[MapFuncDerived[T, ?]] =
    new JsFuncHandler[MapFuncDerived[T, ?]] {
      def handle[M[_]: Monad: MonadFsErr: ExecTimeR]: AlgebraM[M, MapFuncDerived[T, ?], JsCore] =
        ExpandMapFunc.expand(core.handle[M], κ[MapFuncDerived[T, JsCore], Option[M[JsCore]]](None))
    }

  implicit def copk[LL <: TListK](implicit M: Materializer[LL]): JsFuncHandler[CopK[LL, ?]] =
    M.materialize(offset = 0)

  sealed trait Materializer[LL <: TListK] {
    def materialize(offset: Int): JsFuncHandler[CopK[LL, ?]]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[F[_]](
      implicit
      F: JsFuncHandler[F]
    ): Materializer[F ::: TNilK] = new Materializer[F ::: TNilK] {
      override def materialize(offset: Int): JsFuncHandler[CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new JsFuncHandler[CopK[F ::: TNilK, ?]] {
          def handle[M[_]: Monad: MonadFsErr: ExecTimeR]: AlgebraM[M, CopK[F ::: TNilK, ?], JsCore] = {
            case I(fa) => F.handle[M].apply(fa)
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[F[_], LL <: TListK](
      implicit
      F: JsFuncHandler[F],
      LL: Materializer[LL]
    ): Materializer[F ::: LL] = new Materializer[F ::: LL] {
      override def materialize(offset: Int): JsFuncHandler[CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new JsFuncHandler[CopK[F ::: LL, ?]] {
          def handle[M[_]: Monad: MonadFsErr: ExecTimeR]: AlgebraM[M, CopK[F ::: LL, ?], JsCore] = {
            case I(fa) => F.handle[M].apply(fa)
            case other => LL.materialize(offset + 1).handle[M].apply(other.asInstanceOf[CopK[LL, JsCore]])
          }
        }
      }
    }
  }
  
  def handle[F[_]: JsFuncHandler, M[_]: Monad: MonadFsErr: ExecTimeR]: AlgebraM[M, F, JsCore] =
    implicitly[JsFuncHandler[F]].handle[M]
}
