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

package quasar.physical.marklogic.qscript

import quasar.Predef._
import quasar.Data
import quasar.ejson.EJson
import quasar.fp.eitherT._
import quasar.physical.marklogic.ErrorMessages
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript.{MapFunc, MapFuncs}, MapFuncs._

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz.{Const, EitherT, Monad}
import scalaz.syntax.monad._

object MapFuncPlanner {
  import expr.{emptySeq, if_, let_}, axes._, XQuery.flwor

  def apply[T[_[_]]: RecursiveT, F[_]: QNameGenerator: PrologW: MonadPlanErr]: AlgebraM[F, MapFunc[T, ?], XQuery] = {
    case Constant(ejson)              =>
      EncodeXQuery[EitherT[F, ErrorMessages, ?], Const[Data, ?]]
        .encodeXQuery(Const(ejson.cata(Data.fromEJson))).run.flatMap(_.fold(
          msgs => MonadPlanErr[F].raiseError(MarkLogicPlannerError.unrepresentableEJson(ejson.convertTo[Fix[EJson]], msgs)),
          _.point[F]))

    case Undefined()                  => emptySeq.point[F]

    case Length(arrOrstr)             => qscript.length[F] apply arrOrstr

    // time
    case Date(s)                      => qscript.asDate[F] apply s
    case Time(s)                      => xs.time(s).point[F]
    case Timestamp(s)                 => xs.dateTime(s).point[F]
    case Interval(s)                  => xs.dayTimeDuration(s).point[F]
    case TimeOfDay(dt)                => qscript.asDateTime[F] apply dt map xs.time
    case ToTimestamp(millis)          => qscript.timestampToDateTime[F] apply millis
    case Now()                        => fn.currentDateTime.point[F]

    case ExtractCentury(time)         => qscript.asDateTime[F] apply time map (dt =>
                                           fn.ceiling(fn.yearFromDateTime(dt) div 100.xqy))
    case ExtractDayOfMonth(time)      => qscript.asDateTime[F] apply time map fn.dayFromDateTime
    case ExtractDecade(time)          => qscript.asDateTime[F] apply time map (dt =>
                                           fn.floor(fn.yearFromDateTime(dt) div 10.xqy))
    case ExtractDayOfWeek(time)       => qscript.asDate[F].apply(time) map (d => mkSeq_(xdmp.weekdayFromDate(d) mod 7.xqy))
    case ExtractDayOfYear(time)       => qscript.asDate[F].apply(time) map (xdmp.yeardayFromDate)
    case ExtractEpoch(time)           => qscript.asDateTime[F] apply time flatMap (qscript.secondsSinceEpoch[F].apply(_))
    case ExtractHour(time)            => qscript.asDateTime[F] apply time map fn.hoursFromDateTime
    case ExtractIsoDayOfWeek(time)    => qscript.asDate[F].apply(time) map (xdmp.weekdayFromDate)
    case ExtractIsoYear(time)         => qscript.asDateTime[F] apply time flatMap (qscript.isoyearFromDateTime[F].apply(_))
    case ExtractMicroseconds(time)    => qscript.asDateTime[F] apply time map (dt =>
                                           mkSeq_(fn.secondsFromDateTime(dt) * 1000000.xqy))
    case ExtractMillennium(time)      => qscript.asDateTime[F] apply time map (dt =>
                                           fn.ceiling(fn.yearFromDateTime(dt) div 1000.xqy))
    case ExtractMilliseconds(time)    => qscript.asDateTime[F] apply time map (dt =>
                                           mkSeq_(fn.secondsFromDateTime(dt) * 1000.xqy))
    case ExtractMinute(time)          => qscript.asDateTime[F] apply time map fn.minutesFromDateTime
    case ExtractMonth(time)           => qscript.asDateTime[F] apply time map fn.monthFromDateTime
    case ExtractQuarter(time)         => qscript.asDate[F].apply(time) map (xdmp.quarterFromDate)
    case ExtractSecond(time)          => qscript.asDateTime[F] apply time map fn.secondsFromDateTime
    case ExtractTimezone(time)        => qscript.asDateTime[F] apply time flatMap (qscript.timezoneOffsetSeconds[F].apply(_))
    case ExtractTimezoneHour(time)    => qscript.asDateTime[F] apply time map (dt =>
                                           fn.hoursFromDuration(fn.timezoneFromDateTime(dt)))
    case ExtractTimezoneMinute(time)  => qscript.asDateTime[F] apply time map (dt =>
                                           fn.minutesFromDuration(fn.timezoneFromDateTime(dt)))
    case ExtractWeek(time)            => qscript.asDate[F].apply(time) map (xdmp.weekFromDate)
    case ExtractYear(time)            => qscript.asDateTime[F] apply time map fn.yearFromDateTime

    // math
    case Negate(x)                    => (-x).point[F]
    case Add(x, y)                    => castedBinOp[F](x, y)(_ + _)
    case Multiply(x, y)               => castedBinOp[F](x, y)(_ * _)
    case Subtract(x, y)               => castedBinOp[F](x, y)(_ - _)
    case Divide(x, y)                 => castedBinOp[F](x, y)(_ div _)
    case Modulo(x, y)                 => castedBinOp[F](x, y)(_ mod _)
    case Power(b, e)                  => (ejson.castAsAscribed[F].apply(b) |@| ejson.castAsAscribed[F].apply(e))(math.pow)

    // relations
    case Not(x)                       => ejson.castAsAscribed[F].apply(x) map (fn.not)
    case MapFuncs.Eq(x, y)            => castedBinOpF[F](x, y)(qscript.compEq[F].apply(_, _))
    case Neq(x, y)                    => castedBinOpF[F](x, y)(qscript.compNe[F].apply(_, _))
    case Lt(x, y)                     => castedBinOpF[F](x, y)(qscript.compLt[F].apply(_, _))
    case Lte(x, y)                    => castedBinOpF[F](x, y)(qscript.compLe[F].apply(_, _))
    case Gt(x, y)                     => castedBinOpF[F](x, y)(qscript.compGt[F].apply(_, _))
    case Gte(x, y)                    => castedBinOpF[F](x, y)(qscript.compGe[F].apply(_, _))
    case IfUndefined(x, alternate)    => if_(fn.empty(x)).then_(alternate).else_(x).point[F]
    case And(x, y)                    => castedBinOp[F](x, y)(_ and _)
    case Or(x, y)                     => castedBinOp[F](x, y)(_ or _)
    case Between(v1, v2, v3)          => castedTernOp[F](v1, v2, v3)((x1, x2, x3) => mkSeq_(x2 le x1) and mkSeq_(x1 le x3))
    case Cond(p, t, f)                => if_(xs.boolean(p)).then_(t).else_(f).point[F]

    // set
    case Within(x, arr)               => qscript.elementLeftShift[F].apply(arr) map (xs => fn.exists(fn.indexOf(xs, x)))

    // string
    case Lower(s)                     => fn.lowerCase(s).point[F]
    case Upper(s)                     => fn.upperCase(s).point[F]
    case Bool(s)                      => xs.boolean(s).point[F]
    case Integer(s)                   => xs.integer(s).point[F]
    case Decimal(s)                   => xs.double(s).point[F]
    case Null(s)                      => ejson.null_[F] map (n => if_ (s eq "null".xs) then_ n else_ emptySeq)
    case ToString(x)                  => qscript.toString[F] apply x
    case Search(in, ptn, ci)          => fn.matches(in, ptn, Some(if_ (ci) then_ "i".xs else_ "".xs)).point[F]
    case Substring(s, loc, len)       => qscript.safeSubstring[F] apply (s, loc + 1.xqy, len)

    // structural
    case MakeArray(x)                 => ejson.singletonArray[F] apply x

    case MakeMap(k, v)                =>
      k match {
        case XQuery.StringLit(s) =>
          asQName(s) flatMap (qn =>
            ejson.singletonObject[F] apply (xs.QName(qn.xs), v))

        case _ => ejson.singletonObject[F] apply (k, v)
      }

    case ConcatArrays(x, y)           => qscript.concat[F] apply (x, y)
    case ConcatMaps(x, y)             => ejson.objectConcat[F] apply (x, y)
    case ProjectIndex(arr, idx)       => ejson.arrayElementAt[F] apply (arr, idx + 1.xqy)

    case ProjectField(src, field)     =>
      val prj = field match {
        case XQuery.Step(_) =>
          (src `/` field).point[F]

        case XQuery.StringLit(s) =>
          (asQName[F](s) |@| freshName[F])((qn, m) =>
            if (flwor.isMatching(src))
              let_(m := src) return_ (~m `/` child(qn))
            else
              src `/` child(qn))

        case _ => qscript.projectField[F] apply (src, xs.QName(field))
      }

      prj flatMap (ejson.manyToArray[F] apply _)

    case DeleteField(src, field)      => qscript.deleteField[F] apply (src, field)
    case Meta(x)                      => qscript.meta[F] apply x

    // other
    case Range(x, y)                  => (x to y).point[F]

    // FIXME: This isn't correct, just an interim impl to allow some queries to execute.
    case Guard(_, _, cont, _)         => s"(: GUARD CONT :)$cont".xqy.point[F]
  }

  ////

  private def binOpF[F[_]: QNameGenerator: Monad](x: XQuery, y: XQuery)(op: (XQuery, XQuery) => F[XQuery]): F[XQuery] =
    if (flwor.isMatching(x) || flwor.isMatching(y))
      for {
        vx <- freshName[F]
        vy <- freshName[F]
        r  <- op(~vx, ~vy)
      } yield mkSeq_(let_(vx := x, vy := y) return_ r)
    else
      op(x, y)

  private def castedBinOpF[F[_]: QNameGenerator: PrologW](x: XQuery, y: XQuery)(op: (XQuery, XQuery) => F[XQuery]): F[XQuery] =
    binOpF(x, y)((vx, vy) => (ejson.castAsAscribed[F].apply(vx) |@| ejson.castAsAscribed[F].apply(vy))(op).join)

  private def castedBinOp[F[_]: QNameGenerator: PrologW](x: XQuery, y: XQuery)(op: (XQuery, XQuery) => XQuery): F[XQuery] =
    castedBinOpF[F](x, y)((a, b) => op(a, b).point[F])

  private def ternOpF[F[_]: QNameGenerator: Monad](x: XQuery, y: XQuery, z: XQuery)(op: (XQuery, XQuery, XQuery) => F[XQuery]): F[XQuery] =
    if (flwor.isMatching(x) || flwor.isMatching(y) || flwor.isMatching(z))
      for {
        vx <- freshName[F]
        vy <- freshName[F]
        vz <- freshName[F]
        r  <- op(~vx, ~vy, ~vz)
      } yield mkSeq_(let_(vx := x, vy := y, vz := z) return_ r)
    else
      op(x, y, z)

  private def castedTernOp[F[_]: QNameGenerator: PrologW](x: XQuery, y: XQuery, z: XQuery)(op: (XQuery, XQuery, XQuery) => XQuery): F[XQuery] =
    ternOpF(x, y, z)((vx, vy, vz) => (ejson.castAsAscribed[F].apply(vx) |@| ejson.castAsAscribed[F].apply(vy) |@| ejson.castAsAscribed[F].apply(vz))(op))
}
