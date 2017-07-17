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

package quasar.physical.marklogic.qscript

import slamdata.Predef._
import quasar.Data
import quasar.fp._
import quasar.fp.ski.κ
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript.{MapFuncCore, MapFuncsCore}, MapFuncsCore.{Search => QSearch, _}

import eu.timepit.refined.auto._
import matryoshka._, Recursive.ops._
import scalaz.{Monad, Show}
import scalaz.syntax.monad._

private[qscript] final class MapFuncCorePlanner[
  F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr,
  FMT,
  T[_[_]]: RecursiveT
](implicit
  SP: StructuralPlanner[F, FMT]
) extends MapFuncPlanner[F, FMT, MapFuncCore[T, ?]] {
  import expr.{emptySeq, if_, let_, some, try_}, XQuery.flwor

  // wart: uses `eq` (3x) and `ne`
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  val plan: AlgebraM[F, MapFuncCore[T, ?], XQuery] = {
    case Constant(ejson)              => DataPlanner[F, FMT](ejson.cata(Data.fromEJson))
    case Undefined()                  => emptySeq.point[F]
    case JoinSideName(n)              => MonadPlanErr[F].raiseError(MarkLogicPlannerError.unreachable(s"JoinSideName(${Show[Symbol].shows(n)})"))

    case Length(arrOrstr)             => lib.length[F] apply arrOrstr

    // time
    case Date(s)                      => lib.asDate[F] apply s
    case Time(s)                      => xs.time(s).point[F]
    case Timestamp(s)                 => xs.dateTime(s).point[F]
    case Interval(s)                  => xs.dayTimeDuration(s).point[F]
    case StartOfDay(date)             => lib.startOfDay[F] apply date
    case TemporalTrunc(part, src)     => lib.temporalTrunc[F](part) apply src
    case TimeOfDay(dt)                => asDateTime(dt) map xs.time
    case ToTimestamp(millis)          => SP.castIfNode(millis) >>= (lib.timestampToDateTime[F] apply _)
    case TypeOf(x)                    => lib.typeOf[F, FMT] apply x
    case Now()                        => fn.currentDateTime.point[F]

    case ExtractCentury(time)         => asDateTime(time) map (dt =>
                                           fn.ceiling(fn.yearFromDateTime(dt) div 100.xqy))
    case ExtractDayOfMonth(time)      => asDateTime(time) map fn.dayFromDateTime
    case ExtractDecade(time)          => asDateTime(time) map (dt =>
                                           fn.floor(fn.yearFromDateTime(dt) div 10.xqy))
    case ExtractDayOfWeek(time)       => asDate(time) map (d => mkSeq_(xdmp.weekdayFromDate(d) mod 7.xqy))
    case ExtractDayOfYear(time)       => asDate(time) map (xdmp.yeardayFromDate)
    case ExtractEpoch(time)           => asDateTime(time) flatMap (lib.secondsSinceEpoch[F].apply(_))
    case ExtractHour(time)            => asDateTime(time) map fn.hoursFromDateTime
    case ExtractIsoDayOfWeek(time)    => asDate(time) map (xdmp.weekdayFromDate)
    case ExtractIsoYear(time)         => asDateTime(time) flatMap (lib.isoyearFromDateTime[F].apply(_))
    case ExtractMicroseconds(time)    => asDateTime(time) map (dt =>
                                           mkSeq_(fn.secondsFromDateTime(dt) * 1000000.xqy))
    case ExtractMillennium(time)      => asDateTime(time) map (dt =>
                                           fn.ceiling(fn.yearFromDateTime(dt) div 1000.xqy))
    case ExtractMilliseconds(time)    => asDateTime(time) map (dt =>
                                           mkSeq_(fn.secondsFromDateTime(dt) * 1000.xqy))
    case ExtractMinute(time)          => asDateTime(time) map fn.minutesFromDateTime
    case ExtractMonth(time)           => asDateTime(time) map fn.monthFromDateTime
    case ExtractQuarter(time)         => asDate(time) map (xdmp.quarterFromDate)
    case ExtractSecond(time)          => asDateTime(time) map fn.secondsFromDateTime
    case ExtractTimezone(time)        => asDateTime(time) flatMap (lib.timezoneOffsetSeconds[F].apply(_))
    case ExtractTimezoneHour(time)    => asDateTime(time) map (dt =>
                                           fn.hoursFromDuration(fn.timezoneFromDateTime(dt)))
    case ExtractTimezoneMinute(time)  => asDateTime(time) map (dt =>
                                           fn.minutesFromDuration(fn.timezoneFromDateTime(dt)))
    case ExtractWeek(time)            => asDate(time) map (xdmp.weekFromDate)
    case ExtractYear(time)            => asDateTime(time) map fn.yearFromDateTime

    // math
    case Negate(x)                    => SP.castIfNode(x) map (-_)
    case Add(x, y)                    => castedBinOp(x, y)(_ + _)
    case Multiply(x, y)               => castedBinOp(x, y)(_ * _)
    case Subtract(x, y)               => castedBinOp(x, y)(_ - _)
    case Divide(x, y)                 => castedBinOp(x, y)(_ div _)
    case Modulo(x, y)                 => castedBinOp(x, y)(_ mod _)
    case Power(b, e)                  => (SP.castIfNode(b) |@| SP.castIfNode(e))(math.pow)

    // relations
    case Not(x)                       => SP.castIfNode(x) map (fn.not)
    case MapFuncsCore.Eq(x, y)        => castedBinOp(x, y)((a, b) => handleWith(a eq b, fn.False))
    case Neq(x, y)                    => castedBinOp(x, y)((a, b) => handleWith(a ne b, fn.True))
    case Lt(x, y)                     => castedBinOp(x, y)(_ lt _)
    case Lte(x, y)                    => castedBinOp(x, y)(_ le _)
    case Gt(x, y)                     => castedBinOp(x, y)(_ gt _)
    case Gte(x, y)                    => castedBinOp(x, y)(_ ge _)
    case IfUndefined(x, alternate)    => if_(fn.empty(x)).then_(alternate).else_(x).point[F]
    case And(x, y)                    => castedBinOp(x, y)(_ and _)
    case Or(x, y)                     => castedBinOp(x, y)(_ or _)
    case Between(v1, v2, v3)          => castedTernOp(v1, v2, v3)((x1, x2, x3) => mkSeq_(x2 le x1) and mkSeq_(x1 le x3))
    case Cond(p, t, f)                => if_(xs.boolean(p)).then_(t).else_(f).point[F]

    // string
    case Lower(s)                     => fn.lowerCase(s).point[F]
    case Upper(s)                     => fn.upperCase(s).point[F]
    case Bool(s)                      => xs.boolean(s).point[F]
    case Integer(s)                   => xs.integer(s).point[F]
    case Decimal(s)                   => xs.double(s).point[F]
    case Null(s)                      => SP.null_ map (n => if_ (s eq "null".xs) then_ n else_ emptySeq)
    case ToString(x)                  => SP.asString(x)
    case QSearch(in, ptn, ci)         => fn.matches(in, ptn, Some(if_ (ci) then_ "im".xs else_ "m".xs)).point[F]
    case Substring(s, loc, len)       => lib.safeSubstring[F] apply (s, loc + 1.xqy, len)

    // structural
    case MakeArray(x)                 => SP.singletonArray(x)
    case MakeMap(k, v)                => SP.singletonObject(k, v)
    case ConcatArrays(x, y)           => lib.concat[F, FMT] apply (x, y)
    case ConcatMaps(x, y)             => SP.objectMerge(x, y)
    case ProjectIndex(arr, idx)       => SP.arrayElementAt(arr, idx)
    case ProjectField(src, field)     => SP.objectLookup(src, field)
    case DeleteField(src, field)      => SP.objectDelete(src, field)

    case Within(x, arr)               => (freshName[F] |@| freshName[F] |@| SP.leftShift(arr))((a, b, bs) =>
                                           (SP.castIfNode(x) |@| SP.castIfNode(~b))((cx, cb) =>
                                             some(b in bs, a in cx) satisfies (cb eq ~a))).join

    case Meta(x)                      => lib.meta[F, FMT] apply x

    // other
    case Range(x, y)                  => castedBinOp(x, y)(_ to _)

    // FIXME: This isn't correct, just an interim impl to allow some queries to execute.
    case Guard(_, _, cont, _)         => s"(: GUARD CONT :)$cont".xqy.point[F]
  }

  ////

  private def asDate(x: XQuery)     = SP.castIfNode(x) >>= (lib.asDate[F] apply _)
  private def asDateTime(x: XQuery) = SP.castIfNode(x) >>= (lib.asDateTime[F] apply _)

  private def handleWith(xqy: XQuery, alt: XQuery): XQuery =
    try_(xqy).catch_($("_"))(κ(alt))

  private def binOpF(x: XQuery, y: XQuery)(op: (XQuery, XQuery) => F[XQuery]): F[XQuery] =
    if (flwor.nonEmpty(x) || flwor.nonEmpty(y))
      for {
        vx <- freshName[F]
        vy <- freshName[F]
        r  <- op(~vx, ~vy)
      } yield mkSeq_(let_(vx := x, vy := y) return_ r)
    else
      op(x, y)

  private def castedBinOpF(x: XQuery, y: XQuery)(op: (XQuery, XQuery) => F[XQuery]): F[XQuery] =
    binOpF(x, y)((vx, vy) => (SP.castIfNode(vx) |@| SP.castIfNode(vy))(op).join)

  private def castedBinOp(x: XQuery, y: XQuery)(op: (XQuery, XQuery) => XQuery): F[XQuery] =
    castedBinOpF(x, y)((a, b) => op(a, b).point[F])

  private def ternOpF(x: XQuery, y: XQuery, z: XQuery)(op: (XQuery, XQuery, XQuery) => F[XQuery]): F[XQuery] =
    if (flwor.nonEmpty(x) || flwor.nonEmpty(y) || flwor.nonEmpty(z))
      for {
        vx <- freshName[F]
        vy <- freshName[F]
        vz <- freshName[F]
        r  <- op(~vx, ~vy, ~vz)
      } yield mkSeq_(let_(vx := x, vy := y, vz := z) return_ r)
    else
      op(x, y, z)

  private def castedTernOp(x: XQuery, y: XQuery, z: XQuery)(op: (XQuery, XQuery, XQuery) => XQuery): F[XQuery] =
    ternOpF(x, y, z)((vx, vy, vz) => (SP.castIfNode(vx) |@| SP.castIfNode(vy) |@| SP.castIfNode(vz))(op))
}
