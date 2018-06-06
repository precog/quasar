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
import quasar.Type
import quasar.fp._
import quasar.contrib.iota._
import quasar.contrib.iota.mkInject
import quasar.fp.ski._
import quasar.fs.MonadFsErr
import quasar.physical.mongodb.{Bson, BsonCodec, BsonField, BsonVersion}
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner.common._
import quasar.qscript.{MapFuncsDerived => D, _}, MapFuncsCore._
import quasar.qscript.rewrites.{Coalesce => _}
import quasar.time.TemporalPart

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import scalaz.{Divide => _, Split => _, _}, Scalaz._
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::
import simulacrum.typeclass

@typeclass trait FuncHandler[IN[_]] {

  def handleOpsCore[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
    (v: BsonVersion)
    (implicit e32: ExprOpCoreF :<: EX)
      : AlgebraM[M, IN, Fix[EX]]

  def handleOps3_4[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
    (v: BsonVersion)
    (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX)
      : AlgebraM[(Option ∘ M)#λ, IN, Fix[EX]]

  def handleOps3_4_4[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
    (v: BsonVersion)
    (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX, e344: ExprOp3_4_4F :<: EX)
      : AlgebraM[(Option ∘ M)#λ, IN, Fix[EX]]

  def handleOps3_6[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
    (v: BsonVersion)
    (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX, e344: ExprOp3_4_4F :<: EX, e36: ExprOp3_6F :<: EX)
      : AlgebraM[(Option ∘ M)#λ, IN, Fix[EX]]

  def handle3_2[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
    (v: BsonVersion)
    (implicit e32: ExprOpCoreF :<: EX)
      : AlgebraM[M, IN, Fix[EX]] =
    handleOpsCore[EX, M](v)

  def handle3_4[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
    (v: BsonVersion)
    (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX)
      : AlgebraM[M, IN, Fix[EX]] = f => {
    val h34 = handleOps3_4[EX, M](v)
    val h = handle3_2[EX, M](v)
    h34(f) getOrElse h(f)
  }

  def handle3_4_4[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
    (v: BsonVersion)
    (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX, e344: ExprOp3_4_4F :<: EX)
     : AlgebraM[M, IN, Fix[EX]] = f => {
    val h344 = handleOps3_4_4[EX, M](v)
    val h34 = handle3_4[EX, M](v)
    h344(f) getOrElse h34(f)
  }

  def handle3_6[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
    (v: BsonVersion)
    (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX, e344: ExprOp3_4_4F :<: EX, e36: ExprOp3_6F :<: EX)
     : AlgebraM[M, IN, Fix[EX]] = f => {
    val h36 = handleOps3_6[EX, M](v)
    val h344 = handle3_4_4[EX, M](v)
    h36(f) getOrElse h344(f)
  }
}

object FuncHandler {

  implicit def mapFuncCore[T[_[_]]: BirecursiveT: ShowT]: FuncHandler[MapFuncCore[T, ?]] =
    new FuncHandler[MapFuncCore[T, ?]] {

      def execTime[M[_]: Monad: MonadFsErr](implicit MR: ExecTimeR[M]): M[Bson.Date] =
        OptionT[M, Bson.Date](MR.ask.map(Bson.Date.fromInstant(_)))
          .getOrElseF(raiseInternalError("Could not get the current timestamp"))

      def handleOpsCore[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
        (v: BsonVersion)
        (implicit e32: ExprOpCoreF :<: EX)
          : AlgebraM[M, MapFuncCore[T, ?], Fix[EX]] = {

        val fp = new ExprOpCoreF.fixpoint[Fix[EX], EX](_.embed)
        import fp._
        import FormatSpecifier._

        val check = new Check[Fix[EX], EX]

        {
          case Undefined()           => $literal(Bson.Undefined).point[M]
          case Add(a1, a2)           => $add(a1, a2).point[M]
          case Multiply(a1, a2)      => $multiply(a1, a2).point[M]
          case Subtract(a1, a2)      => $subtract(a1, a2).point[M]
          case Divide(a1, $literal(x)) if Bson.isInt(x, 0) =>
            mkDivideBy0(a1).point[M]
          case Divide(a1, a2@$literal(_)) =>
            $divide(a1, a2).point[M]
          case Divide(a1, a2) =>
            // NB: It’s apparently intentional that division by zero crashes
            //     the query in MongoDB. See
            //     https://jira.mongodb.org/browse/SERVER-29410
            $cond($eq(a2, $literal(Bson.Int32(0))),
              mkDivideBy0(a1),
              $divide(a1, a2)).point[M]
          case Modulo(a1, a2)        => $mod(a1, a2).point[M]
          case Negate(a1)            => $multiply($literal(Bson.Int32(-1)), a1).point[M]
          case MapFuncsCore.Eq(a1, a2) => $eq(a1, a2).point[M]
          case Neq(a1, a2)           => $neq(a1, a2).point[M]
          case Lt(a1, a2)            => $lt(a1, a2).point[M]
          case Lte(a1, a2)           => $lte(a1, a2).point[M]
          case Gt(a1, a2)            => $gt(a1, a2).point[M]
          case Gte(a1, a2)           => $gte(a1, a2).point[M]

          // FIXME: this is valid for strings only

          case Lower(a1)             => $toLower(a1).point[M]
          case Upper(a1)             => $toUpper(a1).point[M]
          case Substring(a1, a2, a3) => $substr(a1, a2, a3).point[M]
          case Cond(a1, a2, a3)      => $cond(a1, a2, a3).point[M]

          case Or(a1, a2)            => $or(a1, a2).point[M]
          case And(a1, a2)           => $and(a1, a2).point[M]
          case Not(a1)               => $not(a1).point[M]

          case Null(a1) =>
            $cond($eq(a1, $literal(Bson.Text("null"))),
              $literal(Bson.Null),
              $literal(Bson.Undefined)).point[M]

          case Bool(a1) =>
            $cond($eq(a1, $literal(Bson.Text("true"))),
              $literal(Bson.Bool(true)),
              $cond($eq(a1, $literal(Bson.Text("false"))),
                $literal(Bson.Bool(false)),
                $literal(Bson.Undefined))).point[M]

          case ExtractCentury(a1) => mkYearToCentury($year(a1)).point[M]
          case ExtractDayOfMonth(a1) => $dayOfMonth(a1).point[M]
          case ExtractDecade(a1) => mkYearToDecade($year(a1)).point[M]
          case ExtractDayOfWeek(a1) => $subtract($dayOfWeek(a1), $literal(Bson.Int32(1))).point[M]
          case ExtractDayOfYear(a1) => $dayOfYear(a1).point[M]
          case ExtractEpoch(a1) =>
            $divide(
              $subtract(a1, $literal(Bson.Date(0))),
              $literal(Bson.Int32(1000))).point[M]
          case ExtractHour(a1) => $hour(a1).point[M]
          case ExtractIsoDayOfWeek(a1) =>
            $cond($eq($dayOfWeek(a1), $literal(Bson.Int32(1))),
              $literal(Bson.Int32(7)),
              $subtract($dayOfWeek(a1), $literal(Bson.Int32(1)))).point[M]
          case ExtractMicrosecond(a1) =>
            $multiply(
              $add(
                $multiply($second(a1), $literal(Bson.Int32(1000))),
                $millisecond(a1)),
              $literal(Bson.Int32(1000))).point[M]
          case ExtractMillennium(a1) => mkYearToMillenium($year(a1)).point[M]
          case ExtractMillisecond(a1) =>
            $add(
              $multiply($second(a1), $literal(Bson.Int32(1000))),
              $millisecond(a1)).point[M]
          case ExtractMinute(a1) => $minute(a1).point[M]
          case ExtractMonth(a1) => $month(a1).point[M]
          case ExtractQuarter(a1) =>
            $trunc(
              $add(
                $divide(
                  $subtract($month(a1), $literal(Bson.Int32(1))),
                  $literal(Bson.Int32(3))),
                $literal(Bson.Int32(1)))).point[M]
          case ExtractSecond(a1) =>
            $add($second(a1), $divide($millisecond(a1), $literal(Bson.Int32(1000)))).point[M]
          case ExtractWeek(a1) => $week(a1).point[M]
          case ExtractYear(a1) => $year(a1).point[M]

          case ToTimestamp(a1) =>
           $add($literal(Bson.Date(0)), a1).point[M]

          case Between(a1, a2, a3)   => $and($lte(a2, a1), $lte(a1, a3)).point[M]
          case TimeOfDay(a1) =>
            $dateToString(Hour :: ":" :: Minute :: ":" :: Second :: "." :: Millisecond :: FormatString.empty, a1).point[M]
          case Power(a1, a2) => $pow(a1, a2).point[M]
          case ProjectIndex(a1, a2) => $arrayElemAt(a1, a2).point[M]
          case MakeArray(a1) => $arrayLit(List(a1)).point[M]
          case ConcatArrays(a1, a2) =>
            $let(ListMap(DocVar.Name("a1") -> a1, DocVar.Name("a2") -> a2),
              $cond($and($isArray($field("$a1")), $isArray($field("$a2"))),
                $concatArrays(List($field("$a1"), $field("$a2"))),
                $concat($field("$a1"), $field("$a2")))).point[M]
          case TypeOf(a1) => mkTypeOf(a1, $isArray).point[M]

          case Constant(v1)  =>
            v1.cataM(BsonCodec.fromEJson(v)).fold(
              raisePlannerError(_),
              $literal(_).point[M])
          case Now() => execTime[M] map ($literal(_))

          // NB: The aggregation implementation of `ToString` does not handle ObjectId
          //     Here we force this case to be planned using JS
          case ToString($var(DocVar(_, Some(BsonField.Name("_id"))))) =>
            unimplemented[M, Fix[EX]]("ToString _id expression")
          case ToString(a1) => mkToString(a1, $substr).point[M]

          case ProjectKey($var(dv), $literal(Bson.Text(key))) =>
            $var(dv \ BsonField.Name(key)).point[M]
          case ProjectKey(el @ $arrayElemAt($var(dv), _), $literal(Bson.Text(key))) =>
            $let(ListMap(DocVar.Name("el") -> el),
              $var(DocVar.ROOT(BsonField.Name("$el")) \ BsonField.Name(key))).point[M]

          // NB: This is maybe a NOP for Fix[ExprOp]s, as they (all?) safely
          //     short-circuit when given the wrong type. However, our guards may be
          //     more restrictive than the operation, in which case we still want to
          //     short-circuit, so …
          case Guard(expr, typ, cont, fallback) =>
            import Type._

            // NB: Even if certain checks aren’t needed by ExprOps, we have to
            //     maintain them because we may convert ExprOps to JS.
            //     Hopefully BlackShield will eliminate the need for this.
            @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
            def exprCheck: Type => Option[Fix[EX] => Fix[EX]] =
              generateTypeCheck[Fix[EX], Fix[EX]]($or(_, _)) {
                case Type.Null => check.isNull
                case Type.Int
                   | Type.Dec
                   | Type.Int ⨿ Type.Dec
                   | Type.Int ⨿ Type.Dec ⨿ Type.Interval => check.isNumber // for now intervals check as numbers
                case Type.Str => check.isString
                case Type.Obj(map, _) =>
                  ((expr: Fix[EX]) => {
                    val basic = check.isObject(expr)
                    expr match {
                      case $var(dv) =>
                        map.foldLeft(
                          basic)(
                          (acc, pair) =>
                          exprCheck(pair._2).fold(
                            acc)(
                            e => $and(acc, e($var(dv \ BsonField.Name(pair._1))))))
                      case _ => basic // FIXME: Check fields
                    }
                  })
                case Type.FlexArr(_, _, _) => check.isArray
                case Type.Binary => check.isBinary
                case Type.Id => check.isId
                case Type.Bool => check.isBoolean
                case Type.OffsetDateTime | Type.OffsetDate | Type.OffsetTime |
                    Type.LocalDateTime | Type.LocalDate | Type.LocalTime => check.isDate
                // NB: Some explicit coproducts for adjacent types.
                case Type.Int ⨿ Type.Dec ⨿ Type.Str => check.isNumberOrString
                case Type.Int ⨿ Type.Dec ⨿ Type.Interval ⨿ Type.Str => check.isNumberOrString // for now intervals check as numbers
                case Type.LocalDate ⨿ Type.Bool => check.isDateTimestampOrBoolean
                case Type.Syntaxed => check.isSyntaxed
              }
            exprCheck(typ).fold(cont)(f => $cond(f(expr), cont, fallback)).point[M]

          // NB: catch all added because scala cannot check exhaustiveness here.
          // It's also not trivial to manually make sure that all cases are covered,
          // so without getting triggered by the compiler when this needs to be
          // updated, this will be hard to keep in sync.
          case x =>
            val mf: MapFuncCore[T,quasar.qscript.Hole] = x.as(SrcHole)
            unimplemented[M, Fix[EX]](s"expression ${mf.shows}")
        }
      }

      def handleOps3_4[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
        (v: BsonVersion)
        (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX)
          : AlgebraM[(Option ∘ M)#λ, MapFuncCore[T, ?], Fix[EX]] = { mfc =>

        val fp32  = new ExprOpCoreF.fixpoint[Fix[EX], EX](_.embed)
        val fp34  = new ExprOp3_4F.fixpoint[Fix[EX], EX](_.embed)

        import fp32._, fp34._

        val check = new Check[Fix[EX], EX]

        mfc.some collect {
          case Split(a1, a2) => $split(a1, a2).point[M]
          case Substring(a1, a2, a3) =>
            $cond($or(
                $lt(a2, $literal(Bson.Int32(0))),
                $gt(a2, $strLenCP(a1))),
              $literal(Bson.Text("")),
              $cond(
                $lt(a3, $literal(Bson.Int32(0))),
                $substrCP(a1, a2, $strLenCP(a1)),
                $substrCP(a1, a2, a3))).point[M]

          // NB: The aggregation implementation of `ToString` does not handle ObjectId
          //     Here we force this case to be planned using JS
          case ToString($var(DocVar(_, Some(BsonField.Name("_id"))))) =>
            unimplemented[M, Fix[EX]]("ToString _id expression")
          case ToString(a1) => mkToString(a1, $substrBytes).point[M]

          // NB: Quasar strings are arrays of characters. However, MongoDB
          //     represent strings and arrays as distinct types. Moreoever, SQL^2
          //     exposes two functions: `array_length` to obtain the length of an
          //     array and `length` to obtain the length of a string. This
          //     distinction, however, is lost when LP is translated into
          //     QScript. There's only one `Length` MapFunc. The workaround here
          //     detects calls to array_length or length indirectly through the
          //     typechecks inserted around calls to `Length` or `ArrayLength` in
          //     LP typechecks.
          case Length(a1) => $strLenCP(a1).point[M]
          case Guard(expr, Type.Str, cont @ $strLenCP(_), fallback) =>
            $cond(check.isString(expr), cont, fallback).point[M]
          case Guard(expr, Type.FlexArr(_, _, _), $strLenCP(str), fallback) =>
            $cond(check.isArray(expr), $size(str), fallback).point[M]
        }
      }

      def handleOps3_4_4[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
        (v: BsonVersion)
        (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX, e344: ExprOp3_4_4F :<: EX)
          : AlgebraM[(Option ∘ M)#λ, MapFuncCore[T, ?], Fix[EX]] = κ(None)

      def handleOps3_6[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
        (v: BsonVersion)
        (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX, e344: ExprOp3_4_4F :<: EX, e36: ExprOp3_6F :<: EX)
          : AlgebraM[(Option ∘ M)#λ, MapFuncCore[T, ?], Fix[EX]] = { mfc =>

        val fp32  = new ExprOpCoreF.fixpoint[Fix[EX], EX](_.embed)
        val fp36  = new ExprOp3_6F.fixpoint[Fix[EX], EX](_.embed)

        import fp32._, fp36._
        import ExprOp3_6F.DateParts

        def extractDateFieldIso(date: Fix[EX], fieldName: BsonField.Name): Fix[EX] =
          $let(
            ListMap(DocVar.Name("parts") -> $dateToParts(date, None, true.some)),
            $var(DocVar.ROOT(BsonField.Name("$parts")) \ fieldName))

        val selectPartsField: String => Fix[EX] =
          f => $var(DocVar.ROOT(BsonField.Name("$parts")) \ BsonField.Name(f))

        val selectPartsFieldIf: Boolean => String => Option[Fix[EX]] =
          cond => f =>
            if (cond) selectPartsField(f).some else none

        // i is number of $dateFromParts args to keep apart from year
        // (which is always kept) and timezone (which is never kept)
        def tempTrunc(date: Fix[EX], i: Int): Fix[EX] =
          $let(
            ListMap(
              DocVar.Name("parts") -> $dateToParts(date, None, false.some)),
            $dateFromParts(
              y  = selectPartsField(DateParts.year),
              m  = selectPartsFieldIf(i >= 1)(DateParts.month),
              d  = selectPartsFieldIf(i >= 2)(DateParts.day),
              h  = selectPartsFieldIf(i >= 3)(DateParts.hour),
              mi = selectPartsFieldIf(i >= 4)(DateParts.minute),
              s  = selectPartsFieldIf(i >= 5)(DateParts.second),
              ms = selectPartsFieldIf(i >= 6)(DateParts.millisecond),
              tz = none))

        def dateWith(
          date: Fix[EX],
          year: Option[Fix[EX]],
          month: Option[Option[Fix[EX]]]): Fix[EX] =
          $let(
            ListMap(
              DocVar.Name("parts") -> $dateToParts(date, None, false.some)),
            $dateFromParts(
              y  = year.getOrElse(selectPartsField(DateParts.year)),
              m  = month.getOrElse(selectPartsField(DateParts.month).some),
              d  = none,
              h  = none,
              mi = none,
              s  = none,
              ms = none,
              tz = none))

        mfc.some collect {
          case ExtractIsoDayOfWeek(a1) =>
            extractDateFieldIso(a1, BsonField.Name(DateParts.isoDayOfWeek)).point[M]
          case ExtractIsoYear(a1) =>
            extractDateFieldIso(a1, BsonField.Name(DateParts.isoWeekYear)).point[M]
          case ExtractWeek(a1) =>
            extractDateFieldIso(a1, BsonField.Name(DateParts.isoWeek)).point[M]
          case LocalDate(a1) => $dateFromString(a1, None).point[M]
          case LocalDateTime(a1) => $dateFromString(a1, None).point[M]
          case StartOfDay(a1) => tempTrunc(a1, 2).point[M]
          case tt @ TemporalTrunc(part, a1) =>
            ExprOp3_6F.dateFromPartsArgIndex(part) match {
              case Some(i) => tempTrunc(a1, i).point[M]
              case None =>
                part match {
                  case TemporalPart.Millennium =>
                    dateWith(
                      date = a1,
                      year = mkTruncBy(1000, selectPartsField(DateParts.year)).some,
                      month = Some(None)).point[M]
                  case TemporalPart.Century =>
                    dateWith(
                      date = a1,
                      year = mkTruncBy(100, selectPartsField(DateParts.year)).some,
                      month = Some(None)).point[M]
                  case TemporalPart.Decade =>
                    dateWith(
                      date = a1,
                      year = mkTruncBy(10, selectPartsField(DateParts.year)).some,
                      month = Some(None)).point[M]
                  case TemporalPart.Quarter =>
                    dateWith(
                      date = a1,
                      year = none,
                      month = Some(Some(
                        $add(
                          $literal(Bson.Int32(1)),
                          mkTruncBy(3, $subtract(
                            selectPartsField(DateParts.month),
                            $literal(Bson.Int32(1)))))))).point[M]
                  case TemporalPart.Week =>
                    $let(
                      ListMap(
                        DocVar.Name("parts") -> $dateToParts(a1, None, true.some)),
                      $dateFromPartsIso(
                        y  = selectPartsField(DateParts.isoWeekYear),
                        w  = selectPartsField(DateParts.isoWeek).some,
                        d  = none,
                        h  = none,
                        mi = none,
                        s  = none,
                        ms = none,
                        tz = none)).point[M]
                  case _ =>
                    val mf: MapFuncCore[T,quasar.qscript.Hole] =
                      (tt : MapFuncCore[T, Fix[EX]]).as(SrcHole)
                    unimplemented[M, Fix[EX]](s"expression ${mf.shows}")
                }
            }
        }
      }
    }

  implicit def mapFuncDerived[T[_[_]]: CorecursiveT]
    (implicit core: FuncHandler[MapFuncCore[T, ?]])
      : FuncHandler[MapFuncDerived[T, ?]] =
    new FuncHandler[MapFuncDerived[T, ?]] {
      def handleOpsCore[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
        (v: BsonVersion)
        (implicit e32: ExprOpCoreF :<: EX)
          : AlgebraM[M, MapFuncDerived[T, ?], Fix[EX]] = {

        val fp = new ExprOpCoreF.fixpoint[Fix[EX], EX](_.embed)
        import fp._

        val derived: AlgebraM[(Option ∘ M)#λ, MapFuncDerived[T, ?], Fix[EX]] = {
          _.some.collect {
            case D.Abs(a1)       => $abs(a1).point[M]
            case D.Ceil(a1)      => $ceil(a1).point[M]
            case D.Floor(a1)     => $floor(a1).point[M]
            case D.Trunc(a1)     => $trunc(a1).point[M]
          }
        }

        ExpandMapFunc.expand(core.handle3_2(v), derived)
      }

      def handleOps3_4[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
        (v: BsonVersion)
        (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX)
          : AlgebraM[(Option ∘ M)#λ, MapFuncDerived[T, ?], Fix[EX]] = κ(None)

      def handleOps3_4_4[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
        (v: BsonVersion)
        (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX, e344: ExprOp3_4_4F :<: EX)
          : AlgebraM[(Option ∘ M)#λ, MapFuncDerived[T, ?], Fix[EX]] = κ(None)

      def handleOps3_6[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
        (v: BsonVersion)
        (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX, e344: ExprOp3_4_4F :<: EX, e36: ExprOp3_6F :<: EX)
          : AlgebraM[(Option ∘ M)#λ, MapFuncDerived[T, ?], Fix[EX]] = κ(None)
    }

  implicit def copk[LL <: TListK](implicit M: Materializer[LL]): FuncHandler[CopK[LL, ?]] =
    M.materialize(offset = 0)

  sealed trait Materializer[LL <: TListK] {
    def materialize(offset: Int): FuncHandler[CopK[LL, ?]]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[F[_]](
      implicit
      F: FuncHandler[F]
    ): Materializer[F ::: TNilK] = new Materializer[F ::: TNilK] {
      override def materialize(offset: Int): FuncHandler[CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new FuncHandler[CopK[F ::: TNilK, ?]] {
          def handleOpsCore[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
          (v: BsonVersion)
            (implicit e32: ExprOpCoreF :<: EX)
          : AlgebraM[M, CopK[F ::: TNilK, ?], Fix[EX]] = {
            case I(fa) => F.handleOpsCore[EX, M](v).apply(fa)
          }

          def handleOps3_4[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
          (v: BsonVersion)
            (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX)
          : AlgebraM[(Option ∘ M)#λ, CopK[F ::: TNilK, ?], Fix[EX]] = {
            case I(fa) => F.handleOps3_4[EX, M](v).apply(fa)
          }

          def handleOps3_4_4[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
          (v: BsonVersion)
            (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX, e344: ExprOp3_4_4F :<: EX)
          : AlgebraM[(Option ∘ M)#λ, CopK[F ::: TNilK, ?], Fix[EX]] = {
            case I(fa) => F.handleOps3_4_4[EX, M](v).apply(fa)
          }

          def handleOps3_6[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
          (v: BsonVersion)
            (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX, e344: ExprOp3_4_4F :<: EX, e36: ExprOp3_6F :<: EX)
          : AlgebraM[(Option ∘ M)#λ, CopK[F ::: TNilK, ?], Fix[EX]] = {
            case I(fa) => F.handleOps3_6[EX, M](v).apply(fa)
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[F[_], LL <: TListK](
      implicit
      F: FuncHandler[F],
      LL: Materializer[LL]
    ): Materializer[F ::: LL] = new Materializer[F ::: LL] {
      override def materialize(offset: Int): FuncHandler[CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new FuncHandler[CopK[F ::: LL, ?]] {
          def handleOpsCore[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
            (v: BsonVersion)
            (implicit e32: ExprOpCoreF :<: EX)
              : AlgebraM[M, CopK[F ::: LL, ?], Fix[EX]] = {
            case I(fa) => F.handleOpsCore[EX, M](v).apply(fa)
            case other => LL.materialize(offset + 1).handleOpsCore[EX, M](v).apply(other.asInstanceOf[CopK[LL, Fix[EX]]])
          }

          def handleOps3_4[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
            (v: BsonVersion)
            (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX)
              : AlgebraM[(Option ∘ M)#λ, CopK[F ::: LL, ?], Fix[EX]] = {
            case I(fa) => F.handleOps3_4[EX, M](v).apply(fa)
            case other => LL.materialize(offset + 1).handleOps3_4[EX, M](v).apply(other.asInstanceOf[CopK[LL, Fix[EX]]])
          }

          def handleOps3_4_4[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
            (v: BsonVersion)
            (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX, e344: ExprOp3_4_4F :<: EX)
              : AlgebraM[(Option ∘ M)#λ, CopK[F ::: LL, ?], Fix[EX]] = {
            case I(fa) => F.handleOps3_4_4[EX, M](v).apply(fa)
            case other => LL.materialize(offset + 1).handleOps3_4_4[EX, M](v).apply(other.asInstanceOf[CopK[LL, Fix[EX]]])
          }

          def handleOps3_6[EX[_]: Functor, M[_]: Monad: MonadFsErr: ExecTimeR]
            (v: BsonVersion)
            (implicit e32: ExprOpCoreF :<: EX, e34: ExprOp3_4F :<: EX, e344: ExprOp3_4_4F :<: EX, e36: ExprOp3_6F :<: EX)
              : AlgebraM[(Option ∘ M)#λ, CopK[F ::: LL, ?], Fix[EX]] = {
            case I(fa) => F.handleOps3_6[EX, M](v).apply(fa)
            case other => LL.materialize(offset + 1).handleOps3_6[EX, M](v).apply(other.asInstanceOf[CopK[LL, Fix[EX]]])
          }
        }
      }
    }
  }

  def handle3_2[F[_]: FuncHandler, M[_]: Monad: MonadFsErr: ExecTimeR]
    (v: BsonVersion)
      : AlgebraM[M, F, Fix[Expr3_2]] =
    FuncHandler[F].handle3_2[Expr3_2, M](v)

  def handle3_4[F[_]: FuncHandler, M[_]: Monad: MonadFsErr: ExecTimeR]
    (v: BsonVersion)
      : AlgebraM[M, F, Fix[Expr3_4]] =
    FuncHandler[F].handle3_4[Expr3_4, M](v)

  def handle3_4_4[F[_]: FuncHandler, M[_]: Monad: MonadFsErr: ExecTimeR]
    (v: BsonVersion)
      : AlgebraM[M, F, Fix[Expr3_4_4]] =
    FuncHandler[F].handle3_4_4[Expr3_4_4, M](v)

  def handle3_6[F[_]: FuncHandler, M[_]: Monad: MonadFsErr: ExecTimeR]
    (v: BsonVersion)
      : AlgebraM[M, F, Fix[Expr3_6]] =
    FuncHandler[F].handle3_6[Expr3_6, M](v)
}
