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

import slamdata.Predef.{Map => _, _}
import quasar._, Planner._, Type.{Const => _, Coproduct => _, _}
import quasar.common.{PhaseResult, PhaseResults, PhaseResultTell, SortDir}
import quasar.connector.BackendModule
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.{ADir, AFile}
import quasar.contrib.scalaz._, eitherT._
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski._
import quasar.fs.{FileSystemError, MonadFsErr}, FileSystemError.qscriptPlanningFailed
import quasar.javascript._
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.namegen._
import quasar.physical.mongodb.WorkflowBuilder.{Subset => _, _}
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner._
import quasar.physical.mongodb.planner.common._
import quasar.physical.mongodb.workflow.{ExcludeId => _, IncludeId => _, _}
import quasar.qscript.{Coalesce => _, _}
import quasar.std.StdLib._ // TODO: remove this

import java.time.Instant
import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import org.bson.BsonDocument
import scalaz._, Scalaz.{ToIdOps => _, _}

// TODO: This is generalizable to an arbitrary `Recursive` type, I think.
sealed abstract class InputFinder[T[_[_]]] {
  def apply[A](t: FreeMap[T]): FreeMap[T]
}

final case class Here[T[_[_]]]() extends InputFinder[T] {
  def apply[A](a: FreeMap[T]): FreeMap[T] = a
}

final case class There[T[_[_]]](index: Int, next: InputFinder[T])
    extends InputFinder[T] {
  def apply[A](a: FreeMap[T]): FreeMap[T] =
    a.resume.fold(fa => next(fa.toList.apply(index)), κ(a))
}

object MongoDbPlanner {
  import fixExprOp._

  type Partial[T[_[_]], In, Out] = (PartialFunction[List[In], Out], List[InputFinder[T]])

  type OutputM[A]      = PlannerError \/ A
  type ExecTimeR[F[_]] = MonadReader_[F, Instant]

  implicit def mongoQScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[fs.MongoQScript[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])


  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def generateTypeCheck[In, Out](or: (Out, Out) => Out)(f: PartialFunction[Type, In => Out]):
      Type => Option[In => Out] =
        typ => f.lift(typ).fold(
          typ match {
            case Type.Interval => generateTypeCheck(or)(f)(Type.Dec)
            case Type.Arr(_) => generateTypeCheck(or)(f)(Type.AnyArray)
            case Type.Timestamp
               | Type.Timestamp ⨿ Type.Date
               | Type.Timestamp ⨿ Type.Date ⨿ Type.Time =>
              generateTypeCheck(or)(f)(Type.Date)
            case Type.Timestamp ⨿ Type.Date ⨿ Type.Time ⨿ Type.Interval =>
              // Just repartition to match the right cases
              generateTypeCheck(or)(f)(Type.Interval ⨿ Type.Date)
            case Type.Int ⨿ Type.Dec ⨿ Type.Interval ⨿ Type.Str ⨿ (Type.Timestamp ⨿ Type.Date ⨿ Type.Time) ⨿ Type.Bool =>
              // Just repartition to match the right cases
              generateTypeCheck(or)(f)(
                Type.Int ⨿ Type.Dec ⨿ Type.Interval ⨿ Type.Str ⨿ (Type.Date ⨿ Type.Bool))
            case a ⨿ b =>
              (generateTypeCheck(or)(f)(a) ⊛ generateTypeCheck(or)(f)(b))(
                (a, b) => ((expr: In) => or(a(expr), b(expr))))
            case _ => None
          })(
          Some(_))

  def processMapFuncExpr
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: ExecTimeR: MonadFsErr, EX[_]: Traverse, A]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?], staticHandler: StaticHandler[T, EX])
    (fm: FreeMapA[T, A])
    (recovery: A => Fix[ExprOp])
    (implicit inj: EX :<: ExprOp)
      : M[Fix[ExprOp]] = {

    val alg: AlgebraM[M, CoEnvMapA[T, A, ?], Fix[ExprOp]] =
      interpretM[M, MapFunc[T, ?], A, Fix[ExprOp]](
        recovery(_).point[M],
        expression(funcHandler))

    def convert(e: EX[FreeMapA[T, A]]): M[Fix[ExprOp]] =
      inj(e.map(_.cataM(alg))).sequence.map(_.embed)

    staticHandler.handle(fm).map(convert) getOrElse fm.cataM(alg)
  }

  def getSelector
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr, EX[_]: Traverse]
    (fm: FreeMap[T], galg: GAlgebra[(T[MapFunc[T, ?]], ?), MapFunc[T, ?], OutputM[PartialSelector[T]]])
    (implicit inj: EX :<: ExprOp)
      : OutputM[PartialSelector[T]] =
    fm.zygo(
      interpret[MapFunc[T, ?], Hole, T[MapFunc[T, ?]]](
        κ(MFC(MapFuncsCore.Undefined[T, T[MapFunc[T, ?]]]()).embed),
        _.embed),
      ginterpret[(T[MapFunc[T, ?]], ?), MapFunc[T, ?], Hole, OutputM[PartialSelector[T]]](
        κ(defaultSelector[T].point[OutputM]), galg))

  def processMapFunc[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr: ExecTimeR, A]
    (fm: FreeMapA[T, A])(recovery: A => JsCore)
      : M[JsCore] =
    fm.cataM(interpretM[M, MapFunc[T, ?], A, JsCore](recovery(_).point[M], javascript))

  // FIXME: This is temporary. Should go away when the connector is complete.
  def unimplemented[M[_]: MonadFsErr, A](label: String): M[A] =
    raiseErr(qscriptPlanningFailed(InternalError.fromMsg(s"unimplemented $label")))

  // TODO: Should have a JsFn version of this for $reduce nodes.
  val accumulator: ReduceFunc[Fix[ExprOp]] => AccumOp[Fix[ExprOp]] = {
    import quasar.qscript.ReduceFuncs._

    {
      case Arbitrary(a)     => $first(a)
      case First(a)         => $first(a)
      case Last(a)          => $last(a)
      case Avg(a)           => $avg(a)
      case Count(_)         => $sum($literal(Bson.Int32(1)))
      case Max(a)           => $max(a)
      case Min(a)           => $min(a)
      case Sum(a)           => $sum(a)
      case UnshiftArray(a)  => $push(a)
      case UnshiftMap(k, v) => ???
    }
  }

  private def unpack[T[_[_]]: BirecursiveT, F[_]: Traverse](t: Free[F, T[F]]): T[F] =
    t.cata(interpret[F, T[F], T[F]](ι, _.embed))

  // NB: it's only safe to emit "core" expr ops here, but we always use the
  // largest type in WorkflowOp, so they're immediately injected into ExprOp.
  val check = new Check[Fix[ExprOp], ExprOp]

  def ejsonToExpression[M[_]: Applicative: MonadFsErr, EJ]
    (v: BsonVersion)(ej: EJ)(implicit EJ: Recursive.Aux[EJ, EJson])
      : M[Fix[ExprOp]] =
    ej.cataM(BsonCodec.fromEJson(v)).fold(pe => raiseErr(qscriptPlanningFailed(pe)), $literal(_).point[M])

  // TODO: Use `JsonCodec.encode` and avoid failing.
  def ejsonToJs[M[_]: Applicative: MonadFsErr, EJ: Show]
    (ej: EJ)(implicit EJ: Recursive.Aux[EJ, EJson])
      : M[JsCore] =
    ej.cata(Data.fromEJson).toJs.fold(
      raiseErr[M, JsCore](qscriptPlanningFailed(NonRepresentableEJson(ej.shows))))(
      _.point[M])

  def expression[
    T[_[_]]: RecursiveT: ShowT,
    M[_]: Monad: ExecTimeR: MonadFsErr,
    EX[_]: Traverse](funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?])(
    implicit inj: EX :<: ExprOp
  ): AlgebraM[M, MapFunc[T, ?], Fix[ExprOp]] = {

    import MapFuncsCore._
    import MapFuncsDerived._

    def handleCommon(mf: MapFunc[T, Fix[ExprOp]]): Option[Fix[ExprOp]] =
      funcHandler(mf).map(t => unpack(t.mapSuspension(inj)))

    def execTime(implicit ev: ExecTimeR[M]): M[Bson.Date] =
      OptionT[M, Bson.Date](ev.ask.map(Bson.Date.fromInstant(_)))
        .getOrElseF(raiseErr(
          qscriptPlanningFailed(InternalError.fromMsg("Could not get the current timestamp"))))


    val handleSpecialCore: MapFuncCore[T, Fix[ExprOp]] => M[Fix[ExprOp]] = {
      case Constant(v1) => unimplemented[M, Fix[ExprOp]]("Constant expression")
      case Now() => execTime map ($literal(_))

      case Date(a1) => unimplemented[M, Fix[ExprOp]]("Date expression")
      case Time(a1) => unimplemented[M, Fix[ExprOp]]("Time expression")
      case Timestamp(a1) => unimplemented[M, Fix[ExprOp]]("Timestamp expression")
      case Interval(a1) => unimplemented[M, Fix[ExprOp]]("Interval expression")
      case StartOfDay(a1) => unimplemented[M, Fix[ExprOp]]("StartOfDay expression")
      case TemporalTrunc(a1, a2) => unimplemented[M, Fix[ExprOp]]("TemporalTrunc expression")

      case IfUndefined(a1, a2) => unimplemented[M, Fix[ExprOp]]("IfUndefined expression")

      case Within(a1, a2) => unimplemented[M, Fix[ExprOp]]("Within expression")

      case ExtractIsoYear(a1) =>
        unimplemented[M, Fix[ExprOp]]("ExtractIsoYear expression")
      case Integer(a1) => unimplemented[M, Fix[ExprOp]]("Integer expression")
      case Decimal(a1) => unimplemented[M, Fix[ExprOp]]("Decimal expression")
      case ToString(a1) => unimplemented[M, Fix[ExprOp]]("ToString expression")

      case MakeArray(a1) => unimplemented[M, Fix[ExprOp]]("MakeArray expression")
      case MakeMap(a1, a2) => unimplemented[M, Fix[ExprOp]]("MakeMap expression")
      case ConcatMaps(a1, a2) => unimplemented[M, Fix[ExprOp]]("ConcatMap expression")
      case ProjectField($var(dv), $literal(Bson.Text(field))) =>
        $var(dv \ BsonField.Name(field)).point[M]
      case ProjectField(a1, a2) => unimplemented[M, Fix[ExprOp]](s"ProjectField expression")
      case ProjectIndex(a1, a2)  => unimplemented[M, Fix[ExprOp]]("ProjectIndex expression")
      case DeleteField(a1, a2)  => unimplemented[M, Fix[ExprOp]]("DeleteField expression")

      // NB: Quasar strings are arrays of characters. However, MongoDB
      //     represent strings and arrays as distinct types. Moreoever, SQL^2
      //     exposes two functions: `array_length` to obtain the length of an
      //     array and `length` to obtain the length of a string. This
      //     distinction, however, is lost when LP is translated into
      //     QScript. There's only one `Length` MapFunc. The workaround here
      //     detects calls to array_length or length indirectly through the
      //     typechecks inserted around calls to `Length` or `ArrayLength` in
      //     LP typechecks.

      case Length(a1) => unimplemented[M, Fix[ExprOp]]("Length expression")
      case Guard(expr, Type.Str, cont @ $strLenCP(_), fallback) =>
        $cond(check.isString(expr), cont, fallback).point[M]
      case Guard(expr, Type.FlexArr(_, _, _), $strLenCP(str), fallback) =>
        $cond(check.isArray(expr), $size(str), fallback).point[M]

      // NB: This is maybe a NOP for Fix[ExprOp]s, as they (all?) safely
      //     short-circuit when given the wrong type. However, our guards may be
      //     more restrictive than the operation, in which case we still want to
      //     short-circuit, so …
      case Guard(expr, typ, cont, fallback) =>
        // NB: Even if certain checks aren’t needed by ExprOps, we have to
        //     maintain them because we may convert ExprOps to JS.
        //     Hopefully BlackShield will eliminate the need for this.
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def exprCheck: Type => Option[Fix[ExprOp] => Fix[ExprOp]] =
          generateTypeCheck[Fix[ExprOp], Fix[ExprOp]]($or(_, _)) {
            case Type.Null => check.isNull
            case Type.Int
               | Type.Dec
               | Type.Int ⨿ Type.Dec
               | Type.Int ⨿ Type.Dec ⨿ Type.Interval => check.isNumber
            case Type.Str => check.isString
            case Type.Obj(map, _) =>
              ((expr: Fix[ExprOp]) => {
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
            case Type.Date => check.isDateOrTimestamp // FIXME: use isDate here when >= 3.0
            // NB: Some explicit coproducts for adjacent types.
            case Type.Int ⨿ Type.Dec ⨿ Type.Str => check.isNumberOrString
            case Type.Int ⨿ Type.Dec ⨿ Type.Interval ⨿ Type.Str => check.isNumberOrString
            case Type.Date ⨿ Type.Bool => check.isDateTimestampOrBoolean
            case Type.Syntaxed => check.isSyntaxed
          }
        exprCheck(typ).fold(cont)(f => $cond(f(expr), cont, fallback)).point[M]

      case Range(_, _)     => unimplemented[M, Fix[ExprOp]]("Range expression")
      case Search(_, _, _) => unimplemented[M, Fix[ExprOp]]("Search expression")
      case Split(_, _)     => unimplemented[M, Fix[ExprOp]]("Split expression")
    }

    val handleSpecialDerived: MapFuncDerived[T, Fix[ExprOp]] => M[Fix[ExprOp]] = {
      case Abs(a1) => unimplemented[M, Fix[ExprOp]]("Abs expression")
      case Ceil(a1) => unimplemented[M, Fix[ExprOp]]("Ceil expression")
      case Floor(a1) => unimplemented[M, Fix[ExprOp]]("Floor expression")
      case Trunc(a1) => unimplemented[M, Fix[ExprOp]]("Trunc expression")
      case Round(a1) => unimplemented[M, Fix[ExprOp]]("Round expression")
      case FloorScale(a1, a2) => unimplemented[M, Fix[ExprOp]]("FloorScale expression")
      case CeilScale(a1, a2) => unimplemented[M, Fix[ExprOp]]("CeilScale expression")
      case RoundScale(a1, a2) => unimplemented[M, Fix[ExprOp]]("RoundScale expression")
    }

    val handleSpecial: MapFunc[T, Fix[ExprOp]] => M[Fix[ExprOp]] = {
      case MFC(mfc) => handleSpecialCore(mfc)
      case MFD(mfd) => handleSpecialDerived(mfd)
    }

    mf => handleCommon(mf).cata(_.point[M], handleSpecial(mf))
  }

  def javascript[T[_[_]]: BirecursiveT: ShowT, M[_]: Applicative: MonadFsErr: ExecTimeR]
      : AlgebraM[M, MapFunc[T, ?], JsCore] = {
    import jscore.{
      Add => _, In => _,
      Lt => _, Lte => _, Gt => _, Gte => _, Eq => _, Neq => _,
      And => _, Or => _, Not => _,
      _}

    import MapFuncsCore._
    import MapFuncsDerived._

    val mjs = quasar.physical.mongodb.javascript[JsCore](_.embed)
    import mjs._

    // NB: Math.trunc is not present in MongoDB.
    def trunc(expr: JsCore): JsCore =
      Let(Name("x"), expr,
        BinOp(jscore.Sub,
          ident("x"),
          BinOp(jscore.Mod, ident("x"), Literal(Js.Num(1, false)))))

    def execTime(implicit ev: ExecTimeR[M]): M[JsCore] =
      ev.ask map (ts => Literal(Js.Str(ts.toString)))

    def handleCommon(mf: MapFunc[T, JsCore]): Option[JsCore] =
      JsFuncHandler.handle[MapFunc[T, ?]].apply(mf).map(unpack[Fix, JsCoreF])

    val handleSpecialCore: MapFuncCore[T, JsCore] => M[JsCore] = {
      case Constant(v1) => ejsonToJs[M, T[EJson]](v1)
      case Undefined() => ident("undefined").point[M]
      case JoinSideName(n) =>
        raiseErr[M, JsCore](qscriptPlanningFailed(UnexpectedJoinSide(n)))
      case Now() => execTime map (ts => New(Name("ISODate"), List(ts)))
      case Length(a1) =>
        Call(ident("NumberLong"), List(Select(a1, "length"))).point[M]
      case Date(a1) =>
        If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.dateRegex + "$")))), "test"), List(a1)),
          Call(ident("ISODate"), List(a1)),
          ident("undefined")).point[M]
      case Time(a1) =>
        If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.timeRegex + "$")))), "test"), List(a1)),
          a1,
          ident("undefined")).point[M]
      case Timestamp(a1) =>
        If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.timestampRegex + "$")))), "test"), List(a1)),
          Call(ident("ISODate"), List(a1)),
          ident("undefined")).point[M]
      case Interval(a1) => unimplemented[M, JsCore]("Interval JS")
      case TimeOfDay(a1) => {
        def pad2(x: JsCore) =
          Let(Name("x"), x,
            If(
              BinOp(jscore.Lt, ident("x"), Literal(Js.Num(10, false))),
              BinOp(jscore.Add, Literal(Js.Str("0")), ident("x")),
              ident("x")))
        def pad3(x: JsCore) =
          Let(Name("x"), x,
            If(
              BinOp(jscore.Lt, ident("x"), Literal(Js.Num(100, false))),
              BinOp(jscore.Add, Literal(Js.Str("00")), ident("x")),
              If(
                BinOp(jscore.Lt, ident("x"), Literal(Js.Num(10, false))),
                BinOp(jscore.Add, Literal(Js.Str("0")), ident("x")),
                ident("x"))))
        Let(Name("t"), a1,
          binop(jscore.Add,
            pad2(Call(Select(ident("t"), "getUTCHours"), Nil)),
            Literal(Js.Str(":")),
            pad2(Call(Select(ident("t"), "getUTCMinutes"), Nil)),
            Literal(Js.Str(":")),
            pad2(Call(Select(ident("t"), "getUTCSeconds"), Nil)),
            Literal(Js.Str(".")),
            pad3(Call(Select(ident("t"), "getUTCMilliseconds"), Nil)))).point[M]
      }
      case ToTimestamp(a1) => New(Name("Date"), List(a1)).point[M]

      case ExtractCentury(date) =>
        Call(ident("NumberLong"), List(
          Call(Select(ident("Math"), "ceil"), List(
            BinOp(jscore.Div,
              Call(Select(date, "getUTCFullYear"), Nil),
              Literal(Js.Num(100, false))))))).point[M]
      case ExtractDayOfMonth(date) => Call(Select(date, "getUTCDate"), Nil).point[M]
      case ExtractDecade(date) =>
        Call(ident("NumberLong"), List(
          trunc(
            BinOp(jscore.Div,
              Call(Select(date, "getUTCFullYear"), Nil),
              Literal(Js.Num(10, false)))))).point[M]
      case ExtractDayOfWeek(date) =>
        Call(Select(date, "getUTCDay"), Nil).point[M]
      case ExtractDayOfYear(date) =>
        Call(ident("NumberInt"), List(
          Call(Select(ident("Math"), "floor"), List(
            BinOp(jscore.Add,
              BinOp(jscore.Div,
                BinOp(Sub,
                  date,
                  New(Name("Date"), List(
                    Call(Select(date, "getFullYear"), Nil),
                    Literal(Js.Num(0, false)),
                    Literal(Js.Num(0, false))))),
                Literal(Js.Num(86400000, false))),
              Literal(Js.Num(1, false))))))).point[M]
      case ExtractEpoch(date) =>
        Call(ident("NumberLong"), List(
          BinOp(jscore.Div,
            Call(Select(date, "valueOf"), Nil),
            Literal(Js.Num(1000, false))))).point[M]
      case ExtractHour(date) => Call(Select(date, "getUTCHours"), Nil).point[M]
      case ExtractIsoDayOfWeek(date) =>
        Let(Name("x"), Call(Select(date, "getUTCDay"), Nil),
          If(
            BinOp(jscore.Eq, ident("x"), Literal(Js.Num(0, false))),
            Literal(Js.Num(7, false)),
            ident("x"))).point[M]
      case ExtractIsoYear(date) =>
        Call(Select(date, "getUTCFullYear"), Nil).point[M]
      case ExtractMicroseconds(date) =>
        BinOp(jscore.Mult,
          BinOp(jscore.Add,
            Call(Select(date, "getUTCMilliseconds"), Nil),
            BinOp(jscore.Mult,
              Call(Select(date, "getUTCSeconds"), Nil),
              Literal(Js.Num(1000, false)))),
          Literal(Js.Num(1000, false))).point[M]
      case ExtractMillennium(date) =>
        Call(ident("NumberLong"), List(
          Call(Select(ident("Math"), "ceil"), List(
            BinOp(jscore.Div,
              Call(Select(date, "getUTCFullYear"), Nil),
              Literal(Js.Num(1000, false))))))).point[M]
      case ExtractMilliseconds(date) =>
        BinOp(jscore.Add,
          Call(Select(date, "getUTCMilliseconds"), Nil),
          BinOp(jscore.Mult,
            Call(Select(date, "getUTCSeconds"), Nil),
            Literal(Js.Num(1000, false)))).point[M]
      case ExtractMinute(date) =>
        Call(Select(date, "getUTCMinutes"), Nil).point[M]
      case ExtractMonth(date) =>
        BinOp(jscore.Add,
          Call(Select(date, "getUTCMonth"), Nil),
          Literal(Js.Num(1, false))).point[M]
      case ExtractQuarter(date) =>
        Call(ident("NumberInt"), List(
          BinOp(jscore.Add,
            BinOp(jscore.BitOr,
              BinOp(jscore.Div,
                Call(Select(date, "getUTCMonth"), Nil),
                Literal(Js.Num(3, false))),
              Literal(Js.Num(0, false))),
            Literal(Js.Num(1, false))))).point[M]
      case ExtractSecond(date) =>
        BinOp(jscore.Add,
          Call(Select(date, "getUTCSeconds"), Nil),
          BinOp(jscore.Div,
            Call(Select(date, "getUTCMilliseconds"), Nil),
            Literal(Js.Num(1000, false)))).point[M]
      case ExtractWeek(date) =>
        Call(ident("NumberInt"), List(
          Call(Select(ident("Math"), "floor"), List(
            BinOp(jscore.Add,
              BinOp(jscore.Div,
                Let(Name("startOfYear"),
                  New(Name("Date"), List(
                    Call(Select(date, "getFullYear"), Nil),
                    Literal(Js.Num(0, false)),
                    Literal(Js.Num(1, false)))),
                  BinOp(jscore.Add,
                    BinOp(Div,
                      BinOp(Sub, date, ident("startOfYear")),
                      Literal(Js.Num(86400000, false))),
                    BinOp(jscore.Add,
                      Call(Select(ident("startOfYear"), "getDay"), Nil),
                      Literal(Js.Num(1, false))))),
                Literal(Js.Num(7, false))),
              Literal(Js.Num(1, false))))))).point[M]

      case ExtractYear(date) => Call(Select(date, "getUTCFullYear"), Nil).point[M]

      case Negate(a1)       => UnOp(Neg, a1).point[M]
      case Add(a1, a2)      => BinOp(jscore.Add, a1, a2).point[M]
      case Multiply(a1, a2) => BinOp(Mult, a1, a2).point[M]
      case Subtract(a1, a2) => BinOp(Sub, a1, a2).point[M]
      case Divide(a1, a2)   => BinOp(Div, a1, a2).point[M]
      case Modulo(a1, a2)   => BinOp(Mod, a1, a2).point[M]
      case Power(a1, a2)    => Call(Select(ident("Math"), "pow"), List(a1, a2)).point[M]

      case Not(a1)     => UnOp(jscore.Not, a1).point[M]
      case Eq(a1, a2)  => BinOp(jscore.Eq, a1, a2).point[M]
      case Neq(a1, a2) => BinOp(jscore.Neq, a1, a2).point[M]
      case Lt(a1, a2)  => BinOp(jscore.Lt, a1, a2).point[M]
      case Lte(a1, a2) => BinOp(jscore.Lte, a1, a2).point[M]
      case Gt(a1, a2)  => BinOp(jscore.Gt, a1, a2).point[M]
      case Gte(a1, a2) => BinOp(jscore.Gte, a1, a2).point[M]
      case IfUndefined(a1, a2) =>
        // TODO: Only evaluate `value` once.
        If(BinOp(jscore.Eq, a1, ident("undefined")), a2, a1).point[M]
      case And(a1, a2) => BinOp(jscore.And, a1, a2).point[M]
      case Or(a1, a2)  => BinOp(jscore.Or, a1, a2).point[M]
      case Between(a1, a2, a3) =>
        Call(ident("&&"), List(
          Call(ident("<="), List(a2, a1)),
          Call(ident("<="), List(a1, a3)))).point[M]
      case Cond(a1, a2, a3) => If(a1, a2, a3).point[M]

      case Within(a1, a2) =>
        BinOp(jscore.Neq,
          Literal(Js.Num(-1, false)),
          Call(Select(a2, "indexOf"), List(a1))).point[M]

      // TODO: move these to JsFuncHandler
      case Lower(a1) => Call(Select(a1, "toLowerCase"), Nil).point[M]
      case Upper(a1) => Call(Select(a1, "toUpperCase"), Nil).point[M]
      case Bool(a1) =>
        If(BinOp(jscore.Eq, a1, Literal(Js.Str("true"))),
          Literal(Js.Bool(true)),
          If(BinOp(jscore.Eq, a1, Literal(Js.Str("false"))),
            Literal(Js.Bool(false)),
            ident("undefined"))).point[M]
      case Integer(a1) =>
        If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.intRegex + "$")))), "test"), List(a1)),
          Call(ident("NumberLong"), List(a1)),
          ident("undefined")).point[M]
      case Decimal(a1) =>
        If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.floatRegex + "$")))), "test"), List(a1)),
          Call(ident("parseFloat"), List(a1)),
          ident("undefined")).point[M]
      case Null(a1) =>
        If(BinOp(jscore.Eq, a1, Literal(Js.Str("null"))),
          Literal(Js.Null),
          ident("undefined")).point[M]
      case ToString(a1) =>
        If(isInt(a1),
          // NB: This is a terrible way to turn an int into a string, but the
          //     only one that doesn’t involve converting to a decimal and
          //     losing precision.
          Call(Select(Call(ident("String"), List(a1)), "replace"), List(
            Call(ident("RegExp"), List(
              Literal(Js.Str("[^-0-9]+")),
              Literal(Js.Str("g")))),
            Literal(Js.Str("")))),
          If(binop(jscore.Or, isTimestamp(a1), isDate(a1)),
            Call(Select(a1, "toISOString"), Nil),
            Call(ident("String"), List(a1)))).point[M]
      case Search(a1, a2, a3) =>
        Call(
          Select(
            New(Name("RegExp"), List(
              a2,
              If(a3, Literal(Js.Str("im")), Literal(Js.Str("m"))))),
            "test"),
          List(a1)).point[M]
      case Substring(a1, a2, a3) =>
        Call(Select(a1, "substr"), List(a2, a3)).point[M]
      case Split(a1, a2) =>
        Call(Select(a1, "split"), List(a2)).point[M]

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
      case ProjectField(a1, a2) => Access(a1, a2).point[M]
      case ProjectIndex(a1, a2) => Access(a1, a2).point[M]
      case DeleteField(a1, a2)  => Call(ident("remove"), List(a1, a2)).point[M]

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
            case Type.Date             => isDate
          }
        jsCheck(typ).fold[M[JsCore]](
          raiseErr(qscriptPlanningFailed(InternalError.fromMsg("uncheckable type"))))(
          f => If(f(expr), cont, fallback).point[M])

      // FIXME: Doesn't work for Char.
      case Range(start, end)        =>
        Call(
          Select(
            Call(Select(ident("Array"), "apply"), List(
              Literal(Js.Null),
              Call(ident("Array"), List(BinOp(Sub, end, start))))),
            "map"),
          List(
            Fun(List(Name("element"), Name("index")),
              BinOp(jscore.Add, ident("index"), start)))).point[M]
    }

    val handleSpecialDerived: MapFuncDerived[T, JsCore] => M[JsCore] = {
      case Abs(a1)   => unimplemented[M, JsCore]("Abs JS")
      case Ceil(a1)  => unimplemented[M, JsCore]("Ceil JS")
      case Floor(a1) => unimplemented[M, JsCore]("Floor JS")
      case Trunc(a1) => unimplemented[M, JsCore]("Trunc JS")
      case Round(a1) => unimplemented[M, JsCore]("Round JS")
      case FloorScale(a1, a2) => unimplemented[M, JsCore]("FloorScale JS")
      case CeilScale(a1, a2) => unimplemented[M, JsCore]("CeilScale JS")
      case RoundScale(a1, a2) => unimplemented[M, JsCore]("RoundScale JS")
    }

    val handleSpecial: MapFunc[T, JsCore] => M[JsCore] = {
      case MFC(mfc) => handleSpecialCore(mfc)
      case MFD(mfd) => handleSpecialDerived(mfd)
    }

    mf => handleCommon(mf).cata(_.point[M], handleSpecial(mf))
  }

  // TODO: Need this until the old connector goes away and we can redefine
  //       `Selector` as `Selector[A, B]`, where `A` is the field type
  //       (naturally `BsonField`), and `B` is the recursive parameter.
  type PartialSelector[T[_[_]]] = Partial[T, BsonField, Selector]

  def defaultSelector[T[_[_]]]: PartialSelector[T] = (
    { case List(field) =>
      Selector.Doc(ListMap(
        field -> Selector.Expr(Selector.Eq(Bson.Bool(true)))))
    },
    List(Here[T]()))

  def typeSelector[T[_[_]]: RecursiveT: ShowT]:
      GAlgebra[(T[MapFunc[T, ?]], ?), MapFunc[T, ?], OutputM[PartialSelector[T]]] = {

    import MapFuncsCore._

    def invoke2Nel(x: OutputM[PartialSelector[T]], y: OutputM[PartialSelector[T]])(f: (Selector, Selector) => Selector):
        OutputM[PartialSelector[T]] =
      (x ⊛ y) { case ((f1, p1), (f2, p2)) =>
        ({ case list =>
          f(f1(list.take(p1.size)), f2(list.drop(p1.size)))
        },
          p1.map(There(0, _)) ++ p2.map(There(1, _)))
      }

    {
      case MFC(And(a, b)) => invoke2Nel(a._2, b._2)(Selector.And.apply(_, _))
      case MFC(Or(a, b)) => invoke2Nel(a._2, b._2)(Selector.Or.apply(_, _))

      case node @ MFC(Guard((Embed(MFC(ProjectField(Embed(MFC(Undefined())), _))), _), typ, cont, _)) =>
        def selCheck: Type => Option[BsonField => Selector] =
          generateTypeCheck[BsonField, Selector](Selector.Or(_, _)) {
            case Type.Null => ((f: BsonField) =>  Selector.Doc(f -> Selector.Type(BsonType.Null)))
            case Type.Dec => ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.Dec)))
            case Type.Int =>
              ((f: BsonField) => Selector.Or(
                Selector.Doc(f -> Selector.Type(BsonType.Int32)),
                Selector.Doc(f -> Selector.Type(BsonType.Int64))))
            case Type.Int ⨿ Type.Dec ⨿ Type.Interval =>
              ((f: BsonField) =>
                Selector.Or(
                  Selector.Doc(f -> Selector.Type(BsonType.Int32)),
                  Selector.Doc(f -> Selector.Type(BsonType.Int64)),
                  Selector.Doc(f -> Selector.Type(BsonType.Dec))))
            case Type.Str => ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.Text)))
            case Type.Obj(_, _) =>
              ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.Doc)))

            // NB: Selector.Type(BsonType.Arr) will not match arrays, instead we use the suggestion in Mongo docs
            // See: https://docs.mongodb.com/manual/reference/operator/query/type/#document-querying-by-array-type
            case Type.FlexArr(_, _, _) =>
              ((f: BsonField) => Selector.Doc(f -> Selector.ElemMatch(Selector.Exists(true).right)))
            case Type.Binary =>
              ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.Binary)))
            case Type.Id =>
              ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.ObjectId)))
            case Type.Bool => ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.Bool)))
            case Type.Date =>
              ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.Date)))
          }
        selCheck(typ).fold[OutputM[PartialSelector[T]]](
          -\/(InternalError.fromMsg(node.map(_._1).shows)))(
          f =>
          \/-(cont._2.fold[PartialSelector[T]](
            κ(({ case List(field) => f(field) }, List(There(0, Here[T]())))),
            { case (f2, p2) => ({ case head :: tail => Selector.And(f(head), f2(tail)) }, There(0, Here[T]()) :: p2.map(There(1, _)))
            })))

      case node @ _ => -\/(InternalError fromMsg node.map(_._1).shows)
    }
  }


  /** The selector phase tries to turn expressions into MongoDB selectors – i.e.
    * Mongo query expressions. Selectors are only used for the filtering
    * pipeline op, so it's quite possible we build more stuff than is needed
    * (but it doesn’t matter, unneeded annotations will be ignored by the
    * pipeline phase).
    *
    * Like the expression op phase, this one requires bson field annotations.
    *
    * Most expressions cannot be turned into selector expressions without using
    * the "\$where" operator, which allows embedding JavaScript
    * code. Unfortunately, using this operator turns filtering into a full table
    * scan. We should do a pass over the tree to identify partial boolean
    * expressions which can be turned into selectors, factoring out the
    * leftovers for conversion using \$where.
    */
  def selector[T[_[_]]: RecursiveT: ShowT](v: BsonVersion):
      GAlgebra[(T[MapFunc[T, ?]], ?), MapFunc[T, ?], OutputM[PartialSelector[T]]] = { node =>
    import MapFuncsCore._

    type Output = OutputM[PartialSelector[T]]

    object IsBson {
      def unapply(x: (T[MapFunc[T, ?]], Output)): Option[Bson] =
        x._1.project match {
          case MFC(Constant(b)) => b.cataM(BsonCodec.fromEJson(v)).toOption
          case _ => None
        }
    }

    object IsBool {
      def unapply(v: (T[MapFunc[T, ?]], Output)): Option[Boolean] =
        v match {
          case IsBson(Bson.Bool(b)) => b.some
          case _                    => None
        }
    }

    object IsText {
      def unapply(v: (T[MapFunc[T, ?]], Output)): Option[String] =
        v match {
          case IsBson(Bson.Text(str)) => Some(str)
          case _                      => None
        }
    }

    object IsDate {
      def unapply(v: (T[MapFunc[T, ?]], Output)): Option[Data.Date] =
        v._1.project match {
          case MFC(Constant(d @ Data.Date(_))) => Some(d)
          case _                               => None
        }
    }

    val relFunc: MapFunc[T, _] => Option[Bson => Selector.Condition] = {
      case MFC(Eq(_, _))  => Some(Selector.Eq)
      case MFC(Neq(_, _)) => Some(Selector.Neq)
      case MFC(Lt(_, _))  => Some(Selector.Lt)
      case MFC(Lte(_, _)) => Some(Selector.Lte)
      case MFC(Gt(_, _))  => Some(Selector.Gt)
      case MFC(Gte(_, _)) => Some(Selector.Gte)
      case _              => None
    }

    val default: PartialSelector[T] = defaultSelector[T]

    def invoke(func: MapFunc[T, (T[MapFunc[T, ?]], Output)]): Output = {
      /**
        * All the relational operators require a field as one parameter, and
        * BSON literal value as the other parameter. So we have to try to
        * extract out both a field annotation and a selector and then verify
        * the selector is actually a BSON literal value before we can
        * construct the relational operator selector. If this fails for any
        * reason, it just means the given expression cannot be represented
        * using MongoDB's query operators, and must instead be written as
        * Javascript using the "$where" operator.
        */
      def relop
        (x: (T[MapFunc[T, ?]], Output), y: (T[MapFunc[T, ?]], Output))
        (f: Bson => Selector.Condition, r: Bson => Selector.Condition):
          Output =
        (x, y) match {
          case (_, IsBson(v2)) =>
            \/-(({ case List(f1) => Selector.Doc(ListMap(f1 -> Selector.Expr(f(v2)))) }, List(There(0, Here[T]()))))
          case (IsBson(v1), _) =>
            \/-(({ case List(f2) => Selector.Doc(ListMap(f2 -> Selector.Expr(r(v1)))) }, List(There(1, Here[T]()))))

          case (_, _) => -\/(InternalError fromMsg node.map(_._1).shows)
        }

      def relDateOp1(f: Bson.Date => Selector.Condition, date: Data.Date, g: Data.Date => Data.Timestamp, index: Int): Output =
        Bson.Date.fromInstant(g(date).value).fold[Output](
          -\/(NonRepresentableData(g(date))))(
          d => \/-((
            { case x :: Nil => Selector.Doc(x -> f(d)) },
            List(There(index, Here[T]())))))

      def relDateOp2(conj: (Selector, Selector) => Selector, f1: Bson.Date => Selector.Condition, f2: Bson.Date => Selector.Condition, date: Data.Date, g1: Data.Date => Data.Timestamp, g2: Data.Date => Data.Timestamp, index: Int): Output =
        ((Bson.Date.fromInstant(g1(date).value) \/> NonRepresentableData(g1(date))) ⊛
          (Bson.Date.fromInstant(g2(date).value) \/> NonRepresentableData(g2(date))))((d1, d2) =>
          (
            { case x :: Nil =>
              conj(
                Selector.Doc(x -> f1(d1)),
                Selector.Doc(x -> f2(d2)))
            },
            List(There(index, Here[T]()))))

      def invoke2Nel(x: Output, y: Output)(f: (Selector, Selector) => Selector):
          Output =
        (x ⊛ y) { case ((f1, p1), (f2, p2)) =>
          ({ case list =>
            f(f1(list.take(p1.size)), f2(list.drop(p1.size)))
          },
            p1.map(There(0, _)) ++ p2.map(There(1, _)))
        }

      val flipCore: MapFuncCore[T, _] => Option[MapFuncCore[T, _]] = {
        case Eq(a, b)  => Some(Eq(a, b))
        case Neq(a, b) => Some(Neq(a, b))
        case Lt(a, b)  => Some(Gt(a, b))
        case Lte(a, b) => Some(Gte(a, b))
        case Gt(a, b)  => Some(Lt(a, b))
        case Gte(a, b) => Some(Lte(a, b))
        case And(a, b) => Some(And(a, b))
        case Or(a, b)  => Some(Or(a, b))
        case _         => None
      }

      val flip: MapFunc[T, _] => Option[MapFunc[T, _]] = {
        case MFC(mfc) => flipCore(mfc).map(MFC(_))
        case _ => None
      }

      def reversibleRelop(x: (T[MapFunc[T, ?]], Output), y: (T[MapFunc[T, ?]], Output))(f: MapFunc[T, _]): Output =
        (relFunc(f) ⊛ flip(f).flatMap(relFunc))(relop(x, y)(_, _)).getOrElse(-\/(InternalError fromMsg "couldn’t decipher operation"))

      func match {
        case MFC(Constant(_))        => \/-(default)

        case MFC(Gt(_, IsDate(d2)))  => relDateOp1(Selector.Gte, d2, date.startOfNextDay, 0)
        case MFC(Lt(IsDate(d1), _))  => relDateOp1(Selector.Gte, d1, date.startOfNextDay, 1)

        case MFC(Lt(_, IsDate(d2)))  => relDateOp1(Selector.Lt,  d2, date.startOfDay, 0)
        case MFC(Gt(IsDate(d1), _))  => relDateOp1(Selector.Lt,  d1, date.startOfDay, 1)

        case MFC(Gte(_, IsDate(d2))) => relDateOp1(Selector.Gte, d2, date.startOfDay, 0)
        case MFC(Lte(IsDate(d1), _)) => relDateOp1(Selector.Gte, d1, date.startOfDay, 1)

        case MFC(Lte(_, IsDate(d2))) => relDateOp1(Selector.Lt,  d2, date.startOfNextDay, 0)
        case MFC(Gte(IsDate(d1), _)) => relDateOp1(Selector.Lt,  d1, date.startOfNextDay, 1)

        case MFC(Eq(_, IsDate(d2))) => relDateOp2(Selector.And(_, _), Selector.Gte, Selector.Lt, d2, date.startOfDay, date.startOfNextDay, 0)
        case MFC(Eq(IsDate(d1), _)) => relDateOp2(Selector.And(_, _), Selector.Gte, Selector.Lt, d1, date.startOfDay, date.startOfNextDay, 1)

        case MFC(Neq(_, IsDate(d2))) => relDateOp2(Selector.Or(_, _), Selector.Lt, Selector.Gte, d2, date.startOfDay, date.startOfNextDay, 0)
        case MFC(Neq(IsDate(d1), _)) => relDateOp2(Selector.Or(_, _), Selector.Lt, Selector.Gte, d1, date.startOfDay, date.startOfNextDay, 1)

        case MFC(Eq(a, b))  => reversibleRelop(a, b)(func)
        case MFC(Neq(a, b)) => reversibleRelop(a, b)(func)
        case MFC(Lt(a, b))  => reversibleRelop(a, b)(func)
        case MFC(Lte(a, b)) => reversibleRelop(a, b)(func)
        case MFC(Gt(a, b))  => reversibleRelop(a, b)(func)
        case MFC(Gte(a, b)) => reversibleRelop(a, b)(func)

        case MFC(Within(a, b)) =>
          relop(a, b)(
            Selector.In.apply _,
            x => Selector.ElemMatch(\/-(Selector.In(Bson.Arr(List(x))))))

        case MFC(Search(_, IsText(patt), IsBool(b))) =>
          \/-(({ case List(f1) =>
            Selector.Doc(ListMap(f1 -> Selector.Expr(Selector.Regex(patt, b, true, false, false)))) },
            List(There(0, Here[T]()))))

        case MFC(Between(_, IsBson(lower), IsBson(upper))) =>
          \/-(({ case List(f) => Selector.And(
            Selector.Doc(f -> Selector.Gte(lower)),
            Selector.Doc(f -> Selector.Lte(upper)))
          },
            List(There(0, Here[T]()))))

        case MFC(And(a, b)) => invoke2Nel(a._2, b._2)(Selector.And.apply _)
        case MFC(Or(a, b)) => invoke2Nel(a._2, b._2)(Selector.Or.apply _)
        case MFC(Not((_, v))) =>
          v.map { case (sel, inputs) => (sel andThen (_.negate), inputs.map(There(0, _))) }

        case MFC(Guard(_, typ, (_, cont), _)) => cont.map { case (sel, inputs) => (sel, inputs.map(There(1, _))) }

        case _ => -\/(InternalError fromMsg node.map(_._1).shows)
      }
    }

    invoke(node) <+> \/-(default)
  }

  /** Brings a [[WBM]] into our `M`. */
  def liftM[M[_]: Monad: MonadFsErr, A](meh: WBM[A]): M[A] =
    meh.fold(
      e => raiseErr(qscriptPlanningFailed(e)),
      _.point[M])

  def createFieldName(prefix: String, i: Int): String = prefix + i.toString

  trait Planner[F[_]] {
    type IT[G[_]]

    def plan
      [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse]
      (cfg: PlannerConfig[IT, EX, WF])
      (implicit
        ev0: WorkflowOpCoreF :<: WF,
        ev1: RenderTree[WorkflowBuilder[WF]],
        ev2: WorkflowBuilder.Ops[WF],
        ev3: EX :<: ExprOp):
        AlgebraM[M, F, WorkflowBuilder[WF]]
  }

  object Planner {
    type Aux[T[_[_]], F[_]] = Planner[F] { type IT[G[_]] = T[G] }

    def apply[T[_[_]], F[_]](implicit ev: Planner.Aux[T, F]) = ev

    implicit def shiftedReadFile[T[_[_]]: BirecursiveT: ShowT]: Planner.Aux[T, Const[ShiftedRead[AFile], ?]] =
      new Planner[Const[ShiftedRead[AFile], ?]] {
        type IT[G[_]] = T[G]
        def plan
          [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse]
          (cfg: PlannerConfig[T, EX, WF])
          (implicit
            ev0: WorkflowOpCoreF :<: WF,
            ev1: RenderTree[WorkflowBuilder[WF]],
            WB: WorkflowBuilder.Ops[WF],
            ev3: EX :<: ExprOp) =
          qs => Collection
            .fromFile(qs.getConst.path)
            .fold(
              e => raiseErr(qscriptPlanningFailed(PlanPathError(e))),
              coll => {
                val dataset = WB.read(coll)
                // TODO: exclude `_id` from the value here?
                qs.getConst.idStatus match {
                  case IdOnly    =>
                    getExprBuilder[T, M, WF, EX](
                      cfg.funcHandler, cfg.staticHandler)(
                      dataset,
                        Free.roll(MFC(MapFuncsCore.ProjectField[T, FreeMap[T]](HoleF[T], MapFuncsCore.StrLit("_id")))))
                  case IncludeId =>
                    getExprBuilder[T, M, WF, EX](
                      cfg.funcHandler, cfg.staticHandler)(
                      dataset,
                        MapFuncCore.StaticArray(List(
                          Free.roll(MFC(MapFuncsCore.ProjectField[T, FreeMap[T]](HoleF[T], MapFuncsCore.StrLit("_id")))),
                          HoleF)))
                  case ExcludeId => dataset.point[M]
                }
              })
      }

    implicit def qscriptCore[T[_[_]]: BirecursiveT: EqualT: ShowT]:
        Planner.Aux[T, QScriptCore[T, ?]] =
      new Planner[QScriptCore[T, ?]] {
        type IT[G[_]] = T[G]

        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def plan
          [M[_]: Monad: ExecTimeR: MonadFsErr,
            WF[_]: Functor: Coalesce: Crush: Crystallize,
            EX[_]: Traverse]
          (cfg: PlannerConfig[T, EX, WF])
          (implicit
            ev0: WorkflowOpCoreF :<: WF,
            ev1: RenderTree[WorkflowBuilder[WF]],
            WB: WorkflowBuilder.Ops[WF],
            ev3: EX :<: ExprOp) = {
          case qscript.Map(src, f) =>
            getExprBuilder[T, M, WF, EX](cfg.funcHandler, cfg.staticHandler)(src, f)
          case LeftShift(src, struct, id, repair) =>
            if (repair.contains(LeftSideF))
              (handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, struct) ⊛
                getJsMerge[T, M](
                  repair,
                  jscore.Select(jscore.Ident(JsFn.defaultName), "s"),
                  jscore.Select(jscore.Ident(JsFn.defaultName), "f")))((expr, j) =>
              ExprBuilder(
                FlatteningBuilder(
                  DocBuilder(
                    src,
                    ListMap(
                      BsonField.Name("s") -> docVarToExpr(DocVar.ROOT()),
                      BsonField.Name("f") -> expr)),
                  // TODO: Handle arrays properly
                  Set(StructureType.Object(DocField(BsonField.Name("f")), id))),
                -\&/(j)))
            else
              getExprBuilder[T, M, WF, EX](cfg.funcHandler, cfg.staticHandler)(src, struct) >>= (builder =>
                getExprBuilder[T, M, WF, EX](
                  cfg.funcHandler, cfg.staticHandler)(
                  FlatteningBuilder(
                    builder,
                    Set(StructureType.Object(DocVar.ROOT(), id))),
                    repair.as(SrcHole)))
          case Reduce(src, bucket, reducers, repair) =>
            (bucket.traverse(handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, _)) ⊛
              reducers.traverse(_.traverse(handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, _))))((b, red) => {
                getReduceBuilder[T, M, WF, EX](
                  cfg.funcHandler, cfg.staticHandler)(
                  // TODO: This work should probably be done in `toWorkflow`.
                  semiAlignExpr[λ[α => List[ReduceFunc[α]]]](red)(Traverse[List].compose).fold(
                    WB.groupBy(
                      DocBuilder(
                        src,
                        // FIXME: Doesn’t work with UnshiftMap
                        red.unite.zipWithIndex.map(_.map(i => BsonField.Name(createFieldName("f", i))).swap).toListMap ++
                          b.zipWithIndex.map(_.map(i => BsonField.Name(createFieldName("b", i))).swap).toListMap),
                      b.zipWithIndex.map(p => docVarToExpr(DocField(BsonField.Name(createFieldName("b", p._2))))),
                      red.zipWithIndex.map(ai =>
                        (BsonField.Name(createFieldName("f", ai._2)),
                          accumulator(ai._1.as($field(createFieldName("f", ai._2)))))).toListMap))(
                    exprs => WB.groupBy(src,
                      b,
                      exprs.zipWithIndex.map(ai =>
                        (BsonField.Name(createFieldName("f", ai._2)),
                          accumulator(ai._1))).toListMap)),
                    repair)
              }).join
          case Sort(src, bucket, order) =>
            val (keys, dirs) = (bucket.toIList.map((_, SortDir.asc)) <::: order).unzip
            keys.traverse(handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, _))
              .map(ks => WB.sortBy(src, ks.toList, dirs.toList))
          case Filter(src, cond0) => {
            // TODO: Apply elideMoreGeneralGuards to all FreeMap's in the plan, not only here
            cond0.transCataM(assumeReadType.elideMoreGeneralGuards[M, T](Type.AnyObject)) >>= { cond =>
              val selectors = getSelector[T, M, EX](cond, selector[T](cfg.bsonVersion)).toOption
              val typeSelectors = getSelector[T, M, EX](cond, typeSelector[T]).toOption

              (selectors, typeSelectors) match {
                case (Some((_, Nil)), Some((typeSel, typeInputs))) =>
                  typeInputs.traverse(f => handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, f(cond)))
                    .map(WB.filter(src, _, typeSel))
                case (Some((sel, inputs)), Some((_, Nil))) =>
                  inputs.traverse(f => handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, f(cond)))
                    .map(WB.filter(src, _, sel))
                case (Some((sel, inputs)), Some((typeSel, typeInputs))) =>
                  typeInputs.traverse(f => handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, f(cond)))
                    .map(WB.filter(src, _, typeSel))
                    .flatMap(src0 =>
                      inputs.traverse(f => handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, f(cond)))
                        .map(WB.filter(src0, _, sel)))
                case (Some((sel, inputs)), None) =>
                  inputs.traverse(f => handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, f(cond)))
                    .map(WB.filter(src, _, sel))
                case _ =>
                  handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, cond).map {
                    // TODO: Postpone decision until we know whether we are going to
                    //       need mapReduce anyway.
                    case cond @ HasThat(_) => WB.filter(src, List(cond), {
                      case f :: Nil => Selector.Doc(f -> Selector.Eq(Bson.Bool(true)))
                    })
                    case \&/.This(js) => WB.filter(src, Nil, {
                      case Nil => Selector.Where(js(jscore.ident("this")).toJs)
                    })
                  }
              }
            }
          }
          case Union(src, lBranch, rBranch) =>
            (rebaseWB[T, M, WF, EX](cfg, lBranch, src) ⊛
              rebaseWB[T, M, WF, EX](cfg, rBranch, src))(
              UnionBuilder(_, _))
          case Subset(src, from, sel, count) =>
            (rebaseWB[T, M, WF, EX](cfg, from, src) ⊛
              (rebaseWB[T, M, WF, EX](cfg, count, src) >>= (HasInt[M, WF](_))))(
              sel match {
                case Drop => WB.skip
                case Take => WB.limit
                // TODO: Better sampling
                case Sample => WB.limit
              })
          case Unreferenced() =>
            CollectionBuilder($pure(Bson.Null), WorkflowBuilder.Root(), none).point[M]
        }
      }

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    implicit def equiJoin[T[_[_]]: BirecursiveT: EqualT: ShowT]:
        Planner.Aux[T, EquiJoin[T, ?]] =
      new Planner[EquiJoin[T, ?]] {
        type IT[G[_]] = T[G]
        def plan
          [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse]
          (cfg: PlannerConfig[T, EX, WF])
          (implicit
            ev0: WorkflowOpCoreF :<: WF,
            ev1: RenderTree[WorkflowBuilder[WF]],
            ev2: WorkflowBuilder.Ops[WF],
            ev3: EX :<: ExprOp) =
          qs =>
        (rebaseWB[T, M, WF, EX](cfg, qs.lBranch, qs.src) ⊛
          rebaseWB[T, M, WF, EX](cfg, qs.rBranch, qs.src))(
          (lb, rb) => {
            val (lKey, rKey) = Unzip[List].unzip(qs.key)

            (lKey.traverse(handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, _)) ⊛
              rKey.traverse(handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, _)))(
              (lk, rk) =>
              liftM[M, WorkflowBuilder[WF]](cfg.joinHandler.run(
                qs.f,
                JoinSource(lb, lk),
                JoinSource(rb, rk))) >>=
                (getExprBuilder[T, M, WF, EX](cfg.funcHandler, cfg.staticHandler)(_, qs.combine >>= {
                  case LeftSide => Free.roll(MFC(MapFuncsCore.ProjectField(HoleF, MapFuncsCore.StrLit("left"))))
                  case RightSide => Free.roll(MFC(MapFuncsCore.ProjectField(HoleF, MapFuncsCore.StrLit("right"))))
                }))).join
          }).join
      }

    implicit def coproduct[T[_[_]], F[_], G[_]](
      implicit F: Planner.Aux[T, F], G: Planner.Aux[T, G]):
        Planner.Aux[T, Coproduct[F, G, ?]] =
      new Planner[Coproduct[F, G, ?]] {
        type IT[G[_]] = T[G]
        def plan
          [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse]
          (cfg: PlannerConfig[T, EX, WF])
          (implicit
            ev0: WorkflowOpCoreF :<: WF,
            ev1: RenderTree[WorkflowBuilder[WF]],
            ev2: WorkflowBuilder.Ops[WF],
            ev3: EX :<: ExprOp) =
          _.run.fold(
            F.plan[M, WF, EX](cfg),
            G.plan[M, WF, EX](cfg))
      }

    // TODO: All instances below here only need to exist because of `FreeQS`,
    //       but can’t actually be called.

    def default[T[_[_]], F[_]](label: String): Planner.Aux[T, F] =
      new Planner[F] {
        type IT[G[_]] = T[G]

        def plan
          [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse]
          (cfg: PlannerConfig[T, EX, WF])
          (implicit
            ev0: WorkflowOpCoreF :<: WF,
            ev1: RenderTree[WorkflowBuilder[WF]],
            ev2: WorkflowBuilder.Ops[WF],
            ev3: EX :<: ExprOp) =
          κ(raiseErr(qscriptPlanningFailed(InternalError.fromMsg(s"should not be reached: $label"))))
      }

    implicit def deadEnd[T[_[_]]]: Planner.Aux[T, Const[DeadEnd, ?]] =
      default("DeadEnd")

    implicit def read[T[_[_]], A]: Planner.Aux[T, Const[Read[A], ?]] =
      default("Read")

    implicit def shiftedReadDir[T[_[_]]]: Planner.Aux[T, Const[ShiftedRead[ADir], ?]] =
      default("ShiftedRead[ADir]")

    implicit def thetaJoin[T[_[_]]]: Planner.Aux[T, ThetaJoin[T, ?]] =
      default("ThetaJoin")

    implicit def projectBucket[T[_[_]]]: Planner.Aux[T, ProjectBucket[T, ?]] =
      default("ProjectBucket")
  }

  def getExpr[
    T[_[_]]: BirecursiveT: ShowT,
    M[_]: Monad: ExecTimeR: MonadFsErr, EX[_]: Traverse: Inject[?[_], ExprOp]]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?], staticHandler: StaticHandler[T, EX])(fm: FreeMap[T]
  ) : M[Fix[ExprOp]] =
    processMapFuncExpr[T, M, EX, Hole](funcHandler, staticHandler)(fm)(κ($$ROOT))

  def getJsFn[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr: ExecTimeR]
    (fm: FreeMap[T])
      : M[JsFn] =
    processMapFunc[T, M, Hole](fm)(κ(jscore.Ident(JsFn.defaultName))) ∘
      (JsFn(JsFn.defaultName, _))

  def getBuilder
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr, WF[_], EX[_]: Traverse, A]
    (handler: FreeMapA[T, A] => M[Expr])
    (src: WorkflowBuilder[WF], fm: FreeMapA[T, A])
    (implicit ev: EX :<: ExprOp)
      : M[WorkflowBuilder[WF]] =
    fm.project match {
      case MapFuncCore.StaticMap(elems) =>
        elems.traverse(_.bitraverse({
          case Embed(MapFuncCore.EC(ejson.Str(key))) => BsonField.Name(key).point[M]
          case key => raiseErr[M, BsonField.Name](qscriptPlanningFailed(InternalError.fromMsg(s"Unsupported object key: ${key.shows}")))
        },
          handler)) ∘
        (es => DocBuilder(src, es.toListMap))
      case _ => handler(fm) ∘ (ExprBuilder(src, _))
    }

  def getExprBuilder
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: ExecTimeR: MonadFsErr, WF[_], EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?], staticHandler: StaticHandler[T, EX])
    (src: WorkflowBuilder[WF], fm: FreeMap[T])
    (implicit ev: EX :<: ExprOp)
      : M[WorkflowBuilder[WF]] =
    getBuilder[T, M, WF, EX, Hole](handleFreeMap[T, M, EX](funcHandler, staticHandler, _))(src, fm)

  def getReduceBuilder
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: ExecTimeR: MonadFsErr, WF[_], EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?], staticHandler: StaticHandler[T, EX])
    (src: WorkflowBuilder[WF], fm: FreeMapA[T, ReduceIndex])
    (implicit ev: EX :<: ExprOp)
      : M[WorkflowBuilder[WF]] =
    getBuilder[T, M, WF, EX, ReduceIndex](handleRedRepair[T, M, EX](funcHandler, staticHandler, _))(src, fm)

  def getJsMerge[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr: ExecTimeR]
    (jf: JoinFunc[T], a1: JsCore, a2: JsCore)
      : M[JsFn] =
    processMapFunc[T, M, JoinSide](
      jf) {
      case LeftSide => a1
      case RightSide => a2
    } ∘ (JsFn(JsFn.defaultName, _))

  def exprOrJs[M[_]: Applicative: MonadFsErr, A]
    (a: A)
    (exf: A => M[Fix[ExprOp]], jsf: A => M[JsFn])
      : M[Expr] = {
    // TODO: Return _both_ errors
    val js = jsf(a)
    val expr = exf(a)
    handleErr[M, Expr](
      (js ⊛ expr)(\&/.Both(_, _)))(
      _ => handleErr[M, Expr](js.map(-\&/))(_ => expr.map(\&/-)))
  }

  def handleFreeMap[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: ExecTimeR: MonadFsErr, EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?], staticHandler: StaticHandler[T, EX], fm: FreeMap[T])
    (implicit ev: EX :<: ExprOp)
      : M[Expr] =
    exprOrJs(fm)(getExpr[T, M, EX](funcHandler, staticHandler)(_), getJsFn[T, M])

  def handleRedRepair[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: ExecTimeR: MonadFsErr, EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?], staticHandler: StaticHandler[T, EX], jr: FreeMapA[T, ReduceIndex])
    (implicit ev: EX :<: ExprOp)
      : M[Expr] =
    exprOrJs(jr)(getExprRed[T, M, EX](funcHandler, staticHandler)(_), getJsRed[T, M])

  def getExprRed[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: ExecTimeR: MonadFsErr, EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?], staticHandler: StaticHandler[T, EX])
    (jr: FreeMapA[T, ReduceIndex])
    (implicit ev: EX :<: ExprOp)
      : M[Fix[ExprOp]] =
    processMapFuncExpr[T, M, EX, ReduceIndex](funcHandler, staticHandler)(jr)(_.idx.fold(
      i => $field("_id", i.toString),
      i => $field(createFieldName("f", i))))

  def getJsRed[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr: ExecTimeR]
    (jr: Free[MapFunc[T, ?], ReduceIndex])
      : M[JsFn] =
    processMapFunc[T, M, ReduceIndex](jr)(_.idx.fold(
      i => jscore.Select(jscore.Select(jscore.Ident(JsFn.defaultName), "_id"), i.toString),
      i => jscore.Select(jscore.Ident(JsFn.defaultName), createFieldName("f", i)))) ∘
      (JsFn(JsFn.defaultName, _))

  def rebaseWB
    [T[_[_]]: EqualT, M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse]
    (cfg: PlannerConfig[T, EX, WF],
      free: FreeQS[T],
      src: WorkflowBuilder[WF])
    (implicit
      F: Planner.Aux[T, QScriptTotal[T, ?]],
      ev0: WorkflowOpCoreF :<: WF,
      ev1: RenderTree[WorkflowBuilder[WF]],
      ev2: WorkflowBuilder.Ops[WF],
      ev3: EX :<: ExprOp)
      : M[WorkflowBuilder[WF]] =
    free.cataM(
      interpretM[M, QScriptTotal[T, ?], qscript.Hole, WorkflowBuilder[WF]](κ(src.point[M]), F.plan(cfg)))

  // TODO: Need `Delay[Show, WorkflowBuilder]`
  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  def HasLiteral[M[_]: Applicative: MonadFsErr, WF[_]]
    (wb: WorkflowBuilder[WF])
    (implicit ev0: WorkflowOpCoreF :<: WF)
      : M[Bson] =
    asLiteral(wb).fold(
      raiseErr[M, Bson](qscriptPlanningFailed(NonRepresentableEJson(wb.toString))))(
      _.point[M])

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  def HasInt[M[_]: Monad: MonadFsErr, WF[_]]
    (wb: WorkflowBuilder[WF])
    (implicit ev0: WorkflowOpCoreF :<: WF)
      : M[Long] =
    HasLiteral[M, WF](wb) >>= {
      case Bson.Int32(v) => v.toLong.point[M]
      case Bson.Int64(v) => v.point[M]
      case x => raiseErr(qscriptPlanningFailed(NonRepresentableEJson(x.toString)))
    }

  // This is maybe worth putting in Matryoshka?
  def findFirst[T[_[_]]: RecursiveT, F[_]: Functor: Foldable, A](
    f: PartialFunction[T[F], A]):
      CoalgebraM[A \/ ?, F, T[F]] =
    tf => (f.lift(tf) \/> tf.project).swap

  // TODO: This should perhaps be _in_ PhaseResults or something
  def log[M[_]: Monad, A: RenderTree]
    (label: String, ma: M[A])
    (implicit mtell: MonadTell_[M, PhaseResults])
      : M[A] =
    ma.mproduct(a => mtell.tell(Vector(PhaseResult.tree(label, a)))) ∘ (_._1)

  def toMongoQScript[
      T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      M[_]: Monad: MonadFsErr: PhaseResultTell]
      (anyDoc: Collection => OptionT[M, BsonDocument],
        qs: T[fs.MongoQScript[T, ?]])
      (implicit BR: Branches[T, fs.MongoQScript[T, ?]])
      : M[T[fs.MongoQScript[T, ?]]] = {

    type MQS[A] = fs.MongoQScript[T, A]
    type QST[A] = QScriptTotal[T, A]

    val O = new Optimize[T]
    val R = new Rewrite[T]

    // TODO: All of these need to be applied through branches. We may also be able to compose
    //       them with normalization as the last step and run until fixpoint. Currently plans are
    //       too sensitive to the order in which these are applied.
    for {
      mongoQS0 <- qs.transCataM(liftFGM(assumeReadType[M, T, MQS](Type.AnyObject)))
      mongoQS1 <- mongoQS0.transCataM(elideQuasarSigil[T, MQS, M](anyDoc))
      mongoQS2 =  mongoQS1.transCata[T[MQS]](R.normalizeEJ[MQS])
      mongoQS3 =  BR.branches.modify(
        _.transCata[FreeQS[T]](liftCo(R.normalizeEJCoEnv[QScriptTotal[T, ?]]))
        )(mongoQS2.project).embed
      _ <- BackendModule.logPhase[M](PhaseResult.tree("QScript Mongo", mongoQS3))

      // NB: Normalizing after these appears to revert the effects of `mapBeforeSort`.
      mongoQS4 =  Trans(mapBeforeSort[T], mongoQS3)
      mongoQS5 =  mongoQS4.transCata[T[MQS]](
                    liftFF[QScriptCore[T, ?], MQS, T[MQS]](
                      repeatedly(O.subsetBeforeMap[MQS, MQS](
                        reflNT[MQS]))))
      _ <- BackendModule.logPhase[M](PhaseResult.tree("QScript Mongo (Shuffle Maps)", mongoQS5))

      // TODO: Once field deletion is implemented for 3.4, this could be selectively applied, if necessary.
      mongoQS6 =  PreferProjection.preferProjection[MQS](mongoQS5)
      _ <- BackendModule.logPhase[M](PhaseResult.tree("QScript Mongo (Prefer Projection)", mongoQS6))
    } yield mongoQS6
  }

  def plan0
    [T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      M[_]: Monad: PhaseResultTell: MonadFsErr: ExecTimeR,
      WF[_]: Functor: Coalesce: Crush: Crystallize,
      EX[_]: Traverse]
    (anyDoc: Collection => OptionT[M, BsonDocument],
      cfg: PlannerConfig[T, EX, WF])
    (qs: T[fs.MongoQScript[T, ?]])
    (implicit
      ev0: WorkflowOpCoreF :<: WF,
      ev1: WorkflowBuilder.Ops[WF],
      ev2: EX :<: ExprOp,
      ev3: RenderTree[Fix[WF]])
      : M[Crystallized[WF]] = {

    for {
      opt <- toMongoQScript[T, M](anyDoc, qs)
      wb  <- log(
        "Workflow Builder",
        opt.cataM[M, WorkflowBuilder[WF]](
          Planner[T, fs.MongoQScript[T, ?]].plan[M, WF, EX](cfg).apply(_) ∘
            (_.transCata[Fix[WorkflowBuilderF[WF, ?]]](repeatedly(WorkflowBuilder.normalize[WF, Fix[WorkflowBuilderF[WF, ?]]])))))
      wf1 <- log("Workflow (raw)", liftM[M, Fix[WF]](WorkflowBuilder.build[WBM, WF](wb)))
      wf2 <- log(
        "Workflow (crystallized)",
        Crystallize[WF].crystallize(wf1).point[M])
    } yield wf2
  }

  def planExecTime[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      M[_]: Monad: PhaseResultTell: MonadFsErr](
      qs: T[fs.MongoQScript[T, ?]],
      queryContext: fs.QueryContext,
      queryModel: MongoQueryModel,
      anyDoc: Collection => OptionT[M, BsonDocument],
      execTime: Instant)
      : M[Crystallized[WorkflowF]] = {
    val peek = anyDoc andThen (r => OptionT(r.run.liftM[ReaderT[?[_], Instant, ?]]))
    plan[T, ReaderT[M, Instant, ?]](qs, queryContext, queryModel, peek).run(execTime)
  }

  /** Translate the QScript plan to an executable MongoDB "physical"
    * plan, taking into account the current runtime environment as captured by
    * the given context.
    *
    * Internally, the type of the plan being built constrains which operators
    * can be used, but the resulting plan uses the largest, common type so that
    * callers don't need to worry about it.
    *
    * @param anyDoc returns any document in the given `Collection`
    */
  def plan[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      M[_]: Monad: PhaseResultTell: MonadFsErr: ExecTimeR](
      qs: T[fs.MongoQScript[T, ?]],
      queryContext: fs.QueryContext,
      queryModel: MongoQueryModel,
      anyDoc: Collection => OptionT[M, BsonDocument])
      : M[Crystallized[WorkflowF]] = {
    import MongoQueryModel._

    val bsonVersion = toBsonVersion(queryModel)

    queryModel match {
      case `3.4` =>
        val joinHandler: JoinHandler[Workflow3_2F, WBM] =
          JoinHandler.fallback[Workflow3_2F, WBM](
            JoinHandler.pipeline(queryContext.statistics, queryContext.indexes),
            JoinHandler.mapReduce)
        val cfg = PlannerConfig[T, Expr3_4, Workflow3_2F](
          joinHandler,
          FuncHandler.handle3_4(bsonVersion),
          StaticHandler.v3_2,
          bsonVersion)
        plan0[T, M, Workflow3_2F, Expr3_4](anyDoc, cfg)(qs)

      case `3.2` =>
        val joinHandler: JoinHandler[Workflow3_2F, WBM] =
          JoinHandler.fallback[Workflow3_2F, WBM](
            JoinHandler.pipeline(queryContext.statistics, queryContext.indexes),
            JoinHandler.mapReduce)
        val cfg = PlannerConfig[T, Expr3_2, Workflow3_2F](
          joinHandler,
          FuncHandler.handle3_2(bsonVersion),
          StaticHandler.v3_2,
          bsonVersion)
        plan0[T, M, Workflow3_2F, Expr3_2](anyDoc, cfg)(qs)

      case `3.0` =>
        val cfg = PlannerConfig[T, Expr3_0, Workflow2_6F](
          JoinHandler.mapReduce[WBM, Workflow2_6F],
          FuncHandler.handle3_0(bsonVersion),
          StaticHandler.v2_6,
          bsonVersion)
        plan0[T, M, Workflow2_6F, Expr3_0](anyDoc, cfg)(qs).map(_.inject[WorkflowF])

      case _ =>
        val cfg = PlannerConfig[T, Expr2_6, Workflow2_6F](
          JoinHandler.mapReduce[WBM, Workflow2_6F],
          FuncHandler.handle2_6(bsonVersion),
          StaticHandler.v2_6,
          bsonVersion)
        plan0[T, M, Workflow2_6F, Expr2_6](anyDoc, cfg)(qs).map(_.inject[WorkflowF])
    }
  }
}
