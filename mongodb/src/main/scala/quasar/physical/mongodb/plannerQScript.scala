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

import slamdata.Predef._
import quasar._, Planner._, Type.{Const => _, Coproduct => _, _}
import quasar.common.{PhaseResult, PhaseResults, SortDir}
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.{ADir, AFile}
import quasar.contrib.scalaz._, eitherT._
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski._
import quasar.fs.{FileSystemError, QueryFile}, FileSystemError.qscriptPlanningFailed
import quasar.javascript._
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.frontend.logicalplan.LogicalPlan
import quasar.namegen._
import quasar.physical.mongodb.WorkflowBuilder.{Subset => _, _}
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner.{FuncHandler, JsFuncHandler, JoinHandler, JoinSource, OptionFree}
import quasar.physical.mongodb.workflow.{ExcludeId => _, IncludeId => _, _}
import quasar.qscript.{Coalesce => _, _}
import quasar.std.StdLib._ // TODO: remove this

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

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

object MongoDbQScriptPlanner {
  import fixExprOp._

  // FIXME: Move to Matryoshka.
  def ginterpret[W[_], F[_], A, B](f: A => B, φ: GAlgebra[W, F, B])
      : GAlgebra[W, CoEnv[A, F, ?], B] =
    ginterpretM[W, Id, F, A, B](f, φ)

  type Partial[T[_[_]], In, Out] = (PartialFunction[List[In], Out], List[InputFinder[T]])

  type OutputM[A] = PlannerError \/ A

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
    [T[_[_]]: RecursiveT: ShowT, M[_]: Monad, EX[_]: Traverse, A]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?])
    (fm: FreeMapA[T, A])
    (recovery: A => Fix[ExprOp])
    (implicit merr: MonadError_[M, FileSystemError], inj: EX :<: ExprOp)
      : M[Fix[ExprOp]] =
    fm.cataM(
      interpretM[M, MapFunc[T, ?], A, Fix[ExprOp]](
        recovery(_).point[M],
        expression(funcHandler)))

  def getSelector
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad, EX[_]: Traverse]
    (fm: FreeMap[T])
    (implicit merr: MonadError_[M, FileSystemError], inj: EX :<: ExprOp)
      : OutputM[PartialSelector[T]] =
    fm.zygo(
      interpret[MapFunc[T, ?], Hole, T[MapFunc[T, ?]]](
        κ(MFC(MapFuncsCore.Undefined[T, T[MapFunc[T, ?]]]()).embed),
        _.embed),
      ginterpret[(T[MapFunc[T, ?]], ?), MapFunc[T, ?], Hole, OutputM[PartialSelector[T]]](
        κ(defaultSelector[T].point[OutputM]),
        selector[T]))


  def processMapFunc[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad, A]
    (fm: FreeMapA[T, A])
    (recovery: A => JsCore)
    (implicit merr: MonadError_[M, FileSystemError])
      : M[JsCore] =
    fm.cataM(interpretM[M, MapFunc[T, ?], A, JsCore](recovery(_).point[M], javascript))

  // FIXME: This is temporary. Should go away when the connector is complete.
  def unimplemented[M[_], A](label: String)(implicit merr: MonadError_[M, FileSystemError]): M[A] =
    merr.raiseError(qscriptPlanningFailed(InternalError.fromMsg(s"unimplemented $label")))

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

  def expression[T[_[_]]: RecursiveT: ShowT, M[_]: Applicative, EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?])
    (implicit merr: MonadError_[M, FileSystemError], inj: EX :<: ExprOp)
      : AlgebraM[M, MapFunc[T, ?], Fix[ExprOp]] = {
    import MapFuncsCore._
    import MapFuncsDerived._

    def handleCommon(mf: MapFunc[T, Fix[ExprOp]]): Option[Fix[ExprOp]] =
      funcHandler(mf).map(t => unpack(t.mapSuspension(inj)))

    val handleSpecialCore: MapFuncCore[T, Fix[ExprOp]] => M[Fix[ExprOp]] = {
      case Now() => unimplemented[M, Fix[ExprOp]]("Now expression")

      // FIXME: Will only work for Arrays, not Strings
      // case Length(a1) => $size(a1).point[M]
      case Length(a1) => unimplemented[M, Fix[ExprOp]]("Length expression")
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

      case Range(_, _)        => unimplemented[M, Fix[ExprOp]]("Range expression")
      case Search(_, _, _)    => unimplemented[M, Fix[ExprOp]]("Search expression")
    }

    val handleSpecialDerived: MapFuncDerived[T, Fix[ExprOp]] => M[Fix[ExprOp]] = {
      case Abs(a1) => unimplemented[M, Fix[ExprOp]]("Abs expression")
      case Ceil(a1) => unimplemented[M, Fix[ExprOp]]("Ceil expression")
      case Floor(a1) => unimplemented[M, Fix[ExprOp]]("Floor expression")
      case Trunc(a1) => unimplemented[M, Fix[ExprOp]]("Trunc expression")
    }

    val handleSpecial: MapFunc[T, Fix[ExprOp]] => M[Fix[ExprOp]] = {
      case MFC(mfc) => handleSpecialCore(mfc)
      case MFD(mfd) => handleSpecialDerived(mfd)
    }

    mf => handleCommon(mf).cata(_.point[M], handleSpecial(mf))
  }

  def javascript[T[_[_]]: BirecursiveT: ShowT, M[_]: Applicative]
    (implicit merr: MonadError_[M, FileSystemError])
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

    def handleCommon(mf: MapFunc[T, JsCore]): Option[JsCore] =
      JsFuncHandler.handle[MapFunc[T, ?]].apply(mf).map(unpack[Fix, JsCoreF])

    val handleSpecialCore: MapFuncCore[T, JsCore] => M[JsCore] = {
      case Constant(v1) =>
        v1.cata(Data.fromEJson).toJs.fold(
          merr.raiseError[JsCore](qscriptPlanningFailed(NonRepresentableEJson(v1.shows))))(
          _.point[M])
      // FIXME: Not correct
      case Undefined() => ident("undefined").point[M]
      case JoinSideName(n) =>
        merr.raiseError[JsCore](qscriptPlanningFailed(UnexpectedJoinSide(n)))
      case Now() => New(Name("Date"), Nil).point[M]
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
      case ToTimestamp(a1) =>
        New(Name("Date"), List(Select(a1, "epoch"))).point[M]

      case ExtractCentury(date) =>
        Call(Select(ident("Math"), "ceil"), List(
          BinOp(jscore.Div,
            Call(Select(date, "getUTCFullYear"), Nil),
            Literal(Js.Num(100, false))))).point[M]
      case ExtractDayOfMonth(date) => Call(Select(date, "getUTCDate"), Nil).point[M]
      case ExtractDecade(date) =>
        trunc(
          BinOp(jscore.Div,
            Call(Select(date, "getUTCFullYear"), Nil),
            Literal(Js.Num(10, false)))).point[M]
      case ExtractDayOfWeek(date) =>
        Call(Select(date, "getUTCDay"), Nil).point[M]
      case ExtractDayOfYear(date) => unimplemented[M, JsCore]("ExtractDayOfYear JS")
      case ExtractEpoch(date) =>
        BinOp(jscore.Div,
          Call(Select(date, "valueOf"), Nil),
          Literal(Js.Num(1000, false))).point[M]
      case ExtractHour(date) => Call(Select(date, "getUTCHours"), Nil).point[M]
      case ExtractIsoDayOfWeek(date) =>
        Let(Name("x"), Call(Select(date, "getUTCDay"), Nil),
          If(
            BinOp(jscore.Eq, ident("x"), Literal(Js.Num(0, false))),
            Literal(Js.Num(7, false)),
            ident("x"))).point[M]
      case ExtractIsoYear(date) => unimplemented[M, JsCore]("ExtractIsoYear JS")
      case ExtractMicroseconds(date) =>
        BinOp(jscore.Mult,
          BinOp(jscore.Add,
            Call(Select(date, "getUTCMilliseconds"), Nil),
            BinOp(jscore.Mult,
              Call(Select(date, "getUTCSeconds"), Nil),
              Literal(Js.Num(1000, false)))),
          Literal(Js.Num(1000, false))).point[M]
      case ExtractMillennium(date) =>
        Call(Select(ident("Math"), "ceil"), List(
          BinOp(jscore.Div,
            Call(Select(date, "getUTCFullYear"), Nil),
            Literal(Js.Num(1000, false))))).point[M]
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
        BinOp(jscore.Add,
          BinOp(jscore.BitOr,
            BinOp(jscore.Div,
              Call(Select(date, "getUTCMonth"), Nil),
              Literal(Js.Num(3, false))),
            Literal(Js.Num(0, false))),
          Literal(Js.Num(1, false))).point[M]
      case ExtractSecond(date) =>
        BinOp(jscore.Add,
          Call(Select(date, "getUTCSeconds"), Nil),
          BinOp(jscore.Div,
            Call(Select(date, "getUTCMilliseconds"), Nil),
            Literal(Js.Num(1000, false)))).point[M]
      case ExtractWeek(date) => unimplemented[M, JsCore]("ExtractWeek JS")
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
          //     losing pre222222cision.
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
      // case ToId(a1) => Call(ident("ObjectId"), List(a1)).point[M]

      case MakeMap(Embed(LiteralF(Js.Str(str))), a2) => Obj(ListMap(Name(str) -> a2)).point[M]
      // TODO: pull out the literal, and handle this case in other situations
      case MakeMap(a1, a2) => Obj(ListMap(Name("__Quasar_non_string_map") ->
       Arr(List(Arr(List(a1, a2)))))).point[M]
      case ConcatArrays(Embed(ArrF(a1)), Embed(ArrF(a2))) =>
        Arr(a1 |+| a2).point[M]
      case ConcatArrays(a1, a2) => BinOp(jscore.Add, a1, a2).point[M]
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
          merr.raiseError(qscriptPlanningFailed(InternalError.fromMsg("uncheckable type"))))(
          f => If(f(expr), cont, fallback).point[M])

      case Range(_, _)        => unimplemented[M, JsCore]("Range JS")
    }

    val handleSpecialDerived: MapFuncDerived[T, JsCore] => M[JsCore] = {
      case Abs(a1)       => unimplemented[M, JsCore]("Abs JS")
      case Ceil(a1)      => unimplemented[M, JsCore]("Ceil JS")
      case Floor(a1)     => unimplemented[M, JsCore]("Floor JS")
      case Trunc(a1)     => unimplemented[M, JsCore]("Trunc JS")
    }

    val handleSpecial: MapFunc[T, JsCore] => M[JsCore] = {
      case MFC(mfc) => handleSpecialCore(mfc)
      case MFD(mfd) => handleSpecialDerived(mfd)
    }

    mf => handleCommon(mf).cata(_.point[M], handleSpecial(mf))
  }

  /** Need this until the old connector goes away and we can redefine `Selector`
    * as `Selector[A, B]`, where `A` is the field type (naturally `BsonField`),
    * and `B` is the recursive parameter.
    */
  type PartialSelector[T[_[_]]] = Partial[T, BsonField, Selector]

  def defaultSelector[T[_[_]]]: PartialSelector[T] = (
    { case List(field) =>
      Selector.Doc(ListMap(
        field -> Selector.Expr(Selector.Eq(Bson.Bool(true)))))
    },
    List(Here[T]()))

  /**
   * The selector phase tries to turn expressions into MongoDB selectors -- i.e.
   * Mongo query expressions. Selectors are only used for the filtering pipeline
   * op, so it's quite possible we build more stuff than is needed (but it
   * doesn’t matter, unneeded annotations will be ignored by the pipeline
   * phase).
   *
   * Like the expression op phase, this one requires bson field annotations.
   *
   * Most expressions cannot be turned into selector expressions without using
   * the "\$where" operator, which allows embedding JavaScript
   * code. Unfortunately, using this operator turns filtering into a full table
   * scan. We should do a pass over the tree to identify partial boolean
   * expressions which can be turned into selectors, factoring out the leftovers
   * for conversion using \$where.
   */
  def selector[T[_[_]]: RecursiveT: ShowT]:
      GAlgebra[(T[MapFunc[T, ?]], ?), MapFunc[T, ?], OutputM[PartialSelector[T]]] = { node =>
    import MapFuncsCore._

    type Output = OutputM[PartialSelector[T]]

    object IsBson {
      def unapply(v: (T[MapFunc[T, ?]], Output)): Option[Bson] =
        v._1.project match {
          case MFC(Constant(b)) => b.cataM(BsonCodec.fromEJson).toOption
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

        case MFC(Guard(_, typ, cont, _)) =>
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
              { case (f2, p2) =>
                ({ case head :: tail => Selector.And(f(head), f2(tail)) },
                  There(0, Here[T]()) :: p2.map(There(1, _)))
              })))

        case _ => -\/(InternalError fromMsg node.map(_._1).shows)
      }
    }

    invoke(node) <+> \/-(default)
  }

  /** Brings a [[WorkflowBuilder.EitherE]] into our `M`.
    */
  def liftErr[M[_]: Applicative, A]
    (meh: WorkflowBuilder.EitherE[A])
    (implicit merr: MonadError_[M, FileSystemError])
      : M[A] =
    meh.fold(
      e => merr.raiseError(qscriptPlanningFailed(e)),
      _.point[M])

  /** Brings a [[WorkflowBuilder.M]] into our `M`. */
  def liftM[M[_]: Monad, A]
    (meh: WorkflowBuilder.M[A])
    (implicit merr: MonadError_[M, FileSystemError], mst: MonadState_[M, NameGen])
      : M[A] =
    mst.gets(meh.eval) >>= (liftErr[M, A](_))

  def createFieldName(i: Int): String = "f" + i.toString

  trait Planner[F[_]] {
    type IT[G[_]]

    def plan
      [M[_]: Monad, WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse]
      (joinHandler: JoinHandler[WF, WorkflowBuilder.M],
        funcHandler: MapFunc[IT, ?] ~> OptionFree[EX, ?])
      (implicit
        merr: MonadError_[M, FileSystemError],
        mst:  MonadState_[M, NameGen],
        ev0: WorkflowOpCoreF :<: WF,
        ev1: RenderTree[WorkflowBuilder[WF]],
        ev2: WorkflowBuilder.Ops[WF],
        ev3: EX :<: ExprOp):
        AlgebraM[M, F, WorkflowBuilder[WF]]
  }

  object Planner {
    type Aux[T[_[_]], F[_]] = Planner[F] { type IT[G[_]] = T[G] }

    def apply[T[_[_]], F[_]](implicit ev: Planner.Aux[T, F]) = ev

    implicit def shiftedReadFile[T[_[_]]]: Planner.Aux[T, Const[ShiftedRead[AFile], ?]] =
      new Planner[Const[ShiftedRead[AFile], ?]] {
        type IT[G[_]] = T[G]
        def plan
          [M[_]: Monad, WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse]
          (joinHandler: JoinHandler[WF, WorkflowBuilder.M],
            funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?])
          (implicit
            merr: MonadError_[M, FileSystemError],
            mst:  MonadState_[M, NameGen],
            ev0: WorkflowOpCoreF :<: WF,
            ev1: RenderTree[WorkflowBuilder[WF]],
            WB: WorkflowBuilder.Ops[WF],
            ev3: EX :<: ExprOp) =
          qs => Collection
            .fromFile(qs.getConst.path)
            .fold(
              e => merr.raiseError(qscriptPlanningFailed(PlanPathError(e))),
              coll => {
                val dataset = WB.read(coll)
                // TODO: exclude `_id` from the value here?
                (qs.getConst.idStatus match {
                  case IdOnly => ExprBuilder(dataset, $field("_id").right)
                  case IncludeId =>
                    ArrayBuilder(dataset, List($field("_id").right, $$ROOT.right))
                  case ExcludeId => dataset
                }).point[M]
              })
      }

    implicit def qscriptCore[T[_[_]]: BirecursiveT: EqualT: ShowT]:
        Planner.Aux[T, QScriptCore[T, ?]] =
      new Planner[QScriptCore[T, ?]] {
        type IT[G[_]] = T[G]

        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def plan
          [M[_]: Monad,
            WF[_]: Functor: Coalesce: Crush: Crystallize,
            EX[_]: Traverse]
          (joinHandler: JoinHandler[WF, WorkflowBuilder.M],
            funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?])
          (implicit
            merr: MonadError_[M, FileSystemError],
            mst:  MonadState_[M, NameGen],
            ev0: WorkflowOpCoreF :<: WF,
            ev1: RenderTree[WorkflowBuilder[WF]],
            WB: WorkflowBuilder.Ops[WF],
            ev3: EX :<: ExprOp) = {
          case qscript.Map(src, f) => getExprBuilder[T, M, WF, EX](funcHandler)(src, f)
          case LeftShift(src, struct, id, repair) =>
            (getExprBuilder[T, M, WF, EX](funcHandler)(src, struct) ⊛
              (if (repair.contains(LeftSideF))
                 getJsMerge[T, M](
                   repair,
                   jscore.Access(jscore.Ident(JsFn.defaultName), jscore.Literal(Js.Num(0, false))),
                   jscore.Access(jscore.Ident(JsFn.defaultName), jscore.Literal(Js.Num(1, false)))).map(_.left)
               else
                 getJsFn[T, M](repair.as(SrcHole)).map(_.right)))(
              (expr, jm) => jm.fold(WB.jsArrayExpr(List(src, WB.flattenMap(expr)), _), WB.jsExpr1(WB.flattenMap(expr), _).point[WorkflowBuilder.M])).map(liftM[M, WorkflowBuilder[WF]]).join
          case Reduce(src, bucket, reducers, repair) =>
            (getExprBuilder[T, M, WF, EX](funcHandler)(src, bucket) ⊛
              reducers.traverse(_.traverse(fm => handleFreeMap[T, M, EX](funcHandler, fm))))((b, red) => {
                val newB = b.unFix match {
                  case ArrayBuilderF(src, elems) => DocBuilder(src, elems.zipWithIndex.map(_.map(i => BsonField.Name(i.toString)).swap).toListMap)
                  case _ => b
                }
                getReduceBuilder[T, M, WF, EX](
                  funcHandler)(
                  red.map(_.sequence).sequence.fold(
                    κ(GroupBuilder(DocBuilder(src, red.unite.zipWithIndex.map(_.map(i => BsonField.Name(createFieldName(i))).swap).toListMap), // FIXME: Doesn’t work with UnshiftMap
                      List(newB),
                      Contents.Doc(red.zipWithIndex.map(ai =>
                        (BsonField.Name(createFieldName(ai._2)),
                          accumulator(ai._1.as($field(createFieldName(ai._2)))).left[Fix[ExprOp]])).toListMap))),
                    exprs => GroupBuilder(src,
                      List(newB),
                      Contents.Doc(exprs.zipWithIndex.map(ai =>
                        (BsonField.Name(createFieldName(ai._2)),
                          accumulator(ai._1).left[Fix[ExprOp]])).toListMap))),
                    repair)
              }).join
          case Sort(src, bucket, order) =>
            val (keys, dirs) = (bucket match {
              case MapFuncsCore.NullLit() => order
              case _                  => (bucket, SortDir.Ascending) <:: order
            }).unzip
            keys.traverse(getExprBuilder[T, M, WF, EX](funcHandler)(src, _))
              .map(ks => WB.sortBy(src, ks.toList, dirs.toList))
          case Filter(src, cond) =>
            getSelector[T, M, EX](cond).fold(
              // FIXME: If this is an ExprOp, then do this, but if it’s JS, we
              //        should use a `Where` selector instead.
              _ => getExprBuilder[T, M, WF, EX](funcHandler)(src, cond).map(cond =>
                WB.filter(src, List(cond), {
                  case f :: Nil => Selector.Doc(f -> Selector.Eq(Bson.Bool(true)))
                })),
              {
                case (sel, inputs) =>
                  inputs.traverse(f => getExprBuilder[T, M, WF, EX](funcHandler)(src, f(cond))).map(WB.filter(src, _, sel))
              })
          case Union(src, lBranch, rBranch) =>
            (rebaseWB[T, M, WF, EX](joinHandler, funcHandler, lBranch, src) ⊛
              rebaseWB[T, M, WF, EX](joinHandler, funcHandler, rBranch, src))(
              WB.unionAll).map(liftM[M, WorkflowBuilder[WF]]).join
          case Subset(src, from, sel, count) =>
            (rebaseWB[T, M, WF, EX](joinHandler, funcHandler, from, src) ⊛
              (rebaseWB[T, M, WF, EX](joinHandler, funcHandler, count, src) >>= (HasInt[M, WF](_))))(
              sel match {
                case Drop => WB.skip
                case Take => WB.limit
                // TODO: Better sampling
                case Sample => WB.limit
              })
          case Unreferenced() => ValueBuilder(Bson.Null).point[M]
        }
      }

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    implicit def equiJoin[T[_[_]]: BirecursiveT: EqualT: ShowT]:
        Planner.Aux[T, EquiJoin[T, ?]] =
      new Planner[EquiJoin[T, ?]] {
        type IT[G[_]] = T[G]
        def plan
          [M[_]: Monad, WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse]
          (joinHandler: JoinHandler[WF, WorkflowBuilder.M],
            funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?])
          (implicit
            merr: MonadError_[M, FileSystemError],
            mst:  MonadState_[M, NameGen],
            ev0: WorkflowOpCoreF :<: WF,
            ev1: RenderTree[WorkflowBuilder[WF]],
            ev2: WorkflowBuilder.Ops[WF],
            ev3: EX :<: ExprOp) =
          qs =>
        // FIXME: we should take advantage of the already merged srcs
        (rebaseWB[T, M, WF, EX](joinHandler, funcHandler, qs.lBranch, qs.src) ⊛
          rebaseWB[T, M, WF, EX](joinHandler, funcHandler, qs.rBranch, qs.src) ⊛
          getExprBuilder[T, M, WF, EX](funcHandler)(qs.src, qs.lKey) ⊛
          getExprBuilder[T, M, WF, EX](funcHandler)(qs.src, qs.rKey) ⊛
          getJsFn[T, M](qs.lKey).map(_.some).handleError(κ(none.point[M])) ⊛
          getJsFn[T, M](qs.rKey).map(_.some).handleError(κ(none.point[M])))(
          (lb, rb, lk, rk, lj, rj) =>
          liftM[M, WorkflowBuilder[WF]](joinHandler.run(
            // FIXME: `LogicalPlan` join functions are deprecated in favor of `logicalplan.Join`
            LogicalPlan.funcFromJoinType(qs.f),
            JoinSource(lb, List(lk), lj.map(List(_))),
            JoinSource(rb, List(rk), rj.map(List(_)))))).join
      }

    implicit def coproduct[T[_[_]], F[_], G[_]](
      implicit F: Planner.Aux[T, F], G: Planner.Aux[T, G]):
        Planner.Aux[T, Coproduct[F, G, ?]] =
      new Planner[Coproduct[F, G, ?]] {
        type IT[G[_]] = T[G]
        def plan
          [M[_]: Monad, WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse]
          (joinHandler: JoinHandler[WF, WorkflowBuilder.M],
            funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?])
          (implicit
            merr: MonadError_[M, FileSystemError],
            mst:  MonadState_[M, NameGen],
            ev0: WorkflowOpCoreF :<: WF,
            ev1: RenderTree[WorkflowBuilder[WF]],
            ev2: WorkflowBuilder.Ops[WF],
            ev3: EX :<: ExprOp) =
          _.run.fold(
            F.plan[M, WF, EX](joinHandler, funcHandler),
            G.plan[M, WF, EX](joinHandler, funcHandler))
      }


    // TODO: All instances below here only need to exist because of `FreeQS`,
    //       but can’t actually be called.

    def default[T[_[_]], F[_]](label: String): Planner.Aux[T, F] =
      new Planner[F] {
        type IT[G[_]] = T[G]

        def plan
          [M[_]: Monad, WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse]
          (joinHandler: JoinHandler[WF, WorkflowBuilder.M],
            funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?])
          (implicit
            merr: MonadError_[M, FileSystemError],
            mst:  MonadState_[M, NameGen],
            ev0: WorkflowOpCoreF :<: WF,
            ev1: RenderTree[WorkflowBuilder[WF]],
            ev2: WorkflowBuilder.Ops[WF],
            ev3: EX :<: ExprOp) =
          κ(merr.raiseError(qscriptPlanningFailed(InternalError.fromMsg(s"should not be reached: $label"))))
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

  def getExpr[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad, EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?])
    (fm: FreeMap[T])
    (implicit merr: MonadError_[M, FileSystemError], ev: EX :<: ExprOp)
      : M[Fix[ExprOp]] =
    processMapFuncExpr[T, M, EX, Hole](funcHandler)(fm)(κ($$ROOT))

  def getJsFn[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad]
    (fm: FreeMap[T])
    (implicit merr: MonadError_[M, FileSystemError])
      : M[JsFn] =
    processMapFunc[T, M, Hole](fm)(κ(jscore.Ident(JsFn.defaultName))) ∘
      (JsFn(JsFn.defaultName, _))

  def getBuilder
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad, WF[_], EX[_]: Traverse, A]
    (handler: FreeMapA[T, A] => M[Expr])
    (src: WorkflowBuilder[WF], fm: FreeMapA[T, A])
    (implicit merr: MonadError_[M, FileSystemError], ev: EX :<: ExprOp)
      : M[WorkflowBuilder[WF]] =
    fm.project match {
      // TODO: identify cases for SpliceBuilder.
      case MapFuncCore.StaticArray(elems) =>
        elems.traverse(handler) ∘ (ArrayBuilder(src, _))
      case MapFuncCore.StaticMap(elems) =>
        elems.traverse(_.bitraverse({
          case Embed(MapFuncCore.EC(ejson.Str(key))) => BsonField.Name(key).point[M]
          case key => merr.raiseError[BsonField.Name](qscriptPlanningFailed(InternalError.fromMsg(s"Unsupported object key: ${key.shows}")))
        },
          handler)) ∘
        (es => DocBuilder(src, es.toListMap))
      case co => handler(co.embed) ∘ (ExprBuilder(src, _))
    }

  def getExprBuilder
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad, WF[_], EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?])
    (src: WorkflowBuilder[WF], fm: FreeMap[T])
    (implicit merr: MonadError_[M, FileSystemError], ev: EX :<: ExprOp)
      : M[WorkflowBuilder[WF]] =
    getBuilder[T, M, WF, EX, Hole](handleFreeMap[T, M, EX](funcHandler, _))(src, fm)

  def getReduceBuilder
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad, WF[_], EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?])
    (src: WorkflowBuilder[WF], fm: FreeMapA[T, ReduceIndex])
    (implicit merr: MonadError_[M, FileSystemError], ev: EX :<: ExprOp)
      : M[WorkflowBuilder[WF]] =
    getBuilder[T, M, WF, EX, ReduceIndex](handleRedRepair[T, M, EX](funcHandler, _))(src, fm)

  def getJsMerge[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad]
    (jf: JoinFunc[T], a1: JsCore, a2: JsCore)
    (implicit merr: MonadError_[M, FileSystemError])
      : M[JsFn] =
    processMapFunc[T, M, JoinSide](
      jf) {
      case LeftSide => a1
      case RightSide => a2
    } ∘ (JsFn(JsFn.defaultName, _))

  def exprOrJs[M[_]: Functor, A]
    (a: A)
    (exf: A => M[Fix[ExprOp]], jsf: A => M[JsFn])
    (implicit merr: MonadError_[M, FileSystemError])
      : M[Expr] =
    merr.handleError(
      exf(a).map(_.right[JsFn]))(
      _ => jsf(a).map(_.left[Fix[ExprOp]]))

  def handleFreeMap[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad, EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?], fm: FreeMap[T])
    (implicit merr: MonadError_[M, FileSystemError], ev: EX :<: ExprOp)
      : M[Expr] =
    exprOrJs(fm)(getExpr[T, M, EX](funcHandler)(_), getJsFn[T, M])

  def handleRedRepair[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad, EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?], jr: FreeMapA[T, ReduceIndex])
    (implicit merr: MonadError_[M, FileSystemError], ev: EX :<: ExprOp)
      : M[Expr] =
    exprOrJs(jr)(getExprRed[T, M, EX](funcHandler)(_), getJsRed[T, M])

  def getExprRed[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad, EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?])
    (jr: FreeMapA[T, ReduceIndex])
    (implicit merr: MonadError_[M, FileSystemError], ev: EX :<: ExprOp)
      : M[Fix[ExprOp]] =
    processMapFuncExpr[T, M, EX, ReduceIndex](funcHandler)(jr)(ri => $field(ri.idx.fold("_id")(createFieldName)))

  def getJsRed[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad]
    (jr: Free[MapFunc[T, ?], ReduceIndex])
    (implicit merr: MonadError_[M, FileSystemError])
      : M[JsFn] =
    processMapFunc[T, M, ReduceIndex](jr)(ri => jscore.Access(jscore.Ident(JsFn.defaultName), jscore.Literal(Js.Str(ri.idx.fold("_id")(createFieldName))))) ∘
      (JsFn(JsFn.defaultName, _))

  def rebaseWB
    [T[_[_]]: EqualT, M[_]: Monad, WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse]
    (joinHandler: JoinHandler[WF, WorkflowBuilder.M],
      funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?],
      free: FreeQS[T],
      src: WorkflowBuilder[WF])
    (implicit
      merr: MonadError_[M, FileSystemError],
      mst:  MonadState_[M, NameGen],
      F: Planner.Aux[T, QScriptTotal[T, ?]],
      ev0: WorkflowOpCoreF :<: WF,
      ev1: RenderTree[WorkflowBuilder[WF]],
      ev2: WorkflowBuilder.Ops[WF],
      ev3: EX :<: ExprOp)
      : M[WorkflowBuilder[WF]] =
    free.cataM(
      interpretM[M, QScriptTotal[T, ?], qscript.Hole, WorkflowBuilder[WF]](κ(src.point[M]), F.plan(joinHandler, funcHandler)))

  // TODO: Need `Delay[Show, WorkflowBuilder]`
  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  def HasLiteral[M[_]: Applicative, WF[_]]
    (wb: WorkflowBuilder[WF])
    (implicit merr: MonadError_[M, FileSystemError])
      : M[Bson] =
    asLiteral(wb).fold(
      merr.raiseError[Bson](qscriptPlanningFailed(NonRepresentableEJson(wb.toString))))(
      _.point[M])

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  def HasInt[M[_]: Monad, WF[_]]
    (wb: WorkflowBuilder[WF])
    (implicit merr: MonadError_[M, FileSystemError])
      : M[Long] =
    HasLiteral[M, WF](wb) >>= {
      case Bson.Int32(v) => v.toLong.point[M]
      case Bson.Int64(v) => v.point[M]
      case x => merr.raiseError(qscriptPlanningFailed(NonRepresentableEJson(x.toString)))
    }

  // This is maybe worth putting in Matryoshka?
  def findFirst[T[_[_]]: RecursiveT, F[_]: Functor: Foldable, A](
    f: PartialFunction[T[F], A]):
      CoalgebraM[A \/ ?, F, T[F]] =
    tf => (f.lift(tf) \/> tf.project).swap

  def elideMoreGeneralGuards[M[_]: Applicative, T[_[_]]: RecursiveT]
    (subType: Type)
    (implicit merr: MonadError_[M, FileSystemError])
      : CoEnvMap[T, FreeMap[T]] => M[CoEnvMap[T, FreeMap[T]]] = {
    def f: CoEnvMap[T, FreeMap[T]] => M[CoEnvMap[T, FreeMap[T]]] = {
      case free @ CoEnv(\/-(MFC(MapFuncsCore.Guard(Embed(CoEnv(-\/(SrcHole))), typ, cont, fb)))) =>
        if (typ.contains(subType)) cont.project.point[M]
        // TODO: Error if there is no overlap between the types.
        else {
          val union = subType ⨯ typ
          if (union ≟ Type.Bottom)
            merr.raiseError(qscriptPlanningFailed(InternalError.fromMsg(s"can only contain ${subType.shows}, but a(n) ${typ.shows} is expected")))
          else {
            CoEnv[Hole, MapFunc[T, ?], FreeMap[T]](MFC(MapFuncsCore.Guard[T, FreeMap[T]](HoleF[T], union, cont, fb)).right).point[M]
          }
        }
      case x => x.point[M]
    }
    f
  }

  // TODO: Allow backends to provide a “Read” type to the typechecker, which
  //       represents the type of values that can be stored in a collection.
  //       E.g., for MongoDB, it would be `Map(String, Top)`. This will help us
  //       generate more correct PatternGuards in the first place, rather than
  //       trying to strip out unnecessary ones after the fact
  // FIXME: This doesn’t yet traverse branches, so it leaves in some checks.
  def assumeReadType[M[_]: Monad, T[_[_]]: BirecursiveT, F[_]: Functor]
    (typ: Type)
    (implicit
      merr: MonadError_[M, FileSystemError],
      QC: QScriptCore[T, ?] :<: F,
      SR: Const[ShiftedRead[AFile], ?] :<: F)
      : QScriptCore[T, T[F]] => M[F[T[F]]] = {
    case f @ Filter(src, cond) =>
      SR.prj(src.project) match {
        case Some(Const(ShiftedRead(_, ExcludeId))) =>
          cond.transCataM(elideMoreGeneralGuards[M, T](typ)) ∘
            (c => QC.inj(Filter(src, c)))
        case _ => QC.inj(f).point[M]
      }
    case ls @ LeftShift(src, struct, id, repair) =>
      SR.prj(src.project) match {
        case Some(Const(ShiftedRead(_, ExcludeId))) =>
          struct.transCataM(elideMoreGeneralGuards[M, T](typ)) ∘
          (struct => QC.inj(LeftShift(src, struct, id, repair)))
        case _ => QC.inj(ls).point[M]
      }
    case m @ qscript.Map(src, mf) =>
      SR.prj(src.project) match {
        case Some(Const(ShiftedRead(_, ExcludeId))) =>
          mf.transCataM(elideMoreGeneralGuards[M, T](typ)) ∘
          (mf => QC.inj(qscript.Map(src, mf)))
        case _ => QC.inj(m).point[M]
      }
    case r @ Reduce(src, b, red, rep) =>
      SR.prj(src.project) match {
        case Some(Const(ShiftedRead(_, ExcludeId))) =>
          (b.transCataM(elideMoreGeneralGuards[M, T](typ)) ⊛
            red.traverse(_.traverse(_.transCataM(elideMoreGeneralGuards[M, T](typ)))))(
            (b, red) => QC.inj(Reduce(src, b, red, rep)))
        case _ => QC.inj(r).point[M]
      }
    case qc =>
      QC.inj(qc).point[M]
  }

  type GenT[X[_], A]  = StateT[X, NameGen, A]

  // TODO: This should perhaps be _in_ PhaseResults or something
  def log[M[_]: Monad, A: RenderTree]
    (label: String, ma: M[A])
    (implicit mtell: MonadTell_[M, PhaseResults])
      : M[A] =
    ma.mproduct(a => mtell.tell(Vector(PhaseResult.tree(label, a)))) ∘ (_._1)

  type MongoQScriptCP[T[_[_]]] = QScriptCore[T, ?] :\: EquiJoin[T, ?] :/: Const[ShiftedRead[AFile], ?]
  type MongoQScript[T[_[_]], A] = MongoQScriptCP[T]#M[A]

  def toMongoQScript[T[_[_]] : BirecursiveT: EqualT: RenderTreeT: ShowT, M[_] : Monad](
    lp: T[LogicalPlan],
    listContents: DiscoverPath.ListContents[M])(implicit
    merr: MonadError_[M, FileSystemError],
    mtell: MonadTell_[M, PhaseResults]
  ): M[T[MongoQScript[T, ?]]] = {
    val rewrite = new Rewrite[T]
    val optimize = new Optimize[T]

    implicit val mongoQScripToQScriptTotal: Injectable.Aux[MongoQScript[T, ?], QScriptTotal[T, ?]] =
      ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])

    for {
      qs  <- QueryFile.convertToQScriptRead[T, M, QScriptRead[T, ?]](listContents)(lp)
      // TODO: also need to prefer projections over deletions
      // NB: right now this only outputs one phase, but it’d be cool if we could
      //     interleave phase building in the composed recursion scheme
      opt <- log(
        "QScript (Mongo-specific)",
        Unirewrite[T, MongoQScriptCP[T], M](rewrite, listContents).apply(qs)
          .map(_.transHylo(
            optimize.optimize(reflNT[MongoQScript[T, ?]]),
            Unicoalesce[T, MongoQScriptCP[T]]))
          .flatMap(_.transCataM(liftFGM(assumeReadType[M, T, MongoQScript[T, ?]](Type.AnyObject)))))
    } yield opt
  }

  def plan0
    [T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      M[_]: Monad,
      WF[_]: Functor: Coalesce: Crush: Crystallize,
      EX[_]: Traverse]
    (listContents: DiscoverPath.ListContents[M],
      joinHandler: JoinHandler[WF, WorkflowBuilder.M],
      funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?])
    (lp: T[LogicalPlan])
    (implicit
      merr: MonadError_[M, FileSystemError],
      mtell: MonadTell_[M, PhaseResults],
      ev0: WorkflowOpCoreF :<: WF,
      ev1: WorkflowBuilder.Ops[WF],
      ev2: EX :<: ExprOp,
      ev3: RenderTree[Fix[WF]])
      : M[Crystallized[WF]] = {

    (for {
      opt <- toMongoQScript(lp, listContents).liftM[GenT]
      wb  <- log(
        "Workflow Builder",
        opt.cataM[GenT[M, ?], WorkflowBuilder[WF]](Planner[T, MongoQScript[T, ?]].plan[GenT[M, ?], WF, EX](joinHandler, funcHandler) ∘ (_ ∘ (_.mapR(normalize)))))
      wf1 <- log("Workflow (raw)", liftM[GenT[M, ?], Fix[WF]](WorkflowBuilder.build(wb)))
      wf2 <- log(
        "Workflow (crystallized)",
        Crystallize[WF].crystallize(wf1).point[GenT[M, ?]])
    } yield wf2).evalZero
  }

  /** Translate the QScript plan to an executable MongoDB "physical"
    * plan, taking into account the current runtime environment as captured by
    * the given context.
    * Internally, the type of the plan being built constrains which operators
    * can be used, but the resulting plan uses the largest, common type so that
    * callers don't need to worry about it.
    */
  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, M[_]: Monad]
    (logical: T[LogicalPlan], queryContext: fs.QueryContext[M])
    (implicit
      merr: MonadError_[M, FileSystemError],
      mtell: MonadTell_[M, PhaseResults])
      : M[Crystallized[WorkflowF]] = {
    import MongoQueryModel._

    queryContext.model match {
      case `3.2` =>
        val joinHandler =
          JoinHandler.fallback(
            JoinHandler.pipeline[Workflow3_2F](queryContext.statistics, queryContext.indexes),
            JoinHandler.mapReduce[Workflow3_2F])
        plan0[T, M, Workflow3_2F, Expr3_2](queryContext.listContents, joinHandler, FuncHandler.handle3_2[MapFunc[T, ?]])(logical)

      case `3.0`     =>
        val joinHandler = JoinHandler.mapReduce[Workflow2_6F]
        plan0[T, M, Workflow2_6F, Expr3_0](queryContext.listContents, joinHandler, FuncHandler.handle3_0[MapFunc[T, ?]])(logical).map(_.inject[WorkflowF])

      case _     =>
        val joinHandler = JoinHandler.mapReduce[Workflow2_6F]
        plan0[T, M, Workflow2_6F, Expr2_6](queryContext.listContents, joinHandler, FuncHandler.handle2_6[MapFunc[T, ?]])(logical).map(_.inject[WorkflowF])
    }
  }
}
