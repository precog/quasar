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
import quasar._, Planner._, Type.{Const => _, Coproduct => _, _}
import quasar.fp._, eitherT._
import quasar.fs.{FileSystemError, QueryFile}
import quasar.javascript._
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.namegen._
import quasar.physical.mongodb.WorkflowBuilder._
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.fs.listContents
import quasar.physical.mongodb.planner.{FuncHandler, JoinHandler, JoinSource, InputFinder, Here, There}
import quasar.physical.mongodb.workflow._
import quasar.qscript._
import quasar.std.StdLib._ // TODO: remove this
import javascript._

import matryoshka.{Hole => _, _}, Recursive.ops._, TraverseT.ops._
import matryoshka.patterns.CoEnv
import scalaz._, Scalaz._

object MongoDbQScriptPlanner {
  type Partial[In, Out] = (PartialFunction[List[In], Out], List[InputFinder])

  type OutputM[A] = PlannerError \/ A

  val exprFp = ExprOpCoreF.fixpoint[Fix, ExprOp]
  import exprFp._

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

  def processMapFuncExpr[T[_[_]]: Recursive: ShowT, EX[_]: Traverse, A](
    funcHandler: FuncHandler[T, EX])(
    fm: Free[MapFunc[T, ?],  A])(
    recovery: A => OutputM[Fix[ExprOp]])(
    implicit inj: EX :<: ExprOp):
      OutputM[Fix[ExprOp]] =
    freeCataM(fm)(
      interpretM[OutputM, MapFunc[T, ?], A, Fix[ExprOp]](
        recovery,
        expression(funcHandler)))

  def processMapFunc[T[_[_]]: Recursive: ShowT, A](
    fm: Free[MapFunc[T, ?],  A])(
    recovery: A => JsCore):
      OutputM[JsCore] =
    freeCataM(fm)(interpretM[OutputM, MapFunc[T, ?], A, JsCore](recovery(_).right, javascript))

  // TODO: Should have a JsFn version of this for $reduce nodes.
  val accumulator: ReduceFunc[Fix[ExprOp]] => AccumOp[Fix[ExprOp]] = {
    import quasar.qscript.ReduceFuncs._

    {
      case Arbitrary(a)     => $first(a)
      case Avg(a)           => $avg(a)
      case Count(_)         => $sum($literal(Bson.Int32(1)))
      case Max(a)           => $max(a)
      case Min(a)           => $min(a)
      case Sum(a)           => $sum(a)
      case UnshiftArray(a)  => $push(a)
      case UnshiftMap(k, v) => ???
    }
  }

  def expression[T[_[_]]: Recursive: ShowT, EX[_]: Traverse](
    funcHandler: FuncHandler[T, EX])(
      implicit inj: EX :<: ExprOp):
      AlgebraM[OutputM, MapFunc[T, ?], Fix[ExprOp]] = {
    import MapFuncs._

    val unimplemented = InternalError("unimplemented").left

    def unpack[T[_[_]]: Corecursive: Recursive, F[_]: Traverse](t: Free[F, T[F]]): T[F] =
      freeCata(t)(interpret[F, T[F], T[F]](ι, _.embed))

    def handleCommon(mf: MapFunc[T, Fix[ExprOp]]): Option[Fix[ExprOp]] =
      funcHandler.run(mf).map(t => unpack(t.mapSuspension(inj)))

    val handleSpecial: MapFunc[T, Fix[ExprOp]] => OutputM[Fix[ExprOp]] = {
      case Constant(v1) =>
        v1.cataM(BsonCodec.fromEJson).bimap(
          κ(NonRepresentableEJson(v1.shows)),
          $literal(_))

      case Date(a1) => unimplemented
      case Time(a1) => unimplemented
      case Timestamp(a1) => unimplemented
      case Interval(a1) => unimplemented

      // TODO: when each of these is broken out as a separate Func, these will
      // go to the funcHandler.
      case Extract(Embed($literalF(Bson.Text(field))), a2) => field match {
        case "century" => $divide($year(a2), $literal(Bson.Int32(100))).right
        case "day" => $dayOfMonth(a2).right
        case "decade" => $divide($year(a2), $literal(Bson.Int32(10))).right
        case "dow" => $add($dayOfWeek(a2), $literal(Bson.Int32(-1))).right
        case "doy" => $dayOfYear(a2).right
        // TODO: epoch
        case "hour" => $hour(a2).right
        case "isodow" =>
          $cond($eq($dayOfWeek(a2), $literal(Bson.Int32(1))),
            $literal(Bson.Int32(7)),
            $add($dayOfWeek(a2), $literal(Bson.Int32(-1)))).right
        // TODO: isoyear
        case "microseconds" =>
          $multiply($millisecond(a2), $literal(Bson.Int32(1000))).right
        case "millennium" =>
          $divide($year(a2), $literal(Bson.Int32(1000))).right
        case "milliseconds" => $millisecond(a2).right
        case "minute"       => $minute(a2).right
        case "month"        => $month(a2).right
        case "quarter"      => // TODO: handle leap years
          $add(
            $divide($dayOfYear(a2), $literal(Bson.Int32(92))),
            $literal(Bson.Int32(1))).right
        case "second"       => $second(a2).right
            // TODO: timezone, timezone_hour, timezone_minute
        case "week"         => $week(a2).right
        case "year"         => $year(a2).right
        case _              =>
          InternalError(field + " is not a valid time period").left
      }
      case IfUndefined(a1, a2) => unimplemented

      case Within(a1, a2) => unimplemented

      case Integer(a1) => unimplemented
      case Decimal(a1) => unimplemented
      case ToString(a1) => unimplemented

      case MakeArray(a1) => unimplemented
      case MakeMap(a1, a2) => unimplemented
      case ConcatMaps(a1, a2) => unimplemented
      case ProjectField($var(DocField(base)), $literal(Bson.Text(field))) =>
        $var(DocField(base \ BsonField.Name(field))).right
      case ProjectIndex(a1, a2)  => unimplemented
      case DeleteField(a1, a2)  => unimplemented

      // NB: This is maybe a NOP for Fix[ExprOp]s, as they (all?) safely
      //     short-circuit when given the wrong type. However, our guards may be
      //     more restrictive than the operation, in which case we still want to
      //     short-circuit, so …
      case Guard(_, _, cont, _) => cont.right

      case DupArrayIndices(_) => unimplemented
      case DupMapKeys(_)      => unimplemented
      case Range(_, _)        => unimplemented
      case ZipArrayIndices(_) => unimplemented
      case ZipMapKeys(_)      => unimplemented
    }

    mf => handleCommon(mf).cata(_.right, handleSpecial(mf))
  }

  def javascript[T[_[_]]: Recursive: ShowT]: AlgebraM[OutputM, MapFunc[T, ?], JsCore] = {
    import jscore.{
      Add => _, In => _,
      Lt => _, Lte => _, Gt => _, Gte => _, Eq => _, Neq => _,
      And => _, Or => _, Not => _,
      _}

    import MapFuncs._

    val unimplemented = InternalError("unimplemented").left

    {
      case Constant(v1) => v1.cata(Data.fromEJson).toJs \/> NonRepresentableEJson(v1.shows)
      // FIXME: Not correct
      case Undefined() => ident("undefined").right
      case Now() => unimplemented

      case Length(a1) =>
        Call(ident("NumberLong"), List(Select(a1, "length"))).right

      case Date(a1) =>
        If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.dateRegex + "$")))), "test"), List(a1)),
          Call(ident("ISODate"), List(a1)),
          ident("undefined")).right
      case Time(a1) =>
        If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.timeRegex + "$")))), "test"), List(a1)),
          a1,
          ident("undefined")).right
      case Timestamp(a1) =>
        If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.timestampRegex + "$")))), "test"), List(a1)),
          Call(ident("ISODate"), List(a1)),
          ident("undefined")).right
      case Interval(a1) => unimplemented
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
            pad3(Call(Select(ident("t"), "getUTCMilliseconds"), Nil)))).right
      }
      case ToTimestamp(a1) => unimplemented
      // FIXME: Handle non-constant strings as well
      case Extract(Literal(Js.Str(str)), x) =>
        str match {
          case "century"      => \/-(BinOp(Div, Call(Select(x, "getFullYear"), Nil), Literal(Js.Num(100, false))))
          case "day"          => \/-(Call(Select(x, "getDate"), Nil)) // (day of month)
          case "decade"       => \/-(BinOp(Div, Call(Select(x, "getFullYear"), Nil), Literal(Js.Num(10, false))))
          // NB: MongoDB's Date's getDay (during filtering at least) seems to
          //     be monday=0 ... sunday=6, apparently in violation of the
          // JavaScript convention.
          case "dow"          =>
            \/-(If(BinOp(jscore.Eq,
              Call(Select(x, "getDay"), Nil),
              Literal(Js.Num(6, false))),
              Literal(Js.Num(0, false)),
              BinOp(jscore.Add,
                Call(Select(x, "getDay"), Nil),
                Literal(Js.Num(1, false)))))
          // TODO: case "doy"          => \/- (unimplemented)
          // TODO: epoch
          case "hour"         => \/-(Call(Select(x, "getHours"), Nil))
          case "isodow"       =>
            \/-(BinOp(jscore.Add,
              Call(Select(x, "getDay"), Nil),
              Literal(Js.Num(1, false))))
          // TODO: isoyear
          case "microseconds" =>
            \/-(BinOp(Mult,
              BinOp(jscore.Add,
                Call(Select(x, "getMilliseconds"), Nil),
                BinOp(Mult, Call(Select(x, "getSeconds"), Nil), Literal(Js.Num(1000, false)))),
              Literal(Js.Num(1000, false))))
          case "millennium"   => \/-(BinOp(Div, Call(Select(x, "getFullYear"), Nil), Literal(Js.Num(1000, false))))
          case "milliseconds" =>
            \/-(BinOp(jscore.Add,
              Call(Select(x, "getMilliseconds"), Nil),
              BinOp(Mult, Call(Select(x, "getSeconds"), Nil), Literal(Js.Num(1000, false)))))
          case "minute"       => \/-(Call(Select(x, "getMinutes"), Nil))
          case "month"        =>
            \/-(BinOp(jscore.Add,
              Call(Select(x, "getMonth"), Nil),
              Literal(Js.Num(1, false))))
          case "quarter"      =>
            \/-(BinOp(jscore.Add,
              BinOp(BitOr,
                BinOp(Div,
                  Call(Select(x, "getMonth"), Nil),
                  Literal(Js.Num(3, false))),
                Literal(Js.Num(0, false))),
              Literal(Js.Num(1, false))))
          case "second"       => \/-(Call(Select(x, "getSeconds"), Nil))
          // TODO: timezone, timezone_hour, timezone_minute
          // case "week"         => \/- (unimplemented)
          case "year"         => \/-(Call(Select(x, "getFullYear"), Nil))

          case _ => -\/(FuncApply("extract", "valid time period", str))
        }

      case Negate(a1)       => UnOp(Neg, a1).right
      case Add(a1, a2)      => BinOp(jscore.Add, a1, a2).right
      case Multiply(a1, a2) => BinOp(Mult, a1, a2).right
      case Subtract(a1, a2) => BinOp(Sub, a1, a2).right
      case Divide(a1, a2)   => BinOp(Div, a1, a2).right
      case Modulo(a1, a2)   => BinOp(Mod, a1, a2).right
      case Power(a1, a2)    => Call(Select(ident("Math"), "pow"), List(a1, a2)).right

      case Not(a1)     => UnOp(jscore.Not, a1).right
      case Eq(a1, a2)  => BinOp(jscore.Eq, a1, a2).right
      case Neq(a1, a2) => BinOp(jscore.Neq, a1, a2).right
      case Lt(a1, a2)  => BinOp(jscore.Lt, a1, a2).right
      case Lte(a1, a2) => BinOp(jscore.Lte, a1, a2).right
      case Gt(a1, a2)  => BinOp(jscore.Gt, a1, a2).right
      case Gte(a1, a2) => BinOp(jscore.Gte, a1, a2).right
      case IfUndefined(a1, a2) =>
        // TODO: Only evaluate `value` once.
        If(BinOp(jscore.Eq, a1, ident("undefined")), a2, a1).right
      case And(a1, a2) => BinOp(jscore.And, a1, a2).right
      case Or(a1, a2)  => BinOp(jscore.Or, a1, a2).right
      case Coalesce(a1, a2) => unimplemented
      case Between(a1, a2, a3) =>
        Call(ident("&&"), List(
          Call(ident("<="), List(a2, a1)),
          Call(ident("<="), List(a1, a3)))).right
      case Cond(a1, a2, a3) => If(a1, a2, a3).right

      case Within(a1, a2) =>
        BinOp(jscore.Neq,
          Literal(Js.Num(-1, false)),
          Call(Select(a2, "indexOf"), List(a1))).right

      case Lower(a1) => Call(Select(a1, "toLowerCase"), Nil).right
      case Upper(a1) => Call(Select(a1, "toLUpperCase"), Nil).right
      case Bool(a1) =>
        If(BinOp(jscore.Eq, a1, Literal(Js.Str("true"))),
          Literal(Js.Bool(true)),
          If(BinOp(jscore.Eq, a1, Literal(Js.Str("false"))),
            Literal(Js.Bool(false)),
            ident("undefined"))).right
      case Integer(a1) =>
        If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.intRegex + "$")))), "test"), List(a1)),
          Call(ident("NumberLong"), List(a1)),
          ident("undefined")).right
      case Decimal(a1) =>
        If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + string.floatRegex + "$")))), "test"), List(a1)),
            Call(ident("parseFloat"), List(a1)),
            ident("undefined")).right
      case Null(a1) =>
        If(BinOp(jscore.Eq, a1, Literal(Js.Str("null"))),
          Literal(Js.Null),
          ident("undefined")).right
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
            Call(ident("String"), List(a1)))).right
      case Search(a1, a2, a3) =>
        Call(
          Select(
            New(Name("RegExp"), List(
              a2,
              If(a3, Literal(Js.Str("im")), Literal(Js.Str("m"))))),
            "test"),
          List(a1)).right
      case Substring(a1, a2, a3) =>
        Call(Select(a1, "substr"), List(a2, a3)).right
      // case ToId(a1) => Call(ident("ObjectId"), List(a1)).right

      case MakeArray(a1) => unimplemented
      case MakeMap(a1, a2) => unimplemented
      case ConcatArrays(a1, a2) => BinOp(jscore.Add, a1, a2).right
      case ConcatMaps(a1, a2) => unimplemented
      case ProjectField(a1, a2) => Access(a1, a2).right
      case ProjectIndex(a1, a2) => Access(a1, a2).right
      case DeleteField(a1, a2)  => unimplemented

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
        jsCheck(typ).fold[OutputM[JsCore]](
          InternalError("uncheckable type").left)(
          f => If(f(expr), cont, fallback).right)

      case DupArrayIndices(_) => unimplemented
      case DupMapKeys(_)      => unimplemented
      case Range(_, _)        => unimplemented
      case ZipArrayIndices(_) => unimplemented
      case ZipMapKeys(_)      => unimplemented
    }
  }

  /** Need this until the old connector goes away and we can redefine `Selector`
    * as `Selector[A, B]`, where `A` is the field type (naturally `BsonField`),
    * and `B` is the recursive parameter.
    */
  type PartialSelector = Partial[BsonField, Selector]

  /**
   * The selector phase tries to turn expressions into MongoDB selectors -- i.e.
   * Mongo query expressions. Selectors are only used for the filtering pipeline
   * op, so it's quite possible we build more stuff than is needed (but it
   * doesn't matter, unneeded annotations will be ignored by the pipeline
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
  def selector[T[_[_]]: Recursive: ShowT]:
      GAlgebra[(T[MapFunc[T, ?]], ?), MapFunc[T, ?], OutputM[PartialSelector]] = { node =>
    import MapFuncs._

    type Output = OutputM[PartialSelector]

    object IsBson {
      def unapply(v: (T[MapFunc[T, ?]], Output)): Option[Bson] =
        v._1.project match {
          case Constant(b) => b.cataM(BsonCodec.fromEJson).toOption
          // case InvokeFUnapply(Negate, Sized(Fix(ConstantF(Data.Int(i))))) => Some(Bson.Int64(-i.toLong))
          // case InvokeFUnapply(Negate, Sized(Fix(ConstantF(Data.Dec(x))))) => Some(Bson.Dec(-x.toDouble))
          // case InvokeFUnapply(ToId, Sized(Fix(ConstantF(Data.Str(str))))) => Bson.ObjectId(str).toOption
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
          case Constant(d @ Data.Date(_)) => Some(d)
          case _                         => None
        }
    }

    val relFunc: MapFunc[T, _] => Option[Bson => Selector.Condition] = {
      case Eq(_, _)  => Some(Selector.Eq)
      case Neq(_, _) => Some(Selector.Neq)
      case Lt(_, _)  => Some(Selector.Lt)
      case Lte(_, _) => Some(Selector.Lte)
      case Gt(_, _)  => Some(Selector.Gt)
      case Gte(_, _) => Some(Selector.Gte)
      case _         => None
    }

    val default: PartialSelector = (
      { case List(field) =>
        Selector.Doc(ListMap(
          field -> Selector.Expr(Selector.Eq(Bson.Bool(true)))))
      },
      List(Here))

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
            \/-(({ case List(f1) => Selector.Doc(ListMap(f1 -> Selector.Expr(f(v2)))) }, List(There(0, Here))))
          case (IsBson(v1), _) =>
            \/-(({ case List(f2) => Selector.Doc(ListMap(f2 -> Selector.Expr(r(v1)))) }, List(There(1, Here))))

          case (_, _) => -\/(InternalError(node.map(_._1).shows))
        }

      def relDateOp1(f: Bson.Date => Selector.Condition, date: Data.Date, g: Data.Date => Data.Timestamp, index: Int): Output =
        \/-((
          { case x :: Nil => Selector.Doc(x -> f(Bson.Date(g(date).value))) },
          List(There(index, Here))))

      def relDateOp2(conj: (Selector, Selector) => Selector, f1: Bson.Date => Selector.Condition, f2: Bson.Date => Selector.Condition, date: Data.Date, g1: Data.Date => Data.Timestamp, g2: Data.Date => Data.Timestamp, index: Int): Output =
        \/-((
          { case x :: Nil =>
            conj(
              Selector.Doc(x -> f1(Bson.Date(g1(date).value))),
              Selector.Doc(x -> f2(Bson.Date(g2(date).value))))
          },
          List(There(index, Here))))

      def invoke2Nel(x: Output, y: Output)(f: (Selector, Selector) => Selector):
          Output =
        (x ⊛ y) { case ((f1, p1), (f2, p2)) =>
          ({ case list =>
            f(f1(list.take(p1.size)), f2(list.drop(p1.size)))
          },
            p1.map(There(0, _)) ++ p2.map(There(1, _)))
        }

      val flip: MapFunc[T, _] => Option[MapFunc[T, _]] = {
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

      def reversibleRelop(x: (T[MapFunc[T, ?]], Output), y: (T[MapFunc[T, ?]], Output))(f: MapFunc[T, _]): Output =
        (relFunc(f) ⊛ flip(f).flatMap(relFunc))(relop(x, y)(_, _)).getOrElse(-\/(InternalError("couldn’t decipher operation")))

      func match {
        case Constant(_)        => \/-(default)

        case Gt(_, IsDate(d2))  => relDateOp1(Selector.Gte, d2, date.startOfNextDay, 0)
        case Lt(IsDate(d1), _)  => relDateOp1(Selector.Gte, d1, date.startOfNextDay, 1)

        case Lt(_, IsDate(d2))  => relDateOp1(Selector.Lt,  d2, date.startOfDay, 0)
        case Gt(IsDate(d1), _)  => relDateOp1(Selector.Lt,  d1, date.startOfDay, 1)

        case Gte(_, IsDate(d2)) => relDateOp1(Selector.Gte, d2, date.startOfDay, 0)
        case Lte(IsDate(d1), _) => relDateOp1(Selector.Gte, d1, date.startOfDay, 1)

        case Lte(_, IsDate(d2)) => relDateOp1(Selector.Lt,  d2, date.startOfNextDay, 0)
        case Gte(IsDate(d1), _) => relDateOp1(Selector.Lt,  d1, date.startOfNextDay, 1)

        case Eq(_, IsDate(d2)) => relDateOp2(Selector.And(_, _), Selector.Gte, Selector.Lt, d2, date.startOfDay, date.startOfNextDay, 0)
        case Eq(IsDate(d1), _) => relDateOp2(Selector.And(_, _), Selector.Gte, Selector.Lt, d1, date.startOfDay, date.startOfNextDay, 1)

        case Neq(_, IsDate(d2)) => relDateOp2(Selector.Or(_, _), Selector.Lt, Selector.Gte, d2, date.startOfDay, date.startOfNextDay, 0)
        case Neq(IsDate(d1), _) => relDateOp2(Selector.Or(_, _), Selector.Lt, Selector.Gte, d1, date.startOfDay, date.startOfNextDay, 1)

        case Eq(a, b)  => reversibleRelop(a, b)(func)
        case Neq(a, b) => reversibleRelop(a, b)(func)
        case Lt(a, b)  => reversibleRelop(a, b)(func)
        case Lte(a, b) => reversibleRelop(a, b)(func)
        case Gt(a, b)  => reversibleRelop(a, b)(func)
        case Gte(a, b) => reversibleRelop(a, b)(func)

        case Within(a, b) =>
          relop(a, b)(
            Selector.In.apply _,
            x => Selector.ElemMatch(\/-(Selector.In(Bson.Arr(List(x))))))

        case Search(_, IsText(patt), IsBool(b)) =>
          \/-(({ case List(f1) =>
            Selector.Doc(ListMap(f1 -> Selector.Expr(Selector.Regex(patt, b, true, false, false)))) },
            List(There(0, Here))))

        case Between(_, IsBson(lower), IsBson(upper)) =>
          \/-(({ case List(f) => Selector.And(
            Selector.Doc(f -> Selector.Gte(lower)),
            Selector.Doc(f -> Selector.Lte(upper)))
          },
            List(There(0, Here))))

        case And(a, b) => invoke2Nel(a._2, b._2)(Selector.And.apply _)
        case Or(a, b) => invoke2Nel(a._2, b._2)(Selector.Or.apply _)
        case Not((_, v)) =>
          v.map { case (sel, inputs) => (sel andThen (_.negate), inputs.map(There(0, _))) }

        case Guard(_, typ, cont, _) =>
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
          selCheck(typ).fold[OutputM[PartialSelector]](
            -\/(InternalError(node.map(_._1).shows)))(
            f =>
            \/-(cont._2.fold[PartialSelector](
              κ(({ case List(field) => f(field) }, List(There(0, Here)))),
              { case (f2, p2) =>
                ({ case head :: tail => Selector.And(f(head), f2(tail)) },
                  There(0, Here) :: p2.map(There(1, _)))
              })))

        case _ => -\/(InternalError(node.map(_._1).shows))
      }
    }

    invoke(node) <+> \/-(default)
  }

  trait Planner[F[_]] {
    type IT[G[_]]

    def plan[WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse](
      joinHandler: JoinHandler[WF, WorkflowBuilder.M],
      funcHandler: FuncHandler[IT, EX])(
          implicit ev0: WorkflowOpCoreF :<: WF,
                   ev1: Show[WorkflowBuilder[WF]],
                   ev2: WorkflowBuilder.Ops[WF],
                   ev3: EX :<: ExprOp):
        AlgebraM[StateT[OutputM, NameGen, ?], F, WorkflowBuilder[WF]]

    def unimplemented[WF[_]]: StateT[OutputM, NameGen, WorkflowBuilder[WF]] =
      StateT(κ((InternalError("unimplemented"): PlannerError).left[(NameGen, WorkflowBuilder[WF])]))

    def shouldNotBeReached[WF[_]]: StateT[OutputM, NameGen, WorkflowBuilder[WF]] =
      StateT(κ((InternalError("should not be reached"): PlannerError).left[(NameGen, WorkflowBuilder[WF])]))
  }

  object Planner {
    type Aux[T[_[_]], F[_]] = Planner[F] { type IT[G[_]] = T[G] }

    implicit def shiftedRead[T[_[_]]]: Planner.Aux[T, Const[ShiftedRead, ?]] =
      new Planner[Const[ShiftedRead, ?]] {
        type IT[G[_]] = T[G]
        def plan[WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse](
          joinHandler: JoinHandler[WF, WorkflowBuilder.M],
          funcHandler: FuncHandler[T, EX])(
          implicit ev0: WorkflowOpCoreF :<: WF,
                   ev1: Show[WorkflowBuilder[WF]],
                   WB: WorkflowBuilder.Ops[WF],
                   ev3: EX :<: ExprOp) =
          qs => Collection.fromFile(qs.getConst.path).bimap(PlanPathError(_): PlannerError, WB.read).liftM[GenT]
      }

    implicit def qscriptCore[T[_[_]]: Recursive: ShowT]:
        Planner.Aux[T, QScriptCore[T, ?]] =
      new Planner[QScriptCore[T, ?]] {
        type IT[G[_]] = T[G]
        def plan[WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse](
          joinHandler: JoinHandler[WF, WorkflowBuilder.M],
          funcHandler: FuncHandler[T, EX])(
          implicit ev0: WorkflowOpCoreF :<: WF,
                   ev1: Show[WorkflowBuilder[WF]],
                   WB: WorkflowBuilder.Ops[WF],
                   ev3: EX :<: ExprOp) = {
          case qscript.Map(src, f) => getExprBuilder[T, WF, EX](funcHandler)(src, f).liftM[GenT]
          case LeftShift(src, struct, repair) => unimplemented
          // (getExprBuilder(src, struct) ⊛ getJsMerge(repair))(
          //   (expr, jm) => WB.jsExpr(List(src, WB.flattenMap(expr)), jm))
          case Reduce(src, bucket, reducers, repair) =>
            (getExprBuilder(funcHandler)(src, bucket) ⊛
              reducers.traverse(_.traverse(getExpr(funcHandler))) ⊛
              getJsRed(repair))((b, red, rep) =>
              ExprBuilder(
                GroupBuilder(src,
                  List(b),
                  Contents.Doc(red.zipWithIndex.map(ai =>
                    (BsonField.Name(ai._2.toString),
                      accumulator(ai._1).left[Fix[ExprOp]])).toListMap)),
                rep.left)).liftM[GenT]
          case Sort(src, bucket, order) =>
            val (keys, dirs) = ((bucket, SortDir.Ascending) :: order).unzip
            keys.traverse(getExprBuilder(funcHandler)(src, _))
              .map(WB.sortBy(src, _, dirs)).liftM[GenT]
          case Filter(src, f) =>
            getExprBuilder(funcHandler)(src, f).map(cond =>
              WB.filter(src, List(cond), {
                case f :: Nil => Selector.Doc(f -> Selector.Eq(Bson.Bool(true)))
              })).liftM[GenT]
          case Union(src, lBranch, rBranch) => unimplemented
          case Take(src, from, count) =>
            (rebaseWB(joinHandler, funcHandler, from, src) ⊛
              (rebaseWB(joinHandler, funcHandler, count, src) >>= (HasInt(_).liftM[GenT])))(
              WB.limit)
          case Drop(src, from, count) =>
            (rebaseWB(joinHandler, funcHandler, from, src) ⊛
              (rebaseWB(joinHandler, funcHandler, count, src) >>= (HasInt(_).liftM[GenT])))(
              WB.skip)
          case Unreferenced() => ValueBuilder(Bson.Null).point[M]
        }
      }

    implicit def equiJoin[T[_[_]]: Recursive: ShowT]:
        Planner.Aux[T, EquiJoin[T, ?]] =
      new Planner[EquiJoin[T, ?]] {
        type IT[G[_]] = T[G]
        def plan[WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse](
          joinHandler: JoinHandler[WF, WorkflowBuilder.M],
          funcHandler: FuncHandler[T, EX])(
          implicit ev0: WorkflowOpCoreF :<: WF,
                   ev1: Show[WorkflowBuilder[WF]],
                   ev2: WorkflowBuilder.Ops[WF],
                   ev3: EX :<: ExprOp) =
          qs =>
        // FIXME: we should take advantage of the already merged srcs
        (rebaseWB(joinHandler, funcHandler, qs.lBranch, qs.src) ⊛
          rebaseWB(joinHandler, funcHandler, qs.rBranch, qs.src) ⊛
          getExprBuilder[T, WF, EX](funcHandler)(qs.src, qs.lKey).liftM[GenT] ⊛
          getExprBuilder[T, WF, EX](funcHandler)(qs.src, qs.rKey).liftM[GenT])(
          (lb, rb, lk, rk) =>
          joinHandler.run(
            qs.f match {
              case Inner => set.InnerJoin
              case FullOuter => set.FullOuterJoin
              case LeftOuter => set.LeftOuterJoin
              case RightOuter => set.RightOuterJoin
            },
            JoinSource(lb, List(lk), getJsFn(qs.lKey).toOption.map(List(_))),
            JoinSource(rb, List(rk), getJsFn(qs.rKey).toOption.map(List(_))))).join
      }

    implicit def coproduct[T[_[_]], F[_], G[_]](
      implicit F: Planner.Aux[T, F], G: Planner.Aux[T, G]):
        Planner.Aux[T, Coproduct[F, G, ?]] =
      new Planner[Coproduct[F, G, ?]] {
        type IT[G[_]] = T[G]
        def plan[WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse](
          joinHandler: JoinHandler[WF, WorkflowBuilder.M],
          funcHandler: FuncHandler[T, EX])(
          implicit ev0: WorkflowOpCoreF :<: WF,
                   ev1: Show[WorkflowBuilder[WF]],
                   ev2: WorkflowBuilder.Ops[WF],
                   ev3: EX :<: ExprOp) =
          _.run.fold(F.plan(joinHandler, funcHandler), G.plan(joinHandler, funcHandler))
      }


    // TODO: All instances below here only need to exist because of `FreeQS`, but
    //       can’t actually be called.

    implicit def deadEnd[T[_[_]]]: Planner.Aux[T, Const[DeadEnd, ?]] =
      new Planner[Const[DeadEnd, ?]] {
        type IT[G[_]] = T[G]
        def plan[WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse](
          joinHandler: JoinHandler[WF, WorkflowBuilder.M],
          funcHandler: FuncHandler[T, EX])(
          implicit ev0: WorkflowOpCoreF :<: WF,
                   ev1: Show[WorkflowBuilder[WF]],
                   ev2: WorkflowBuilder.Ops[WF],
                   ev3: EX :<: ExprOp) =
          κ(shouldNotBeReached)
      }

    implicit def read[T[_[_]]]: Planner.Aux[T, Const[Read, ?]] =
      new Planner[Const[Read, ?]] {
        type IT[G[_]] = T[G]
        def plan[WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse](
          joinHandler: JoinHandler[WF, WorkflowBuilder.M],
          funcHandler: FuncHandler[T, EX])(
          implicit ev0: WorkflowOpCoreF :<: WF,
                   ev1: Show[WorkflowBuilder[WF]],
                   ev2: WorkflowBuilder.Ops[WF],
                   ev3: EX :<: ExprOp) =
          κ(shouldNotBeReached)
      }

    implicit def thetaJoin[T[_[_]]]: Planner.Aux[T, ThetaJoin[T, ?]] =
      new Planner[ThetaJoin[T, ?]] {
        type IT[G[_]] = T[G]
        def plan[WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse](
          joinHandler: JoinHandler[WF, WorkflowBuilder.M],
          funcHandler: FuncHandler[T, EX])(
          implicit ev0: WorkflowOpCoreF :<: WF,
                   ev1: Show[WorkflowBuilder[WF]],
                   ev2: WorkflowBuilder.Ops[WF],
                   ev3: EX :<: ExprOp) =
          κ(shouldNotBeReached)
      }

    implicit def projectBucket[T[_[_]]]: Planner.Aux[T, ProjectBucket[T, ?]] =
      new Planner[ProjectBucket[T, ?]] {
        type IT[G[_]] = T[G]
        def plan[WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse](
          joinHandler: JoinHandler[WF, WorkflowBuilder.M],
          funcHandler: FuncHandler[T, EX])(
          implicit ev0: WorkflowOpCoreF :<: WF,
                   ev1: Show[WorkflowBuilder[WF]],
                   ev2: WorkflowBuilder.Ops[WF],
                   ev3: EX :<: ExprOp) =
          κ(shouldNotBeReached)
      }
  }

  def getExpr[T[_[_]]: Recursive: ShowT, EX[_]: Traverse](
    funcHandler: FuncHandler[T, EX])(
    fm: FreeMap[T])(
    implicit ev: EX :<: ExprOp): OutputM[Fix[ExprOp]] =
    processMapFuncExpr(funcHandler)(fm)(κ($$ROOT.right))

  def getJsFn[T[_[_]]: Recursive: ShowT](fm: FreeMap[T]): OutputM[JsFn] =
    processMapFunc(fm)(κ(jscore.Ident(JsFn.defaultName))) ∘ (JsFn(JsFn.defaultName, _))

  def getExprBuilder[T[_[_]]: Recursive: ShowT, WF[_], EX[_]: Traverse](
    funcHandler: FuncHandler[T, EX])(
    src: WorkflowBuilder[WF], fm: FreeMap[T])(
    implicit ev: EX :<: ExprOp):
      OutputM[WorkflowBuilder[WF]] =
    (getExpr(funcHandler)(fm).map(_.right[JsFn]) <+> getJsFn(fm).map(_.left[Fix[ExprOp]])) ∘
      (ExprBuilder(src, _))

  def getJsMerge[T[_[_]]: Recursive: ShowT](jf: JoinFunc[T], a1: JsCore, a2: JsCore):
      OutputM[JsFn] =
    processMapFunc(jf) {
      case LeftSide => a1
      case RightSide => a2
    } ∘ (JsFn(JsFn.defaultName, _))

  def getJsRed[T[_[_]]: Recursive: ShowT](jr: Free[MapFunc[T, ?], ReduceIndex]):
      OutputM[JsFn] =
    processMapFunc(jr)(ri => jscore.ident(ri.idx.toString)) ∘ (JsFn(JsFn.defaultName, _))

  def rebaseWB[T[_[_]], WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse](
    joinHandler: JoinHandler[WF, WorkflowBuilder.M],
    funcHandler: FuncHandler[T, EX],
    free: FreeQS[T],
    src: WorkflowBuilder[WF])(
    implicit F: Planner.Aux[T, QScriptTotal[T, ?]],
             ev0: WorkflowOpCoreF :<: WF,
             ev1: Show[WorkflowBuilder[WF]],
             ev2: WorkflowBuilder.Ops[WF],
             ev3: EX :<: ExprOp):
      StateT[OutputM, NameGen, WorkflowBuilder[WF]] =
    freeCataM(free)(
      interpretM[StateT[OutputM, NameGen, ?], QScriptTotal[T, ?], qscript.Hole, WorkflowBuilder[WF]](κ(StateT.stateT(src)), F.plan(joinHandler, funcHandler)))

  def HasLiteral[WF[_]]: WorkflowBuilder[WF] => OutputM[Bson] =
    wb => asLiteral(wb) \/> FuncApply("", "literal", wb.toString)

  def HasInt[WF[_]]: WorkflowBuilder[WF] => OutputM[Long] = HasLiteral(_) >>= {
    case Bson.Int32(v) => \/-(v.toLong)
    case Bson.Int64(v) => \/-(v)
    case x => -\/(FuncApply("", "64-bit integer", x.toString))
  }

  // This is maybe worth putting in Matryoshka?
  def findFirst[T[_[_]]: Recursive, F[_]: Functor: Foldable, A](
    f: PartialFunction[T[F], A]):
      CoalgebraM[A \/ ?, F, T[F]] =
    tf => (f.lift(tf) \/> tf.project).swap

  object Roll {
    def unapply[S[_]: Functor, A](obj: Free[S, A]): Option[S[Free[S, A]]] =
      obj.resume.swap.toOption
  }

  object Point {
    def unapply[S[_]: Functor, A](obj: Free[S, A]): Option[A] = obj.resume.toOption
  }

  def elideMoreGeneralGuards[T[_[_]]: Recursive](subType: Type):
      CoEnv[Hole, MapFunc[T, ?], T[CoEnv[Hole, MapFunc[T, ?], ?]]] =>
        PlannerError \/ CoEnv[Hole, MapFunc[T, ?], T[CoEnv[Hole, MapFunc[T, ?], ?]]] = {
    case free @ CoEnv(\/-(MapFuncs.Guard(Embed(CoEnv(-\/(SrcHole))), typ, cont, _))) =>
      if (typ.contains(subType)) cont.project.right
      else if (!subType.contains(typ))
        InternalError("can only contain " + subType + ", but a(n) " + typ + " is expected").left
      else free.right
    case x => x.right
  }

  private type QScript0[T[_[_]], A] = Coproduct[Const[Read, ?], QScriptCore[T, ?], A]
  type QScript[T[_[_]], A] = Coproduct[EquiJoin[T, ?], QScript0[T, ?], A]

  // TODO: Allow backends to provide a “Read” type to the typechecker, which
  //       represents the type of values that can be stored in a collection.
  //       E.g., for MongoDB, it would be `Map(String, Top)`. This will help us
  //       generate more correct PatternGuards in the first place, rather than
  //       trying to strip out unnecessary ones after the fact
  def assumeReadType[T[_[_]]: Recursive: Corecursive, F[_]: Functor](typ: Type)(
    implicit QC: QScriptCore[T, ?] :<: F, R: Const[Read, ?] :<: F):
      QScriptCore[T, T[F]] => PlannerError \/ F[T[F]] = {
    case m @ qscript.Map(src, mf) =>
      R.prj(src.project).fold(
        QC.inj(m).right[PlannerError])(
        κ(freeTransCataM(mf)(elideMoreGeneralGuards(typ)) ∘
          (mf => QC.inj(qscript.Map(src, mf)))))
    case qc => QC.inj(qc).right
  }

  type GenT[X[_], A]  = StateT[X, NameGen, A]

  def plan0[T[_[_]]: Recursive: Corecursive: EqualT: ShowT,
            WF[_]: Functor: Coalesce: Crush: Crystallize,
            EX[_]: Traverse](
    joinHandler: JoinHandler[WF, WorkflowBuilder.M],
    funcHandler: FuncHandler[T, EX])(
    lp: T[LogicalPlan])(
    implicit ev0: WorkflowOpCoreF :<: WF,
             ev1: Show[WorkflowBuilder[WF]],
             ev2: WorkflowBuilder.Ops[WF],
             ev3: EX :<: ExprOp,
             ev4: RenderTree[Fix[WF]]):
      EitherT[WriterT[MongoDbIO, PhaseResults, ?], FileSystemError, Crystallized[WF]] = {
    val optimize = new Optimize[T]

    // NB: Locally add state on top of the result monad so everything
    //     can be done in a single for comprehension.
    type PlanT[X[_], A] = EitherT[X, FileSystemError, A]
    type W[A]           = WriterT[MongoDbIO, PhaseResults, A]
    type F[A]           = PlanT[W, A]
    type M[A]           = GenT[F, A]

    def log[A: RenderTree](label: String)(ma: M[A]): M[A] =
      ma flatMap { a =>
        val result = PhaseResult.Tree(label, RenderTree[A].render(a))
        (WriterT((Vector(result): PhaseResults, a).point[MongoDbIO])).liftM[PlanT].liftM[GenT]
      }

    def swizzle[A](sa: StateT[PlannerError \/ ?, NameGen, A]): M[A] =
      StateT[F, NameGen, A](ng => EitherT(sa.run(ng).leftMap(FileSystemError.planningFailed(lp.convertTo[Fix], _)).point[W]))

    def liftError[A](ea: PlannerError \/ A): M[A] =
      EitherT(ea.leftMap(FileSystemError.planningFailed(lp.convertTo[Fix], _)).point[W]).liftM[GenT]

    val P = scala.Predef.implicitly[Planner.Aux[T, QScriptTotal[T, ?]]]

    val lc = listContents andThen (ss => EitherT(ss.run.liftM[PhaseResultT]))

    (for {
      qs  <- QueryFile.convertToQScript(some(lc))(lp).liftM[GenT]
      // TODO: also need to prefer projections over deletions
      // NB: right now this only outputs one phase, but it’d be cool if we could
      //     interleave phase building in the composed recursion scheme
      opt <- log("QScript (Mongo-specific)")(liftError(
        qs.transCataM[PlannerError \/ ?, QScriptTotal[T, ?]](tf =>
          (liftFGM(assumeReadType[T, QScriptTotal[T, ?]](Type.Obj(ListMap(), Some(Type.Top)))) ⋘
            liftFG(optimize.simplifyJoin[QScriptTotal[T, ?]])
          ).apply(tf) ∘
            Normalizable[QScriptTotal[T, ?]].normalize).map(transFutu(_)(ShiftRead[T, QScriptTotal[T, ?], QScriptTotal[T, ?]].shiftRead(idPrism.reverseGet)(_: QScriptTotal[T, T[QScriptTotal[T, ?]]])) ∘
            Normalizable[QScriptTotal[T, ?]].normalize)))
      wb  <- log("Workflow Builder")(swizzle(opt.cataM[StateT[OutputM, NameGen, ?], WorkflowBuilder[WF]](P.plan(joinHandler, funcHandler) ∘ (_ ∘ (_ ∘ normalize)))))
      wf1 <- log("Workflow (raw)")         (swizzle(WorkflowBuilder.build(wb)))
      wf2 <- log("Workflow (crystallized)")(Crystallize[WF].crystallize(wf1).point[M])
    } yield wf2).evalZero
  }

  /** Translate the QScript plan to an executable MongoDB "physical"
    * plan, taking into account the current runtime environment as captured by
    * the given context.
    * Internally, the type of the plan being built constrains which operators
    * can be used, but the resulting plan uses the largest, common type so that
    * callers don't need to worry about it.
    */
  def plan[T[_[_]]: Recursive: Corecursive: EqualT: ShowT](
    logical: T[LogicalPlan], queryContext: fs.QueryContext):
      EitherT[WriterT[MongoDbIO, PhaseResults, ?], FileSystemError, Crystallized[WorkflowF]] = {
    import MongoQueryModel._

    queryContext.model match {
      case `3.2` =>
        val joinHandler =
          JoinHandler.fallback(
            JoinHandler.pipeline[Workflow3_2F](queryContext.statistics, queryContext.indexes),
            JoinHandler.mapReduce[Workflow3_2F])
        plan0[T, Workflow3_2F, Expr3_2](joinHandler, FuncHandler.handle3_2)(logical)

      case `3.0`     =>
        val joinHandler = JoinHandler.mapReduce[Workflow2_6F]
        plan0[T, Workflow2_6F, Expr3_0](joinHandler, FuncHandler.handle3_0)(logical).map(_.inject[WorkflowF])

      case _     =>
        val joinHandler = JoinHandler.mapReduce[Workflow2_6F]
        plan0[T, Workflow2_6F, Expr2_6](joinHandler, FuncHandler.handle2_6)(logical).map(_.inject[WorkflowF])
    }
  }
}
