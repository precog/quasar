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
import quasar.fp._
import quasar.fs.mkAbsolute
import quasar.javascript._
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.namegen._
import quasar.qscript._
import quasar.std.StdLib._
import Type._
import Workflow._
import javascript._

import matryoshka._, Recursive.ops._, TraverseT.ops._
import org.threeten.bp.Instant
import pathy.Path.rootDir
import scalaz._, Scalaz._
import shapeless.{Data => _, :: => _, _}

object MongoDbPlanner {
  import LogicalPlan._
  import Planner._
  import WorkflowBuilder._

  import agg._
  import array._
  import date._
  import identity._
  import math._
  import relations._
  import set._
  import string._
  import structural._

  type Partial[In, Out] =
    (PartialFunction[List[In], Out], List[InputFinder])

  type OutputM[A] = PlannerError \/ A

  type PartialJs = Partial[JsFn, JsFn]

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
              (generateTypeCheck(or)(f)(a) |@| generateTypeCheck(or)(f)(b))(
                (a, b) => ((expr: In) => or(a(expr), b(expr))))
            case _ => None
          })(
          Some(_))


  val jsExprƒ: Algebra[LogicalPlan, OutputM[PartialJs]] = {
    type Output = OutputM[PartialJs]

    import jscore.{
      Add => _, In => _,
      Lt => _, Lte => _, Gt => _, Gte => _, Eq => _, Neq => _,
      And => _, Or => _, Not => _,
      _}

    val HasJs: Output => OutputM[PartialJs] =
      _ <+> \/-(({ case List(field) => field }, List(Here)))

    def invoke[N <: Nat](func: GenericFunc[N], args: Func.Input[Output, N]): Output = {
      val HasStr: Output => OutputM[String] = _.flatMap {
        _._1(Nil)(ident("_")) match {
          case Literal(Js.Str(str)) => str.right
          case x => FuncApply(func.name, "JS string", x.toString).left
        }
      }

      def Arity1(f: JsCore => JsCore): Output = args match {
        case Sized(a1) =>
          HasJs(a1).map {
            case (f1, p1) => ({ case list => JsFn(JsFn.defaultName, f(f1(list)(Ident(JsFn.defaultName)))) }, p1.map(There(0, _)))
          }
      }

      def Arity2(f: (JsCore, JsCore) => JsCore): Output =
        args match {
          case Sized(a1, a2) => (HasJs(a1) |@| HasJs(a2)) {
            case ((f1, p1), (f2, p2)) =>
              ({ case list => JsFn(JsFn.defaultName, f(f1(list.take(p1.size))(Ident(JsFn.defaultName)), f2(list.drop(p1.size))(Ident(JsFn.defaultName)))) },
                p1.map(There(0, _)) ++ p2.map(There(1, _)))
          }
        }

      def Arity3(f: (JsCore, JsCore, JsCore) => JsCore): Output = args match {
        case Sized(a1, a2, a3) => (HasJs(a1) |@| HasJs(a2) |@| HasJs(a3)) {
          case ((f1, p1), (f2, p2), (f3, p3)) =>
            ({ case list => JsFn(JsFn.defaultName, f(
              f1(list.take(p1.size))(Ident(JsFn.defaultName)),
              f2(list.drop(p1.size).take(p2.size))(Ident(JsFn.defaultName)),
              f3(list.drop(p1.size + p2.size))(Ident(JsFn.defaultName))))
            },
              p1.map(There(0, _)) ++ p2.map(There(1, _)) ++ p3.map(There(2, _)))
        }
      }

      def makeSimpleCall(func: String, args: List[JsCore]): JsCore =
        Call(ident(func), args)

      def makeSimpleBinop(op: BinaryOperator): Output =
        Arity2(BinOp(op, _, _))

      def makeSimpleUnop(op: UnaryOperator): Output =
        Arity1(UnOp(op, _))

      func match {
        case Constantly => Arity1(ι)
        case Length =>
          Arity1(expr => Call(ident("NumberLong"), List(Select(expr, "length"))))
        case Add      => makeSimpleBinop(jscore.Add)
        case Multiply => makeSimpleBinop(Mult)
        case Subtract => makeSimpleBinop(Sub)
        case Divide   => makeSimpleBinop(Div)
        case Modulo   => makeSimpleBinop(Mod)
        case Negate   => makeSimpleUnop(Neg)

        case Eq  => makeSimpleBinop(jscore.Eq)
        case Neq => makeSimpleBinop(jscore.Neq)
        case Lt  => makeSimpleBinop(jscore.Lt)
        case Lte => makeSimpleBinop(jscore.Lte)
        case Gt  => makeSimpleBinop(jscore.Gt)
        case Gte => makeSimpleBinop(jscore.Gte)
        case And => makeSimpleBinop(jscore.And)
        case Or  => makeSimpleBinop(jscore.Or)
        case Squash      => Arity1(v => v)
        case IfUndefined => Arity2((value, fallback) =>
          // TODO: Only evaluate `value` once.
          If(BinOp(jscore.Eq, value, ident("undefined")), fallback, value))
        case Not => makeSimpleUnop(jscore.Not)
        case Concat => makeSimpleBinop(jscore.Add)
        case In | Within =>
          Arity2((value, array) =>
            BinOp(jscore.Neq,
              Literal(Js.Num(-1, false)),
              Call(Select(array, "indexOf"), List(value))))
        case Substring =>
          Arity3((field, start, len) =>
            Call(Select(field, "substr"), List(start, len)))
        case Search =>
          Arity3((field, pattern, insen) =>
            Call(
              Select(
                New(Name("RegExp"), List(
                  pattern,
                  If(insen, Literal(Js.Str("im")), Literal(Js.Str("m"))))),
                "test"),
              List(field)))
        case Null => Arity1(str => If(BinOp(jscore.Eq, str, Literal(Js.Str("null"))), Literal(Js.Null), ident("undefined")))
        case Boolean => Arity1(str => If(BinOp(jscore.Eq, str, Literal(Js.Str("true"))), Literal(Js.Bool(true)), If(BinOp(jscore.Eq, str, Literal(Js.Str("false"))), Literal(Js.Bool(false)), ident("undefined"))))
        case Integer => Arity1(str =>
          If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + intRegex + "$")))), "test"), List(str)),
            Call(ident("NumberLong"), List(str)),
            ident("undefined")))
        case Decimal =>
          Arity1(str =>
            If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + floatRegex + "$")))), "test"), List(str)),
              Call(ident("parseFloat"), List(str)),
              ident("undefined")))
        case Date => Arity1(str =>
          If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + dateRegex + "$")))), "test"), List(str)),
            Call(ident("ISODate"), List(str)),
            ident("undefined")))
        case Time => Arity1(str =>
          If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + timeRegex + "$")))), "test"), List(str)),
            str,
            ident("undefined")))
        case Timestamp => Arity1(str =>
          If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + timestampRegex + "$")))), "test"), List(str)),
            Call(ident("ISODate"), List(str)),
            ident("undefined")))
        case ToString => Arity1(value =>
          If(isInt(value),
            // NB: This is a terrible way to turn an int into a string, but the
            //     only one that doesn’t involve converting to a decimal and
            //     losing precision.
            Call(Select(Call(ident("String"), List(value)), "replace"), List(
              Call(ident("RegExp"), List(
                Literal(Js.Str("[^-0-9]+")),
                Literal(Js.Str("g")))),
              Literal(Js.Str("")))),
            If(binop(jscore.Or, isTimestamp(value), isDate(value)),
              Call(Select(value, "toISOString"), Nil),
              Call(ident("String"), List(value)))))
        case Extract =>
          args match {
            case Sized(a1, a2) => (HasStr(a1) |@| HasJs(a2)) {
              case (field, (sel, inputs)) => ((field match {
                case "century"      => \/-(x => BinOp(Div, Call(Select(x, "getFullYear"), Nil), Literal(Js.Num(100, false))))
                case "day"          => \/-(x => Call(Select(x, "getDate"), Nil)) // (day of month)
                case "decade"       => \/-(x => BinOp(Div, Call(Select(x, "getFullYear"), Nil), Literal(Js.Num(10, false))))
                // Note: MongoDB's Date's getDay (during filtering at least) seems to be monday=0 ... sunday=6,
                // apparently in violation of the JavaScript convention.
                case "dow"          =>
                  \/-(x => If(BinOp(jscore.Eq,
                    Call(Select(x, "getDay"), Nil),
                    Literal(Js.Num(6, false))),
                    Literal(Js.Num(0, false)),
                    BinOp(jscore.Add,
                      Call(Select(x, "getDay"), Nil),
                      Literal(Js.Num(1, false)))))
                // TODO: case "doy"          => \/- (???)
                // TODO: epoch
                case "hour"         => \/-(x => Call(Select(x, "getHours"), Nil))
                case "isodow"       =>
                  \/-(x => BinOp(jscore.Add,
                    Call(Select(x, "getDay"), Nil),
                    Literal(Js.Num(1, false))))
                // TODO: isoyear
                case "microseconds" =>
                  \/-(x => BinOp(Mult,
                    BinOp(jscore.Add,
                      Call(Select(x, "getMilliseconds"), Nil),
                      BinOp(Mult, Call(Select(x, "getSeconds"), Nil), Literal(Js.Num(1000, false)))),
                    Literal(Js.Num(1000, false))))
                case "millennium"   => \/-(x => BinOp(Div, Call(Select(x, "getFullYear"), Nil), Literal(Js.Num(1000, false))))
                case "milliseconds" =>
                  \/-(x => BinOp(jscore.Add,
                    Call(Select(x, "getMilliseconds"), Nil),
                    BinOp(Mult, Call(Select(x, "getSeconds"), Nil), Literal(Js.Num(1000, false)))))
                case "minute"       => \/-(x => Call(Select(x, "getMinutes"), Nil))
                case "month"        =>
                  \/-(x => BinOp(jscore.Add,
                    Call(Select(x, "getMonth"), Nil),
                    Literal(Js.Num(1, false))))
                case "quarter"      =>
                  \/-(x => BinOp(jscore.Add,
                    BinOp(BitOr,
                      BinOp(Div,
                        Call(Select(x, "getMonth"), Nil),
                        Literal(Js.Num(3, false))),
                      Literal(Js.Num(0, false))),
                    Literal(Js.Num(1, false))))
                case "second"       => \/-(x => Call(Select(x, "getSeconds"), Nil))
                // TODO: timezone, timezone_hour, timezone_minute
                // case "week"         => \/- (???)
                case "year"         => \/-(x => Call(Select(x, "getFullYear"), Nil))

                case _ => -\/(FuncApply(func.name, "valid time period", field))
              }): PlannerError \/ (JsCore => JsCore)).map(x =>
                ({ case (list: List[JsFn]) => JsFn(JsFn.defaultName, x(sel(list)(Ident(JsFn.defaultName)))) },
                  inputs.map(There(1, _))): PartialJs)
            }.join
          }

        case TimeOfDay    => {
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
          Arity1(date =>
            Let(Name("t"), date,
              binop(jscore.Add,
                pad2(Call(Select(ident("t"), "getUTCHours"), Nil)),
                Literal(Js.Str(":")),
                pad2(Call(Select(ident("t"), "getUTCMinutes"), Nil)),
                Literal(Js.Str(":")),
                pad2(Call(Select(ident("t"), "getUTCSeconds"), Nil)),
                Literal(Js.Str(".")),
                pad3(Call(Select(ident("t"), "getUTCMilliseconds"), Nil)))))
        }

        case ToId => Arity1(id => Call(ident("ObjectId"), List(id)))
        case Between =>
          Arity3((value, min, max) =>
            makeSimpleCall(
              "&&",
              List(
                makeSimpleCall("<=", List(min, value)),
                makeSimpleCall("<=", List(value, max))))
          )
        case ObjectProject => Arity2(Access(_, _))
        case ArrayProject  => Arity2(Access(_, _))
        case MakeObject => args match {
          case Sized(a1, a2) => (HasStr(a1) |@| HasJs(a2)) {
            case (field, (sel, inputs)) =>
              (({ case (list: List[JsFn]) => JsFn(JsFn.defaultName, Obj(ListMap(Name(field) -> sel(list)(Ident(JsFn.defaultName))))) },
                inputs.map(There(1, _))): PartialJs)
          }
        }
        case DeleteField => args match {
          case Sized(a1, a2) => (HasJs(a1) |@| HasStr(a2)) {
            case ((sel, inputs), field) =>
              (({ case (list: List[JsFn]) => JsFn(JsFn.defaultName, Call(ident("remove"),
                List(sel(list)(Ident(JsFn.defaultName)), Literal(Js.Str(field))))) },
                inputs.map(There(0, _))): PartialJs)
          }
        }
        case MakeArray => Arity1(x => Arr(List(x)))
        case _ => -\/(UnsupportedFunction(func.name, "in JS planner".some))
      }
    }

    {
      case c @ ConstantF(x)     => x.toJs.map[PartialJs](js => ({ case Nil => JsFn.const(js) }, Nil)) \/> UnsupportedPlan(c, None)
      case InvokeF(f, a)    => invoke(f, a)
      case FreeF(_)         => \/-(({ case List(x) => x }, List(Here)))
      case LogicalPlan.LetF(_, _, body) => body
      case x @ TypecheckF(expr, typ, cont, fallback) =>
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
        jsCheck(typ).fold[OutputM[PartialJs]](
          -\/(UnsupportedPlan(x, None)))(
          f =>
          (HasJs(expr) |@| HasJs(cont) |@| HasJs(fallback)) {
            case ((f1, p1), (f2, p2), (f3, p3)) =>
              ({ case list => JsFn(JsFn.defaultName,
                If(f(f1(list.take(p1.size))(Ident(JsFn.defaultName))),
                  f2(list.drop(p1.size).take(p2.size))(Ident(JsFn.defaultName)),
                  f3(list.drop(p1.size + p2.size))(Ident(JsFn.defaultName))))
              },
                p1.map(There(0, _)) ++ p2.map(There(1, _)) ++ p3.map(There(2, _)))
          })
      case x => -\/(UnsupportedPlan(x, None))
    }
  }

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
  val selectorƒ:
      GAlgebra[(Fix[LogicalPlan], ?), LogicalPlan, OutputM[PartialSelector]] = { node =>
    type Output = OutputM[PartialSelector]

    object IsBson {
      def unapply(v: (Fix[LogicalPlan], Output)): Option[Bson] =
        v._1.unFix match {
          case ConstantF(b) => BsonCodec.fromData(b).toOption
          case InvokeFUnapply(Negate, Sized(Fix(ConstantF(Data.Int(i))))) => Some(Bson.Int64(-i.toLong))
          case InvokeFUnapply(Negate, Sized(Fix(ConstantF(Data.Dec(x))))) => Some(Bson.Dec(-x.toDouble))
          case InvokeFUnapply(ToId, Sized(Fix(ConstantF(Data.Str(str))))) => Bson.ObjectId(str).toOption
          case _ => None
        }
    }

    object IsBool {
      def unapply(v: (Fix[LogicalPlan], Output)): Option[Boolean] =
        v match {
          case IsBson(Bson.Bool(b)) => b.some
          case _                    => None
        }
    }

    object IsText {
      def unapply(v: (Fix[LogicalPlan], Output)): Option[String] =
        v match {
          case IsBson(Bson.Text(str)) => Some(str)
          case _                      => None
        }
    }

    object IsDate {
      def unapply(v: (Fix[LogicalPlan], Output)): Option[Data.Date] =
        v._1.unFix match {
          case ConstantF(d @ Data.Date(_)) => Some(d)
          case _                           => None
        }
    }

    def relFunc(f: GenericFunc[_]): Option[Bson => Selector.Condition] = f match {
      case Eq  => Some(Selector.Eq)
      case Neq => Some(Selector.Neq)
      case Lt  => Some(Selector.Lt)
      case Lte => Some(Selector.Lte)
      case Gt  => Some(Selector.Gt)
      case Gte => Some(Selector.Gte)
      case _   => None
    }

    def invoke[N <: Nat](func: GenericFunc[N], args: Func.Input[(Fix[LogicalPlan], Output), N]): Output = {
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
      def relop(f: Bson => Selector.Condition, r: Bson => Selector.Condition): Output = args match {
        case Sized(_, IsBson(v2)) =>
          \/-(({ case List(f1) => Selector.Doc(ListMap(f1 -> Selector.Expr(f(v2)))) }, List(There(0, Here))))
        case Sized(IsBson(v1), _) =>
          \/-(({ case List(f2) => Selector.Doc(ListMap(f2 -> Selector.Expr(r(v1)))) }, List(There(1, Here))))

        case _ => -\/(UnsupportedPlan(node, None))
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

      def stringOp(f: String => Selector.Condition, arg: (Fix[LogicalPlan], Output)): Output =
        arg match {
          case IsText(str2) =>  \/-(({ case List(f1) => Selector.Doc(ListMap(f1 -> Selector.Expr(f(str2)))) }, List(There(0, Here))))
          case _            => -\/ (UnsupportedPlan(node, None))
        }

      def invoke2Nel(f: (Selector, Selector) => Selector): Output = {
        val Sized(x, y) = args.map(_._2)

        (x |@| y) { case ((f1, p1), (f2, p2)) =>
          ({ case list =>
            f(f1(list.take(p1.size)), f2(list.drop(p1.size)))
          },
            p1.map(There(0, _)) ++ p2.map(There(1, _)))
        }
      }

      def reversibleRelop(f: GenericFunc[nat._2]): Output =
        (relFunc(f) |@| flip(f).flatMap(relFunc))(relop).getOrElse(-\/(InternalError("couldn’t decipher operation")))

      (func, args) match {
        case (Gt, Sized(_, IsDate(d2)))  => relDateOp1(Selector.Gte, d2, date.startOfNextDay, 0)
        case (Lt, Sized(IsDate(d1), _))  => relDateOp1(Selector.Gte, d1, date.startOfNextDay, 1)

        case (Lt, Sized(_, IsDate(d2)))  => relDateOp1(Selector.Lt,  d2, date.startOfDay, 0)
        case (Gt, Sized(IsDate(d1), _))  => relDateOp1(Selector.Lt,  d1, date.startOfDay, 1)

        case (Gte, Sized(_, IsDate(d2))) => relDateOp1(Selector.Gte, d2, date.startOfDay, 0)
        case (Lte, Sized(IsDate(d1), _)) => relDateOp1(Selector.Gte, d1, date.startOfDay, 1)

        case (Lte, Sized(_, IsDate(d2))) => relDateOp1(Selector.Lt,  d2, date.startOfNextDay, 0)
        case (Gte, Sized(IsDate(d1), _)) => relDateOp1(Selector.Lt,  d1, date.startOfNextDay, 1)

        case (Eq, Sized(_, IsDate(d2))) => relDateOp2(Selector.And(_, _), Selector.Gte, Selector.Lt, d2, date.startOfDay, date.startOfNextDay, 0)
        case (Eq, Sized(IsDate(d1), _)) => relDateOp2(Selector.And(_, _), Selector.Gte, Selector.Lt, d1, date.startOfDay, date.startOfNextDay, 1)

        case (Neq, Sized(_, IsDate(d2))) => relDateOp2(Selector.Or(_, _), Selector.Lt, Selector.Gte, d2, date.startOfDay, date.startOfNextDay, 0)
        case (Neq, Sized(IsDate(d1), _)) => relDateOp2(Selector.Or(_, _), Selector.Lt, Selector.Gte, d1, date.startOfDay, date.startOfNextDay, 1)

        case (Eq, _)  => reversibleRelop(Eq)
        case (Neq, _) => reversibleRelop(Neq)
        case (Lt, _)  => reversibleRelop(Lt)
        case (Lte, _) => reversibleRelop(Lte)
        case (Gt, _)  => reversibleRelop(Gt)
        case (Gte, _) => reversibleRelop(Gte)

        case (In | Within, _)  =>
          relop(
            Selector.In.apply _,
            x => Selector.ElemMatch(\/-(Selector.In(Bson.Arr(List(x))))))

        case (Search, Sized(_, patt, IsBool(b))) =>
          stringOp(Selector.Regex(_, b, true, false, false), patt)

        case (Between, Sized(_, IsBson(lower), IsBson(upper))) =>
          \/-(({ case List(f) => Selector.And(
            Selector.Doc(f -> Selector.Gte(lower)),
            Selector.Doc(f -> Selector.Lte(upper)))
          },
            List(There(0, Here))))
        case (Between, _) => -\/(UnsupportedPlan(node, None))

        case (And, _) => invoke2Nel(Selector.And.apply _)
        case (Or, _) => invoke2Nel(Selector.Or.apply _)
        case (Not, Sized((_, v))) =>
          v.map { case (sel, inputs) => (sel andThen (_.negate), inputs.map(There(0, _))) }

        case (Constantly, Sized(const, _)) => const._2

        case _ => -\/(UnsupportedFunction(func.name, "in Selector planner".some))
      }
    }

    val default: PartialSelector = (
      { case List(field) =>
        Selector.Doc(ListMap(
          field -> Selector.Expr(Selector.Eq(Bson.Bool(true)))))
      },
      List(Here))

    node match {
      case ConstantF(_)   => \/-(default)
      case InvokeF(f, a)  => invoke(f, a) <+> \/-(default)
      case LetF(_, _, in) => in._2
      case TypecheckF(_, typ, cont, _) =>
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
          -\/(UnsupportedPlan(node, None)))(
          f =>
          \/-(cont._2.fold[PartialSelector](
            κ(({ case List(field) => f(field) }, List(There(0, Here)))),
            { case (f2, p2) =>
              ({ case head :: tail => Selector.And(f(head), f2(tail)) },
                There(0, Here) :: p2.map(There(1, _)))
            })))
      case _ => -\/(UnsupportedPlan(node, None))
    }
  }

  def workflowƒ[F[_]: Functor: Coalesce: Crush: Crystallize]
    (implicit I: WorkflowOpCoreF :<: F, ev: Show[WorkflowBuilder[F]], WB: WorkflowBuilder.Ops[F])  // FIXME: don't need first two?
    : LogicalPlan[
        Cofree[LogicalPlan, (
          (OutputM[PartialSelector],
           OutputM[PartialJs]),
          OutputM[WorkflowBuilder[F]])]] =>
      State[NameGen, OutputM[Fix[WorkflowBuilderF[F, ?]]]] = {
    import WorkflowBuilder._
    import quasar.physical.mongodb.accumulator._
    import quasar.physical.mongodb.expression._

    type Input  = (OutputM[PartialSelector], OutputM[PartialJs])
    type Output = M[WorkflowBuilder[F]]
    type Ann    = Cofree[LogicalPlan, (Input, OutputM[WorkflowBuilder[F]])]

    import LogicalPlan._

    object HasData {
      def unapply(node: LogicalPlan[Ann]): Option[Data] = node match {
        case LogicalPlan.ConstantF(data) => Some(data)
        case _                           => None
      }
    }

    val HasKeys: Ann => OutputM[List[WorkflowBuilder[F]]] = {
      case MakeArrayN.Attr(array) => array.traverse(_.head._2)
      case n                      => n.head._2.map(List(_))
    }

    val HasSortDirs: Ann => OutputM[List[SortDir]] = {
      def isSortDir(node: LogicalPlan[Ann]): OutputM[SortDir] =
        node match {
          case HasData(Data.Str("ASC"))  => \/-(SortDir.Ascending)
          case HasData(Data.Str("DESC")) => \/-(SortDir.Descending)
          case x => -\/(InternalError("malformed sort dir: " + x))
        }

      _ match {
        case MakeArrayN.Attr(array) =>
          array.traverse(d => isSortDir(d.tail))
        case Cofree(_, ConstantF(Data.Arr(dirs))) =>
          dirs.traverse(d => isSortDir(ConstantF(d)))
        case n => isSortDir(n.tail).map(List(_))
      }
    }

    val HasSelector: Ann => OutputM[PartialSelector] = _.head._1._1

    val HasJs: Ann => OutputM[PartialJs] = _.head._1._2

    val HasWorkflow: Ann => OutputM[WorkflowBuilder[F]] = _.head._2

    def invoke[N <: Nat](func: GenericFunc[N], args: Func.Input[Ann, N]): Output = {

      val HasLiteral: Ann => OutputM[Bson] = ann => HasWorkflow(ann).flatMap { p =>
        asLiteral(p) match {
          case Some(value) => \/-(value)
          case _           => -\/(FuncApply(func.name, "literal", p.toString))
        }
      }

      val HasInt: Ann => OutputM[Long] = HasLiteral(_).flatMap {
        case Bson.Int32(v) => \/-(v.toLong)
        case Bson.Int64(v) => \/-(v)
        case x => -\/(FuncApply(func.name, "64-bit integer", x.toString))
      }

      val HasText: Ann => OutputM[String] = HasLiteral(_).flatMap {
        case Bson.Text(v) => \/-(v)
        case x => -\/(FuncApply(func.name, "text", x.toString))
      }

      def Arity1[A](f: Ann => OutputM[A]): OutputM[A] = args match {
        case Sized(a1) => f(a1)
      }

      def Arity2[A, B](f1: Ann => OutputM[A], f2: Ann => OutputM[B]): OutputM[(A, B)] = args match {
        case Sized(a1, a2) => (f1(a1) |@| f2(a2))((_, _))
      }

      def Arity3[A, B, C](f1: Ann => OutputM[A], f2: Ann => OutputM[B], f3: Ann => OutputM[C]):
          OutputM[(A, B, C)] = args match {
        case Sized(a1, a2, a3) => (f1(a1) |@| f2(a2) |@| f3(a3))((_, _, _))
      }

      def expr1(f: Expression => Expression): Output =
        lift(Arity1(HasWorkflow)).flatMap(WB.expr1(_)(f))

      def groupExpr1(f: Expression => Accumulator): Output =
        lift(Arity1(HasWorkflow).map(WB.reduce(_)(f)))

      def mapExpr(p: WorkflowBuilder[F])(f: Expression => Expression): Output =
        WB.expr1(p)(f)

      def expr2[A](f: (Expression, Expression) => Expression): Output =
        lift(Arity2(HasWorkflow, HasWorkflow)).flatMap {
          case (p1, p2) => WB.expr2(p1, p2)(f)
        }

      def expr3(f: (Expression, Expression, Expression) => Expression): Output =
        lift(Arity3(HasWorkflow, HasWorkflow, HasWorkflow)).flatMap {
          case (p1, p2, p3) => WB.expr(List(p1, p2, p3)) {
            case List(e1, e2, e3) => f(e1, e2, e3)
          }
        }

      def makeKeys(k: List[Ann], comp: Ann):
          Option[List[JsFn]] =
        k.traverse(HasJs).flatMap(k =>
          findArgs(k, comp).map(applyPartials(k, _))).toOption

      /** Check for any values used in a selector which reach into the
        * "consequent" branches of Typecheck or Cond nodes, and which
        * involve expressions that are not safe to evaluate before
        * evaluating the typecheck/condition. Using such values in
        * selectors causes runtime errors when the expression ends up
        * in a $project or $simpleMap prior to $match.
        */
      def breaksEvalOrder(ann: Cofree[LogicalPlan, OutputM[WorkflowBuilder[F]]], f: InputFinder): Boolean = {
        def isSimpleRef =
          f(ann).fold(
            κ(true),
            _.unFix match {
              case ExprBuilderF(_, \/-($var(_))) => true
              case _ => false
            })

        (ann.tail, f) match {
          case (TypecheckF(_, _, _, _), There(1, _)) => !isSimpleRef
          case (InvokeFUnapply(Cond, _), There(x, _))
              if x == 1 || x == 2                    => !isSimpleRef
          case _                                     => false
        }
      }

      func match {
        case MakeArray => lift(Arity1(HasWorkflow).map(WB.makeArray))
        case MakeObject =>
          lift(Arity2(HasText, HasWorkflow).map {
            case (name, wf) => WB.makeObject(wf, name)
          })
        case ObjectConcat =>
          lift(Arity2(HasWorkflow, HasWorkflow)).flatMap((WB.objectConcat(_, _)).tupled)
        case ArrayConcat =>
          lift(Arity2(HasWorkflow, HasWorkflow)).flatMap((WB.arrayConcat(_, _)).tupled)
        case Filter =>
          args match {
            case Sized(a1, a2) =>
              lift(HasWorkflow(a1).flatMap(wf => {
                val on = a2.map(_._2)
                HasSelector(a2).flatMap { case (sel, inputs) =>
                  if (!inputs.exists(breaksEvalOrder(on, _)))
                    inputs.traverse(_(on)).map(WB.filter(wf, _, sel))
                  else
                    HasWorkflow(a2).map(wf2 => WB.filter(wf, List(wf2), {
                      case f :: Nil => Selector.Doc(f -> Selector.Eq(Bson.Bool(true)))
                    }))
                } <+>
                  HasJs(a2).flatMap(js =>
                    // TODO: have this pass the JS args as the list of inputs … but right now, those inputs get converted to BsonFields, not ExprOps.
                    js._2.traverse(_(on)).map(args => WB.filter(wf, Nil, { case Nil => Selector.Where(js._1(args.map(κ(JsFn.identity)))(jscore.ident("this")).toJs) })))
              }))
          }
        case Drop =>
          lift(Arity2(HasWorkflow, HasInt).map((WB.skip(_, _)).tupled))
        case Take =>
          lift(Arity2(HasWorkflow, HasInt).map((WB.limit(_, _)).tupled))
        case InnerJoin | LeftOuterJoin | RightOuterJoin | FullOuterJoin =>
          args match {
            case Sized(left, right, comp) =>
              splitConditions(comp).fold[M[WorkflowBuilder[F]]](
                fail(UnsupportedJoinCondition(Recursive[Cofree[?[_], (Input, OutputM[WorkflowBuilder[F]])]].convertTo[LogicalPlan, Fix](comp))))(
                c => {
                  val (leftKeys, rightKeys) = c.unzip
                  lift((HasWorkflow(left) |@|
                    HasWorkflow(right) |@|
                    leftKeys.traverse(HasWorkflow) |@|
                    rightKeys.traverse(HasWorkflow))((l, r, lk, rk) =>
                    join(l, r, func, lk, makeKeys(leftKeys, comp), rk, makeKeys(rightKeys, comp)))).join
                })
          }
        case GroupBy =>
          lift(Arity2(HasWorkflow, HasKeys).map((WB.groupBy(_, _)).tupled))
        case OrderBy =>
          lift(Arity3(HasWorkflow, HasKeys, HasSortDirs).map {
            case (p1, p2, dirs) => WB.sortBy(p1, p2, dirs)
          })

        case Constantly => expr2((v, s) => v)

        case Add        => expr2($add(_, _))
        case Multiply   => expr2($multiply(_, _))
        case Subtract   => expr2($subtract(_, _))
        case Divide     => expr2($divide(_, _))
        case Modulo     => expr2($mod(_, _))
        case Negate     => expr1($multiply($literal(Bson.Int32(-1)), _))

        case Eq         => expr2($eq(_, _))
        case Neq        => expr2($neq(_, _))
        case Lt         => expr2($lt(_, _))
        case Lte        => expr2($lte(_, _))
        case Gt         => expr2($gt(_, _))
        case Gte        => expr2($gte(_, _))

        case Coalesce   => expr2($ifNull(_, _))

        case Concat     => expr2($concat(_, _))
        case Lower      => expr1($toLower(_))
        case Upper      => expr1($toUpper(_))
        case Substring  => expr3($substr(_, _, _))

        case Cond       => expr3($cond(_, _, _))

        case Count      => groupExpr1(κ($sum($literal(Bson.Int32(1)))))
        case Sum        => groupExpr1($sum(_))
        case Avg        => groupExpr1($avg(_))
        case Min        => groupExpr1($min(_))
        case Max        => groupExpr1($max(_))
        case Arbitrary  => groupExpr1($first(_))

        case Or         => expr2($or(_, _))
        case And        => expr2($and(_, _))
        case Not        => expr1($not(_))

        case ArrayLength =>
          lift(Arity2(HasWorkflow, HasInt)).flatMap {
            case (p, 1)   => mapExpr(p)($size(_))
            case (_, dim) => fail(FuncApply(func.name, "lower array dimension", dim.toString))
          }

        case Extract   =>
          lift(Arity2(HasText, HasWorkflow)).flatMap {
            case (field, p) =>
              field match {
                case "century"      =>
                  mapExpr(p)(v => $divide($year(v), $literal(Bson.Int32(100))))
                case "day"          => mapExpr(p)($dayOfMonth(_))
                case "decade"       =>
                  mapExpr(p)(x => $divide($year(x), $literal(Bson.Int32(10))))
                case "dow"          =>
                  mapExpr(p)(x => $add($dayOfWeek(x), $literal(Bson.Int32(-1))))
                case "doy"          => mapExpr(p)($dayOfYear(_))
                // TODO: epoch
                case "hour"         => mapExpr(p)($hour(_))
                case "isodow"       => mapExpr(p)(x =>
                  $cond($eq($dayOfWeek(x), $literal(Bson.Int32(1))),
                    $literal(Bson.Int32(7)),
                    $add($dayOfWeek(x), $literal(Bson.Int32(-1)))))
                // TODO: isoyear
                case "microseconds" =>
                  mapExpr(p)(v =>
                    $multiply($millisecond(v), $literal(Bson.Int32(1000))))
                case "millennium"   =>
                  mapExpr(p)(v => $divide($year(v), $literal(Bson.Int32(1000))))
                case "milliseconds" => mapExpr(p)($millisecond(_))
                case "minute"       => mapExpr(p)($minute(_))
                case "month"        => mapExpr(p)($month(_))
                case "quarter"      => // TODO: handle leap years
                  mapExpr(p)(v =>
                    $add(
                      $divide($dayOfYear(v), $literal(Bson.Int32(92))),
                      $literal(Bson.Int32(1))))
                case "second"       => mapExpr(p)($second(_))
                // TODO: timezone, timezone_hour, timezone_minute
                case "week"         => mapExpr(p)($week(_))
                case "year"         => mapExpr(p)($year(_))
                case _              => fail(FuncApply(func.name, "valid time period", field))
              }
          }

        case Null => expr1(str =>
          $cond($eq(str, $literal(Bson.Text("null"))),
            $literal(Bson.Null),
            $literal(Bson.Undefined)))

        case Boolean => expr1(str =>
          $cond($eq(str, $literal(Bson.Text("true"))),
            $literal(Bson.Bool(true)),
            $cond($eq(str, $literal(Bson.Text("false"))),
              $literal(Bson.Bool(false)),
              $literal(Bson.Undefined))))

        // TODO: If we had the type available, this could be more efficient in
        //       cases where we have a more restricted type. And right now we
        //       can’t use this, because it doesn’t cover every type.
        // case ToString => expr1(value =>
        //   $cond(Check.isNull(value), $literal(Bson.Text("null")),
        //     $cond(Check.isString(value), value,
        //       $cond($eq(value, $literal(Bson.Bool(true))), $literal(Bson.Text("true")),
        //         $cond($eq(value, $literal(Bson.Bool(false))), $literal(Bson.Text("false")),
        //           $literal(Bson.Undefined))))))

        case ToTimestamp => expr1($add($literal(Bson.Date(Instant.ofEpochMilli(0))), _))

        case ToId        =>
          lift(Arity1(HasText).flatMap(str =>
            BsonCodec.fromData(Data.Id(str)).map(WB.pure)))

        case Between       => expr3((x, l, u) => $and($lte(l, x), $lte(x, u)))

        case ObjectProject =>
          lift(Arity2(HasWorkflow, HasText).flatMap((WB.projectField(_, _)).tupled))
        case ArrayProject =>
          lift(Arity2(HasWorkflow, HasInt).flatMap {
            case (p, index) => WB.projectIndex(p, index.toInt)
          })
        case DeleteField  =>
          lift(Arity2(HasWorkflow, HasText).flatMap((WB.deleteField(_, _)).tupled))
        case FlattenMap   => lift(Arity1(HasWorkflow).map(WB.flattenMap(_)))
        case FlattenArray => lift(Arity1(HasWorkflow).map(WB.flattenArray(_)))
        case Squash       => lift(Arity1(HasWorkflow).map(WB.squash))
        case Distinct     =>
          lift(Arity1(HasWorkflow)).flatMap(WB.distinct(_))
        case DistinctBy   =>
          lift(Arity2(HasWorkflow, HasKeys)).flatMap((WB.distinctBy(_, _)).tupled)

        case _ => fail(UnsupportedFunction(func.name, "in workflow planner".some))
      }
    }

    def splitConditions: Ann => Option[List[(Ann, Ann)]] = _.tail match {
      case InvokeFUnapply(relations.And, terms) =>
        terms.unsized.traverse(splitConditions).map(_.concatenate)
      case InvokeFUnapply(relations.Eq, Sized(left, right)) => Some(List((left, right)))
      case ConstantF(Data.Bool(true)) => Some(List())
      case _ => None
    }

    def findArgs(partials: List[PartialJs], comp: Ann):
        OutputM[List[List[WorkflowBuilder[F]]]] =
      partials.traverse(_._2.traverse(_(comp.map(_._2))))

    def applyPartials(partials: List[PartialJs], args: List[List[WorkflowBuilder[F]]]):
        List[JsFn] =
      (partials zip args).map(l => l._1._1(l._2.map(κ(JsFn.identity))))

    // Tricky: It's easier to implement each step using StateT[\/, ...], but we
    // need the fold’s Monad to be State[..., \/], so that the morphism
    // flatMaps over the State but not the \/. That way it can evaluate to left
    // for an individual node without failing the fold. This code takes care of
    // mapping from one to the other.
    node => node match {
      case ReadF(path) =>
        // Documentation on `QueryFile` guarantees absolute paths, so calling `mkAbsolute`
        state(Collection.fromFile(mkAbsolute(rootDir, path)).bimap(PlanPathError, WB.read))
      case ConstantF(data) =>
        state(BsonCodec.fromData(data).bimap(
          κ(NonRepresentableData(data)),
          WB.pure))
      case InvokeF(func, args) =>
        val v = invoke(func, args) <+>
          lift(jsExprƒ(node.map(HasJs))).flatMap(pjs =>
            lift(pjs._2.traverse(_(Cofree(UnsupportedPlan(node, None).left, node.map(_.map(_._2)))))).flatMap(args =>
              WB.jsExpr(args, x => pjs._1(x.map(JsFn.const))(jscore.Ident(JsFn.defaultName)))))
        State(s => v.run(s).fold(e => s -> -\/(e), t => t._1 -> \/-(t._2)))
      case FreeF(name) =>
        state(-\/(InternalError("variable " + name + " is unbound")))
      case LetF(_, _, in) => state(in.head._2)
      case TypecheckF(exp, typ, cont, fallback) =>
        // NB: Even if certain checks aren’t needed by ExprOps, we have to
        //     maintain them because we may convert ExprOps to JS.
        //     Hopefully BlackShield will eliminate the need for this.
        def exprCheck: Type => Option[Expression => Expression] =
          generateTypeCheck[Expression, Expression]($or(_, _)) {
            case Type.Null => ((expr: Expression) => $eq($literal(Bson.Null), expr))
            case Type.Int
               | Type.Dec
               | Type.Int ⨿ Type.Dec
               | Type.Int ⨿ Type.Dec ⨿ Type.Interval => Check.isNumber
            case Type.Str => Check.isString
            case Type.Obj(map, _) =>
              ((expr: Expression) => {
                val basic = Check.isObject(expr)
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
            case Type.FlexArr(_, _, _) => Check.isArray
            case Type.Binary => Check.isBinary
            case Type.Id => Check.isId
            case Type.Bool => Check.isBoolean
            case Type.Date => Check.isDate
            // NB: Some explicit coproducts for adjacent types.
            case Type.Int ⨿ Type.Dec ⨿ Type.Str =>
              ((expr: Expression) => $and(
                $lt($literal(Bson.Null), expr),
                $lt(expr, $literal(Bson.Doc(ListMap())))))
            case Type.Int ⨿ Type.Dec ⨿ Type.Interval ⨿ Type.Str =>
              ((expr: Expression) => $and(
                $lt($literal(Bson.Null), expr),
                $lt(expr, $literal(Bson.Doc(ListMap())))))
            case Type.Date ⨿ Type.Bool =>
              ((expr: Expression) =>
                $and(
                  $lte($literal(Bson.Bool(false)), expr),
                  // TODO: in Mongo 3.0, we can have a tighter type check.
                  // $lt(expr, $literal(Bson.Timestamp(Instant.ofEpochMilli(0), 0)))))
                  $lt(expr, $literal(Bson.Regex("", "")))))
            case Type.Syntaxed =>
              ((expr: Expression) =>
                $or(
                  $lt(expr, $literal(Bson.Doc(ListMap()))),
                  $and(
                    $lte($literal(Bson.ObjectId(minOid)), expr),
                    $lt(expr, $literal(Bson.Regex("", ""))))))
          }

        val v =
          exprCheck(typ).fold(
            lift(HasWorkflow(cont)))(
            f => lift((HasWorkflow(exp) |@| HasWorkflow(cont) |@| HasWorkflow(fallback))(
              (exp, cont, fallback) => {
                WB.expr1(exp)(f).flatMap(t => WB.expr(List(t, cont, fallback)) {
                  case List(t, c, a) => $cond(t, c, a)
                })
              })).join)

        State(s => v.run(s).fold(e => s -> -\/(e), t => t._1 -> \/-(t._2)))
    }
  }

  import Planner._

  val annotateƒ =
    GAlgebraZip[(Fix[LogicalPlan], ?), LogicalPlan].zip(
      selectorƒ,
      toAlgebraOps(jsExprƒ).generalize[(Fix[LogicalPlan], ?)])

  private type LPorLP = Fix[LogicalPlan] \/ Fix[LogicalPlan]

  private def elideJoin(in: Func.Input[Fix[LogicalPlan], nat._3]): Func.Input[LPorLP, nat._3] =
    in match {
      case Sized(l, r, cond) =>
        Func.Input3(\/-(l), \/-(r), -\/(cond.cata(Optimizer.elideTypeCheckƒ)))
    }

  // FIXME: This removes all type checks from join conditions. Shouldn’t do
  //        this, but currently need it in order to align the joins.
  val elideJoinCheckƒ: Fix[LogicalPlan] => LogicalPlan[LPorLP] = _.unFix match {
    case InvokeFUnapply(JoinFunc(jf), Sized(a1, a2, a3)) =>
      InvokeF(jf, elideJoin(Func.Input3(a1, a2, a3)))
    case x => x.map(\/-(_))
  }

  def alignJoinsƒ: LogicalPlan[Fix[LogicalPlan]] => OutputM[Fix[LogicalPlan]] = {

    def containsTableRefs(condA: Fix[LogicalPlan], tableA: Fix[LogicalPlan], condB: Fix[LogicalPlan], tableB: Fix[LogicalPlan]) =
      condA.contains(tableA) && condB.contains(tableB) &&
        condA.all(_ ≠ tableB) && condB.all(_ ≠ tableA)

    def alignCondition(lt: Fix[LogicalPlan], rt: Fix[LogicalPlan]):
        Fix[LogicalPlan] => OutputM[Fix[LogicalPlan]] =
      _.unFix match {
        case TypecheckF(expr, typ, cont, fb) =>
          alignCondition(lt, rt)(cont).map(Typecheck(expr, typ, _, fb))
        case InvokeFUnapply(And, Sized(t1, t2)) =>
          Func.Input2(t1, t2).traverse(alignCondition(lt, rt)).map(Invoke(And, _))
        case InvokeFUnapply(Or, Sized(t1, t2)) =>
          Func.Input2(t1, t2).traverse(alignCondition(lt, rt)).map(Invoke(Or, _))
        case InvokeFUnapply(Not, Sized(t1)) =>
          Func.Input1(t1).traverse(alignCondition(lt, rt)).map(Invoke(Not, _))
        case x @ InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(left, right)) if func.effect ≟ Mapping =>
          if (containsTableRefs(left, lt, right, rt))
            \/-(Invoke(func, Func.Input2(left, right)))
          else if (containsTableRefs(left, rt, right, lt))
            flip(func).fold[PlannerError \/ Fix[LogicalPlan]](
              -\/(UnsupportedJoinCondition(Fix(x))))(
              f => \/-(Invoke[nat._2](f, Func.Input2(right, left))))
          else -\/(UnsupportedJoinCondition(Fix(x)))

        case LetF(name, form, in) =>
          alignCondition(lt, rt)(in).map(Let(name, form, _))

        case x => \/-(Fix(x))
      }

    {
      case x @ InvokeFUnapply(JoinFunc(f), Sized(l, r, cond)) =>
        alignCondition(l, r)(cond).map(c => Invoke[nat._3](f, Func.Input3(l, r, c)))

      case x => \/-(Fix(x))
    }
  }

  def plan0[F[_]: Functor: Coalesce: Crush: Crystallize](logical: Fix[LogicalPlan])
      (implicit ev0: WorkflowOpCoreF :<: F, ev1: Show[Fix[WorkflowBuilderF[F, ?]]], ev2: RenderTree[Fix[F]])
      : EitherT[Writer[PhaseResults, ?], PlannerError, Crystallized[F]] = {
    // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
    import EitherT.eitherTMonad
    import StateT.stateTMonadState

    // NB: Locally add state on top of the result monad so everything
    //     can be done in a single for comprehension.
    type PlanT[X[_], A] = EitherT[X, PlannerError, A]
    type GenT[X[_], A]  = StateT[X, NameGen, A]
    type W[A]           = Writer[PhaseResults, A]
    type P[A]           = PlanT[W, A]
    type M[A]           = GenT[P, A]

    def log[A: RenderTree](label: String)(ma: M[A]): M[A] =
      ma flatMap { a =>
        val result = PhaseResult.Tree(label, RenderTree[A].render(a))
        (Writer(Vector(result), a): W[A]).liftM[PlanT].liftM[GenT]
      }

    def swizzle[A](sa: StateT[PlannerError \/ ?, NameGen, A]): M[A] =
      StateT[P, NameGen, A](ng => EitherT(sa.run(ng).point[W]))

    def liftError[A](ea: PlannerError \/ A): M[A] =
      EitherT(ea.point[W]).liftM[GenT]

    val wfƒ = workflowƒ[F] ⋙ (_ ∘ (_ ∘ (_ ∘ normalize[F])))

    (for {
      cleaned <- log("Logical Plan (reduced typechecks)")(liftError(logical.cataM[PlannerError \/ ?, Fix[LogicalPlan]](Optimizer.assumeReadObjƒ)))
      align <- log("Logical Plan (aligned joins)")       (liftError(cleaned.apo(elideJoinCheckƒ).cataM(alignJoinsƒ ⋘ repeatedly(Optimizer.simplifyƒ[Fix]))))
      prep <- log("Logical Plan (projections preferred)")(Optimizer.preferProjections(align).point[M])
      wb   <- log("Workflow Builder")                    (swizzle(swapM(lpParaZygoHistoS(prep)(annotateƒ, wfƒ))))
      wf1  <- log("Workflow (raw)")                      (swizzle(build(wb)))
      wf2  <- log("Workflow (crystallized)")             (Crystallize[F].crystallize(wf1).point[M])
    } yield wf2).evalZero
  }

  /** Translate the high-level "logical" plan to an executable MongoDB "physical"
    * plan, taking into account the current runtime environment as captured by
    * the given context (which is for the time being just the "query model"
    * associated with the backend version.)
    * Internally, the type of the plan being built constrains which operators
    * can be used, but the resulting plan uses the largest, common type so that
    * callers don't need to worry about it.
    */
  def plan(logical: Fix[LogicalPlan], queryContext: MongoQueryModel)
    : EitherT[Writer[PhaseResults, ?], PlannerError, Crystallized[WorkflowF]] = {
    import MongoQueryModel._

    queryContext match {
      case `3.2` =>
        plan0[Workflow3_2F](logical)
      case _     =>
        plan0[Workflow2_6F](logical).map(_.inject[WorkflowF])
    }
  }
}
