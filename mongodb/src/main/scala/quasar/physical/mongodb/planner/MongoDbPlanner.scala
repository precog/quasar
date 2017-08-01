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

package quasar.physical.mongodb.planner

import scala.Predef.$conforms
import slamdata.Predef._
import quasar.{fs => _, _}, RenderTree.ops._, Type._
import quasar.common.{PhaseResult, PhaseResults}
import quasar.contrib.pathy.mkAbsolute
import quasar.contrib.shapeless._
import quasar.fp._
import quasar.fp.ski._
import quasar.fp.tree._
import quasar.javascript._
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP, _}
import quasar.namegen._
import quasar.physical.mongodb._
import quasar.physical.mongodb.workflow._
import quasar.qscript.MapFunc
import quasar.std.StdLib._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path.rootDir
import scalaz.{Free => _, _}, Scalaz._
import shapeless.{Data => _, :: => _, _}

object MongoDbPlanner {
  import Planner._
  import WorkflowBuilder._

  private val optimizer = new Optimizer[Fix[LP]]
  private val lpr = optimizer.lpr

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import EitherT.eitherTMonad

  import agg._
  import array._
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
  implicit def partialJsRenderTree: RenderTree[PartialJs] = RenderTree.make {
    case (f, ifs) => NonTerminal(List("PartialJs"), None,
      f(List.range(0, ifs.length).map(x => JsFn(JsFn.defaultName, jscore.Ident(jscore.Name(s"_$x"))))).render ::
      ifs.map(f => RenderTree.fromShow("InputFinder")(Show.showFromToString[InputFinder]).render(f)))
  }

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
              (generateTypeCheck(or)(f)(a) |@| generateTypeCheck(or)(f)(b))(
                (a, b) => ((expr: In) => or(a(expr), b(expr))))
            case _ => None
          })(
          Some(_))


  val jsExprƒ: Algebra[LP, OutputM[PartialJs]] = {
    type Output = OutputM[PartialJs]

    import jscore.{
      Add => _, In => _,
      Lt => _, Lte => _, Gt => _, Gte => _, Eq => _, Neq => _,
      And => _, Or => _, Not => _,
      _}

    val mjs = quasar.physical.mongodb.javascript[JsCore](_.embed)
    import mjs._

    val HasJs: Output => OutputM[PartialJs] =
      _ <+> \/-(({ case List(field) => field }, List(Here)))

    def invoke[N <: Nat](func: GenericFunc[N], args: Func.Input[Output, N]): Output = {
      val HasStr: Output => OutputM[String] = _.flatMap {
        _._1(Nil)(ident("_")) match {
          case Literal(Js.Str(str)) => str.right
          case x => FuncApply(func, "JS string", x.render.shows).left
        }
      }

      def Arity1(f: JsCore => JsCore): Output = args match {
        case Sized(a1) =>
          HasJs(a1).map {
            case (f1, p1) =>
              ({ case list => JsFn(JsFn.defaultName, f(
                f1(list)(Ident(JsFn.defaultName))))
              },
                p1.map(There(0, _)))
          }
      }

      def Arity2(f: (JsCore, JsCore) => JsCore): Output =
        args match {
          case Sized(a1, a2) => (HasJs(a1) |@| HasJs(a2)) {
            case ((f1, p1), (f2, p2)) =>
              ({ case list => JsFn(JsFn.defaultName, f(
                f1(list.take(p1.size))(Ident(JsFn.defaultName)),
                f2(list.drop(p1.size))(Ident(JsFn.defaultName))))
              },
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

      ((func, args) match {
        // NB: this one is missing from MapFuncCore.
        case (ToId, _) => None

        // NB: this would get mapped to the same MapFunc as string.Concat, which
        // doesn't make sense here.
        case (ArrayConcat, _) => None

        case (func @ UnaryFunc(_, _, _, _, _, _, _), Sized(a1)) if func.effect ≟ Mapping =>
          val mf = (MapFunc.translateUnaryMapping[Fix, MapFunc[Fix, ?], UnaryArg].apply _)(func)(UnaryArg._1)
          JsFuncHandler.handle[MapFunc[Fix, ?]].apply(mf).map(exp => Arity1(exp.eval))
        case (func @ BinaryFunc(_, _, _, _, _, _, _), Sized(a1, a2)) if func.effect ≟ Mapping =>
          val mf = (MapFunc.translateBinaryMapping[Fix, MapFunc[Fix, ?], BinaryArg] _)(func)(BinaryArg._1, BinaryArg._2)
          JsFuncHandler.handle[MapFunc[Fix, ?]].apply(mf).map(exp => Arity2(exp.eval))
        case (func @ TernaryFunc(_, _, _, _, _, _, _), Sized(a1, a2, a3)) if func.effect ≟ Mapping =>
          val mf = (MapFunc.translateTernaryMapping[Fix, MapFunc[Fix, ?], TernaryArg] _)(func)(TernaryArg._1, TernaryArg._2, TernaryArg._3)
          JsFuncHandler.handle[MapFunc[Fix, ?]].apply(mf).map(exp => Arity3(exp.eval))
        case _ => None
      }).getOrElse(func match {
        case Constantly => Arity1(ι)  // FIXME: this cannot possibly be right

        case Squash      => Arity1(v => v)
        case IfUndefined => Arity2((value, fallback) =>
          // TODO: Only evaluate `value` once.
          If(BinOp(jscore.Eq, value, ident("undefined")), fallback, value))
        case In | Within =>
          Arity2((value, array) =>
            BinOp(jscore.Neq,
              Literal(Js.Num(-1, false)),
              Call(Select(array, "indexOf"), List(value))))

        case ToId => Arity1(id => Call(ident("ObjectId"), List(id)))

        case Concat => Arity2(BinOp(jscore.Add, _, _))

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

        case _ => -\/(UnsupportedFunction(func, "in JS planner".some))
      })
    }

    {
      case c @ Constant(x)     => x.toJs.map[PartialJs](js => ({ case Nil => JsFn.const(js) }, Nil)) \/> UnsupportedPlan(c, None)
      case Invoke(f, a)    => invoke(f, a)
      case Free(_)         => \/-(({ case List(x) => x }, List(Here)))
      case lp.Let(_, _, body) => body

      case x @ Typecheck(expr, typ, cont, fallback) =>
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
            case Type.FlexArr(_, _, _) ⨿ Type.Str
                                       => isArrayOrString
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
  implicit def partialSelRenderTree(implicit S: RenderTree[Selector]): RenderTree[PartialSelector] = RenderTree.make {
    case (f, ifs) => NonTerminal(List("PartialSelector"), None,
      f(List.range(0, ifs.length).map(x => BsonField.Name("_" + x.toString))).render ::
      ifs.map(f => RenderTree.fromShow("InputFinder")(Show.showFromToString[InputFinder]).render(f)))
  }

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
      GAlgebra[(Fix[LP], ?), LP, OutputM[PartialSelector]] = { node =>
    type Output = OutputM[PartialSelector]

    object IsBson {
      def unapply(v: (Fix[LP], Output)): Option[Bson] =
        v._1.unFix match {
          case Constant(b) => BsonCodec.fromData(b).toOption
          case InvokeUnapply(Negate, Sized(Fix(Constant(Data.Int(i))))) => Some(Bson.Int64(-i.toLong))
          case InvokeUnapply(Negate, Sized(Fix(Constant(Data.Dec(x))))) => Some(Bson.Dec(-x.toDouble))
          case InvokeUnapply(ToId, Sized(Fix(Constant(Data.Str(str))))) => Bson.ObjectId.fromString(str).toOption
          case _ => None
        }
    }

    object IsBool {
      def unapply(v: (Fix[LP], Output)): Option[Boolean] =
        v match {
          case IsBson(Bson.Bool(b)) => b.some
          case _                    => None
        }
    }

    object IsText {
      def unapply(v: (Fix[LP], Output)): Option[String] =
        v match {
          case IsBson(Bson.Text(str)) => Some(str)
          case _                      => None
        }
    }

    object IsDate {
      def unapply(v: (Fix[LP], Output)): Option[Data.Date] =
        v._1.unFix match {
          case Constant(d @ Data.Date(_)) => Some(d)
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

    def invoke[N <: Nat](func: GenericFunc[N], args: Func.Input[(Fix[LP], Output), N]): Output = {
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
        Bson.Date.fromInstant(g(date).value).fold[Output](
          -\/(NonRepresentableData(g(date))))(
          d => \/-((
            { case x :: Nil => Selector.Doc(x -> f(d)) },
            List(There(index, Here)))))

      def relDateOp2(conj: (Selector, Selector) => Selector, f1: Bson.Date => Selector.Condition, f2: Bson.Date => Selector.Condition, date: Data.Date, g1: Data.Date => Data.Timestamp, g2: Data.Date => Data.Timestamp, index: Int): Output =
        ((Bson.Date.fromInstant(g1(date).value) \/> NonRepresentableData(g1(date))) ⊛
          (Bson.Date.fromInstant(g2(date).value) \/> NonRepresentableData(g2(date))))((d1, d2) =>
          (
            { case x :: Nil =>
              conj(
                Selector.Doc(x -> f1(d1)),
                Selector.Doc(x -> f2(d2)))
            },
            List(There(index, Here))))

      def stringOp(f: String => Selector.Condition, arg: (Fix[LP], Output)): Output =
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
        (relFunc(f) |@| flip(f).flatMap(relFunc))(relop).getOrElse(-\/(InternalError fromMsg "couldn’t decipher operation"))

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

        case _ => -\/(UnsupportedFunction(func, "in Selector planner".some))
      }
    }

    val default: PartialSelector = (
      { case List(field) =>
        Selector.Doc(ListMap(
          field -> Selector.Expr(Selector.Eq(Bson.Bool(true)))))
      },
      List(Here))

    node match {
      case Constant(_)   => \/-(default)
      case Invoke(f, a)  => invoke(f, a) <+> \/-(default)
      case Let(_, _, in) => in._2
      case Typecheck(_, typ, cont, _) =>
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
            case Type.FlexArr(_, _, _) ⨿ Type.Str =>
              ((f: BsonField) =>
                Selector.Or(
                  Selector.Doc(f -> Selector.Type(BsonType.Arr)),
                  Selector.Doc(f -> Selector.Type(BsonType.Text))))
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

  import quasar.physical.mongodb.expression._
  import quasar.physical.mongodb.accumulator._

  def workflowƒ[F[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse]
    (joinHandler: JoinHandler[F, WorkflowBuilder.M], funcHandler: MapFunc[Fix, ?] ~> OptionFree[EX, ?])
    (implicit
      ev0: WorkflowOpCoreF :<: F,
      ev1: RenderTree[WorkflowBuilder[F]],
      ev2: ExprOpCoreF :<: EX,
      inj: EX :<: ExprOp,
      WB: WorkflowBuilder.Ops[F])
      : LP[
          Cofree[LP, (
            (OutputM[PartialSelector], OutputM[PartialJs]),
            OutputM[WorkflowBuilder[F]])]] =>
          State[NameGen, OutputM[WorkflowBuilder[F]]] = {
    import WorkflowBuilder._

    type Input  = (OutputM[PartialSelector], OutputM[PartialJs])
    type Output = M[WorkflowBuilder[F]]
    type Ann    = Cofree[LP, (Input, OutputM[WorkflowBuilder[F]])]

    import LP._

    // NB: it's only safe to emit "core" expr ops here, but we always use the
    // largest type in WorkflowOp, so they're immediately injected into ExprOp.
    import fixExprOp._
    val check = new Check[Fix[ExprOp], ExprOp]

    object HasData {
      def unapply(node: LP[Ann]): Option[Data] = constant.getOption(node)
    }

    val HasKeys: Ann => OutputM[List[WorkflowBuilder[F]]] = {
      case MakeArrayN.Attr(array) => array.traverse(_.head._2)
      case n                      => n.head._2.map(List(_))
    }

    val HasSelector: Ann => OutputM[PartialSelector] = _.head._1._1

    val HasJs: Ann => OutputM[PartialJs] = _.head._1._2

    val HasWorkflow: Ann => OutputM[WorkflowBuilder[F]] = _.head._2

    def invoke[N <: Nat](func: GenericFunc[N], args: Func.Input[Ann, N]): Output = {

      val HasLiteral: Ann => OutputM[Bson] = ann => HasWorkflow(ann).flatMap { p =>
        asLiteral(p) match {
          case Some(value) => \/-(value)
          case _           => -\/(FuncApply(func, "literal", p.render.shows))
        }
      }

      val HasInt: Ann => OutputM[Long] = HasLiteral(_).flatMap {
        case Bson.Int32(v) => \/-(v.toLong)
        case Bson.Int64(v) => \/-(v)
        case x => -\/(FuncApply(func, "64-bit integer", x.shows))
      }

      val HasText: Ann => OutputM[String] = HasLiteral(_).flatMap {
        case Bson.Text(v) => \/-(v)
        case x => -\/(FuncApply(func, "text", x.shows))
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

      def groupExpr0(f: AccumOp[Fix[ExprOp]]): Output = {
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def reduce0(wb: WorkflowBuilder[F])(fʹ: AccumOp[Fix[ExprOp]])
            : WorkflowBuilder[F] =
          wb.unFix match {
            case GroupBuilderF(Fix(ExprBuilderF(wb0, _)), keys, WorkflowBuilder.Contents.Expr(\/-(_))) =>
              GroupBuilder(wb0, keys, WorkflowBuilder.Contents.Expr(-\/(fʹ)))
            case GroupBuilderF(wb0, keys, WorkflowBuilder.Contents.Expr(\/-(expr))) =>
              GroupBuilder(wb0, keys, WorkflowBuilder.Contents.Expr(-\/(fʹ)))
            case ShapePreservingBuilderF(src @ Fix(GroupBuilderF(_, _, WorkflowBuilder.Contents.Expr(\/-(_)))), inputs, op) =>
              ShapePreservingBuilder(reduce0(src)(fʹ), inputs, op)
            case ExprBuilderF(src0, _) =>
              GroupBuilder(src0, Nil, WorkflowBuilder.Contents.Expr(-\/(fʹ)))
            case _ =>
              GroupBuilder(wb, Nil, WorkflowBuilder.Contents.Expr(-\/(fʹ)))
          }

        lift(Arity1(HasWorkflow).map(reduce0(_)(f)))
      }

      def groupExpr1(f: Fix[ExprOp] => AccumOp[Fix[ExprOp]]): Output =
        lift(Arity1(HasWorkflow).map(WB.reduce(_)(f)))

      def mapExpr(p: WorkflowBuilder[F])(f: Fix[ExprOp] => Fix[ExprOp]): Output =
        WB.expr1(p)(f)

      /** Check for any values used in a selector which reach into the
        * "consequent" branches of Typecheck or Cond nodes, and which
        * involve expressions that are not safe to evaluate before
        * evaluating the typecheck/condition. Using such values in
        * selectors causes runtime errors when the expression ends up
        * in a $project or $simpleMap prior to $match.
        */
      def breaksEvalOrder(ann: Cofree[LP, OutputM[WorkflowBuilder[F]]], f: InputFinder): Boolean = {
        def isSimpleRef =
          f(ann).fold(
            κ(true),
            _.unFix match {
              case ExprBuilderF(_, \/-($var(_))) => true
              case _ => false
            })

        (ann.tail, f) match {
          case (Typecheck(_, _, _, _), There(1, _)) => !isSimpleRef
          case (InvokeUnapply(Cond, _), There(x, _))
              if x ≟ 1 || x ≟ 2                     => !isSimpleRef
          case _                                    => false
        }
      }

      def createOp[A](f: scalaz.Free[EX, A]): scalaz.Free[ExprOp, A] =
        f.mapSuspension(inj)

      ((func, args) match {
        // NB: this one is missing from MapFuncCore.
        case (ToId, _) => None

        // NB: this would get mapped to the same MapFunc as string.Concat, which
        // doesn't make sense here.
        case (ArrayConcat, _) => None

        case (func @ UnaryFunc(_, _, _, _, _, _, _), Sized(a1)) if func.effect ≟ Mapping =>
          val mf: MapFunc[Fix, UnaryArg] = (MapFunc.translateUnaryMapping[Fix, MapFunc[Fix, ?], UnaryArg].apply _)(func)(UnaryArg._1)
          (HasWorkflow(a1).toOption |@|
            funcHandler(mf)) { (wb1, f) =>
            val exp: Unary[ExprOp] = createOp[UnaryArg](f)
            WB.expr1(wb1)(exp.eval)
          }
        case (func @ BinaryFunc(_, _, _, _, _, _, _), Sized(a1, a2)) if func.effect ≟ Mapping =>
          val mf = (MapFunc.translateBinaryMapping[Fix, MapFunc[Fix, ?], BinaryArg] _)(func)(BinaryArg._1, BinaryArg._2)
          (HasWorkflow(a1).toOption |@|
            HasWorkflow(a2).toOption |@|
            funcHandler(mf)) { (wb1, wb2, f) =>
            val exp: Binary[ExprOp] = createOp[BinaryArg](f)
            WB.expr2(wb1, wb2)(exp.eval)
          }
        case (func @ TernaryFunc(_, _, _, _, _, _, _), Sized(a1, a2, a3)) if func.effect ≟ Mapping =>
          val mf = (MapFunc.translateTernaryMapping[Fix, MapFunc[Fix, ?], TernaryArg] _)(func)(TernaryArg._1, TernaryArg._2, TernaryArg._3)
          (HasWorkflow(a1).toOption |@|
            HasWorkflow(a2).toOption |@|
            HasWorkflow(a3).toOption |@|
            funcHandler(mf)) { (wb1, wb2, wb3, f) =>
            val exp: Ternary[ExprOp] = createOp[TernaryArg](f)
            WB.expr(List(wb1, wb2, wb3)) {
              case List(_1, _2, _3) => exp.eval[Fix[ExprOp]](_1, _2, _3)
            }
          }
        case _ => None
      }).getOrElse(func match {
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
        case Sample =>
          // TODO: Use Mongo’s $sample operation when possible.
          lift(Arity2(HasWorkflow, HasInt).map((WB.limit(_, _)).tupled))
        case Take =>
          lift(Arity2(HasWorkflow, HasInt).map((WB.limit(_, _)).tupled))
        case Union =>
          lift(Arity2(HasWorkflow, HasWorkflow)) >>= ((WB.unionAll(_, _)).tupled)
        case InnerJoin | LeftOuterJoin | RightOuterJoin | FullOuterJoin =>
          args match {
            case Sized(left, right, comp) =>
              splitConditions(comp).fold[M[WorkflowBuilder[F]]](
                fail(UnsupportedJoinCondition(forgetAnnotation[Cofree[LP, (Input, OutputM[WorkflowBuilder[F]])], Fix[LP], LP, (Input, OutputM[WorkflowBuilder[F]])](comp))))(
                c => {
                  val (leftKeys, rightKeys) = c.unzip
                  lift((HasWorkflow(left) |@|
                    HasWorkflow(right) |@|
                    leftKeys.traverse(HasWorkflow) |@|
                    rightKeys.traverse(HasWorkflow))((l, r, lk, rk) =>
                    joinHandler.run(
                      func.asInstanceOf[TernaryFunc],
                      JoinSource(l, lk, makeKeys(leftKeys)),
                      JoinSource(r, rk, makeKeys(rightKeys))))).join
                })
          }
        case GroupBy =>
          lift(Arity2(HasWorkflow, HasKeys) ∘ (WB.groupBy(_, _)).tupled)

        // TODO: pull these out into a groupFuncHandler (which will also provide stdDev)
        case Count        => groupExpr0($sum($literal(Bson.Int32(1))))
        case Sum          => groupExpr1($sum(_))
        case Avg          => groupExpr1($avg(_))
        case Min          => groupExpr1($min(_))
        case Max          => groupExpr1($max(_))
        case First        => groupExpr1($first(_))
        case Last         => groupExpr1($last(_))
        case UnshiftArray => groupExpr1($push(_))
        case Arbitrary    => groupExpr1($first(_))

        case ArrayLength =>
          lift(Arity2(HasWorkflow, HasInt)).flatMap {
            case (p, 1)   => mapExpr(p)($size(_))
            case (_, dim) => fail(FuncApply(func, "lower array dimension", dim.toString))
          }

        // TODO: If we had the type available, this could be more efficient in
        //       cases where we have a more restricted type. And right now we
        //       can’t use this, because it doesn’t cover every type.
        // case ToString => expr1(value =>
        //   $cond(check.isNull(value), $literal(Bson.Text("null")),
        //     $cond(check.isString(value), value,
        //       $cond($eq(value, $literal(Bson.Bool(true))), $literal(Bson.Text("true")),
        //         $cond($eq(value, $literal(Bson.Bool(false))), $literal(Bson.Text("false")),
        //           $literal(Bson.Undefined))))))

        case ToId        =>
          lift(Arity1(HasText).flatMap(str =>
            BsonCodec.fromData(Data.Id(str)).map(WB.pure)))

        case ObjectProject =>
          lift(Arity2(HasWorkflow, HasText).flatMap((WB.projectField(_, _)).tupled))
        case ArrayProject =>
          lift(Arity2(HasWorkflow, HasInt).flatMap {
            case (p, index) => WB.projectIndex(p, index.toInt)
          })
        case DeleteField  =>
          lift(Arity2(HasWorkflow, HasText) >>= (WB.deleteField(_, _)).tupled)
        case FlattenMap   => lift(Arity1(HasWorkflow) ∘ WB.flattenMap)
        case FlattenArray => lift(Arity1(HasWorkflow) ∘ WB.flattenArray)
        case Squash       => lift(Arity1(HasWorkflow))
        case Distinct     =>
          lift(Arity1(HasWorkflow)) >>= WB.distinct
        case DistinctBy   =>
          lift(Arity2(HasWorkflow, HasKeys)) >>= (WB.distinctBy(_, _)).tupled

        case _ => fail(UnsupportedFunction(func, "in workflow planner".some))
      })
    }

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def splitConditions: Ann => Option[List[(Ann, Ann)]] = _.tail match {
      case InvokeUnapply(relations.And, terms) =>
        terms.unsized.traverse(splitConditions).map(_.concatenate)
      case InvokeUnapply(relations.Eq, Sized(left, right)) => Some(List((left, right)))
      case Constant(Data.Bool(true)) => Some(List())
      case _ => None
    }

    def makeKeys(k: List[Ann]): Option[List[JsFn]] =
      k.traverse(a => HasJs(a).strengthR(a))
        .flatMap(ts => findArgs(ts)(_._2).map(ll => applyPartials(ts.map(_._1), ll.map(_.length))))
        .toOption

    def findArgs[A, B](partials: List[(PartialJs, Cofree[LP, A])])(f: A => OutputM[B]):
        OutputM[List[List[B]]] =
      partials.traverse { case ((_, ifs), ann) => ifs.traverse(i => f(i(ann))) }

    def applyPartials(partials: List[PartialJs], arities: List[Int]): List[JsFn] =
      (partials zip arities).map(l => l._1._1(List.fill(l._2)(JsFn.identity)))

    // Tricky: It's easier to implement each step using StateT[\/, ...], but we
    // need the fold’s Monad to be State[..., \/], so that the morphism
    // flatMaps over the State but not the \/. That way it can evaluate to left
    // for an individual node without failing the fold. This code takes care of
    // mapping from one to the other.
    node => node match {
      case Read(path) =>
        // Documentation on `QueryFile` guarantees absolute paths, so calling `mkAbsolute`
        state(Collection.fromFile(mkAbsolute(rootDir, path)).bimap(PlanPathError, WB.read))
      case Constant(data) =>
        state(BsonCodec.fromData(data).bimap(
          κ(NonRepresentableData(data)),
          WB.pure))
      case Invoke(func, args) =>
        val wb: Output = invoke(func, args)
        val js: Output = lift(jsExprƒ(node.map(HasJs))).flatMap(pjs =>
            lift(pjs._2.traverse(_(Cofree(UnsupportedPlan(node, None).left, node.map(_.map(_._2)))))).flatMap(args =>
              WB.jsExpr(args, x => pjs._1(x.map(JsFn.const))(jscore.Ident(JsFn.defaultName)))))
        /** Like `\/.orElse`, but always uses the error from the first arg. */
        def orElse[A, B](v1: A \/ B, v2: A \/ B): A \/ B =
          v1.swapped(_.flatMap(e => v2.toOption <\/ e))
        State(s => orElse(wb.run(s), js.run(s)).fold(e => s -> -\/(e), t => t._1 -> \/-(t._2)))
      case JoinSideName(_) =>
        state(-\/(InternalError fromMsg s"unexpected JoinSideName"))
      case Join(_, _, _, _) =>
        state(-\/(InternalError fromMsg s"unexpected Join"))
      case Free(name) =>
        state(-\/(InternalError fromMsg s"variable $name is unbound"))
      case Let(_, _, in) => state(in.head._2)
      case Sort(src, ords) =>
        state((HasWorkflow(src) |@| ords.traverse { case (a, sd) => HasWorkflow(a) strengthR sd }) { (src, os) =>
          val (keys, dirs) = os.toList.unzip
          WB.sortBy(src, keys, dirs)
        })
      case TemporalTrunc(part, src) =>
        state(-\/(UnsupportedPlan(node, "TemporalTrunc not supported".some)))
      case Typecheck(exp, typ, cont, fallback) =>
        // NB: Even if certain checks aren’t needed by ExprOps, we have to
        //     maintain them because we may convert ExprOps to JS.
        //     Hopefully BlackShield will eliminate the need for this.
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def exprCheck: Type => Option[Fix[ExprOp] => Fix[ExprOp]] =
          generateTypeCheck[Fix[ExprOp], Fix[ExprOp]]($or(_, _)) {
            case Type.Null => check.isNull//((expr: Fix[ExprOp]) => $eq($literal(Bson.Null), expr))
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
            case Type.FlexArr(_, _, _) ⨿ Type.Str => check.isArrayOrString
            // NB: Some explicit coproducts for adjacent types.
            case Type.Int ⨿ Type.Dec ⨿ Type.Str => check.isNumberOrString
            case Type.Int ⨿ Type.Dec ⨿ Type.Interval ⨿ Type.Str => check.isNumberOrString
            case Type.Date ⨿ Type.Bool => check.isDateTimestampOrBoolean
            case Type.Syntaxed => check.isSyntaxed
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

  val annotateƒ =
    GAlgebraZip[(Fix[LP], ?), LP].zip(
      selectorƒ,
      toAlgebraOps(jsExprƒ).generalize[(Fix[LP], ?)])

  private type LPorLP = Fix[LP] \/ Fix[LP]

  private def elideJoin(in: Func.Input[Fix[LP], nat._3]): Func.Input[LPorLP, nat._3] =
    in match {
      case Sized(l, r, cond) =>
        Func.Input3(\/-(l), \/-(r), -\/(cond.cata(optimizer.elideTypeCheckƒ)))
    }

  // FIXME: This removes all type checks from join conditions. Shouldn’t do
  //        this, but currently need it in order to align the joins.
  val elideJoinCheckƒ: Fix[LP] => LP[LPorLP] = _.unFix match {
    case InvokeUnapply(JoinFunc(jf), Sized(a1, a2, a3)) =>
      Invoke(jf, elideJoin(Func.Input3(a1, a2, a3)))
    case x => x.map(\/-(_))
  }

  def alignJoinsƒ: LP[Fix[LP]] => OutputM[Fix[LP]] = {

    def containsTableRefs(condA: Fix[LP], tableA: Fix[LP], condB: Fix[LP], tableB: Fix[LP]) =
      condA.contains(tableA) && condB.contains(tableB) &&
        condA.all(_ ≠ tableB) && condB.all(_ ≠ tableA)

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def alignCondition(lt: Fix[LP], rt: Fix[LP]):
        Fix[LP] => OutputM[Fix[LP]] =
      _.unFix match {
        case Typecheck(expr, typ, cont, fb) =>
          alignCondition(lt, rt)(cont).map(lpr.typecheck(expr, typ, _, fb))
        case InvokeUnapply(And, Sized(t1, t2)) =>
          Func.Input2(t1, t2).traverse(alignCondition(lt, rt)).map(lpr.invoke(And, _))
        case InvokeUnapply(Or, Sized(t1, t2)) =>
          Func.Input2(t1, t2).traverse(alignCondition(lt, rt)).map(lpr.invoke(Or, _))
        case InvokeUnapply(Not, Sized(t1)) =>
          Func.Input1(t1).traverse(alignCondition(lt, rt)).map(lpr.invoke(Not, _))
        case x @ InvokeUnapply(func @ BinaryFunc(_, _, _, _, _, _, _), Sized(left, right)) if func.effect ≟ Mapping =>
          if (containsTableRefs(left, lt, right, rt))
            \/-(lpr.invoke(func, Func.Input2(left, right)))
          else if (containsTableRefs(left, rt, right, lt))
            flip(func).fold[PlannerError \/ Fix[LP]](
              -\/(UnsupportedJoinCondition(Fix(x))))(
              f => \/-(lpr.invoke(f, Func.Input2(right, left))))
          else -\/(UnsupportedJoinCondition(Fix(x)))

        case Let(name, form, in) =>
          alignCondition(lt, rt)(in).map(lpr.let(name, form, _))

        case x => \/-(x.embed)
      }

    {
      case x @ InvokeUnapply(JoinFunc(f), Sized(l, r, cond)) =>
        alignCondition(l, r)(cond).map(c => lpr.invoke(f, Func.Input3(l, r, c)))

      case x => \/-(x.embed)
    }
  }

  /** In QScript mongo we will not remove typecheck filters and this will be deleted. */
  def removeTypecheckFilters:
      AlgebraM[PlannerError \/ ?, LP, Fix[LP]] = {
    case Typecheck(_, _, t @ Embed(Constant(Data.Bool(true))), Embed(Constant(Data.Bool(false)))) =>
      \/-(t)
    case x => \/-(x.embed)
  }

  /** To be used by backends that require collections to contain Obj, this
    * looks at type checks on `Read` then either eliminates them if they are
    * trivial, leaves them if they check field contents, or errors if they are
    * incompatible.
    */
  def assumeReadObjƒ:
      AlgebraM[PlannerError \/ ?, LP, Fix[LP]] = {
    case x @ Let(n, r @ Embed(Read(_)),
      Embed(Typecheck(Embed(Free(nf)), typ, cont, _)))
        if n == nf =>
      typ match {
        case Type.Obj(m, Some(Type.Top)) if m == ListMap() =>
          \/-(lpr.let(n, r, cont))
        case Type.Obj(_, _) =>
          \/-(x.embed)
        case _ =>
          -\/(UnsupportedPlan(x,
            Some("collections can only contain objects, but a(n) " +
              typ.shows +
              " is expected")))
      }
    case x => \/-(x.embed)
  }

  def plan0[WF[_]: Functor: Coalesce: Crush: Crystallize, EX[_]: Traverse]
    (joinHandler: JoinHandler[WF, WorkflowBuilder.M], funcHandler: MapFunc[Fix, ?] ~> OptionFree[EX, ?])
    (logical: Fix[LP])
    (implicit
      ev0: WorkflowOpCoreF :<: WF,
      ev1: RenderTree[Fix[WF]],
      ev2: ExprOpCoreF :<: EX,
      ev3: EX :<: ExprOp)
      : EitherT[Writer[PhaseResults, ?], PlannerError, Crystallized[WF]] = {

    // NB: Locally add state on top of the result monad so everything
    //     can be done in a single for comprehension.
    type PlanT[X[_], A] = EitherT[X, PlannerError, A]
    type GenT[X[_], A]  = StateT[X, NameGen, A]
    type W[A]           = Writer[PhaseResults, A]
    type P[A]           = PlanT[W, A]
    type M[A]           = GenT[P, A]

    def log[A: RenderTree](label: String)(ma: M[A]): M[A] =
      ma flatMap { a =>
        (Writer(Vector(PhaseResult.tree(label, a)), a): W[A]).liftM[PlanT].liftM[GenT]
      }

    def swizzle[A](sa: StateT[PlannerError \/ ?, NameGen, A]): M[A] =
      StateT[P, NameGen, A](ng => EitherT(sa.run(ng).point[W]))

    def liftError[A](ea: PlannerError \/ A): M[A] =
      EitherT(ea.point[W]).liftM[GenT]

    val wfƒ = workflowƒ[WF, EX](joinHandler, funcHandler) ⋙ (_ ∘ (_ ∘ ((_: Fix[WorkflowBuilderF[WF, ?]]).mapR(normalize[WF]))))

    (for {
      cleaned <- log("Logical Plan (reduced typechecks)")      (liftError(logical.cataM[PlannerError \/ ?, Fix[LP]](assumeReadObjƒ)))
      elided  <- log("Logical Plan (remove typecheck filters)")(liftError(cleaned.cataM[PlannerError \/ ?, Fix[LP]](removeTypecheckFilters ⋘ repeatedly(optimizer.simplifyƒ))))
      align   <- log("Logical Plan (aligned joins)")           (liftError(elided.apo[Fix[LP]](elideJoinCheckƒ).cataM(alignJoinsƒ ⋘ repeatedly(optimizer.simplifyƒ))))
      prep    <- log("Logical Plan (projections preferred)")   (optimizer.preferProjections(align).point[M])
      wb      <- log("Workflow Builder")                       (swizzle(swapM(lpr.lpParaZygoHistoS(prep)(annotateƒ, wfƒ))))
      wf1     <- log("Workflow (raw)")                         (swizzle(build(wb)))
      wf2     <- log("Workflow (crystallized)")                (Crystallize[WF].crystallize(wf1).point[M])
    } yield wf2).evalZero
  }

  /** Translate the high-level "logical" plan to an executable MongoDB "physical"
    * plan, taking into account the current runtime environment as captured by
    * the given context.
    * Internally, the type of the plan being built constrains which operators
    * can be used, but the resulting plan uses the largest, common type so that
    * callers don't need to worry about it.
    */
  def plan[M[_]](logical0: Fix[LP], queryContext: fs.QueryContext[M])
      : EitherT[Writer[PhaseResults, ?], PlannerError, Crystallized[WorkflowF]] = {
    import MongoQueryModel._

    val logical: Fix[LP] =
      logical0.cata[Fix[LP]](optimizer.reconstructOldJoins)

    queryContext.model match {
      case `3.2` =>
        val joinHandler = JoinHandler.fallback(
          JoinHandler.pipeline[Workflow3_2F](queryContext.statistics, queryContext.indexes),
          JoinHandler.mapReduce[Workflow3_2F])
        plan0[Workflow3_2F, Expr3_2](joinHandler, FuncHandler.handle3_2[MapFunc[Fix, ?]])(logical)

      case `3.0` =>
        val joinHandler = JoinHandler.mapReduce[Workflow2_6F]
        plan0[Workflow2_6F, Expr3_0](joinHandler, FuncHandler.handle3_0[MapFunc[Fix, ?]])(logical).map(_.inject[WorkflowF])

      case _     =>
        val joinHandler = JoinHandler.mapReduce[Workflow2_6F]
        plan0[Workflow2_6F, Expr2_6](joinHandler, FuncHandler.handle2_6[MapFunc[Fix, ?]])(logical).map(_.inject[WorkflowF])
    }
  }
}
