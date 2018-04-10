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

package quasar.physical.mongodb

import slamdata.Predef.{Map => _, _}
import quasar._, Planner._, Type.{Const => _, Coproduct => _, _}
import quasar.common.{PhaseResult, PhaseResults, PhaseResultT, PhaseResultTell, SortDir}
import quasar.connector.BackendModule
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.{ADir, AFile}
import quasar.contrib.scalaz._, eitherT._
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski._
import quasar.fs.{FileSystemError, FileSystemErrT, MonadFsErr}, FileSystemError.qscriptPlanningFailed
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.physical.mongodb.WorkflowBuilder.{Subset => _, _}
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner._
import quasar.physical.mongodb.planner.common._
import quasar.physical.mongodb.workflow.{ExcludeId => _, IncludeId => _, _}
import quasar.qscript._, RenderQScriptDSL._
import quasar.qscript.rewrites.{Coalesce => _, Optimize, PreferProjection, Rewrite}

import java.time.Instant
import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import org.bson.BsonDocument
import scalaz._, Scalaz.{ToIdOps => _, _}

// TODO: This is generalizable to an arbitrary `Recursive` type, I think.
sealed abstract class InputFinder[T[_[_]]] {
  def apply[A](t: FreeMapA[T, A]): FreeMapA[T, A]
}

final case class Here[T[_[_]]]() extends InputFinder[T] {
  def apply[A](a: FreeMapA[T, A]): FreeMapA[T, A] = a
}

final case class There[T[_[_]]](index: Int, next: InputFinder[T])
    extends InputFinder[T] {
  def apply[A](a: FreeMapA[T, A]): FreeMapA[T, A] =
    a.resume.fold(fa => next(fa.toList.apply(index)), κ(a))
}

object MongoDbPlanner {
  import fixExprOp._

  type Partial[T[_[_]], In, Out] = (PartialFunction[List[In], Out], List[InputFinder[T]])

  type OutputM[A]      = PlannerError \/ A

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
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr, EX[_]: Traverse, A]
    (fm: FreeMapA[T, A], default: OutputM[PartialSelector[T]], galg: GAlgebra[(T[MapFunc[T, ?]], ?), MapFunc[T, ?], OutputM[PartialSelector[T]]])
    (implicit inj: EX :<: ExprOp)
      : OutputM[PartialSelector[T]] =
    fm.zygo(
      interpret[MapFunc[T, ?], A, T[MapFunc[T, ?]]](
        κ(MFC(MapFuncsCore.Undefined[T, T[MapFunc[T, ?]]]()).embed),
        _.embed),
      ginterpret[(T[MapFunc[T, ?]], ?), MapFunc[T, ?], A, OutputM[PartialSelector[T]]](
        κ(default), galg))

  def processMapFunc[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr: ExecTimeR, A]
    (fm: FreeMapA[T, A])(recovery: A => JsCore)
      : M[JsCore] =
    fm.cataM(interpretM[M, MapFunc[T, ?], A, JsCore](recovery(_).point[M], javascript))

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

  // NB: it's only safe to emit "core" expr ops here, but we always use the
  // largest type in WorkflowOp, so they're immediately injected into ExprOp.
  val check = new Check[Fix[ExprOp], ExprOp]

  def ejsonToExpression[M[_]: Applicative: MonadFsErr, EJ]
    (v: BsonVersion)(ej: EJ)(implicit EJ: Recursive.Aux[EJ, EJson])
      : M[Fix[ExprOp]] =
    ej.cataM(BsonCodec.fromEJson(v)).fold(pe => raiseErr(qscriptPlanningFailed(pe)), $literal(_).point[M])


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
      case ToId(a1) => unimplemented[M, Fix[ExprOp]]("ToId expression")

      case OffsetDate(a1) => unimplemented[M, Fix[ExprOp]]("OffsetDate expression")
      case OffsetTime(a1) => unimplemented[M, Fix[ExprOp]]("OffsetTime expression")
      case OffsetDateTime(a1) => unimplemented[M, Fix[ExprOp]]("OffsetDateTime expression")
      case LocalDate(a1) => unimplemented[M, Fix[ExprOp]]("LocalDate expression")
      case LocalTime(a1) => unimplemented[M, Fix[ExprOp]]("LocalTime expression")
      case LocalDateTime(a1) => unimplemented[M, Fix[ExprOp]]("LocalDateTime expression")
      case Interval(a1) => unimplemented[M, Fix[ExprOp]]("Interval expression")
      case StartOfDay(a1) => unimplemented[M, Fix[ExprOp]]("StartOfDay expression")
      case TemporalTrunc(a1, a2) => unimplemented[M, Fix[ExprOp]]("TemporalTrunc expression")

      case IfUndefined(a1, a2) => unimplemented[M, Fix[ExprOp]]("IfUndefined expression")

      case Within(a1, a2) => unimplemented[M, Fix[ExprOp]]("Within expression")

      case ExtractIsoYear(a1) =>
        unimplemented[M, Fix[ExprOp]]("ExtractIsoYear expression")
      case Integer(a1) => unimplemented[M, Fix[ExprOp]]("Integer expression")
      case Decimal(a1) => unimplemented[M, Fix[ExprOp]]("Decimal expression")
      // NB: The aggregation implementation of `ToString` does not handle ObjectId
      //     Here we force this case to be planned using JS
      case ToString($var(DocVar(_, Some(BsonField.Name("_id"))))) =>
        unimplemented[M, Fix[ExprOp]]("ToString _id expression")
      // FIXME: $substr is deprecated in Mongo 3.4. This implementation should be
      //        versioned along with the other functions in FuncHandler, taking into
      //        account the special case for ObjectId above. Mongo 3.4 should
      //        use $substrBytes instead of $substr
      case ToString(a1) => mkToString(a1, $substr).point[M]

      case MakeArray(a1) => unimplemented[M, Fix[ExprOp]]("MakeArray expression")
      case MakeMap(a1, a2) => unimplemented[M, Fix[ExprOp]]("MakeMap expression")
      case ConcatMaps(a1, a2) => unimplemented[M, Fix[ExprOp]]("ConcatMap expression")
      case ProjectKey($var(dv), $literal(Bson.Text(key))) =>
        $var(dv \ BsonField.Name(key)).point[M]
      case ProjectKey(el @ $arrayElemAt($var(dv), _), $literal(Bson.Text(key))) =>
        $let(ListMap(DocVar.Name("el") -> el),
          $var(DocVar.ROOT(BsonField.Name("$el")) \ BsonField.Name(key))).point[M]
      case ProjectKey(a1, a2) => unimplemented[M, Fix[ExprOp]](s"ProjectKey expression")
      case ProjectIndex(a1, a2)  => unimplemented[M, Fix[ExprOp]]("ProjectIndex expression")
      case DeleteKey(a1, a2)  => unimplemented[M, Fix[ExprOp]]("DeleteKey expression")

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
               | Type.Int ⨿ Type.Dec ⨿ Type.Interval => check.isNumber // for now intervals check as numbers
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
            case Type.OffsetDateTime | Type.OffsetDate | Type.OffsetTime |
                Type.LocalDateTime | Type.LocalDate | Type.LocalTime => check.isDate
            // NB: Some explicit coproducts for adjacent types.
            case Type.Int ⨿ Type.Dec ⨿ Type.Str => check.isNumberOrString
            case Type.Int ⨿ Type.Dec ⨿ Type.Interval ⨿ Type.Str => check.isNumberOrString // for now intervals check as numbers
            case Type.LocalDate ⨿ Type.Bool => check.isDateTimestampOrBoolean
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

  def javascript[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr: ExecTimeR]
      : AlgebraM[M, MapFunc[T, ?], JsCore] =
    JsFuncHandler.handle[MapFunc[T, ?], M]

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

  def invoke2Nel[T[_[_]]](x: OutputM[PartialSelector[T]], y: OutputM[PartialSelector[T]])(f: (Selector, Selector) => Selector):
      OutputM[PartialSelector[T]] =
    (x ⊛ y) { case ((f1, p1), (f2, p2)) =>
      ({ case list =>
        f(f1(list.take(p1.size)), f2(list.drop(p1.size)))
      },
        p1.map(There(0, _)) ++ p2.map(There(1, _)))
    }

  def invoke2Rel[T[_[_]]](x: OutputM[PartialSelector[T]], y: OutputM[PartialSelector[T]])(f: (Selector, Selector) => Selector):
      OutputM[PartialSelector[T]] =
    (x.toOption, y.toOption) match {
      case (Some((f1, p1)), Some((f2, p2)))=>
        invoke2Nel(x, y)(f)
      case (Some((f1, p1)), None) =>
        (f1, p1.map(There(0, _))).right
      case (None, Some((f2, p2))) =>
        (f2, p2.map(There(1, _))).right
      case _ => InternalError.fromMsg("No selectors in either side of a binary MapFunc").left
    }

  def typeSelector[T[_[_]]: RecursiveT: ShowT]:
      GAlgebra[(T[MapFunc[T, ?]], ?), MapFunc[T, ?], OutputM[PartialSelector[T]]] = { node =>

    import MapFuncsCore._

    node match {
      // NB: the pick of Selector for these two cases determine how restrictive the
      //     extracted typechecks are. See qz-3500 for more details
      case MFC(And(a, b)) => invoke2Rel(a._2, b._2)(Selector.And(_, _))
      case MFC(Or(a, b))  => invoke2Rel(a._2, b._2)(Selector.Or(_, _))

      // NB: we want to extract typechecks from both sides of a comparison operator
      //     Typechecks extracted from both sides are ANDed. Similarly to the `And`
      //     and `Or` case above, the selector choice can be tweaked depending on how
      //     strict we want to be with extracted typechecks. See qz-3500
      case MFC(Eq(a, b))  => invoke2Rel(a._2, b._2)(Selector.And(_, _))
      case MFC(Neq(a, b)) => invoke2Rel(a._2, b._2)(Selector.And(_, _))
      case MFC(Lt(a, b))  => invoke2Rel(a._2, b._2)(Selector.And(_, _))
      case MFC(Lte(a, b)) => invoke2Rel(a._2, b._2)(Selector.And(_, _))
      case MFC(Gt(a, b))  => invoke2Rel(a._2, b._2)(Selector.And(_, _))
      case MFC(Gte(a, b)) => invoke2Rel(a._2, b._2)(Selector.And(_, _))

      // NB: Undefined() is Hole in disguise here. We don't have a way to directly represent
      //     a FreeMap's leaves with this fixpoint, so we use Undefined() instead.
      case MFC(Guard((Embed(MFC(ProjectKey(Embed(MFC(Undefined())), _))), _), typ, cont, _)) =>
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
            case Type.Bool =>
              ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.Bool)))
            case Type.OffsetDateTime | Type.OffsetDate | Type.OffsetTime |
                Type.LocalDateTime | Type.LocalDate | Type.LocalTime =>
              ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.Date)))
          }
        selCheck(typ).fold[OutputM[PartialSelector[T]]](
          -\/(InternalError.fromMsg(node.map(_._1).shows)))(
          f =>
          \/-(cont._2.fold[PartialSelector[T]](
            κ(({ case List(field) => f(field) }, List(There(0, Here[T]())))),
            { case (f2, p2) => ({ case head :: tail => Selector.And(f(head), f2(tail)) }, There(0, Here[T]()) :: p2.map(There(1, _)))
            })))

      case _ => -\/(InternalError fromMsg node.map(_._1).shows)
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
        case MFC(Constant(_)) => \/-(default)
        case MFC(And(a, b))   => invoke2Nel(a._2, b._2)(Selector.And.apply _)
        case MFC(Or(a, b))    => invoke2Nel(a._2, b._2)(Selector.Or.apply _)

        case MFC(Eq(a, b))  => reversibleRelop(a, b)(func)
        case MFC(Neq(a, b)) => reversibleRelop(a, b)(func)
        case MFC(Lt(a, b))  => reversibleRelop(a, b)(func)
        case MFC(Lte(a, b)) => reversibleRelop(a, b)(func)
        case MFC(Gt(a, b))  => reversibleRelop(a, b)(func)
        case MFC(Gte(a, b)) => reversibleRelop(a, b)(func)

        // NB: workaround patmat exhaustiveness checker bug. Merge with previous `match`
        //     once solved.
        case x => x match {
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

          case MFC(Not((_, v))) =>
            v.map { case (sel, inputs) => (sel andThen (_.negate), inputs.map(There(0, _))) }

          case MFC(Guard(_, typ, (_, cont), (Embed(MFC(Undefined())), _))) =>
            cont.map { case (sel, inputs) => (sel, inputs.map(There(1, _))) }
          case MFC(Guard(_, typ, (_, cont), (Embed(MFC(MakeArray(Embed(MFC(Undefined()))))), _))) =>
            cont.map { case (sel, inputs) => (sel, inputs.map(There(1, _))) }

          case _ => -\/(InternalError fromMsg node.map(_._1).shows)
        }
      }
    }

    invoke(node)
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
      [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
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
          [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
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
                      cfg.funcHandler, cfg.staticHandler, cfg.bsonVersion)(
                      dataset,
                        Free.roll(MFC(MapFuncsCore.ProjectKey[T, FreeMap[T]](HoleF[T], MapFuncsCore.StrLit("_id")))))
                  case IncludeId =>
                    getExprBuilder[T, M, WF, EX](
                      cfg.funcHandler, cfg.staticHandler, cfg.bsonVersion)(
                      dataset,
                        MapFuncCore.StaticArray(List(
                          Free.roll(MFC(MapFuncsCore.ProjectKey[T, FreeMap[T]](HoleF[T], MapFuncsCore.StrLit("_id")))),
                          HoleF)))
                  case ExcludeId => dataset.point[M]
                }
              })
      }

    implicit def qscriptCore[T[_[_]]: BirecursiveT: EqualT: ShowT]:
        Planner.Aux[T, QScriptCore[T, ?]] =
      new Planner[QScriptCore[T, ?]] {
        import MapFuncsCore._
        import MapFuncCore._

        type IT[G[_]] = T[G]

        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def plan
          [M[_]: Monad: ExecTimeR: MonadFsErr,
            WF[_]: Functor: Coalesce: Crush,
            EX[_]: Traverse]
          (cfg: PlannerConfig[T, EX, WF])
          (implicit
            ev0: WorkflowOpCoreF :<: WF,
            ev1: RenderTree[WorkflowBuilder[WF]],
            WB: WorkflowBuilder.Ops[WF],
            ev3: EX :<: ExprOp) = {
          case qscript.Map(src, f) =>
            getExprBuilder[T, M, WF, EX](cfg.funcHandler, cfg.staticHandler, cfg.bsonVersion)(src, f)
          case LeftShift(src, struct, id, shiftType, onUndef, repair) => {
            def rewriteUndefined[A]: CoMapFuncR[T, A] => Option[CoMapFuncR[T, A]] = {
              case CoEnv(\/-(MFC(Guard(exp, tpe @ Type.FlexArr(_, _, _), exp0, Embed(CoEnv(\/-(MFC(Undefined()))))))))
                if (onUndef === OnUndefined.Emit) =>
                  rollMF[T, A](MFC(Guard(exp, tpe, exp0, Free.roll(MFC(MakeArray(Free.roll(MFC(Undefined())))))))).some
              case _ => none
            }

            if (repair.contains(LeftSideF)) {
              val rootKey = BsonField.Name("s")
              val structKey = BsonField.Name("f")

              val exprMerge: JoinFunc[T] => M[Fix[ExprOp]] =
                getExprMerge[T, M, EX](
                  cfg.funcHandler, cfg.staticHandler)(_, DocField(rootKey), DocField(structKey))
              val jsMerge: JoinFunc[T] => M[JsFn] =
                getJsMerge[T, M](
                  _, jscore.Select(jscore.Ident(JsFn.defaultName), rootKey.value), jscore.Select(jscore.Ident(JsFn.defaultName), structKey.value))

              val struct0 = struct.linearize.transCata[FreeMap[T]](orOriginal(rewriteUndefined[Hole]))
              val repair0 = repair.transCata[JoinFunc[T]](orOriginal(rewriteUndefined[JoinSide]))

              val src0: M[WorkflowBuilder[WF]] =
                getStructBuilder[T, M, WF, EX](
                  handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, _), cfg.bsonVersion)(
                  src, struct0, rootKey, structKey)

              src0 >>= (src1 =>
                getBuilder[T, M, WF, EX, JoinSide](
                  exprOrJs(_)(exprMerge, jsMerge), cfg.bsonVersion)(
                  FlatteningBuilder(
                    src1,
                    Set(StructureType.mk(shiftType, structKey, id)),
                    List(rootKey).some),
                  repair0))

            } else {

              val struct0 = struct.linearize.transCata[FreeMap[T]](orOriginal(rewriteUndefined[Hole]))
              val repair0 =
                repair.as[Hole](SrcHole).transCata[FreeMap[T]](orOriginal(rewriteUndefined[Hole])) >>=
                  κ(Free.roll(MFC(MapFuncsCore.ProjectKey[T, FreeMap[T]](HoleF[T], MapFuncsCore.StrLit(Keys.wrap)))))

              val wrapKey = BsonField.Name(Keys.wrap)

              getBuilder[T, M, WF, EX, Hole](
                handleFreeMap[T, M, EX](
                  cfg.funcHandler, cfg.staticHandler, _), cfg.bsonVersion)(src, struct0) >>= (builder =>
                getBuilder[T, M, WF, EX, Hole](
                  handleFreeMap[T, M, EX](
                    cfg.funcHandler, cfg.staticHandler, _),
                  cfg.bsonVersion)(
                  FlatteningBuilder(
                    DocBuilder(builder, ListMap(wrapKey -> docVarToExpr(DocVar.ROOT()))),
                    Set(StructureType.mk(shiftType, wrapKey, id)),
                    List().some),
                  repair0))
            }

          }
          case Reduce(src, bucket, reducers, repair) =>
            (bucket.traverse(handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, _)) ⊛
              reducers.traverse(_.traverse(handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, _))))((b, red) => {
                getReduceBuilder[T, M, WF, EX](
                  cfg.funcHandler, cfg.staticHandler, cfg.bsonVersion)(
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
          case Filter(src0, cond) => {
            val selectors = getSelector[T, M, EX, Hole](
              cond, defaultSelector[T].right, selector[T](cfg.bsonVersion) ∘ (_ <+> defaultSelector[T].right))
            val typeSelectors = getSelector[T, M, EX, Hole](
              cond, InternalError.fromMsg(s"not a typecheck").left , typeSelector[T])

            def filterBuilder(src: WorkflowBuilder[WF], partialSel: PartialSelector[T]):
                M[WorkflowBuilder[WF]] = {
              val (sel, inputs) = partialSel

              inputs.traverse(f => handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, f(cond)))
                .map(WB.filter(src, _, sel))
            }

            (selectors.toOption, typeSelectors.toOption) match {
              case (None, Some(typeSel)) => filterBuilder(src0, typeSel)
              case (Some(sel), None) => filterBuilder(src0, sel)
              case (Some(sel), Some(typeSel)) => filterBuilder(src0, typeSel) >>= (filterBuilder(_, sel))
              case _ =>
                handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, cond).map {
                  // TODO: Postpone decision until we know whether we are going to
                  //       need mapReduce anyway.
                  case cond @ HasThat(_) => WB.filter(src0, List(cond), {
                    case f :: Nil => Selector.Doc(f -> Selector.Eq(Bson.Bool(true)))
                  })
                  case \&/.This(js) => WB.filter(src0, Nil, {
                    case Nil => Selector.Where(js(jscore.ident("this")).toJs)
                  })
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
          [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
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
                (getExprBuilder[T, M, WF, EX](cfg.funcHandler, cfg.staticHandler, cfg.bsonVersion)(_, qs.combine >>= {
                  case LeftSide => Free.roll(MFC(MapFuncsCore.ProjectKey(HoleF, MapFuncsCore.StrLit("left"))))
                  case RightSide => Free.roll(MFC(MapFuncsCore.ProjectKey(HoleF, MapFuncsCore.StrLit("right"))))
                }))).join
          }).join
      }

    implicit def coproduct[T[_[_]], F[_], G[_]](
      implicit F: Planner.Aux[T, F], G: Planner.Aux[T, G]):
        Planner.Aux[T, Coproduct[F, G, ?]] =
      new Planner[Coproduct[F, G, ?]] {
        type IT[G[_]] = T[G]
        def plan
          [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
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
          [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
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


  /* Given a handler of type FreeMapA[T, A] => Expr, a FreeMapA[T, A]
   *  and a source WorkflowBuilder, return a new WorkflowBuilder
   *  filtered according to the `Cond`s found in the FreeMapA[T, A].
   *  The result is tupled with whatever remains to be planned out
   *  of the FreeMapA[T, A]
   */
  def getFilterBuilder
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr, WF[_], EX[_]: Traverse, A]
    (handler: FreeMapA[T, A] => M[Expr], v: BsonVersion)
    (src: WorkflowBuilder[WF], fm: FreeMapA[T, A])
    (implicit ev: EX :<: ExprOp, WB: WorkflowBuilder.Ops[WF])
     : M[(WorkflowBuilder[WF], FreeMapA[T, A])] = {

    import MapFuncCore._
    import MapFuncsCore._

    def filterBuilder(src0: WorkflowBuilder[WF], partialSel: PartialSelector[T]):
        M[WorkflowBuilder[WF]] = {
      val (sel, inputs) = partialSel

      inputs.traverse(f => handler(f(fm))) ∘ (WB.filter(src0, _, sel))
    }

    def elideCond: CoMapFuncR[T, A] => Option[CoMapFuncR[T, A]] = {
      case CoEnv(\/-(MFC(Cond(if_, then_, Embed(CoEnv(\/-(MFC(Undefined())))))))) =>
        CoEnv(then_.resume.swap).some
      case _ => none
    }

    def toCofree[B](ann: B): Algebra[CoEnv[A, MapFunc[T, ?], ?], Cofree[MapFunc[T, ?], B]] =
      interpret(κ(Cofree(ann, MFC(Undefined()))), attributeAlgebra[MapFunc[T, ?], B](κ(ann)))

    val undefinedF: MapFunc[T, Cofree[MapFunc[T, ?], Boolean] \/ FreeMapA[T, A]] = MFC(Undefined())
    val gcoalg: GCoalgebra[Cofree[MapFunc[T, ?], Boolean] \/ ?, EnvT[Boolean, MapFunc[T, ?], ?], FreeMapA[T, A]] =
      _.fold(κ(envT(false, undefinedF)), {
        case MFC(Cond(if_, then_, undef @ Embed(CoEnv(\/-(MFC(Undefined())))))) =>
          envT(false, MFC(Cond(if_.cata(toCofree(true)).left, then_.cata(toCofree(false)).left, undef.cata(toCofree(false)).left)))

        case otherwise => envT(false, otherwise ∘ (_.right))
      })

    val galg: GAlgebra[(Cofree[MapFunc[T, ?], Boolean], ?), EnvT[Boolean, MapFunc[T, ?], ?], OutputM[PartialSelector[T]]] = { node =>
      def forgetAnn: Cofree[MapFunc[T, ?], Boolean] => T[MapFunc[T, ?]] = _.transCata[T[MapFunc[T, ?]]](_.lower)

      node.runEnvT match {
        case (true, wa) =>
          /* The `selector` algebra requires one side of a comparison
           * to be a Constant. The comparisons present inside Cond do
           * not necessarily have this shape, hence the decorator
           */
          selector[T](v).apply(wa.map { case (tree, sl) => (forgetAnn(tree), sl) }) <+> (wa match {
            case MFC(Eq(_, _))
               | MFC(Neq(_, _))
               | MFC(Lt(_, _))
               | MFC(Lte(_, _))
               | MFC(Gt(_, _))
               | MFC(Gte(_, _))
               | MFC(Undefined()) => defaultSelector[T].right
            /** The cases here don't readily imply selectors, but
              *  still need to be handled in case a `Cond` is nested
              *  inside one of these.  For instance, if ConcatMaps
              *  includes `Cond`s in BOTH maps, these are extracted
              *  and `Or`ed using Selector.Or. In the case of unary
              *  `MapFunc`s, we simply need to fix the InputFinder to
              *  look in the right place.
              */
            case MFC(MakeMap((_, _), (_, v))) => v.map { case (sel, inputs) => (sel, inputs.map(There(1, _))) }
            case MFC(ConcatMaps((_, lhs), (_, rhs))) => invoke2Rel(lhs, rhs)(Selector.Or(_, _))
            case MFC(Guard((_, if_), _, _, _)) => if_.map { case (sel, inputs) => (sel, inputs.map(There(0, _))) }
            case otherwise => InternalError.fromMsg(otherwise.map(_._1).shows).left
          })

        case (false, wa) => wa match {
          case MFC(MakeMap((_, _), (_, v))) => v.map { case (sel, inputs) => (sel, inputs.map(There(1, _))) }
          case MFC(ProjectKey((_, v), _)) => v.map { case (sel, inputs) => (sel, inputs.map(There(0, _))) }
          case MFC(ConcatMaps((_, lhs), (_, rhs))) => invoke2Rel(lhs, rhs)(Selector.Or(_, _))
          case MFC(Guard((_, if_), _, _, _)) => if_.map { case (sel, inputs) => (sel, inputs.map(There(0, _))) }
          case MFC(Cond((_, pred), _, _)) => pred.map { case (sel, inputs) => (sel, inputs.map(There(0, _))) }
          case otherwise => InternalError.fromMsg(otherwise.map(_._1).shows).left
        }
      }
    }

    val sels: Option[PartialSelector[T]] =
      fm.ghylo[(Cofree[MapFunc[T, ?], Boolean], ?), Cofree[MapFunc[T, ?], Boolean] \/ ?]
        [EnvT[Boolean, MapFunc[T, ?], ?], OutputM[PartialSelector[T]]](distPara, distApo, galg, gcoalg).toOption

    (sels ∘ (filterBuilder(src, _))).cata(
      _ strengthR fm.transCata[FreeMapA[T, A]](orOriginal(elideCond)),
      (src, fm).point[M])
  }

  def getStructBuilder
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr, WF[_]: WorkflowBuilder.Ops[?[_]], EX[_]: Traverse]
    (handler: FreeMap[T] => M[Expr], v: BsonVersion)
    (src: WorkflowBuilder[WF], struct: FreeMap[T], rootKey: BsonField.Name, structKey: BsonField.Name)
    (implicit ev: EX :<: ExprOp): M[WorkflowBuilder[WF]] =
    getFilterBuilder[T, M, WF, EX, Hole](handler, v)(src, struct) >>= { case (source, f) =>
      handler(f) ∘ (fm => DocBuilder(source, ListMap(rootKey -> docVarToExpr(DocVar.ROOT()), structKey -> fm)))
    }

  def getBuilder
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr, WF[_]: WorkflowBuilder.Ops[?[_]], EX[_]: Traverse, A]
    (handler: FreeMapA[T, A] => M[Expr], v: BsonVersion)
    (src: WorkflowBuilder[WF], fm: FreeMapA[T, A])
    (implicit ev: EX :<: ExprOp)
     : M[WorkflowBuilder[WF]] =
    getFilterBuilder[T, M, WF, EX, A](handler, v)(src, fm) >>= { case (src0, fm0) =>
      fm0.project match {
        case MapFuncCore.StaticMap(elems) =>
          elems.traverse(_.bitraverse({
            case Embed(MapFuncCore.EC(ejson.Str(key))) => BsonField.Name(key).point[M]
            case key => raiseErr[M, BsonField.Name](qscriptPlanningFailed(InternalError.fromMsg(s"Unsupported object key: ${key.shows}")))
          }, handler)) ∘ (es => DocBuilder(src0, es.toListMap))
        case _ => handler(fm0) ∘ (ExprBuilder(src0, _))
      }
    }

  def getExprBuilder
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: ExecTimeR: MonadFsErr, WF[_], EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?], staticHandler: StaticHandler[T, EX], v: BsonVersion)
    (src: WorkflowBuilder[WF], fm: FreeMap[T])
    (implicit ev: EX :<: ExprOp, WF: WorkflowBuilder.Ops[WF])
      : M[WorkflowBuilder[WF]] =
    getBuilder[T, M, WF, EX, Hole](handleFreeMap[T, M, EX](funcHandler, staticHandler, _), v)(src, fm)

  def getReduceBuilder
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: ExecTimeR: MonadFsErr, WF[_], EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?], staticHandler: StaticHandler[T, EX], v: BsonVersion)
    (src: WorkflowBuilder[WF], fm: FreeMapA[T, ReduceIndex])
    (implicit ev: EX :<: ExprOp, WF: WorkflowBuilder.Ops[WF])
      : M[WorkflowBuilder[WF]] =
    getBuilder[T, M, WF, EX, ReduceIndex](handleRedRepair[T, M, EX](funcHandler, staticHandler, _), v)(src, fm)

  def getJsMerge[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr: ExecTimeR]
    (jf: JoinFunc[T], a1: JsCore, a2: JsCore)
      : M[JsFn] =
    processMapFunc[T, M, JoinSide](
      jf) {
      case LeftSide => a1
      case RightSide => a2
    } ∘ (JsFn(JsFn.defaultName, _))

  def getExprMerge[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr: ExecTimeR, EX[_]: Traverse]
    (funcHandler: MapFunc[T, ?] ~> OptionFree[EX, ?], staticHandler: StaticHandler[T, EX])
    (jf: JoinFunc[T], a1: DocVar, a2: DocVar)
    (implicit inj: EX :<: ExprOp)
      : M[Fix[ExprOp]] =
    processMapFuncExpr[T, M, EX, JoinSide](funcHandler, staticHandler)(
      jf) {
      case LeftSide => $var(a1)
      case RightSide => $var(a2)
    }

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
    [T[_[_]]: EqualT, M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
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

    def normalize(mqs: T[MQS]): M[T[MQS]] = {
      val mqs1 = mqs.transCata[T[MQS]](R.normalizeEJ[MQS])
      val mqs2 = BR.branches.modify(
          _.transCata[FreeQS[T]](liftCo(R.normalizeEJCoEnv[QScriptTotal[T, ?]]))
        )(mqs1.project).embed
      Trans(assumeReadType[T, MQS, M](Type.AnyObject), mqs2)
    }

    // TODO: All of these need to be applied through branches. We may also be able to compose
    //       them with normalization as the last step and run until fixpoint. Currently plans are
    //       too sensitive to the order in which these are applied.
    //       Some constraints:
    //       - elideQuasarSigil should only be applied once
    //       - elideQuasarSigil needs assumeReadType to be applied in order to
    //         work properly in all cases
    //       - R.normalizeEJ/R.normalizeEJCoEnv may change the structure such
    //         that assumeReadType can elide more guards
    //         E.g. Map(x, SrcHole) is normalized into x. assumeReadType does
    //         not recognize any Map as shape preserving, but it may recognize
    //         x being shape preserving (e.g. when x = ShiftedRead(y, ExcludeId))
    for {
      mongoQS1 <- Trans(assumeReadType[T, MQS, M](Type.AnyObject), qs)
      mongoQS2 <- mongoQS1.transCataM(elideQuasarSigil[T, MQS, M](anyDoc))
      mongoQS3 <- normalize(mongoQS2)
      _ <- BackendModule.logPhase[M](PhaseResult.treeAndCode("QScript Mongo", mongoQS3))

      mongoQS4 =  mongoQS3.transCata[T[MQS]](
                    liftFF[QScriptCore[T, ?], MQS, T[MQS]](
                      repeatedly(O.subsetBeforeMap[MQS, MQS](
                        reflNT[MQS]))))
      _ <- BackendModule.logPhase[M](
             PhaseResult.treeAndCode("QScript Mongo (Subset Before Map)",
             mongoQS4))

      // TODO: Once field deletion is implemented for 3.4, this could be selectively applied, if necessary.
      mongoQS5 =  PreferProjection.preferProjection[MQS](mongoQS4)
      _ <- BackendModule.logPhase[M](PhaseResult.treeAndCode("QScript Mongo (Prefer Projection)", mongoQS5))
    } yield mongoQS5
  }

  def buildWorkflow
    [T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      M[_]: Monad: PhaseResultTell: MonadFsErr: ExecTimeR,
      WF[_]: Functor: Coalesce: Crush,
      EX[_]: Traverse]
    (cfg: PlannerConfig[T, EX, WF])
    (qs: T[fs.MongoQScript[T, ?]])
    (implicit
      ev0: WorkflowOpCoreF :<: WF,
      ev1: EX :<: ExprOp,
      ev2: RenderTree[Fix[WF]])
      : M[Fix[WF]] =
    for {
      wb <- log(
        "Workflow Builder",
        qs.cataM[M, WorkflowBuilder[WF]](
          Planner[T, fs.MongoQScript[T, ?]].plan[M, WF, EX](cfg).apply(_) ∘
            (_.transCata[Fix[WorkflowBuilderF[WF, ?]]](repeatedly(WorkflowBuilder.normalize[WF, Fix[WorkflowBuilderF[WF, ?]]])))))
      wf <- log("Workflow (raw)", liftM[M, Fix[WF]](WorkflowBuilder.build[WBM, WF](wb, cfg.queryModel)))
    } yield wf

  def plan0
    [T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      M[_]: Monad: PhaseResultTell: MonadFsErr: ExecTimeR,
      WF[_]: Traverse: Coalesce: Crush: Crystallize,
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

    def doBuildWorkflow[F[_]: Monad: ExecTimeR](qs0: T[fs.MongoQScript[T, ?]]) =
      buildWorkflow[T, FileSystemErrT[PhaseResultT[F, ?], ?], WF, EX](cfg)(qs0).run.run

    for {
      qs0 <- toMongoQScript[T, M](anyDoc, qs)
      logRes0 <- doBuildWorkflow[M](qs0)
      (log0, res0) = logRes0
      wf0 <- res0 match {
               case \/-(wf) if (needsMapBeforeSort(wf)) =>
                 // TODO look into adding mapBeforeSort to WorkflowBuilder or Workflow stage
                 // instead, so that we can avoid having to rerun some transformations.
                 // See #3063
                 log("QScript Mongo (Map Before Sort)",
                   Trans(mapBeforeSort[T, M], qs0)) >>= buildWorkflow[T, M, WF, EX](cfg)
               case \/-(wf) =>
                 PhaseResultTell[M].tell(log0) *> wf.point[M]
               case -\/(err) =>
                 PhaseResultTell[M].tell(log0) *> raiseErr[M, Fix[WF]](err)
             }
      wf1 <- log(
        "Workflow (crystallized)",
        Crystallize[WF].crystallize(wf0).point[M])
    } yield wf1
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
    val peek = anyDoc andThen (_.mapT(_.liftM[ReaderT[?[_], Instant, ?]]))
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

    def joinHandler[WF[_]: Functor: Coalesce: Crush: Crystallize]
      (implicit ev0: Classify[WF], ev1: WorkflowOpCoreF :<: WF, ev2: RenderTree[WorkflowBuilder[WF]])
        : JoinHandler[WF, WBM] =
      JoinHandler.fallback[WF, WBM](
        JoinHandler.pipeline[WBM, WF](queryModel, queryContext.statistics, queryContext.indexes),
        JoinHandler.mapReduce[WBM, WF](queryModel))

    queryModel match {
      case `3.4.4` =>
        val cfg = PlannerConfig[T, Expr3_4_4, Workflow3_4F](
          joinHandler[Workflow3_4F],
          FuncHandler.handle3_4_4(bsonVersion),
          StaticHandler.handle,
          queryModel,
          bsonVersion)
        plan0[T, M, Workflow3_4F, Expr3_4_4](anyDoc, cfg)(qs)

      case `3.4` =>
        val cfg = PlannerConfig[T, Expr3_4, Workflow3_4F](
          joinHandler[Workflow3_4F],
          FuncHandler.handle3_4(bsonVersion),
          StaticHandler.handle,
          queryModel,
          bsonVersion)
        plan0[T, M, Workflow3_4F, Expr3_4](anyDoc, cfg)(qs)

      case `3.2` =>
        val cfg = PlannerConfig[T, Expr3_2, Workflow3_2F](
          joinHandler[Workflow3_2F],
          FuncHandler.handle3_2(bsonVersion),
          StaticHandler.handle,
          queryModel,
          bsonVersion)
        plan0[T, M, Workflow3_2F, Expr3_2](anyDoc, cfg)(qs).map(_.inject[WorkflowF])

    }
  }
}
