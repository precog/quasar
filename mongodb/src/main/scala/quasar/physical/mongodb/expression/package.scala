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
import quasar.{Type, RenderTree, Terminal}, Type.⨿
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.Planner.{PlannerError, UnsupportedJS}
import quasar.physical.mongodb.javascript._

import matryoshka._, Recursive.ops._
import monocle.Prism
import scalaz._, Scalaz._

package object expression {

  /** The type for expressions targeting MongoDB 2.6 specifically. */
  type Expr2_6[A] = ExprOpCoreF[A]
  /** The type for expressions targeting MongoDB 3.0 specifically. */
  type Expr3_0[A] = Coproduct[ExprOp3_0F, ExprOpCoreF, A]
  /** The type for expressions targeting MongoDB 3.2 specifically. */
  type Expr3_2[A] = Coproduct[ExprOp3_2F, Expr3_0, A]

  /** The type for expressions supporting the most advanced capabilities. */
  type ExprOp[A] = Expr3_2[A]

  val DocField = Prism.partial[DocVar, BsonField] {
    case DocVar.ROOT(Some(tail)) => tail
  } (DocVar.ROOT(_))

  // The following few cases are places where the ExprOp created from
  // the LogicalPlan needs special handling to behave the same when
  // converted to JS.
  // TODO: See SD-736 for the way forward.
  def translate[T[_[_]]: Corecursive: Recursive, EX[_]: Traverse](implicit
      I: ExprOpCoreF :<: EX,
      ev0: Equal[T[EX]],
      ev1: ExprOpOps.Uni[EX])
      : PartialFunction[T[EX], PlannerError \/ JsFn] = {
    def app(x: T[EX], tc: JsCore => JsCore): PlannerError \/ JsFn =
      Recursive[T].para(x)(toJs[T, EX]).map(f => JsFn(JsFn.defaultName, tc(f(jscore.Ident(JsFn.defaultName)))))

    {
      // matches the pattern the planner generates for converting epoch time
      // values to timestamps. Adding numbers to dates works in ExprOp, but not
      // in Javacript.
      case $add($literal(Bson.Date(inst)), r) if inst.toEpochMilli ≟ 0 =>
        Recursive[T].para(r)(toJs[T, EX])
          .map(r => JsFn(JsFn.defaultName,
            jscore.New(jscore.Name("Date"), List(r(jscore.Ident(JsFn.defaultName))))))

      case Check(expr, typ) => typ match {
        case Type.Numeric   => app(expr, isAnyNumber)
        case Type.Str       => app(expr, isString)
        case Type.AnyObject => app(expr, isObject)
        case Type.AnyArray  => app(expr, isArray)
        case Type.Binary    => app(expr, isBinary)
        case Type.Id        => app(expr, isObjectId)
        case Type.Bool      => app(expr, isBoolean)
        case Type.Date ⨿ Type.Timestamp =>
          app(expr, x =>
            jscore.BinOp(jscore.Or,
              isDate(x),
              isTimestamp(x)))
        case Type.Date      => app(expr, isDate)
        case Type.Timestamp => app(expr, isTimestamp)
        case Type.AnyObject ⨿ Type.AnyArray =>
          app(expr, isObjectOrArray)
        case Type.Binary ⨿ Type.Id ⨿ Type.Bool ⨿ Type.Date ⨿ Type.Timestamp =>
          app(expr, x =>
            jscore.binop(jscore.Or,
              isBinary(x),
              isObjectId(x),
              isBoolean(x),
              isDate(x),
              isTimestamp(x)))
        case Type.Date ⨿ Type.Timestamp =>
          app(expr, x =>
            jscore.BinOp(jscore.Or,
              isDate(x),
              isTimestamp(x)))
        case Type.Bool ⨿ Type.Date ⨿ Type.Timestamp =>
          app(expr, x =>
            jscore.binop(jscore.Or,
              isDate(x),
              isTimestamp(x),
              isBoolean(x)))
        case Type.Int ⨿ Type.Dec ⨿ Type.Str =>
          app(expr, x =>
            jscore.binop(jscore.Or,
              isAnyNumber(x),
              isString(x)))
        case Type.Null ⨿ Type.Numeric ⨿ Type.Str ⨿ Type.Id ⨿ Type.Bool ⨿ Type.Date ⨿ Type.Timestamp =>
          app(expr, x =>
            jscore.binop(jscore.Or,
              isNull(x),
              isAnyNumber(x),
              isString(x),
              isObjectId(x),
              isBoolean(x),
              isDate(x),
              isTimestamp(x)))
        case _ => UnsupportedJS("type check not converted: " + typ).left
      }
    }
  }

  /** "Idiomatic" translation to JS, accounting for patterns needing special
    * handling. */
  def toJs[T[_[_]]: Corecursive: Recursive, EX[_]: Traverse](implicit
      ev0: ExprOpCoreF :<: EX,
      ev1: Equal[T[EX]],
      ops: ExprOpOps.Uni[EX])
      : GAlgebra[(T[EX], ?), EX, PlannerError \/ JsFn] =
    { t =>
      def expr = Traverse[EX].map(t)(_._1).embed
      def js = Traverse[EX].traverse(t)(_._2)
      translate[T, EX].lift(expr).getOrElse(js.flatMap(ops.toJsSimple))
    }

  // FIXME: no way to put this in anybody's companion where it will be found?
  implicit def exprOpRenderTree[T[_[_]]: Recursive, EX[_]: Functor](implicit ops: ExprOpOps.Uni[EX]): RenderTree[T[EX]] =
    new RenderTree[T[EX]] {
      def render(v: T[EX]) = Terminal(List("ExprOp"), v.cata(ops.bson).toJs.pprint(0).some)
    }
}
