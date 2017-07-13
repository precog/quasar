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

package quasar.physical.mongodb.expression

import slamdata.Predef._
import quasar.Type
import quasar.fp._
import quasar.physical.mongodb.Bson

import scala.Long

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

/** Runtime type checks, exploiting MongoDB's total ordering for Bson values,
  * seen in the order of declarations in this class.
  */
final class Check[T, EX[_]]
  (implicit
    TR: Recursive.Aux[T, EX],
    TC: Corecursive.Aux[T, EX],
    EX: Functor[EX],
    I: ExprOpCoreF :<: EX) {
  import Check._

  private val exprCoreFp = new ExprOpCoreF.fixpoint[T, EX](_.embed)
  import exprCoreFp._

  def isNull(expr: T): T = $eq($literal(Bson.Null), expr)

  def isNumber(expr: T)     = betweenExcl(Bson.Null,              expr, Bson.Text(""))
  def isString(expr: T)     = between    (Bson.Text(""),          expr, Bson.Doc())
  def isObject(expr: T)     = between    (Bson.Doc(),             expr, Bson.Arr())
  def isArray(expr: T)      = between    (Bson.Arr(),             expr, Bson.Binary(minBinary))
  def isBinary(expr: T)     = between    (Bson.Binary(minBinary), expr, Bson.ObjectId(minOid))
  def isId(expr: T)         = between    (Bson.ObjectId(minOid),  expr, Bson.Bool(false))
  def isBoolean(expr: T)    = betweenIncl(Bson.Bool(false),       expr, Bson.Bool(true))
  /** As of MongoDB 3.0, dates sort before timestamps. The type constraint here
    * ensures that this check is used only when it's safe, although we don't
    * actually use any new op here.
    */
  def isDate(expr: T)(implicit ev: ExprOp3_0F :<: EX) =
    between(minDate, expr, minTimestamp)
  /** As of MongoDB 3.0, dates sort before timestamps. The type constraint here
    * ensures that this check is used only when it's safe, although we don't
    * actually use any new op here.
    */
  def isTimestamp(expr: T)(implicit ev: ExprOp3_0F :<: EX) =
    between(minTimestamp, expr, minRegex)

  def isDateOrTimestamp(expr: T) = between(minDate, expr, minRegex)

  def isArrayOrString(expr: T) =
    $or(isArray(expr), isString(expr))

  // Some types that happen to be adjacent:
  def isNumberOrString(expr: T) = betweenExcl(Bson.Null, expr, Bson.Doc())
  def isDateTimestampOrBoolean(expr: T) = between(Bson.Bool(false), expr, minRegex)
  def isSyntaxed(expr: T) =
    $or(
      $lt(expr, $literal(Bson.Doc())),
      between(Bson.ObjectId(Check.minOid), expr, minRegex))


  private def between(lower: Bson, expr: T, upper: Bson): T =
    $and(
      $lte($literal(lower), expr),
      $lt(expr, $literal(upper)))
  private def betweenExcl(lower: Bson, expr: T, upper: Bson): T =
    $and(
      $lt($literal(lower), expr),
      $lt(expr, $literal(upper)))
  private def betweenIncl(lower: Bson, expr: T, upper: Bson): T =
    $and(
      $lte($literal(lower), expr),
      $lte(expr, $literal(upper)))
}
object Check {
  /** Recognize typecheck constructions after the fact so that they can be
    * translated to other forms. This has to kept strictly in sync with the
    * constructors above.
    */
  // TODO: remove this when we no longer perform after-the-fact translation
  def unapply[T: Equal, EX[_]]
    (expr: T)
    (implicit TR: Recursive.Aux[T, EX], TC: Corecursive.Aux[T, EX], EX: Functor[EX], ev1: ExprOpCoreF :<: EX)
      : Option[(T, Type)] = {
    val exp = new ExprOpCoreF.fixpoint[T, EX](_.embed)
    import exp._

    val nilMap = ListMap.empty[String, Bson]
    expr match {
      case IsBetweenExcl(Bson.Null,            x, Bson.Text(""))            => (x, Type.Numeric).some
      case IsBetween(Bson.Text(""),            x, Bson.Doc(`nilMap`))       => (x, Type.Str).some
      case IsBetween(Bson.Doc(`nilMap`),       x, Bson.Arr(Nil))            => (x, Type.AnyObject).some
      case IsBetween(Bson.Arr(Nil),            x, Bson.Binary(`minBinary`)) => (x, Type.AnyArray).some
      case IsBetween(Bson.Binary(`minBinary`), x, Bson.ObjectId(`minOid`))  => (x, Type.Binary).some
      case IsBetween(Bson.ObjectId(`minOid`),  x, Bson.Bool(false))         => (x, Type.Id).some
      case IsBetweenIncl(Bson.Bool(false),     x, Bson.Bool(true))          => (x, Type.Bool).some
      case IsBetween(`minDate`,  x, `minRegex`)               => (x, Type.Date ⨿ Type.Timestamp).some
      case IsBetween(`minDate`,  x, `minTimestamp`)           => (x, Type.Date).some
      case IsBetween(`minTimestamp`,           x, `minRegex`)               => (x, Type.Timestamp).some

      case IsBetween(Bson.Doc(`nilMap`),       x, Bson.Binary(`minBinary`)) => (x, Type.AnyObject ⨿ Type.AnyArray).some
      case IsBetween(Bson.Binary(`minBinary`), x, `minRegex`)               => (x, Type.Binary ⨿ Type.Id ⨿ Type.Bool ⨿ Type.Date ⨿ Type.Timestamp).some
      case IsBetween(Bson.Bool(false),         x, `minRegex`)               => (x, Type.Bool ⨿ Type.Date ⨿ Type.Timestamp).some
      case IsBetweenExcl(Bson.Null,            x, Bson.Doc(`nilMap`))       => (x, Type.Numeric ⨿ Type.Str).some

      case $or(
            $lt(x1, $literal(Bson.Doc(`nilMap`))),
            IsBetween(Bson.ObjectId(`minOid`), x2, `minRegex`))
            if x1 ≟ x2 =>
        (x1, Type.Null ⨿ Type.Numeric ⨿ Type.Str ⨿ Type.Id ⨿ Type.Bool ⨿ Type.Date ⨿ Type.Timestamp).some

      case _ => None
    }
  }

  object IsBetween {
    def unapply[T: Equal, EX[_]]
      (expr: T)
      (implicit TR: Recursive.Aux[T, EX], TC: Corecursive.Aux[T, EX], EX: Functor[EX], ev1: ExprOpCoreF :<: EX)
        : Option[(Bson, T, Bson)] =
      expr match {
        case $and(
              $lte($literal(const1), x1),
              $lt(x2, $literal(const2)))
            if x1 ≟ x2 =>
          (const1, x1, const2).some
        case _ => None
      }
  }

  object IsBetweenExcl {
    def unapply[T: Equal, EX[_]]
      (expr: T)
      (implicit TR: Recursive.Aux[T, EX], TC: Corecursive.Aux[T, EX], EX: Functor[EX], ev1: ExprOpCoreF :<: EX)
        : Option[(Bson, T, Bson)] =
      expr match {
        case $and(
              $lt($literal(const1), x1),
              $lt(x2, $literal(const2)))
              if x1 ≟ x2 =>
          (const1, x1, const2).some
        case _ => None
      }
  }

  object IsBetweenIncl {
    def unapply[T: Equal, EX[_]]
      (expr: T)
      (implicit TR: Recursive.Aux[T, EX], TC: Corecursive.Aux[T, EX], EX: Functor[EX], ev1: ExprOpCoreF :<: EX)
        : Option[(Bson, T, Bson)] =
      expr match {
        case $and(
              $lte($literal(const1), x1),
              $lte(x2, $literal(const2)))
              if x1 ≟ x2 =>
          (const1, x1, const2).some
        case _ => None
      }
  }

  val minBinary = ImmutableArray.fromArray(scala.Array[Byte]())
  val minDate = Bson.Date(Long.MinValue)
  val minTimestamp = Bson.Timestamp(Int.MinValue, 0)
  val minOid =
    ImmutableArray.fromArray(scala.Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
  val minRegex = Bson.Regex("", "")
}
