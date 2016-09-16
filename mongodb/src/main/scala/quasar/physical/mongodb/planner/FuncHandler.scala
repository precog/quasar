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

package quasar.physical.mongodb.planner

import quasar.Predef._
import quasar.{UnaryFunc, BinaryFunc, TernaryFunc}
import quasar.fp.tree._
import quasar.physical.mongodb.Bson
import quasar.physical.mongodb.expression._
import quasar.std.StdLib._

import org.threeten.bp.Instant
import matryoshka._
import scalaz._

trait FuncHandler[EX[_]] {
  def translateUnary(func: UnaryFunc): Option[Unary[EX]]
  def translateBinary(func: BinaryFunc): Option[Binary[EX]]
  def translateTernary(func: TernaryFunc): Option[Ternary[EX]]
}

object FuncHandler {
  def fromPartial[EX[_]](
    unary: PartialFunction[UnaryFunc, Unary[EX]],
    binary: PartialFunction[BinaryFunc, Binary[EX]],
    ternary: PartialFunction[TernaryFunc, Ternary[EX]]): FuncHandler[EX] =
    new FuncHandler[EX] {
      def translateUnary(func: UnaryFunc) = unary.lift(func)
      def translateBinary(func: BinaryFunc) = binary.lift(func)
      def translateTernary(func: TernaryFunc) = ternary.lift(func)
    }

  val handle2_6: FuncHandler[Expr2_6] = FuncHandler.fromPartial[Expr2_6](
    {
      val coreFp = ExprOpCoreF.fixpoint[Unary, Expr2_6]
      import coreFp._
      import Unary._
      {
        case math.Negate        => $multiply($literal(Bson.Int32(-1)), arg)

        case string.Lower       => $toLower(arg)
        case string.Upper       => $toUpper(arg)

        case relations.Not      => $not(arg)

        case string.Null        =>
          $cond($eq(arg, $literal(Bson.Text("null"))),
            $literal(Bson.Null),
            $literal(Bson.Undefined))

        case string.Boolean     =>
          $cond($eq(arg, $literal(Bson.Text("true"))),
            $literal(Bson.Bool(true)),
            $cond($eq(arg, $literal(Bson.Text("false"))),
              $literal(Bson.Bool(false)),
              $literal(Bson.Undefined)))

        case date.ToTimestamp   => $add($literal(Bson.Date(Instant.ofEpochMilli(0))), arg)
      }
    },
    {
      val coreFp = ExprOpCoreF.fixpoint[Binary, Expr2_6]
      import coreFp._
      import Binary._
      {
        case set.Constantly     => arg1

        case math.Add           => $add(arg1, arg2)
        case math.Multiply      => $multiply(arg1, arg2)
        case math.Subtract      => $subtract(arg1, arg2)
        case math.Divide        => $divide(arg1, arg2)
        case math.Modulo        => $mod(arg1, arg2)

        case relations.Eq       => $eq(arg1, arg2)
        case relations.Neq      => $neq(arg1, arg2)
        case relations.Lt       => $lt(arg1, arg2)
        case relations.Lte      => $lte(arg1, arg2)
        case relations.Gt       => $gt(arg1, arg2)
        case relations.Gte      => $gte(arg1, arg2)

        case relations.Coalesce => $ifNull(arg1, arg2)

        case string.Concat      => $concat(arg1, arg2)

        case relations.Or       => $or(arg1, arg2)
        case relations.And      => $and(arg1, arg2)
      }
    },
    {
      val coreFp = ExprOpCoreF.fixpoint[Ternary, Expr2_6]
      import coreFp._
      import Ternary._
      {
        case string.Substring   => $substr(arg1, arg2, arg3)

        case relations.Cond     => $cond(arg1, arg2, arg3)

        case relations.Between  => $and($lte(arg2, arg1), $lte(arg1, arg3))
      }
    })

  def handle3_0(implicit I: Expr2_6 :<: Expr3_0): FuncHandler[Expr3_0] = new FuncHandler[Expr3_0] {
    // TODO
    def translateUnary(func: UnaryFunc) = handle2_6.translateUnary(func).map(_.mapSuspension(I))
    def translateBinary(func: BinaryFunc) = handle2_6.translateBinary(func).map(_.mapSuspension(I))
    def translateTernary(func: TernaryFunc) = handle2_6.translateTernary(func).map(_.mapSuspension(I))
  }

  def handle3_2(implicit I: Expr2_6 :<: Expr3_2): FuncHandler[Expr3_2] = new FuncHandler[Expr3_2] {
    // TODO
    def translateUnary(func: UnaryFunc) = handle2_6.translateUnary(func).map(_.mapSuspension(I))
    def translateBinary(func: BinaryFunc) = handle2_6.translateBinary(func).map(_.mapSuspension(I))
    def translateTernary(func: TernaryFunc) = handle2_6.translateTernary(func).map(_.mapSuspension(I))
  }
}
