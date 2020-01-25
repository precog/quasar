/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.std

import slamdata.Predef._
import quasar.{Func, Mapping, UnaryFunc, BinaryFunc, TernaryFunc}
import quasar.common.data.Data
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}

import matryoshka._
import scalaz._, Scalaz._
import shapeless._

trait RelationsLib extends Library {
  val Eq = BinaryFunc(
    Mapping,
    "Determines if two values are equal",
    noSimplification)

  val Neq = BinaryFunc(
    Mapping,
    "Determines if two values are not equal",
    noSimplification)

  val Lt = BinaryFunc(
    Mapping,
    "Determines if one value is less than another value of the same type",
    noSimplification)

  val Lte = BinaryFunc(
    Mapping,
    "Determines if one value is less than or equal to another value of the same type",
    noSimplification)

  val Gt = BinaryFunc(
    Mapping,
    "Determines if one value is greater than another value of the same type",
    noSimplification)

  val Gte = BinaryFunc(
    Mapping,
    "Determines if one value is greater than or equal to another value of the same type",
    noSimplification)

  val Between = TernaryFunc(
    Mapping,
    "Determines if a value is between two other values of the same type, inclusive",
    noSimplification)

  val IfUndefined = BinaryFunc(
    Mapping,
    "This is the only way to recognize an undefined value. If the first argument is undefined, return the second argument, otherwise, return the first.",
    noSimplification)

  val And = BinaryFunc(
    Mapping,
    "Performs a logical AND of two boolean values",
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(Constant(Data.True)), Embed(r))) => r.some
          case Invoke(_, Sized(Embed(l), Embed(Constant(Data.True)))) => l.some
          case _                                                       => None
        }
    })

  val Or = BinaryFunc(
    Mapping,
    "Performs a logical OR of two boolean values",
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(Constant(Data.False)), Embed(r))) => r.some
          case Invoke(_, Sized(Embed(l), Embed(Constant(Data.False)))) => l.some
          case _                                                        => None
        }
    })

  val Not = UnaryFunc(
    Mapping,
    "Performs a logical negation of a boolean value",
    noSimplification)

  val Cond = TernaryFunc(
    Mapping,
    "Chooses between one of two cases based on the value of a boolean expression",
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(Constant(Data.True)),  Embed(c), _)) => c.some
          case Invoke(_, Sized(Embed(Constant(Data.False)), _, Embed(a))) => a.some
          case _                                                            => None
        }
    })
}

object RelationsLib extends RelationsLib
