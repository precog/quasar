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

package quasar.std

import slamdata.Predef._
import quasar._
import quasar.common.data.Data
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._
import shapeless.{Data => _, :: => _, _}

trait StringLib extends Library {

  val Concat = BinaryFunc(
    Mapping,
    "Concatenates two (or more) string values",
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case InvokeUnapply(_, Sized(Embed(Constant(Data.Str(""))), Embed(second))) =>
            second.some
          case InvokeUnapply(_, Sized(Embed(first), Embed(Constant(Data.Str(""))))) =>
            first.some
          case _ => None
        }
    })

  val Like = TernaryFunc(
    Mapping,
    "Determines if a string value matches a pattern.",
    noSimplification)

  val Search = TernaryFunc(
    Mapping,
    "Determines if a string value matches a regular expression. If the third argument is true, then it is a case-insensitive match.",
    noSimplification)

  val Length = UnaryFunc(
    Mapping,
    "Counts the number of characters in a string.",
    noSimplification)

  val Lower = UnaryFunc(
    Mapping,
    "Converts the string to lower case.",
    noSimplification)

  val Upper = UnaryFunc(
    Mapping,
    "Converts the string to upper case.",
    noSimplification)

  /** Substring which always gives a result, no matter what offsets are provided.
    * Reverse-engineered from MongoDb's \$substr op, for lack of a better idea
    * of how this should work. Note: if `start` < 0, the result is `""`.
    * If `length` < 0, then result includes the rest of the string. Otherwise
    * the behavior is as you might expect.
    */
  def safeSubstring(str: String, start: Int, length: Int): String =
    if (start < 0 || start > str.length) ""
    else if (length < 0) str.substring(start, str.length)
    else str.substring(start, (start + length) min str.length)

  val Substring: TernaryFunc = TernaryFunc(
    Mapping,
    "Extracts a portion of the string",
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case InvokeUnapply(f @ TernaryFunc(_, _, _), Sized(
            Embed(Constant(Data.Str(str))),
            Embed(Constant(Data.Int(from))),
            for0)) if from != 0 =>
              if (from < 0 || from > str.length) Constant[T](Data.Str("")).some
              else
                Invoke(f, Func.Input3(
                  Constant[T](Data.Str(str.substring(from.intValue))).embed,
                  Constant[T](Data.Int(0)).embed,
                  for0)).some
          case _ => None
        }
    })

  val Split = BinaryFunc(
    Mapping,
    "Splits a string into an array of substrings based on a delimiter.",
    noSimplification)

  val Boolean = UnaryFunc(
    Mapping,
    "Converts the strings “true” and “false” into boolean values. This is a partial function – arguments that don’t satisfy the constraint have undefined results.",
    noSimplification)

  val Integer = UnaryFunc(
    Mapping,
    "Converts strings containing integers into integer values. This is a partial function – arguments that don’t satisfy the constraint have undefined results.",
    noSimplification)

  val Decimal = UnaryFunc(
    Mapping,
    "Converts strings containing decimals into decimal values. This is a partial function – arguments that don’t satisfy the constraint have undefined results.",
    noSimplification)

  val Number = UnaryFunc(
    Mapping,
    "Converts strings containing a number into a number. This is a partial function – arguments that don’t satisfy the constraint have undefined results.",
    noSimplification)

  val Null = UnaryFunc(
    Mapping,
    "Converts strings containing “null” into the null value. This is a partial function – arguments that don’t satisfy the constraint have undefined results.",
    noSimplification)

  val ToString: UnaryFunc = UnaryFunc(
    Mapping,
    "Converts any primitive type to a string.",
    noSimplification)
}

object StringLib extends StringLib
