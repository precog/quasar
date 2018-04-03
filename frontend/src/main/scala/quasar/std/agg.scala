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

import quasar._

import scalaz._, Validation.success
import shapeless.{Data => _, :: => _, _}

trait AggLib extends Library {
  private val MathRel = Type.Numeric ⨿ Type.Interval

  private val reflexiveUntyper: Func.Untyper[nat._1] =
    untyper[nat._1](t => success(Func.Input1(t)))

  val Count = UnaryFunc(
    Reduction,
    "Counts the values in a set",
    Type.Int,
    Func.Input1(Type.Top),
    noSimplification,
    basicTyper[nat._1],
    basicUntyper[nat._1])

  val Sum = UnaryFunc(
    Reduction,
    "Sums the values in a set",
    Type.Numeric ⨿ Type.Interval,
    Func.Input1(Type.Numeric ⨿ Type.Interval),
    noSimplification,
    widenConstTyper(_(0)),
    reflexiveUntyper)

  val Min = UnaryFunc(
    Reduction,
    "Finds the minimum in a set of values",
    Type.Comparable,
    Func.Input1(Type.Comparable),
    noSimplification,
    widenConstTyper(_(0)),
    reflexiveUntyper)

  val Max = UnaryFunc(
    Reduction,
    "Finds the maximum in a set of values",
    Type.Comparable,
    Func.Input1(Type.Comparable),
    noSimplification,
    widenConstTyper(_(0)),
    reflexiveUntyper)

  val First = UnaryFunc(
    Reduction,
    "Finds the first value in a set.",
    Type.Top,
    Func.Input1(Type.Top),
    noSimplification,
    widenConstTyper(_(0)),
    reflexiveUntyper)

  val Last = UnaryFunc(
    Reduction,
    "Finds the last value in a set.",
    Type.Top,
    Func.Input1(Type.Top),
    noSimplification,
    widenConstTyper(_(0)),
    reflexiveUntyper)

  val Avg = UnaryFunc(
    Reduction,
    "Finds the average in a set of numeric values",
    Type.Numeric,
    Func.Input1(Type.Numeric),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(t) if MathRel.contains(t) =>
        success(t.widenConst)
    },
    reflexiveUntyper)

  val Arbitrary = UnaryFunc(
    Reduction,
    "Returns an arbitrary value from a set",
    Type.Top,
    Func.Input1(Type.Top),
    noSimplification,
    widenConstTyper(_(0)),
    reflexiveUntyper)
}

object AggLib extends AggLib
