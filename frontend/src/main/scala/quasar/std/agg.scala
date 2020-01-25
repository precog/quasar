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
import quasar._
import quasar.common.data.Data
import quasar.fp.ski._
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._
import shapeless.{Data => _, :: => _, _}

trait AggLib extends Library {

  private def simplifiesOnConstants(cont: Data => LP[Nothing]) = new Func.Simplifier {
    def apply[T]
      (orig: LP[T])
      (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
      orig match {
        case Invoke(_, Sized(src)) => src.project match {
          case Constant(d) => cont(d).map(ι).some
          case _ => None
        }
        case _ => None
      }
  }

  val Count = UnaryFunc(
    Reduction,
    "Counts the values in a set",
    simplifiesOnConstants(_ => Constant(Data.Int(1))))

  val Sum = UnaryFunc(
    Reduction,
    "Sums the values in a set",
    noSimplification)

  val Min = UnaryFunc(
    Reduction,
    "Finds the minimum in a set of values",
    simplifiesOnConstants(Constant(_)))

  val Max = UnaryFunc(
    Reduction,
    "Finds the maximum in a set of values",
    simplifiesOnConstants(Constant(_)))

  val First = UnaryFunc(
    Reduction,
    "Finds the first value in a set.",
    simplifiesOnConstants(Constant(_)))

  val Last = UnaryFunc(
    Reduction,
    "Finds the last value in a set.",
    simplifiesOnConstants(Constant(_)))

  val Avg = UnaryFunc(
    Reduction,
    "Finds the average in a set of numeric values",
    simplifiesOnConstants(Constant(_)))

  val Arbitrary = UnaryFunc(
    Reduction,
    "Returns an arbitrary value from a set",
    simplifiesOnConstants(Constant(_)))
}

object AggLib extends AggLib
