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
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._
import shapeless.{Data => _, _}

trait SetLib extends Library {
  val Take: BinaryFunc = BinaryFunc(
    Sifting,
    "Takes the first N elements from a set",
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case InvokeUnapply(_, Sized(Embed(InvokeUnapply(Take, Sized(src, Embed(Constant(Data.Int(m)))))), Embed(Constant(Data.Int(n))))) =>
            Take(src, Constant[T](Data.Int(m.min(n))).embed).some
          case _ => None
        }
    })

  val Sample: BinaryFunc = BinaryFunc(
    Sifting,
    "Randomly selects N elements from a set",
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case InvokeUnapply(_, Sized(Embed(InvokeUnapply(Take, Sized(src, Embed(Constant(Data.Int(m)))))), Embed(Constant(Data.Int(n))))) =>
            Take(src, Constant[T](Data.Int(m.min(n))).embed).some
          case _ => None
        }
    })

  val Drop: BinaryFunc = BinaryFunc(
    Sifting,
    "Drops the first N elements from a set",
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(set), Embed(Constant(Data.Int(n)))))
              if n == 0 =>
            set.some
          case InvokeUnapply(_, Sized(Embed(InvokeUnapply(Drop, Sized(src, Embed(Constant(Data.Int(m)))))), Embed(Constant(Data.Int(n))))) =>
            Drop(src, Constant[T](Data.Int(m + n)).embed).some
          case _ => None
        }
    })

  val Range = BinaryFunc(
    Mapping,
    "Creates an array of values in the range from `a` to `b`, inclusive.",
    noSimplification)

  val Filter = BinaryFunc(
    Sifting,
    "Filters a set to include only elements where a projection is true",
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(set), Embed(Constant(Data.True)))) =>
            set.some
          case _ => None
        }
    })

  val GroupBy = BinaryFunc(
    Transformation,
    "Groups a projection of a set by another projection",
    noSimplification)

  val Union = BinaryFunc(
    Transformation,
    "Creates a new set with all the elements of each input set, keeping duplicates.",
    noSimplification)

  val Intersect = BinaryFunc(
    Transformation,
    "Creates a new set with only the elements that exist in both input sets, keeping duplicates.",
    noSimplification)

  val Except = BinaryFunc(
    Transformation,
    "Removes the elements of the second set from the first set.",
    noSimplification)

  val In = BinaryFunc(
    Sifting,
    "Determines whether a value is in a given set.",
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(item, set)) => set.project match {
            case Constant(_) => RelationsLib.Eq(item, set).some
            case _ => Within(item, StructuralLib.UnshiftArray(set).embed).some
          }
          case _ => None
        }
    })

  val Within = BinaryFunc(
    Mapping,
    "Determines whether a value is in a given array.",
    noSimplification)

  val Constantly = BinaryFunc(
    Transformation,
    "Always return the same value",
    noSimplification)
}

object SetLib extends SetLib
