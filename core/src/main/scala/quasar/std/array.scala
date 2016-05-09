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

package quasar.std

import quasar.Predef._
import quasar.{Data, Type, Func, BinaryFunc, GenericFunc, Mapping, SemanticError}, SemanticError._

import scalaz._, NonEmptyList.nels, Validation.{success, failure}
import shapeless._

trait ArrayLib extends Library {
  val ArrayLength = BinaryFunc(
    Mapping,
    "array_length",
    "Gets the length of a given dimension of an array.",
    Type.Int,
    Func.Input2(Type.AnyArray, Type.Int),
    noSimplification,
    partialTyperV[nat._2] {
      case Sized(_, Type.Const(Data.Int(dim))) if (dim < 1) =>
        failure(nels(GenericError("array dimension out of range")))
      case Sized(Type.Const(Data.Arr(arr)), Type.Const(Data.Int(i)))
          if (i == 1) =>
        // TODO: we should support dims other than 1, but it's work
        success(Type.Const(Data.Int(arr.length)))
      case Sized(Type.AnyArray, Type.Const(Data.Int(_))) =>
        success(Type.Int)
    },
    basicUntyper)

  def unaryFunctions: List[GenericFunc[nat._1]] = Nil
  def binaryFunctions: List[GenericFunc[nat._2]] = ArrayLength :: Nil
  def ternaryFunctions: List[GenericFunc[nat._3]] = Nil
}

object ArrayLib extends ArrayLib
