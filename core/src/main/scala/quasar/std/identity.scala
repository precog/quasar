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
import quasar._

import scalaz._
import shapeless.{Data => _, _}

trait IdentityLib extends Library {
  import Type._
  import Validation.success

  val Squash = UnaryFunc(
    Squashing,
    "SQUASH",
    "Squashes all dimensional information",
    Top,
    Func.Input1(Top),
    noSimplification,
    partialTyper[nat._1] { case Sized(x) => x },
    untyper[nat._1](t => success(Func.Input1(t))))

  val ToId = UnaryFunc(
    Mapping,
    "oid",
    "Converts a string to a (backend-specific) object identifier.",
    Type.Id,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => Type.Const(Data.Id(str))
      case Sized(Type.Str)                  => Type.Id
    },
    basicUntyper)

  def unaryFunctions: List[GenericFunc[nat._1]] = Squash :: ToId :: Nil
  def binaryFunctions: List[GenericFunc[nat._2]] = Nil
  def ternaryFunctions: List[GenericFunc[nat._3]] = Nil
}

object IdentityLib extends IdentityLib
