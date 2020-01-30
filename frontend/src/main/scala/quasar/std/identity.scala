/*
 * Copyright 2014–2020 SlamData Inc.
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
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}

import matryoshka._
import scalaz._, Scalaz._
import shapeless._

trait IdentityLib extends Library {

  val Squash: UnaryFunc = UnaryFunc(
    Squashing,
    "Squashes all dimensional information",
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case InvokeUnapply(_, Sized(Embed(InvokeUnapply(Squash, Sized(x))))) =>
            Squash(x).some
          case _ => none
        }
    })

  val TypeOf = UnaryFunc(
    Mapping,
    "Returns the simple type of a value.",
    noSimplification)
}

object IdentityLib extends IdentityLib
