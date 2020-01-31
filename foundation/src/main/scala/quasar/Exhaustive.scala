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

package quasar

import cats.data.NonEmptyList
import cats.syntax.list._

import shapeless.{Prism => _, _}
import shapeless.ops.hlist.{Align, ToTraversable}
import shapeless.ops.coproduct.ToHList

import scala.List
import scala.annotation.implicitNotFound

/** Proof that `H` is comprised of a term for each constructor of the sum type `I`.
  *
  * Based on a similar construct in morphling
  * @see https://github.com/danslapman/morphling
  */
@implicitNotFound(msg = "Cannot prove exhaustivity, you must provide a term for each constructor of ${I}")
sealed trait Exhaustive[I, H <: HList] {
  def toNel(h: H): NonEmptyList[I]
}

object Exhaustive {
  implicit def evidence[I, C <: Coproduct, H0 <: HList, H <: HList](
      implicit
      G: Generic.Aux[I, C],
      L: ToHList.Aux[C, H0],
      A: Align[H, H0],
      T: ToTraversable.Aux[H, List, I])
      : Exhaustive[I, H] =
    new Exhaustive[I, H] {
      def toNel(h: H): NonEmptyList[I] =
        h.toList.toNel.get
    }
}
