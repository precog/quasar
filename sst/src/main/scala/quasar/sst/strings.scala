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

package quasar.sst

import slamdata.Predef._
import quasar.contrib.matryoshka.envT
import quasar.ejson.{EJson, TypeTag}
import quasar.tpe.{SimpleType, TypeF}
import quasar.contrib.iota.copkTraverse

import matryoshka.{Corecursive, Recursive}
import scalaz.{IList, Order}
import scalaz.std.option._
import scalaz.syntax.either._
import scalaz.syntax.foldable._
import spire.algebra.Field
import spire.math.ConvertableTo

object strings {
  import StructuralType.{TagST, TypeST, STF}

  val StructuralString = TypeTag("_structural.string")

  /** Compresses a string into a generic char[]. */
  def compress[T, J, A: ConvertableTo: Order](strStat: TypeStat[A], s: String)(
    implicit
    A: Field[A],
    C: Corecursive.Aux[T, SSTF[J, A, ?]],
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, T] = {
    // NB: Imported here so as not to pollute outer scope given Iterable's
    //     pervasiveness.
    import scalaz.std.iterable._

    val charStat =
      s.toIterable.foldMap(c => some(TypeStat.fromEJson(A.one, EJson.char(c))))

    val charArr =
      charStat.fold(IList.empty[T].left[T]) { ts =>
        C.embed(envT(ts, TypeST(TypeF.simple(SimpleType.Char)))).right
      }

    stringTagged(strStat, C.embed(envT(strStat, TypeST(TypeF.arr(charArr)))))
  }

  /** Widens a string into an array of its characters.
    *
    * FIXME: Overly specific, define in terms of [Co]Recursive.
    */
  def widen[J: Order, A: ConvertableTo: Field: Order](count: A, s: String)(
    implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, SST[J, A]] = {
    val charArr =
      SST.fromEJson(count, EJson.arr(s.map(EJson.char[J](_)) : _*))

    stringTagged(charArr.copoint, charArr)
  }

  ////

  private def stringTagged[T, L, V](v: V, t: T): STF[L, V, T] =
    envT(v, TagST[L](Tagged(StructuralString, t)))
}
