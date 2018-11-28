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
import quasar.fp.numeric.SampleStats
import quasar.tpe.{SimpleType, TypeF}
import quasar.contrib.iota.copkTraverse

import matryoshka.{Corecursive, Recursive}
import matryoshka.implicits._
import scalaz.{IList, Order}
import scalaz.std.option._
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
      JR: Recursive.Aux[J, EJson])
      : SSTF[J, A, T] = {
    // NB: Imported here so as not to pollute outer scope given Iterable's
    //     pervasiveness.
    import scalaz.std.iterable._

    val charStat =
      s.toIterable.foldMap(c => some(TypeStat.fromEJson(A.one, EJson.char(c))))

    val charArr =
      charStat map { ts =>
        C.embed(envT(ts, TypeST(TypeF.simple(SimpleType.Char))))
      }

    val arrStat =
      TypeStat.coll(strStat.size, some(A.fromInt(s.length)), some(A.fromInt(s.length)))

    stringTagged(strStat, C.embed(envT(arrStat, TypeST(TypeF.arr(IList[T](), charArr)))))
  }

  def simple[T, J, A](strStat: TypeStat[A]): SSTF[J, A, T] =
    envT(strStat, TypeST[J, T](TypeF.Simple(SimpleType.Str)))

  /** Widens a string into an array of its characters. */
  def widen[J: Order, A: ConvertableTo: Field: Order](strStat: TypeStat[A], s: String)(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : SSTF[J, A, SST[J, A]] = {

    val charArr =
      SST.fromEJson(strStat.size, EJson.arr(s.map(EJson.char[J](_)) : _*))

    stringTagged(strStat, charArr)
  }

  /** Widens a string into a character array, maintaining stats on literals,
    * but dropping the literals themselves.
    */
  def widenStats[J: Order, A: ConvertableTo: Field: Order](strStat: TypeStat[A], s: String)(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : SSTF[J, A, SST[J, A]] = {

    val len =
      some(ConvertableTo[A].fromInt(s.length))

    val chars =
      s.foldLeft(IList.empty[SST[J, A]]) { (l, c) =>
        val s =
          envT(
            TypeStat.char(SampleStats.one(ConvertableTo[A].fromInt(c.toInt)), c, c),
            TypeST(TypeF.simple[J, SST[J, A]](SimpleType.Char))).embed

        s :: l
      }

    val charArr =
      envT(
        TypeStat.coll(strStat.size, len, len),
        TypeST(TypeF.arr[J, SST[J, A]](chars.reverse, none))).embed

    stringTagged(strStat, charArr)
  }

  ////

  private def stringTagged[T, L, V](v: V, t: T): STF[L, V, T] =
    envT(v, TagST[L](Tagged(StructuralString, t)))
}
