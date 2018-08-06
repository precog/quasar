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

package quasar.ejson

import slamdata.Predef.{Char => SChar, Int => _, Map => _, _}
import quasar.contrib.matryoshka.birecursiveIso
import quasar.ejson.implicits._
import quasar.contrib.iota.copkTraverse

import matryoshka._
import monocle.Prism
import scalaz.==>>

final class Fixed[J] private ()(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]) {
  private val iso = birecursiveIso[J, EJson]

  val arr: Prism[J, List[J]] =
    iso composePrism optics.arr

  val bool: Prism[J, Boolean] =
    iso composePrism optics.bool

  val char: Prism[J, SChar] =
    iso composePrism optics.char

  val dec: Prism[J, BigDecimal] =
    iso composePrism optics.dec

  val int: Prism[J, BigInt] =
    iso composePrism optics.int

  val imap: Prism[J, J ==>> J] =
    iso composePrism optics.imap[J]

  val map: Prism[J, List[(J, J)]] =
    iso composePrism optics.map

  val meta: Prism[J, (J, J)] =
    iso composePrism optics.meta

  val nul: Prism[J, Unit] =
    iso composePrism optics.nul

  val sizedTpe: Prism[J, (TypeTag, BigInt)] =
    Prism[J, (TypeTag, BigInt)](j => SizedType.unapply(JR.project(j))) {
      case (tag, s) => SizedType(tag, s)
    }

  val str: Prism[J, String] =
    iso composePrism optics.str

  val tpe: Prism[J, TypeTag] =
    Prism[J, TypeTag](j => Type.unapply(JR.project(j)))(Type(_))
}

object Fixed {
  def apply[J](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Fixed[J] =
    new Fixed[J]
}
