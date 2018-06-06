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

package quasar

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.{ejson => ejs}
import quasar.ejson.{CommonEJson => C, EJson, ExtEJson => E}
import quasar.contrib.iota.copkTraverse

import algebra.PartialOrder
import algebra.lattice._
import matryoshka._
import monocle.Prism
import scalaz._, Scalaz._

package object tpe {
  type PrimaryType = SimpleType \/ CompositeType

  object PrimaryType {
    val name: Prism[String, PrimaryType] =
      Prism((s: String) =>
        SimpleType.name.getOption(s).map(_.left) orElse
        CompositeType.name.getOption(s).map(_.right))(
        _.fold(SimpleType.name(_), CompositeType.name(_)))
  }

  object SimpleEJson {
    def unapply[J](ejs: J)(implicit J: Recursive.Aux[J, EJson]): Option[SimpleType] =
      simpleTypeOf(ejs)
  }

  /** Returns the primary type of the given EJson value, if it is composite. */
  def compositeTypeOf[J](ejs: J)(
    implicit J: Recursive.Aux[J, EJson]
  ): Option[CompositeType] =
    primaryTypeOf(ejs).toOption

  /** Returns the primary type of the given EJson value. */
  def primaryTypeOf[J](ejs: J)(implicit J: Recursive.Aux[J, EJson]): PrimaryType =
    J.cata(ejs)(primaryTypeOfƒ)

  /** Fold `EJson` to its PrimaryType. */
  val primaryTypeOfƒ: Algebra[EJson, PrimaryType] = {
    case E(ejs.Meta(v, _)) => v
    // Simple
    case C(ejs.Null() )    => SimpleType.Null.left
    case C(ejs.Bool(_))    => SimpleType.Bool.left
    case E(ejs.Byte(_))    => SimpleType.Byte.left
    case E(ejs.Char(_))    => SimpleType.Char.left
    case C(ejs.Str(_))     => SimpleType.Str.left
    case E( ejs.Int(_))    => SimpleType.Int.left
    case C( ejs.Dec(_))    => SimpleType.Dec.left
    // Composite
    case C(ejs.Arr(_))     => CompositeType.Arr.right
    case E(ejs.Map(_))     => CompositeType.Map.right
  }

  /** Returns the primary type of the given EJson value, if it is simple. */
  def simpleTypeOf[J](ejs: J)(
    implicit J: Recursive.Aux[J, EJson]
  ): Option[SimpleType] =
    primaryTypeOf(ejs).swap.toOption

  // Instances

  implicit def birecursiveTTypePartialOrder[T[_[_]]: BirecursiveT, J: Order](
    implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): PartialOrder[T[TypeF[J, ?]]] =
    TypeF.subtypingPartialOrder[J, T[TypeF[J, ?]]]

  implicit def birecursiveTTypeBoundedDistributiveLattice[T[_[_]]: BirecursiveT, J: Order](
    implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): BoundedDistributiveLattice[T[TypeF[J, ?]]] =
    TypeF.boundedDistributiveLattice[J, T[TypeF[J, ?]]]
}
