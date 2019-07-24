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

package quasar.impl.schema

import slamdata.Predef.{Product, Serializable}
import quasar.contrib.iota._
import quasar.sst._

import monocle.Prism
import matryoshka.Coalgebra
import matryoshka.patterns.EnvT
import matryoshka.implicits._
import scalaz.{@@, Bifunctor, Cord, Equal, Order, Show}
import scalaz.syntax.order._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import spire.algebra.{AdditiveSemigroup, Field, NRoot}
import spire.syntax.field._
import shims._

sealed trait SstSchema[J, A] extends Product with Serializable {
  import SstSchema._

  def size(implicit A: AdditiveSemigroup[A]): A =
    this match {
      case PopulationSchema(s) => s.copoint.value.unwrap.size
      case SampleSchema(s) => s.copoint.value.size
    }
}

object SstSchema extends SstSchemaInstances {
  import StructuralType.{ST, STF}

  /** The schema inferred for an entire dataset. */
  final case class PopulationSchema[J, A](value: StructuralType[J, Occurred[A, TypeStat[A] @@ Population]])
      extends SstSchema[J, A]

  /** A schema inferred from a sample of a larger dataset. */
  final case class SampleSchema[J, A](value: StructuralType[J, Occurred[A, TypeStat[A]]])
      extends SstSchema[J, A]

  def populationSchema[J, A]: Prism[SstSchema[J, A], StructuralType[J, Occurred[A, TypeStat[A] @@ Population]]] =
    Prism.partial[SstSchema[J, A], StructuralType[J, Occurred[A, TypeStat[A] @@ Population]]] {
      case PopulationSchema(s) => s
    } (PopulationSchema(_))

  def sampleSchema[J, A]: Prism[SstSchema[J, A], StructuralType[J, Occurred[A, TypeStat[A]]]] =
    Prism.partial[SstSchema[J, A], StructuralType[J, Occurred[A, TypeStat[A]]]] {
      case SampleSchema(s) => s
    } (SampleSchema(_))

  def fromSampled[J, A: Field: Order](sampleSize: A, sst: SST[J, A]): SstSchema[J, A] = {
    type T[X] = StructuralType[J, Occurred[A, X]]

    val size = SST.size(sst)

    val occurred =
      (Field[A].one, size, sst)
        .ana[StructuralType[J, Occurred[A, TypeStat[A]]]](occurrenceƒ[J, A])

    if (size < sampleSize)
      populationSchema(Population.subst[T, TypeStat[A]](occurred))
    else
      sampleSchema(occurred)
  }

  def occurrenceƒ[J, A: Field: Order]: Coalgebra[STF[J, Occurred[A, TypeStat[A]], ?], (A, A, SST[J, A])] = {
    case (pp, psize, sst) =>
      val csize = SST.size(sst)
      val cp = Field[A].one.min((csize / psize) * pp)
      Bifunctor[EnvT[?, ST[J, ?], ?]].bimap(sst.project)(Occurred(cp, _), (cp, csize, _))
  }
}

sealed abstract class SstSchemaInstances {
  import SstSchema._

  implicit def equal[J: Equal, A: Equal]: Equal[SstSchema[J, A]] = {
    type T[X] = StructuralType[J, Occurred[A, X]]

    Equal.equal {
      case (PopulationSchema(x), PopulationSchema(y)) =>
        Population.unsubst[T, TypeStat[A]](x) ≟ Population.unsubst[T, TypeStat[A]](y)

      case (SampleSchema(x), SampleSchema(y)) =>
        x ≟ y

      case _ => false
    }
  }

  implicit def show[J: Show, A: Equal: Field: NRoot: Show]: Show[SstSchema[J, A]] = {
    type T[X] = StructuralType[J, Occurred[A, X]]

    Show.show {
      case PopulationSchema(s) =>
        Cord("Population(") ++ Population.unsubst[T, TypeStat[A]](s).show ++ Cord(")")

      case SampleSchema(x) =>
        Cord("Sample(") ++ x.show ++ Cord(")")
    }
  }
}
