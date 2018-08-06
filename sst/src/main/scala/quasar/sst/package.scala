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

import quasar.common.data.Data
import quasar.contrib.matryoshka._
import quasar.ejson.{
  DecodeEJson,
  EJson,
  CommonEJson => C,
  ExtEJson => E,
  EncodeEJson,
  Meta,
  Null,
  SizedType,
  Str,
  TypeTag,
  Type => EType
}
import quasar.ejson.implicits._
import quasar.fp.ski.κ
import quasar.fp._
import quasar.contrib.iota._
import quasar.tpe._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import spire.algebra.{AdditiveSemigroup, Field, NRoot}
import spire.math.ConvertableTo

package object sst {
  type PrimaryTag = PrimaryType \/ TypeTag
  /** Statistical Structural Type */
  type SSTF[J, A, B]       = StructuralType.STF[J, TypeStat[A], B]
  type SST[J, A]           = StructuralType[J, TypeStat[A]]
  type PopulationSST[J, A] = StructuralType[J, TypeStat[A] @@ Population]

  /** Type tag indicating the tagged value represents an entire population
    * as opposed to a sample of a population.
    */
  sealed abstract class Population
  val Population = Tag.of[Population]

  object SST {
    def fromData[J: Order, A: ConvertableTo: Field: Order](
      count: A,
      data: Data
    )(implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson]
    ): SST[J, A] =
      fromEJson0(count, data.hylo[CoEnv[Data, EJson, ?], J](
        interpret(κ(C(Null[J]()).embed), elideNonTypeMetadata[J] >>> (_.embed)),
        Data.toEJson[EJson]))

    def fromEJson[J: Order, A: ConvertableTo: Field: Order](
      count: A,
      ejson: J
    )(implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson]
    ): SST[J, A] =
      fromEJson0(count, ejson.transAna[J](elideNonTypeMetadata[J]))

    def size[J, A: AdditiveSemigroup](sst: SST[J, A]): A =
      sst.copoint.size

    ////

    private def elideNonTypeMetadata[J](
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson]
    ): EJson[J] => EJson[J] = {
      case E(Meta(v, Embed(SizedType(t, s)))) => E(Meta(v, SizedType(t, s)))
      case E(Meta(v, Embed(EType(t))))        => E(Meta(v, EType(t)))
      case other                              => EJson.elideMetadata[J] apply other
    }

    private def fromEJson0[J: Order, A: ConvertableTo: Field: Order](
      count: A,
      ejson: J
    )(implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson]
    ): SST[J, A] = {
      type T = Fix[TypeF[J, ?]]

      val unfoldStructural: Coalgebra[StructuralType.ST[J, ?], T] =
        t => StructuralType.fromTypeƒ[J, T].apply(t)

      StructuralType(
        TypeF.const[J, T](ejson).embed.hylo(
          StructuralType.attributeSTƒ(TypeStat.fromTypeFƒ(count)),
          unfoldStructural))
    }
  }

  /** Returns the `PrimaryTag` for the given EJson value. */
  def primaryTagOf[J](ejs: J)(implicit J: Recursive.Aux[J, EJson]): PrimaryTag =
    ejs.project match {
      case E(Meta(_, Embed(EType(tag)))) => tag.right
      case C(Str(_))                     => strings.StructuralString.right
      case _                             => primaryTypeOf(ejs).left
    }

  // NB: Defined here as adding the tag causes the compiler not to consider the TypeStat companion.
  implicit def populationTypeStatDecodeEJson[A: DecodeEJson: Equal: Field: NRoot]: DecodeEJson[TypeStat[A] @@ Population] =
    Population.subst(DecodeEJson[TypeStat[A]])

  implicit def populationTypeStatEncodeEJson[A: EncodeEJson: Equal: Field: NRoot]: EncodeEJson[TypeStat[A] @@ Population] =
    new EncodeEJson[TypeStat[A] @@ Population] {
      def encode[J](ts: TypeStat[A] @@ Population)(
        implicit
        JC: Corecursive.Aux[J, EJson],
        JR: Recursive.Aux[J, EJson]
      ): J =
        TypeStat.encodeEJson0(Population.unwrap(ts), isPopulation = true)
    }
}
