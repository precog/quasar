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

import slamdata.Predef._
import quasar.ejson.implicits._
import quasar.contrib.iota.copkTraverse

import matryoshka._
import matryoshka.implicits._
import matryoshka.patterns.EnvT
import scalaz._
import simulacrum.typeclass

/** Typeclass for higher-kinded types that can be encoded as EJson. */
@typeclass
trait EncodeEJsonK[F[_]] {
  def encodeK[J](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Algebra[F, J]

  def contramapK[G[_]](f: G ~> F): EncodeEJsonK[G] = {
    val orig = this
    new EncodeEJsonK[G] {
      def encodeK[J](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Algebra[G, J] =
        gj => orig.encodeK[J].apply(f(gj))
    }
  }
}

object EncodeEJsonK extends EncodeEJsonKInstances {
  def envT[E: EncodeEJson, F[_]: EncodeEJsonK](
    askLabel: String,
    lowerLabel: String
  ): EncodeEJsonK[EnvT[E, F, ?]] =
    new EncodeEJsonK[EnvT[E, F, ?]] {
      def encodeK[J](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Algebra[EnvT[E, F, ?], J] = {
        case EnvT((ask, lower)) =>
          val j = Fixed[J]
          val fAlg = EncodeEJsonK[F].encodeK[J]
          j.map(List(
            j.str(askLabel)   -> ask.asEJson[J],
            j.str(lowerLabel) -> fAlg(lower)))
      }
    }
}

sealed abstract class EncodeEJsonKInstances {
  implicit val ejsonEncodeEJsonK: EncodeEJsonK[EJson] =
    new EncodeEJsonK[EJson] {
      def encodeK[J](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Algebra[EJson, J] =
        _.embed
    }

  implicit def coproductEncodeEJsonK[F[_], G[_]](
    implicit
    F: EncodeEJsonK[F],
    G: EncodeEJsonK[G]
  ): EncodeEJsonK[Coproduct[F, G, ?]] =
    new EncodeEJsonK[Coproduct[F, G, ?]] {
      def encodeK[J](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Algebra[Coproduct[F, G, ?], J] =
        _.run.fold(F.encodeK[J], G.encodeK[J])
    }
}
