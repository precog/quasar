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
import quasar.contrib.matryoshka.{envT => cenvT}
import quasar.contrib.iota.copkTraverse
import matryoshka._
import matryoshka.implicits._
import matryoshka.patterns.EnvT
import scalaz._
import scalaz.syntax.applicative._
import scalaz.syntax.plus._
import simulacrum.typeclass

/** Typeclass for higher-kinded types that can be decoded from EJson. */
@typeclass
trait DecodeEJsonK[F[_]] {
  def decodeK[J](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): CoalgebraM[Decoded, F, J]

  def mapK[G[_]](f: F ~> G): DecodeEJsonK[G] = {
    val orig = this
    new DecodeEJsonK[G] {
      def decodeK[J](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): CoalgebraM[Decoded, G, J] =
        j => orig.decodeK[J].apply(j) map f
    }
  }
}

object DecodeEJsonK extends DecodeEJsonKInstances {
  def envT[E: DecodeEJson, F[_]: DecodeEJsonK](
    askLabel: String,
    lowerLabel: String
  ): DecodeEJsonK[EnvT[E, F, ?]] =
    new DecodeEJsonK[EnvT[E, F, ?]] {
      def decodeK[J](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): CoalgebraM[Decoded, EnvT[E, F, ?], J] = {
        val J = Fixed[J]
        val F = DecodeEJsonK[F].decodeK[J]

        _ match {
          case J.map((J.str(`askLabel`), ask) :: (J.str(`lowerLabel`), lower) :: Nil) =>
            (DecodeEJson[E].decode[J](ask) |@| F(lower))(cenvT)

          case J.map((J.str(`lowerLabel`), lower) :: (J.str(`askLabel`), ask) :: Nil) =>
            (DecodeEJson[E].decode[J](ask) |@| F(lower))(cenvT)

          case j =>
            Decoded.failureFor[EnvT[E, F, J]](j, s"EnvT($askLabel, $lowerLabel).")
        }
      }
    }
}

sealed abstract class DecodeEJsonKInstances {
  implicit val ejsonDecodeEJsonK: DecodeEJsonK[EJson] =
    new DecodeEJsonK[EJson] {
      def decodeK[J](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): CoalgebraM[Decoded, EJson, J] =
        _.project.point[Decoded]
    }

  implicit def coproductDecodeEJsonK[F[_], G[_]](
    implicit
    F: DecodeEJsonK[F],
    G: DecodeEJsonK[G]
  ): DecodeEJsonK[Coproduct[F, G, ?]] =
    new DecodeEJsonK[Coproduct[F, G, ?]] {
      def decodeK[J](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): CoalgebraM[Decoded, Coproduct[F, G, ?], J] =
        j => F.decodeK[J].apply(j).map(Coproduct.left[G](_)) <+>
             G.decodeK[J].apply(j).map(Coproduct.right[F](_))
    }
}
