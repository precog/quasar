/*
 * Copyright 2014–2017 SlamData Inc.
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
import quasar.ejson.EJson
import quasar.tpe._

import matryoshka._
import matryoshka.patterns.EnvT
import scalaz._, Scalaz._
import simulacrum._

/** Defines how to extract the `PrimaryTag` from `F`. */
@typeclass
trait ExtractPrimary[F[_]] {
  def primaryTag[A](fa: F[A]): Option[PrimaryTag]
}

object ExtractPrimary {
  import ops._

  implicit def coproductExtractPrimary[F[_]: ExtractPrimary, G[_]: ExtractPrimary]
    : ExtractPrimary[Coproduct[F, G, ?]] =
    new ExtractPrimary[Coproduct[F, G, ?]] {
      def primaryTag[A](fa: Coproduct[F, G, A]) =
        fa.run.fold(_.primaryTag, _.primaryTag)
    }

  implicit val taggedExtractPrimary: ExtractPrimary[Tagged] =
    new ExtractPrimary[Tagged] {
      def primaryTag[A](fa: Tagged[A]) = some(fa.tag.right)
    }

  implicit def typeFExtractPrimary[L](
    implicit L: Recursive.Aux[L, EJson]
  ): ExtractPrimary[TypeF[L, ?]] =
    new ExtractPrimary[TypeF[L, ?]] {
      def primaryTag[A](fa: TypeF[L, A]) = TypeF.primary(fa) map (_.left)
    }

  implicit def envTExtractPrimary[E, F[_]: ExtractPrimary]
    : ExtractPrimary[EnvT[E, F, ?]] =
    new ExtractPrimary[EnvT[E, F, ?]] {
      def primaryTag[A](fa: EnvT[E, F, A]) = fa.lower.primaryTag
    }
}
