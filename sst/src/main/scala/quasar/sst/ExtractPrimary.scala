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
import quasar.ejson.EJson
import quasar.tpe._
import quasar.contrib.iota.mkInject

import matryoshka._
import matryoshka.patterns.EnvT
import scalaz._, Scalaz._
import simulacrum._
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::

/** Defines how to extract the `PrimaryTag` from `F`. */
@typeclass
trait ExtractPrimary[F[_]] {
  def primaryTag[A](fa: F[A]): Option[PrimaryTag]
}

object ExtractPrimary {
  import ops._

  implicit def copk[LL <: TListK](implicit M: Materializer[LL]): ExtractPrimary[CopK[LL, ?]] =
    M.materialize(offset = 0)

  sealed trait Materializer[LL <: TListK] {
    def materialize(offset: Int): ExtractPrimary[CopK[LL, ?]]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[F[_]](
      implicit
      F: ExtractPrimary[F]
    ): Materializer[F ::: TNilK] = new Materializer[F ::: TNilK] {
      override def materialize(offset: Int): ExtractPrimary[CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new ExtractPrimary[CopK[F ::: TNilK, ?]] {
          override def primaryTag[A](cfa: CopK[F ::: TNilK, A]): Option[PrimaryTag] = cfa match {
            case I(fa) => fa.primaryTag
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[F[_], LL <: TListK](
      implicit
      F: ExtractPrimary[F],
      LL: Materializer[LL]
    ): Materializer[F ::: LL] = new Materializer[F ::: LL] {
      override def materialize(offset: Int): ExtractPrimary[CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new ExtractPrimary[CopK[F ::: LL, ?]] {
          override def primaryTag[A](cfa: CopK[F ::: LL, A]): Option[PrimaryTag] = cfa match {
            case I(fa) => fa.primaryTag
            case other => LL.materialize(offset + 1).primaryTag(other.asInstanceOf[CopK[LL, A]])
          }
        }
      }
    }
  }

  implicit val taggedExtractPrimary: ExtractPrimary[Tagged] =
    new ExtractPrimary[Tagged] {
      def primaryTag[A](fa: Tagged[A]) = some(fa.tag.right)
    }

  implicit def typeFExtractPrimary[L](
    implicit L: Recursive.Aux[L, EJson]
  ): ExtractPrimary[TypeF[L, ?]] =
    new ExtractPrimary[TypeF[L, ?]] {
      def primaryTag[A](fa: TypeF[L, A]) =
        fa match {
          case TypeF.Const(l) => some(primaryTagOf(l))
          case _ => TypeF.primary(fa) map (_.left)
        }
    }

  implicit def envTExtractPrimary[E, F[_]: ExtractPrimary]
    : ExtractPrimary[EnvT[E, F, ?]] =
    new ExtractPrimary[EnvT[E, F, ?]] {
      def primaryTag[A](fa: EnvT[E, F, A]) = fa.lower.primaryTag
    }
}
