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

package quasar.impl.datasource

import slamdata.Predef._

import quasar.api.datasource.DatasourceType
import quasar.api.destination.DestinationType
import quasar.api.resource.ResourcePath
import quasar.contrib.scalaz.MonadError_
import quasar.fp.ski.ι

import java.nio.file.{Paths, Path => JPath}

import argonaut.{DecodeJson, Json}

import cats.data.EitherT
import cats.effect.Sync
import cats.syntax.applicative._

import eu.timepit.refined.auto._

import pathy.Path

import scalaz.\/
import scalaz.syntax.foldable._

import shims.monadToScalaz

package object local {
  val LocalType = DatasourceType("local", 1L)
  val LocalParsedType = DatasourceType("local-parsed", 1L)

  val LocalDestinationType = DestinationType("local", 1L, 1L)

  def toNio[F[_]: Sync](root: JPath, rp: ResourcePath): F[JPath] =
    Path.flatten("", "", "", ι, ι, rp.toPath).foldLeftM(root) { (p, n) =>
      if (n.isEmpty) p.pure[F]
      else MonadError_[F, Throwable].unattempt_(\/.fromTryCatchNonFatal(p.resolve(n)))
    }

  def attemptConfig[F[_]: Sync, A: DecodeJson, B](
    config: Json,
    errorPrefix: String)(onError: (Json, String) => B)
      : EitherT[F, B, A] =
    EitherT.fromEither(config.as[A].toEither)
      .leftMap[B] {
        case (s, _) => onError(config, errorPrefix + s)
      }

  def validatePath[F[_]: Sync, B](
    path: String,
    config: Json,
    errorPrefix: String)(onError: (Json, String) => B)
      : EitherT[F, B, JPath] =
    EitherT(Sync[F].attempt(Sync[F].delay(Paths.get(path))))
      .leftMap[B](t => onError(config, errorPrefix + t.getMessage))
}
