/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.effect.Failure
import quasar.fs.Path
import quasar.fs.mount.MountingsConfig2

import argonaut._, Argonaut._
import pathy.Path.{File, Dir, Sandboxed}
import scalaz.{Coyoneda, EitherT}
import scalaz.concurrent.Task

package object config {
  type FsFile = FsPath[File, Sandboxed]
  type FsDir  = FsPath[Dir, Sandboxed]

  type CfgErr[A]  = Failure[ConfigError, A]
  type CfgErrF[A] = Coyoneda[CfgErr, A]

  type CfgErrT[F[_], A] = EitherT[F, ConfigError, A]
  type CfgTask[A]       = CfgErrT[Task, A]

  // NB: Deprecated
  type MountingsConfig = Map[Path, MountConfig]

  object MountingsConfig {
    // TODO: !!!!
    def fromMC2(mc2: MountingsConfig2): MountingsConfig =
      mc2.asJson.as[MountingsConfig].getOr(Map.empty)

    implicit val mountingsConfigCodecJson: CodecJson[MountingsConfig] =
      CodecJson[MountingsConfig](
        encoder = map => map.map(t => t._1.pathname -> t._2).asJson,
        decoder = cursor => implicitly[DecodeJson[Map[String, MountConfig]]]
          .decode(cursor).map(_.map(t => Path(t._1) -> t._2)))
  }

}
