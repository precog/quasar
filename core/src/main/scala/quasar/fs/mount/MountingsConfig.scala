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

package quasar.fs.mount

import slamdata.Predef._
import quasar.fp.ski._
import quasar.contrib.pathy.APath

import argonaut._, Argonaut._, DecodeResultScalaz._
import monocle.Iso
import pathy.Path._
import scalaz.syntax.bifunctor._
import scalaz.syntax.foldable._
import scalaz.std.tuple._
import scalaz.std.list._

final case class MountingsConfig(toMap: Map[APath, MountConfig]) extends AnyVal

object MountingsConfig {
  import APath._

  val empty: MountingsConfig =
    MountingsConfig(Map())

  val mapIso: Iso[MountingsConfig, Map[APath, MountConfig]] =
    Iso((_: MountingsConfig).toMap)(MountingsConfig(_))

  implicit val mountingsConfigEncodeJson: EncodeJson[MountingsConfig] =
    EncodeJson.of[Map[String, MountConfig]]
      .contramap(_.toMap.map(_.leftMap(posixCodec.printPath(_))))

  implicit val mountingsConfigDecodeJson: DecodeJson[MountingsConfig] =
    DecodeJson.of[Map[String, MountConfig]]
      .flatMap(m0 => DecodeJson(κ(m0.toList.foldLeftM(Map[APath, MountConfig]()) {
        case (m, (s, mc)) => jString(s).as[APath].map(p => m + (p -> mc))
      }))).map(MountingsConfig(_))
}
