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

package quasar.impl.external

import slamdata.Predef.List

import java.nio.file.{Path, Paths}

import argonaut._, Argonaut._
import cats.effect.Sync
import cats.syntax.functor._

final case class Plugin(mainJar: Path, classPath: ClassPath) {
  def withAbsolutePaths[F[_]: Sync](parent: Path): F[Plugin] =
    Sync[F].delay(parent.toAbsolutePath) map { absParent =>
      Plugin(
        absParent.resolve(mainJar),
        ClassPath(classPath.value.map(absParent.resolve(_))))
    }
}

object Plugin {
  val FileExtension = "plugin"
  val ManifestAttributeName = "Datasource-Module"
  val ManifestAttributeDestinationName = "Destination-Module"
  val ManifestVersionName = "Implementation-Version"

  def fromJson[F[_]: Sync](json: Json): F[DecodeResult[Plugin]] =
    Sync[F].delay(pluginDecodeJson.decodeJson(json))

  ////

  // Private as calling `Paths.get` is a side-effect.
  private val pluginDecodeJson: DecodeJson[Plugin] = {
    implicit val pathDecodeJson: DecodeJson[Path] =
      optionDecoder(json => json.string.map(Paths.get(_)), "Path")

    implicit val classPathDecodeJson: DecodeJson[ClassPath] =
      DecodeJson.of[List[Path]].map(ClassPath(_))

    jdecode2L(Plugin.apply)("mainJar", "classPath")
  }
}
