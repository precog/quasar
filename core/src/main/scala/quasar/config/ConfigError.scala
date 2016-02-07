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

package quasar.config

import quasar.Predef.{String, StringContext, Some, None}

import monocle.Prism
import scalaz.Show

trait ConfigError

object ConfigError {
  final case class MalformedConfig private[config] (src: String, reason: String)
    extends ConfigError

  final case class FileNotFound private[config] (file: FsFile)
    extends ConfigError

  val malformedConfig: Prism[ConfigError, (String, String)] =
    Prism[ConfigError, (String, String)] {
      case MalformedConfig(src, rsn) => Some((src, rsn))
      case _                         => None
    } ((MalformedConfig(_, _)).tupled)

  val fileNotFound: Prism[ConfigError, FsFile] =
    Prism[ConfigError, FsFile] {
      case FileNotFound(f) => Some(f)
      case _               => None
    } (FileNotFound(_))

  implicit val configErrorShow: Show[ConfigError] = {
    def printFile(f: FsFile) =
      FsPath.printFsPath(FsPath.codecForOS(OS.posix), f)

    Show.shows {
      case MalformedConfig(src, rsn) =>
        s"Configuration is malformed: $rsn"
      case FileNotFound(f) =>
        s"Configuration file not found: ${printFile(f)}"
    }
  }
}
