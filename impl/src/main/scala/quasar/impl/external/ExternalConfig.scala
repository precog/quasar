/*
 * Copyright 2014–2020 SlamData Inc.
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

import slamdata.Predef._

import java.nio.file.{Files, Path}

import cats.effect.Sync
import scalaz.std.list._
import scalaz.syntax.traverse._

import shims.applicativeToScalaz

sealed trait ExternalConfig extends Product with Serializable

object ExternalConfig {

  /**
   * This should only be used for testing purposes.  It represents a
   * configuration in which no backends will be loaded at all.
   */
  val Empty: ExternalConfig = ExplodedDirs(Nil)

  def fromDatasources[F[_]: Sync](datasources: List[(String, List[Path])]): F[ExternalConfig] = {
    val entriesF = datasources traverse {
      case (name, paths) =>
        Sync[F] delay {
          ClassName(name) -> ClassPath(paths.filter(Files.exists(_)))
        }
    }

    entriesF.map(ExternalConfig.ExplodedDirs(_))
  }

  /**
   * A single directory containing plugin files, each of which will be
   * loaded as a backend. Plugin is defined by a json file containing
   * a path to the main plugin jar from which `LightweightDatasourceModule`
   * or `HeavyweightDatasourceModule` class name will be determined using the
   * `Manifest.mf` file. The other thing in plugin file is a classpath that
   * will be loaded for this plugin.
   */
  final case class PluginDirectory(dir: Path) extends ExternalConfig

  /**
   * Similar to PluginDirectory, but explicitly referencing files. This
   * makes it possible to specify plugin files that are in different
   * directories, rather than forcing them all to be gathered in one location
   */
  final case class PluginFiles(files: List[Path]) extends ExternalConfig

  /**
   * Any files in the classpath will be loaded as jars; any directories
   * will be assumed to contain class files (e.g. the target output of
   * SBT compile).  The class name should be the fully qualified Java
   * class name of the `LightweightDatasourceModule` or
   * `HeavyweightDatasourceModule` implemented as a Scala object.
   * In most cases, this means the class name here will end with a `$`
   */
  final case class ExplodedDirs(modules: List[(ClassName, ClassPath)]) extends ExternalConfig
}
