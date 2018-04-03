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

package quasar.main

import slamdata.Predef._

import java.io.File
import scala.collection.Seq   // uh, yeah

import scalaz.IList
import scalaz.concurrent.Task
import scalaz.syntax.traverse._

sealed trait BackendConfig extends Product with Serializable

/**
 * APaths relative to real filesystem root
 */
object BackendConfig {

  /**
   * This should only be used for testing purposes.  It represents a
   * configuration in which no backends will be loaded at all.
   */
  val Empty: BackendConfig = ExplodedDirs(IList.empty)

  def fromBackends(backends: IList[(String, Seq[File])]): Task[BackendConfig] = {
    val entriesM = backends traverse {
      case (name, paths) =>
        Task delay {
          ClassName(name) -> ClassPath(IList(paths.filter(_.exists()): _*))
        }
    }

    entriesM.map(BackendConfig.ExplodedDirs(_))
  }

  /**
   * A single directory containing plugin files, each of which will be
   * loaded as a backend. Plugin is defined by a json file containing
   * a path to the main plugin jar from which `BackendModule` class
   * name will be determined using the `Manifest.mf` file. The other thing
   * in plugin file is a classpath that will be loaded for this plugin.
   */
  final case class PluginDirectory(dir: File) extends BackendConfig

  /**
   * Any files in the classpath will be loaded as jars; any directories
   * will be assumed to contain class files (e.g. the target output of
   * SBT compile).  The class name should be the fully qualified Java
   * class name of the `BackendModule` implemented as a Scala object.
   * In most cases, this means the class name here will end with a `$`
   */
  final case class ExplodedDirs(backends: IList[(ClassName, ClassPath)]) extends BackendConfig
}
