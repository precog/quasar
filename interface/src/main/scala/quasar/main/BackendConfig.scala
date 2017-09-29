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

package quasar.main

import slamdata.Predef._
import quasar.contrib.pathy.{ADir, AFile, APath}
import quasar.contrib.scalaz._

import java.io.File
import scala.collection.Seq // uh, yeah

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
    val entriesM: Task[IList[(ClassName, ClassPath)]] = backends traverse {
      case (name, paths) =>
        for {
          unflattened <- IList(paths: _*) traverse { path =>
            val results =
              ADir.fromFile(path).covary[APath].orElse(AFile.fromFile(path).covary[APath])

            results.toListT.run.map(IList.fromList(_))
          }

          apaths = unflattened.flatten
        } yield ClassName(name) -> ClassPath(apaths)
    }

    entriesM.map(BackendConfig.ExplodedDirs(_))
  }

  /**
   * A single directory containing jars, each of which will be
   * loaded as a backend.  With each jar, the `BackendModule` class
   * name will be determined from the `Manifest.mf` file.
   */
  final case class JarDirectory(dir: ADir) extends BackendConfig

  /**
   * Any files in the classpath will be loaded as jars; any directories
   * will be assumed to contain class files (e.g. the target output of
   * SBT compile).  The class name should be the fully qualified Java
   * class name of the `BackendModule` implemented as a Scala object.
   * In most cases, this means the class name here will end with a `$`
   */
  final case class ExplodedDirs(backends: IList[(ClassName, ClassPath)]) extends BackendConfig
}
