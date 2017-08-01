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
import quasar.console.stderr
import quasar.config._

import argonaut._
import scalaz.{Lens => _, _}, Scalaz._
import scalaz.concurrent.Task

object config {
  /** Attempts to load the specified config file or one found at any of the
    * default paths. If an error occurs, it is logged to STDERR and the
    * default configuration is returned.
    */
  def loadConfigFile[C: DecodeJson](configFile: Option[FsFile])(implicit cfgOps: ConfigOps[C]): Task[C] = {
    import ConfigError._
    cfgOps.fromOptionalFile(configFile).run flatMap {
      case \/-(c) => c.point[Task]

      case -\/(FileNotFound(f)) => for {
        codec <- FsPath.systemCodec
        fstr  =  FsPath.printFsPath(codec, f)
        _     <- stderr(s"Configuration file '$fstr' not found, using default configuration.")
        cfg   <- cfgOps.default
      } yield cfg

      case -\/(MalformedConfig(_, rsn)) =>
        stderr(s"Error in configuration file, using default configuration: $rsn") *>
        cfgOps.default
    }
  }
}
