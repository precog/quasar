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

package quasar.server

import slamdata.Predef._
import quasar.build.BuildInfo
import quasar.cli.Cmd, Cmd._

import monocle.Lens
import monocle.macros.Lenses
import scopt.OptionParser

/** Command-line options supported by Quasar. */
@Lenses
final case class CliOptions(
  cmd: Cmd,
  config: Option[String],
  contentLoc: Option[String],
  contentPath: Option[String],
  contentPathRelative: Boolean,
  openClient: Boolean,
  port: Option[Int])

object CliOptions {
  val default: CliOptions =
    CliOptions(Cmd.Start, None, None, None, false, false, None)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  val parser = new CliOptionsParser(Lens.id[CliOptions], "quasar") {
      head("quasar", BuildInfo.version)

      help("help") text("prints this usage text\n")

      cmd("initUpdateMetaStore")
        .text("Initializes and updates the metastore.\n")
        .action((_, c) =>
          (Lens.id[CliOptions] composeLens CliOptions.cmd).set(InitUpdateMetaStore)(c))
    }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  class CliOptionsParser[C](l: Lens[C, CliOptions], cmdName: String)
    extends OptionParser[C](cmdName) {

    opt[String]('c', "config") action { (x, c) =>
      (l composeLens config).set(Some(x))(c)
    } text("path to the config file to use")

    opt[String]('L', "content-location") action { (x, c) =>
      (l composeLens contentLoc).set(Some(x))(c)
    } text("location where static content is hosted")

    opt[String]('C', "content-path") action { (x, c) =>
      (l composeLens contentPath).set(Some(x))(c)
    } text("path where static content lives")

    opt[Unit]('r', "content-path-relative") action { (_, c) =>
      (l composeLens contentPathRelative).set(true)(c)
    } text("specifies that the content-path is relative to the install directory (not the current dir)")

    opt[Unit]('o', "open-client") action { (_, c) =>
      (l composeLens openClient).set(true)(c)
    } text("opens a browser window to the client on startup")

    opt[Int]('p', "port") action { (x, c) =>
      (l composeLens port).set(Some(x))(c)
    } text("the port to run Quasar on")
  }
}
