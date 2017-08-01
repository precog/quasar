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

package quasar.repl

import slamdata.Predef._
import quasar.build.BuildInfo
import quasar.cli.Cmd, Cmd._

import scopt.OptionParser

/** Command-line options supported by the Quasar REPL. */
final case class CliOptions(
  cmd: Cmd,
  config: Option[String])

object CliOptions {
  val default: CliOptions =
    CliOptions(Start, None)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  val parser: OptionParser[CliOptions] = new OptionParser[CliOptions]("quasar") {
    head("quasar", BuildInfo.version)

    opt[String]('c', "config") action { (x, c) =>
      c.copy(config = Some(x))
    } text("path to the config file to use")

    help("help") text("prints this usage text\n")

    cmd("initUpdateMetaStore")
      .text("Initialize and update the metastore\n")
      .action((_, c) => c.copy(cmd = InitUpdateMetaStore))

  }
}
