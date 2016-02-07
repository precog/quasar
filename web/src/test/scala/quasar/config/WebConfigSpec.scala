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

import quasar.Predef._
import quasar.fp.prism._
import quasar.fs.mount._
import quasar.physical.mongodb.fs.MongoDBFsType

import pathy.Path._

class WebConfigSpec extends ConfigSpec[WebConfig] {

  def configOps: ConfigOps[WebConfig] = WebConfig

  def sampleConfig(uri: ConnectionUri): WebConfig = WebConfig(
    server = ServerConfig(Some(92)),
    mountings = MountingsConfig2(Map(
      rootDir -> MountConfig2.fileSystemConfig(MongoDBFsType, uri))))

  override val ConfigStr =
    s"""{
      |  "server": {
      |    "port": 92
      |  },
      |  "mountings": {
      |    "/": {
      |      "mongodb": {
      |        "connectionUri": "${testUri.value}"
      |      }
      |    }
      |  }
      |}""".stripMargin
}
