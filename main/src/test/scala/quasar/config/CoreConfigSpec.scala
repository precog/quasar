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
import quasar.fs.mount._
import quasar.physical.mongodb.fs.MongoDBFsType

import org.specs2.ScalaCheck
import pathy.Path._

/** This tests a component of the “core” subproject, but it relies on something
  * from the “mongodb” subproject, so we’re testing it in “main”.
  */
class CoreConfigSpec extends ConfigSpec[CoreConfig] with ScalaCheck {
  import CoreConfigArbitrary._

  def sampleConfig(uri: ConnectionUri): CoreConfig = {
    CoreConfig(MountingsConfig(Map(
      rootDir -> MountConfig.fileSystemConfig(MongoDBFsType, uri)
    )))
  }

  def OldConfigStr =
    s"""{
      |  "mountings": {
      |    "/": {
      |      "mongodb": {
      |        "database": "$dbName",
      |        "connectionUri": "${testUri.value}"
      |      }
      |    }
      |  }
      |}""".stripMargin

  "fromString" should {
    "parse previous config" in {
      configOps.fromString(OldConfigStr) must beRightDisjunction(TestConfig)
    }
  }

  "encoding" should {
    "round-trip any well-formed config" >> prop { (cfg: CoreConfig) =>
      val json = CoreConfig.codec.encode(cfg)
      val cfg2 = CoreConfig.codec.decode(json.hcursor)
      cfg2.result must beRight(cfg)
    }.set(minTestsOk = 5)
  }
}
