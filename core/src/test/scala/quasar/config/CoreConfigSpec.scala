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

package quasar.config

import slamdata.Predef._
import quasar.config.CoreConfigArbitrary._
import quasar.db.DbConnectionConfig

import scalaz._, Scalaz._

class CoreConfigSpec extends ConfigSpec[CoreConfig] {

  val TestConfig: CoreConfig = CoreConfig(
    metastore = MetaStoreConfig(DbConnectionConfig.H2("/h2")).some)

  val TestConfigStr: String =
    s"""{
       |  "metastore": {
       |    "database": {
       |      "h2": {
       |        "location": "/h2"
       |      }
       |    }
       |  }
       |}""".stripMargin
}
