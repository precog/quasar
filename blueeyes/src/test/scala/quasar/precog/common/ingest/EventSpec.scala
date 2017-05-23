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

package quasar.precog.common
package ingest

import quasar.blueeyes._, json._
import quasar.blueeyes.json.serialization.DefaultSerialization._
import quasar.blueeyes.json.serialization.Extractor._
import scalaz._
import quasar.precog.TestSupport._

class EventSpec extends Specification with ArbitraryEventMessage with ScalaCheck {
  implicit val arbEvent = Arbitrary(genRandomIngest)
  "serialization of an event" should {
    "read back the data that was written" in prop { in: Ingest =>
      in.serialize.validated[Ingest] must beLike {
        case Success(out) => out must_== in
      }
    }
  }

  "Event serialization" should {
    "Handle V0 format" in {
      (JObject("tokenId" -> JString("1234"),
               "path"    -> JString("/test/"),
               "data"    -> JObject("test" -> JNum(1)))).validated[Ingest] must beLike {
        case Success(_) => ok
      }
    }

    "Handle V1 format" in {
      (JObject("apiKey" -> JString("1234"),
               "path"    -> JString("/test/"),
               "data"    -> JObject("test" -> JNum(1)),
               "metadata" -> JArray())).validated[Ingest] must beLike {
        case Success(_) => ok
        case Failure(Thrown(ex)) =>
          throw ex
      }
    }
  }

  "Archive serialization" should {
    "Handle V0 format" in {
      JObject("tokenId" -> JString("1234"),
              "path"    -> JString("/test/")).validated[Archive] must beLike {
        case Success(_) => ok
      }
    }

    "Handle V1 format" in {
      JObject("apiKey" -> JString("1234"),
              "path"   -> JString("/test/")).validated[Archive] must beLike {
        case Success(_) => ok
      }
    }
  }
}
