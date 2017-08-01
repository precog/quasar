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

import quasar.blueeyes.json._
import quasar.blueeyes.json.serialization.DefaultSerialization._
import scalaz._
import quasar.precog.TestSupport._

class ArchiveSpecs extends Specification with ArbitraryEventMessage with ScalaCheck {
  implicit val arbArchive = Arbitrary(genRandomArchive)
  "serialization of an archive" should {
    "read back the data that was written" in prop { in: Archive =>
      in.serialize.validated[Archive] must beLike {
        case Success(out) => in must_== out
      }
    }

    "read legacy archives" in {
      val Success(JArray(input)) = JParser.parseFromString("""[
{"path":"/test/test/php/query/T10170960455069fb56d061c690884208/","tokenId":"test1"},
{"path":"/test/test/php/query/T9345418045069fd119e9ed256256425/","tokenId": "test1"},
{"path":"/test/test/php/query/T1373621163506a00891eb60240629876/","tokenId":"test1"},
{"path":"/test/test/php/query/T1564471072506a01ed32be5009280574/","tokenId":"test1"},
{"path":"/test/test/php/query/T1172864121506c4ea9e2308492490793/","tokenId":"test1"},
{"path":"/test/test/ttt/","tokenId":"test2"},
{"path":"/test/nathan/politicalsentiment/twitter/test/1/","tokenId":"test3"},
{"path":"/test/test/","tokenId":"test2"},
{"path":"/test/foo/","tokenId":"test4"}
]""")

      val results = input.map(_.validated[Archive]).collect {
        case Success(result) => result
      }

      results.size mustEqual 9
      results.map(_.apiKey).toSet mustEqual Set("test1", "test2", "test3", "test4")
    }

    "read new archives" in {
      val Success(JArray(input)) = JParser.parseFromString("""[
{"apiKey":"test1","path":"/foo1/test/js/delete/"},
{"apiKey":"test2","path":"/foo2/blargh/"},
{"apiKey":"test2","path":"/foo2/blargh/"},
{"apiKey":"test2","path":"/foo2/testing/"},
{"apiKey":"test2","path":"/foo2/testing/"}
]""")

      val results = input.map(_.validated[Archive]).collect {
        case Success(result) => result
      }

      results.size mustEqual 5
      results.map(_.apiKey).toSet mustEqual Set("test1", "test2")
    }

    "read archives with reversed fields" in {
      val Success(JArray(input)) = JParser.parseFromString("""[
{"path":"test1","apiKey":"/foo1/test/js/delete/"},
{"path":"test2","apiKey":"/foo2/blargh/"},
{"path":"test2","apiKey":"/foo2/blargh/"},
{"path":"test2","apiKey":"/foo2/testing/"},
{"path":"test2","apiKey":"/foo2/testing/"}
]""")

      val results = input.map(_.validated[Archive]).collect {
        case Success(result) => result
      }

      results.size mustEqual 5
      results.map(_.apiKey).toSet mustEqual Set("test1", "test2")
    }

  }
}


