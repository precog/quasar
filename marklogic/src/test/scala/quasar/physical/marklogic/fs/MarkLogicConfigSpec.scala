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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar.physical.marklogic.{DocType, ErrorMessages}

import java.net.URI

import pathy.Path._
import scalaz._
import scalaz.std.option._

final class MarkLogicConfigSpec extends quasar.Qspec {
  "fromUriString" >> {
    val validUri = "xcc://user:pass@ml.example.com/somedb/foo/bar?format=json"

    "builds config from a valid uri" >> {
      MarkLogicConfig.fromUriString[ErrorMessages \/ ?](validUri)
        .toOption must_= Some(MarkLogicConfig(
          new URI("xcc://user:pass@ml.example.com/somedb"),
          rootDir </> dir("foo") </> dir("bar"),
          DocType.json))
    }

    "root dir is root when only db specified" >> {
      MarkLogicConfig.fromUriString[ErrorMessages \/ ?](
        "xcc://ml.example.com/adb"
      ).toOption.map(_.rootDir) must_= Some(rootDir)
    }

    "uses XML doc type when no format parameter given" >> {
      MarkLogicConfig.fromUriString[ErrorMessages \/ ?](
        "xcc://ml.example.com/adb"
      ).toOption.map(_.docType) must_= Some(DocType.xml)
    }

    "fails when no db specified" >> {
      MarkLogicConfig.fromUriString[ErrorMessages \/ ?](
        "xcc://ml.example.com/"
      ).toOption must beNone
    }

    "fails when unknown format specified" >> {
      MarkLogicConfig.fromUriString[ErrorMessages \/ ?](
        "xcc://ml.example.com/db?format=fake"
      ).toOption must beNone
    }

    "fails when non-xcc URL specified" >> {
      MarkLogicConfig.fromUriString[ErrorMessages \/ ?](
        "http://ml.example.com/db"
      ).toOption must beNone
    }
  }
}
