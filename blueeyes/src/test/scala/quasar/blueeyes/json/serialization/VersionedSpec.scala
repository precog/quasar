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

package quasar.blueeyes
package json
package serialization

import DefaultSerialization._
import Extractor.Invalid
import scalaz._
import quasar.precog.JsonTestSupport._

object VersionedSpec extends Specification with ScalaCheck {
  import IsoSerializationSpec._
  import Versioned._

  "versioned serialization" should {
    "serialize a simple case class" in {
      val fooDecomp = decomposerV[Foo](fooSchema, Some("1.0".v))

      val result = fooDecomp.decompose(foo)

      result must_== JParser.parseUnsafe("""{ "s": "Hello world", "i": 23, "b": true, "schemaVersion": "1.0" }""")
    }
  }

  "versioned deserialization" should {
    "extract to a simple case class" in {
      val fooExtract = extractorV[Foo](fooSchema, Some("1.0".v))

      val result = fooExtract.extract(
        jobject(
          jfield("s", "Hello world"),
          jfield("i", 23),
          jfield("b", true),
          jfield("schemaVersion", "1.0")
        )
      )

      result must_== foo
    }

    "refuse to deserialize an object missing a version" in {
      val fooExtract = extractorV[Foo](fooSchema, Some("1.0".v))

      val result = fooExtract.validated(
        jobject(
          jfield("s", "Hello world"),
          jfield("i", 23),
          jfield("b", true)
        )
      )

      result must beLike {
        case Failure(Invalid(message, None)) => message must startWith(".schemaVersion property missing")
      }
    }

    "refuse to deserialize an object from a future version" in {
      val fooExtract = extractorV[Foo](fooSchema, Some("1.0".v))

      val result = fooExtract.validated(
        jobject(
          jfield("s", "Hello world"),
          jfield("i", 23),
          jfield("b", true),
          jfield("schemaVersion", "1.1")
        )
      )

      result must beLike {
        case Failure(Invalid(message, None)) => message must contain("was incompatible with desired version")
      }
    }

    "deserialize an object from a major-compatible prior version" in {
      val fooExtract = extractorV[Foo](fooSchema, Some("1.1".v))

      val result = fooExtract.validated(
        jobject(
          jfield("s", "Hello world"),
          jfield("i", 23),
          jfield("b", true),
          jfield("schemaVersion", "1.0")
        )
      )

      result must beLike {
        case Success(v) => v must_== foo
      }
    }
  }
}
