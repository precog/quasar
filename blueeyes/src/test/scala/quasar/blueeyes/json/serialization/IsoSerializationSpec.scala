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
import IsoSerialization._
import shapeless._
import quasar.precog.TestSupport._

object IsoSerializationSpec {
  case class Foo(s: String, i: Option[Int], b: Boolean)
  val foo                    = Foo("Hello world", Some(23), true)
  val foo2                   = Foo("Hello world", None, true)
  val foo3                   = Foo("Hello default world", Some(23), true)
  val fooSchema              = "s" :: "i" :: "b" :: HNil
  val defaultedFooSchema     = ("s" ||| "Hello default world") :: "i" :: "b" :: HNil
  val fooVariantSchema       = ("s" | "z") :: "i" :: "b" :: HNil
  val safeFooSchema          = "s" :: Omit :: "b" :: HNil
  val safeDefaultedFooSchema = (Omit ||| "Hello default world") :: "i" :: "b" :: HNil

  case class Bar(d: Double, f: Foo, l: List[String])
  val bar              = Bar(2.3, foo, List("foo", "bar", "baz"))
  val bar2             = Bar(2.3, foo2, List("foo", "bar", "baz"))
  val barSchema        = "d" :: "f" :: "l" :: HNil
  val inlinedBarSchema = "d" :: Inline :: "l" :: HNil

  case class Baz(s: String, l: List[Foo])
  val baz       = Baz("Hello world", List(foo, foo2))
  val bazSchema = "s" :: "l" :: HNil
}

class IsoSerializationSpec extends Specification {
  import IsoSerializationSpec._
  "serialization" should {
    "serialize a simple case class" in {
      val fooDecomp = decomposer[Foo](fooSchema)

      val result = fooDecomp.decompose(foo)

      result must_== JParser.parseUnsafe("""{ "s": "Hello world", "i": 23, "b": true }""")
    }

    "serialize a simple case class with field aliases" in {
      val fooDecomp = decomposer[Foo](fooVariantSchema)

      val result = fooDecomp.decompose(foo)

      result must_== JParser.parseUnsafe("""{ "s": "Hello world", "i": 23, "b": true }""")
    }

    "serialize a simple case class omitting absent optional fields" in {
      val fooDecomp = decomposer[Foo](fooSchema)

      val result = fooDecomp.decompose(foo2)

      result must_== JParser.parseUnsafe("""{ "s": "Hello world", "b": true }""")
    }

    "serialize a simple case class with defaulted fields" in {
      val fooDecomp = decomposer[Foo](defaultedFooSchema)

      val result = fooDecomp.decompose(foo)

      result must_== JParser.parseUnsafe("""{ "s": "Hello world", "i": 23, "b": true }""")
    }

    "serialize a simple case class with omitted fields" in {
      val fooDecomp = decomposer[Foo](safeFooSchema)

      val result = fooDecomp.decompose(foo)

      result must_== JParser.parseUnsafe("""{ "s": "Hello world", "b": true }""")
    }

    "serialize a simple case class with defaulted omitted fields" in {
      val fooDecomp = decomposer[Foo](safeDefaultedFooSchema)

      val result = fooDecomp.decompose(foo)

      result must_== JParser.parseUnsafe("""{ "i": 23, "b": true }""")
    }

    "serialize a case class with a nested case class element" in {
      implicit val fooDecomp = decomposer[Foo](fooSchema)
      val barDecomp          = decomposer[Bar](barSchema)

      val result = barDecomp.decompose(bar)

      result must_== JParser.parseUnsafe("""{
        "d": 2.3,
        "f": { "s": "Hello world", "i": 23, "b": true },
        "l": ["foo", "bar", "baz"]
      }""")
    }

    "serialize a case class with a nested case class element respecting alternative schema" in {
      implicit val fooDecomp = decomposer[Foo](safeFooSchema)
      val barDecomp          = decomposer[Bar](barSchema)

      val result = barDecomp.decompose(bar)

      result must_== JParser.parseUnsafe("""{
        "d": 2.3,
        "f": { "s": "Hello world", "b": true },
        "l": ["foo", "bar", "baz"]
      }""")
    }

    "serialize a case class with an inlined case class element" in {
      implicit val fooDecomp = decomposer[Foo](fooSchema)
      val barDecomp          = decomposer[Bar](inlinedBarSchema)

      val result = barDecomp.decompose(bar)

      result must_== JParser.parseUnsafe("""{
        "d": 2.3, "s": "Hello world", "i": 23, "b": true, "l": ["foo", "bar", "baz"]
      }""")
    }

    "serialize a case class with a list of nested case class elements" in {
      implicit val fooDecomp = decomposer[Foo](fooSchema)
      val bazDecomp          = decomposer[Baz](bazSchema)

      val result = bazDecomp.decompose(baz)

      result must_== JParser.parseUnsafe("""{
        "s": "Hello world",
        "l": [{ "s": "Hello world", "i": 23, "b": true }, { "s": "Hello world", "b": true }]
      }""")
    }
  }

  "deserialization" should {
    "extract to a simple case class" in {
      val fooExtract = extractor[Foo](fooSchema)

      val result = fooExtract.extract(
        jobject(
          jfield("s", "Hello world"),
          jfield("i", 23),
          jfield("b", true)
        )
      )

      result must_== foo
    }

    "extract to a simple case class with field aliases" in {
      val fooExtract = extractor[Foo](fooVariantSchema)

      val result1 = fooExtract.extract(
        jobject(
          jfield("s", "Hello world"),
          jfield("i", 23),
          jfield("b", true)
        )
      )

      result1 must_== foo

      val result2 = fooExtract.extract(
        jobject(
          jfield("z", "Hello world"),
          jfield("i", 23),
          jfield("b", true)
        )
      )

      result2 must_== foo
    }

    "extract to a simple case class with an absent optional field" in {
      val fooExtract = extractor[Foo](fooSchema)

      val result = fooExtract.extract(
        jobject(
          jfield("s", "Hello world"),
          jfield("b", true)
        )
      )

      result must_== foo2
    }

    "extract to a simple case class with an absent defaulted field" in {
      val fooExtract = extractor[Foo](defaultedFooSchema)

      val result = fooExtract.extract(
        jobject(
          jfield("i", 23),
          jfield("b", true)
        )
      )

      result must_== foo3
    }

    "extract to a simple case class with omitted fields" in {
      val fooExtract = extractor[Foo](safeFooSchema)

      val result = fooExtract.extract(
        jobject(
          jfield("s", "Hello world"),
          jfield("b", true)
        )
      )

      result must_== foo2
    }

    "extract to a simple case class with defaulted omitted fields" in {
      val fooExtract = extractor[Foo](safeDefaultedFooSchema)

      val result = fooExtract.extract(
        jobject(
          jfield("i", 23),
          jfield("b", true)
        )
      )

      result must_== foo3
    }

    "extract to a case class with a nested case class element" in {
      implicit val fooExtract = extractor[Foo](fooSchema)
      val barExtract          = extractor[Bar](barSchema)

      val result = barExtract.extract(
        jobject(
          jfield("d", 2.3),
          jfield(
            "f",
            jobject(
              jfield("s", "Hello world"),
              jfield("i", 23),
              jfield("b", true)
            )),
          jfield("l", jarray(JString("foo"), JString("bar"), JString("baz")))
        )
      )

      result must_== bar
    }

    "extract to a case class with a nested case class element respecting alternative schema" in {
      implicit val fooExtract = extractor[Foo](fooSchema)
      val barExtract          = extractor[Bar](barSchema)

      val result = barExtract.extract(
        jobject(
          jfield("d", 2.3),
          jfield(
            "f",
            jobject(
              jfield("s", "Hello world"),
              jfield("b", true)
            )),
          jfield("l", jarray(JString("foo"), JString("bar"), JString("baz")))
        )
      )

      result must_== bar2
    }

    "extract to a case class with a nested case class element from an inlined serialization" in {
      implicit val fooExtract = extractor[Foo](fooSchema)
      val barExtract          = extractor[Bar](inlinedBarSchema)

      val result = barExtract.extract(
        jobject(
          jfield("d", 2.3),
          jfield("s", "Hello world"),
          jfield("i", 23),
          jfield("b", true),
          jfield("l", jarray(JString("foo"), JString("bar"), JString("baz")))
        )
      )

      result must_== bar
    }

    "extract to a case class with a list of nested case class elements" in {
      implicit val fooExtract = extractor[Foo](fooSchema)
      val bazExtract          = extractor[Baz](bazSchema)

      val result = bazExtract.extract(
        jobject(
          jfield("s", "Hello world"),
          jfield(
            "l",
            jarray(
              jobject(
                jfield("s", "Hello world"),
                jfield("i", 23),
                jfield("b", true)
              ),
              jobject(jfield("s", "Hello world"), jfield("b", true))
            ))
        )
      )

      result must_== baz
    }
  }
}
