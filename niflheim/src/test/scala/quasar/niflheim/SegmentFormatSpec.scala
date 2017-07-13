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

package quasar.niflheim

import quasar.precog.common._

import quasar.precog.util._

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck._, Prop._

import java.time.ZonedDateTime

class V1SegmentFormatSpec extends SegmentFormatSpec {
  val format = V1SegmentFormat
}

class VersionedSegmentFormatSpec extends Specification with ScalaCheck with SegmentFormatSupport with SegmentFormatMatchers {
  val format = VersionedSegmentFormat(Map(
    1 -> V1SegmentFormat,
    2 -> new StubSegmentFormat)) // Much faster version of segment formats.

  "versioned segment formats" should {
    "read older versions" in {
      implicit val arbSegment = Arbitrary(genSegment(100))
      val old = VersionedSegmentFormat(Map(1 -> V1SegmentFormat))

      forAll { (segment0: Segment) =>
        val out = new InMemoryWritableByteChannel
        old.writer.writeSegment(out, segment0) must beLike { case _root_.scalaz.Success(_) =>
          val in = new InMemoryReadableByteChannel(out.toArray)
          format.reader.readSegment(in) must beLike { case _root_.scalaz.Success(segment1) =>
            areEqual(segment0, segment1)
          }
        }
      }
    }
  }
}

trait SegmentFormatSpec extends Specification with ScalaCheck with SegmentFormatSupport with SegmentFormatMatchers {
  def format: SegmentFormat

  def surviveRoundTrip(segment: Segment) = surviveRoundTripWithFormat(format)(segment)

  val EmptyBitSet = BitSetUtil.create()

  "segment formats" should {
    "roundtrip trivial null segments" in {
      surviveRoundTrip(NullSegment(1234L, CPath("a.b.c"), CNull, EmptyBitSet, 0))
      surviveRoundTrip(NullSegment(1234L, CPath("a.b.c"), CEmptyObject, EmptyBitSet, 0))
      surviveRoundTrip(NullSegment(1234L, CPath("a.b.c"), CEmptyArray, EmptyBitSet, 0))
    }
    "roundtrip trivial boolean segments" in surviveRoundTrip(BooleanSegment(1234L, CPath("a.b.c"), EmptyBitSet, EmptyBitSet, 0))
    "roundtrip trivial array segments" in {
      surviveRoundTrip(ArraySegment(1234L, CPath("a.b.c"), CLong, EmptyBitSet, new Array[Long](0)))
      surviveRoundTrip(ArraySegment(1234L, CPath("a.b.c"), CDouble, EmptyBitSet, new Array[Double](0)))
      surviveRoundTrip(ArraySegment(1234L, CPath("a.b.c"), CNum, EmptyBitSet, new Array[BigDecimal](0)))
      surviveRoundTrip(ArraySegment(1234L, CPath("a.b.c"), CString, EmptyBitSet, new Array[String](0)))
      surviveRoundTrip(ArraySegment(1234L, CPath("a.b.c"), CDate, EmptyBitSet, new Array[ZonedDateTime](0)))
    }
    "roundtrip simple boolean segment" in {
      val segment = BooleanSegment(1234L, CPath("a.b.c"),
        BitSetUtil.create(Seq(0)), BitSetUtil.create(Seq(0)), 1)
      surviveRoundTrip(segment)
    }
    "roundtrip undefined boolean segment" in {
      val segment = BooleanSegment(1234L, CPath("a.b.c"),
        EmptyBitSet, EmptyBitSet, 10)
      surviveRoundTrip(segment)
    }
    "roundtrip simple array segment" in {
      val segment = ArraySegment(1234L, CPath("a.b.c"), CDouble,
        BitSetUtil.create(Seq(0)), Array(4.2))
      surviveRoundTrip(segment)
    }
    "roundtrip undefined array segment" in {
      val segment = ArraySegment(1234L, CPath("a.b.c"), CDouble,
        EmptyBitSet, new Array[Double](100))
      surviveRoundTrip(segment)
    }
    "roundtrip arbitrary small segments" in {
      implicit val arbSegment = Arbitrary(genSegment(100))
      forAll { (segment: Segment) =>
        surviveRoundTrip(segment)
      }
    }

    // this test can take a crazy long time
    /*"roundtrip arbitrary large segments" in {
      implicit val arbSegment = Arbitrary(genSegment(10000))
      forAll { (segment: Segment) =>
        surviveRoundTrip(segment)
      }
    }*/
  }
}

