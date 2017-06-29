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

import org.specs2.mutable.Specification
import org.specs2._
import org.scalacheck._

import java.io.File

class V1CookedBlockFormatSpecs extends CookedBlockFormatSpecs {
  val format = V1CookedBlockFormat
}

case class VersionedCookedBlockFormatSpecs() extends CookedBlockFormatSpecs {
  val format = VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat))
}

trait CookedBlockFormatSpecs extends Specification with ScalaCheck with SegmentFormatSupport {
  def format: CookedBlockFormat

  implicit val arbFile = Arbitrary(for {
    parts <- Gen.listOfN(3, Gen.identifier map { part =>
      part.substring(0, math.min(part.length, 5))
    })
  } yield new File(parts.mkString("/", "/", ".cooked")))

  implicit val arbSegmentId = Arbitrary(genSegmentId)

  "cooked block format" should {
    "round trip empty segments" in {
      surviveRoundTrip(format)(CookedBlockMetadata(999L, 0, new Array[(SegmentId, File)](0)))
    }

    "round trip simple segments" in {
      surviveRoundTrip(format)(CookedBlockMetadata(999L, 1,
          Array(SegmentId(1234L, CPath("a.b.c"), CLong) -> new File("/hello/there/abc.cooked"))
      ))
    }

    // this test seems to run forever?
    /*"roundtrip arbitrary blocks" in {
      forAll { files: List[(SegmentId, File)] =>
        surviveRoundTrip(format)(CookedBlockMetadata(999L, files.length, files.toArray))
      }.set(maxDiscardRatio = 20f)
    }*/
  }

  //def surviveRoundTrip(format: CookedBlockFormat)(segments0: Array[(SegmentId, File)]) = {
  def surviveRoundTrip(format: CookedBlockFormat)(segments0: CookedBlockMetadata) = {
    val out = new InMemoryWritableByteChannel
    format.writeCookedBlock(out, segments0) must beLike {
      case _root_.scalaz.Success(_) =>
        val in = new InMemoryReadableByteChannel(out.toArray)
        format.readCookedBlock(in) must beLike {
          case _root_.scalaz.Success(segments1) =>
            segments1 must_== segments0
        }
    }
  }
}
