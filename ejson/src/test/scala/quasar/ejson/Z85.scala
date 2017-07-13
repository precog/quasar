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

package quasar.ejson.z85

import scala.Predef.$conforms
import slamdata.Predef._

import org.scalacheck._
import org.typelevel.discipline.specs2.mutable._
import scalaz._, Scalaz._
import scodec.bits._

class Z85Specs extends org.specs2.mutable.SpecificationLike with Discipline { // with quasar.QuasarSpecification with Discipline {
  implicit val arbitraryByteVectors: Arbitrary[ByteVector] =
    Arbitrary(Arbitrary.arbitrary[List[Byte]].map(ByteVector(_)))

  implicit val shrinkByteVector: Shrink[ByteVector] =
    Shrink[ByteVector] { b =>
      if (b.nonEmpty)
        Stream.iterate(b.take(b.size / 2))(b2 => b2.take(b2.size / 2))
              .takeWhile(_.nonEmpty) ++ Stream(ByteVector.empty)
      else Stream.empty
    }

  "decodeBlock" should {
    "give the expected string" in {
      val hello = decodeBlock("hello")
      hello.map(encodeBlock) must beSome("hello")
    }
  }

  def padBV(bv: ByteVector): ByteVector = {
    val size = bv.size
    if ((size % 4) ≟ 0) bv
    else bv.padTo(size + 4 - (size % 4))
  }

  "decode >>> encode" should {
    "be identity, modulo padding" >> prop { (b: ByteVector) =>
      decode(encode(b)) must beSome(padBV(b))
    }
  }

  // FIXME: frames (full conversions) don’t form a prism, due to padding, but
  //        blocks should. However, we need an arbitrary that generates only
  //        4-byte blocks … or a more constrained type.
  // checkAll("Z85 block", PrismTests(block))
}
