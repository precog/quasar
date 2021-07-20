/*
 * Copyright 2020 Precog Data
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

package quasar.connector

import slamdata.Predef._

import fs2.{Stream, Chunk}

import ResultData._

final class ResultDataSpec extends quasar.Qspec {

  "delimited" >> {
    "of Delimited is constructor argument" >> {
      val arg0 = Stream.emits(List(Part.Output(Chunk(1, 2, 3)), Part.emptyOffsetKey))
      Delimited(arg0).delimited must_=== arg0
      val arg1 = Stream.emits(List(
        Part.Output(Chunk("foo", "bar")),
        Part.emptyOffsetKey,
        Part.offsetKeyFromBytes(Array(0x11, 0x23)),
        Part.Output(Chunk("baz"))))
      Delimited(arg1).delimited must_=== arg1
    }
    "of Continuous is chunked constructor argument wrapped with Output" >> {
      val arg0 = Stream.emits(List(1, 2, 3, 4))
      Continuous(arg0).delimited.compile.to(List) must_=== arg0.chunks.map(Part.Output(_)).compile.to(List)
      val arg1 = Stream.emits(List("foo", "bar", "baz")) ++ Stream.emits(List("baz", "quux"))
      Continuous(arg1).delimited.compile.to(List) must_=== arg1.chunks.map(Part.Output(_)).compile.to(List)
    }
  }
  "data" >> {
    "of Continuous is constructor argument" >> {
      val arg0 = Stream.emits(List(1, 2, 3, 4))
      Continuous(arg0).data must_=== arg0
      val arg1 = Stream.emits(List("foo", "bar", "baz")) ++ Stream.emits(List("baz", "quux"))
      Continuous(arg1).data must_=== arg1
    }
    "of Delimited are chunks squished together" >> {
      val chunk0 = Chunk(0, 1, 2)
      val chunk1 = Chunk(1, 2, 3)
      val chunk2 = Chunk(2, 3, 4)

      val exKey0 = Part.emptyOffsetKey
      val exKey1 = Part.offsetKeyFromBytes(Array(0x00, 0x01, 0x02))
      val exKey2 = Part.offsetKeyFromBytes(Array(0x10, 0x22, 0x0f))

      val delimited0 = Delimited(Stream.emits(List(
        Part.Output(chunk0),
        exKey0,
        Part.Output(chunk1),
        exKey1,
        Part.Output(chunk2),
        exKey2)))

      val expected0 = Stream.chunk(chunk0) ++ Stream.chunk(chunk1) ++ Stream.chunk(chunk2)

      delimited0.data.compile.to(List) must_=== expected0.compile.to(List)

      val delimited1 = Delimited(Stream.emits(List(
        Part.Output(chunk1),
        Part.Output(chunk0),
        Part.Output(chunk2),
        exKey0,
        exKey1,
        Part.Output(chunk1),
        Part.Output(chunk2),
        exKey2)))

      val expected1 = Stream.emits(List(chunk1, chunk0, chunk2, chunk1, chunk2)).flatMap(Stream.chunk)

      delimited1.data.compile.to(List) must_=== expected1.compile.to(List)
    }
  }
}
