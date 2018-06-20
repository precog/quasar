/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.yggdrasil
package table

import quasar.blueeyes._, json._
import quasar.precog.TestSupport._
import quasar.precog.common._

import cats.effect.IO
import scalaz.StreamT
import shims._

import org.specs2.ScalaCheck
import org.scalacheck.Gen

trait CanonicalizeSpec extends ColumnarTableModuleTestSupport with SpecificationLike with ScalaCheck {
  import SampleData._

  val table = {
    val JArray(elements) = JParser.parse("""[
      {"foo":1},
      {"foo":2, "bar2": 2},
      {"foo":3, "bar3": 3, "baz": 3},
      {"foo":4, "bar4": 4, "baz": 4},
      {"foo":5, "bar5": 5},
      {"foo":6},
      {"foo":7, "bar7": 7, "baz": 7, "quux": 7},
      {"foo":8, "baz": 8},
      {"foo":9, "bar9": 9},
      {"foo":10},
      {"foo":11, "baz": 11},
      {"foo":12, "baz": 12},
      {"foo":13, "baz": 13},
      {"foo":14, "bar14": 14}
    ]""")

    val sample = SampleData(elements.toStream)
    fromSample(sample, Some(3))
  }

  def valueCalcs(values: Vector[RValue]) = {
    val totalRows = values.size

    val columnsPerValue: Vector[Vector[ColumnRef]] = values.map(_.flattenWithPath.map { case (p, v) => ColumnRef(p, v.cType) })
    val totalColumns = columnsPerValue.flatten.toSet.size
    val nrColumnsBiggestValue = columnsPerValue.map(_.size).foldLeft(0)(Math.max(_, _))

    (totalRows, nrColumnsBiggestValue, totalColumns)
  }

  def checkBoundedCanonicalize = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val table = fromSample(sample)
      val (totalRows, _, totalColumns) = valueCalcs(sample.data.toVector)
      val minLength = Gen.choose(0, totalRows / 2).sample.get
      val maxLength = minLength + Gen.choose(1, totalRows / 2 + 1).sample.get

      val canonicalizedTable = table.canonicalize(minLength, Some(maxLength), totalColumns)
      val slices = canonicalizedTable.slices.toStream.unsafeRunSync map (_.size)
      if (totalRows > 0) {
        slices.init must contain(like[Int]({ case size: Int => size must beBetween(minLength, maxLength) })).forall
        slices.last must be_<=(maxLength)
      }
      else {
        slices must haveSize(0)
      }
    }
  }

  def checkCanonicalize = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val table = fromSample(sample)
      val (totalRows, _, totalColumns) = valueCalcs(sample.data.toVector)
      val length = Gen.choose(1, totalRows + 3).sample.get

      val canonicalizedTable = table.canonicalize(length, None, totalColumns)
      val resultSlices = canonicalizedTable.slices.toStream.unsafeRunSync
      val resultSizes = resultSlices.map(_.size)

      val expected = {
        val num = totalRows / length
        val remainder = totalRows % length
        val prefix = Stream.fill(num)(length)
        if (remainder > 0) prefix :+ remainder else prefix
      }

      resultSizes mustEqual expected
    }
  }.set(minTestsOk =  1000)

  def testCanonicalizeRowsAndColumnsJustFitsIn1Slice = {
    val result = table.canonicalize(14, None, 10)

    val slices = result.slices.toStream.unsafeRunSync
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(14)
  }

  def testCanonicalizeColumnsJustFitsIn1Slice = {
    val result = table.canonicalize(1000, None, 10)

    val slices = result.slices.toStream.unsafeRunSync
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(14)
  }

  def testCanonicalizeRowsJustFitsIn1Slice = {
    val result = table.canonicalize(14, None, 1000)

    val slices = result.slices.toStream.unsafeRunSync
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(14)
  }

  def testCanonicalizeRowsBoundaryHit = {
    val result = table.canonicalize(13, None, 1000)

    val slices = result.slices.toStream.unsafeRunSync
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(13, 1)
  }

  def testCanonicalizeColumnsBoundaryHit = {
    val result = table.canonicalize(1000, None, 9)

    val slices = result.slices.toStream.unsafeRunSync
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(14)
  }

  def testCanonicalize = {
    val result = table.canonicalize(3, None, 25)

    val slices = result.slices.toStream.unsafeRunSync
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(3, 3, 3, 3, 2)
  }

  def testCanonicalizeColumnBoundary = {
    val result = table.canonicalize(14, None, 4)

    val slices = result.slices.toStream.unsafeRunSync
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(6, 3, 5)
  }

  def testCanonicalizeRowAndColumnBoundaryColumnFirst = {
    val result = table.canonicalize(3, None, 4)

    val slices = result.slices.toStream.unsafeRunSync
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(3, 3, 3, 3, 2)
  }

  def testCanonicalizeRowAndColumnBoundaryRowFirst = {
    val result = table.canonicalize(2, None, 4)

    val slices = result.slices.toStream.unsafeRunSync
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(2, 2, 2, 2, 2, 2, 2)
  }

  def testCanonicalizeColumnBoundaryExceededIn1Row = {
    val result = table.canonicalize(3, None, 3)

    val slices = result.slices.toStream.unsafeRunSync
    val sizes = slices.map(_.size)
    sizes mustEqual Stream(3, 3, 3, 3, 2)
  }

  def testCanonicalizeColumnBoundaryExceededIn1RowMultipleTimes = {
    val result = table.canonicalize(1000, None, 2)

    val slices = result.slices.toStream.unsafeRunSync
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(3, 3, 3, 5)
  }

  def testCanonicalizeZero = {
    table.canonicalize(0, None, 1) must throwA[IllegalArgumentException]
  }

  def testCanonicalizeRowBoundary = {
    val result = table.canonicalize(5, None, 25)

    val slices = result.slices.toStream.unsafeRunSync
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(5, 5, 4)
  }

  def testCanonicalizeOverRowBoundary = {
    val result = table.canonicalize(12, None, 25)

    val slices = result.slices.toStream.unsafeRunSync
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(12, 2)
  }

  def testCanonicalizeEmptySlices = {
    def tableTakeRange(table: Table, start: Int, numToTake: Long) =
      table.takeRange(start, numToTake).slices.toStream.unsafeRunSync

    val emptySlice = Slice(Map(), 0)
    val slices =
      Stream(emptySlice) ++ tableTakeRange(table, 0, 5) ++
      Stream(emptySlice) ++ tableTakeRange(table, 5, 4) ++
      Stream(emptySlice) ++ tableTakeRange(table, 9, 5) ++
      Stream(emptySlice)

    val newTable     = Table(StreamT.fromStream(IO.pure(slices)), table.size)
    val result       = newTable.canonicalize(4, None, 25)
    val resultSlices = result.slices.toStream.unsafeRunSync
    val resultSizes  = resultSlices.map(_.size)

    resultSizes mustEqual Stream(4, 4, 4, 2)
  }

  def testCanonicalizeEmpty = {
    val table  = Table.empty
    val result = table.canonicalize(3, None, 1)
    val slices = result.slices.toStream.unsafeRunSync
    val sizes  = slices.map(_.size)

    sizes mustEqual Stream()
  }
}
