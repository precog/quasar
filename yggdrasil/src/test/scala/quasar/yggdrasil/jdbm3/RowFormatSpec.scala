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
package jdbm3

import quasar.precog.common._
import quasar.precog.TestSupport._
import quasar.yggdrasil.table._

import org.scalacheck.Shrink

import scala.annotation.tailrec

class RowFormatSpec extends Specification with ScalaCheck with SJValueGenerators {
  import Arbitrary._

  // This should generate some jpath ids, then generate CTypes for these.
  def genJpathIds: Gen[List[String]] = Gen.alphaStr filter (_.length > 0) list
  def genColumnRefs: Gen[List[ColumnRef]] = genJpathIds >> { ids =>
    val generators = ids.distinct map (id => listOf(ctype) ^^ (_.distinct map (tp => ColumnRef(CPath(id), tp))))
    Gen.sequence(generators) ^^ (_.flatten.toList)
  }

  def groupConsecutive[A, B](as: List[A])(f: A => B) = {
    @tailrec
    def build(as: List[A], prev: List[List[A]]): List[List[A]] = as match {
      case Nil =>
        prev.reverse
      case a :: _ =>
        val bs = as takeWhile { b => f(a) == f(b) }
        build(as drop bs.size, bs :: prev)
    }

    build(as, Nil)
  }

  def genCValuesForColumnRefs(refs: List[ColumnRef]): Gen[List[CValue]] = {
    val generators = groupConsecutive(refs)(_.selector) map (refs =>
      genIndex(refs.size) >> (i =>
        Gen.sequence(refs.zipWithIndex map {
          case (ColumnRef(_, cType), `i`) => Gen.frequency(5 -> genCValueForType(cType), 1 -> Gen.const(CUndefined))
          case _                          => Gen.const(CUndefined)
        })
      )
    )
    (Gen sequence generators) ^^ (_.flatten.toList)
  }

  def arrayColumnsFor(size: Int, refs: List[ColumnRef]): List[ArrayColumn[_]] =
    refs map JDBMSlice.columnFor(CPath.Identity, size) map (_._2)

  def verify(rows: List[List[CValue]], cols: List[Column]) = {
    rows.zipWithIndex foreach { case (values, row) =>
      (values zip cols) foreach (_ must beLike {
        case (CUndefined, col) if !col.isDefinedAt(row)      => ok
        case (_, col) if !col.isDefinedAt(row)               => ko
        case (CString(s), col: StrColumn)                    => col(row) must_== s
        case (CBoolean(x), col: BoolColumn)                  => col(row) must_== x
        case (CLong(x), col: LongColumn)                     => col(row) must_== x
        case (CDouble(x), col: DoubleColumn)                 => col(row) must_== x
        case (CNum(x), col: NumColumn)                       => col(row) must_== x
        case (COffsetDateTime(x), col: OffsetDateTimeColumn) => col(row) must_== x
        case (COffsetTime(x), col: OffsetTimeColumn)         => col(row) must_== x
        case (COffsetDate(x), col: OffsetDateColumn)         => col(row) must_== x
        case (CLocalDateTime(x), col: LocalDateTimeColumn)   => col(row) must_== x
        case (CLocalTime(x), col: LocalTimeColumn)           => col(row) must_== x
        case (CLocalDate(x), col: LocalDateColumn)           => col(row) must_== x
        case (CInterval(x), col: IntervalColumn)             => col(row) must_== x
        case (CNull, col: NullColumn)                        => ok
        case (CEmptyObject, col: EmptyObjectColumn)          => ok
        case (CEmptyArray, col: EmptyArrayColumn)            => ok
        case (CArray(xs, cType), col: HomogeneousArrayColumn[_]) if cType == col.tpe => col(row) must_== xs
      })
    }
  }

  implicit lazy val arbColumnRefs = Arbitrary(genColumnRefs)

  implicit val shrinkCValues: Shrink[List[CValue]] = Shrink.shrinkAny[List[CValue]]
  implicit val shrinkRows: Shrink[List[List[CValue]]] = Shrink.shrinkAny[List[List[CValue]]]

  "ValueRowFormat" should {
    checkRoundTrips(RowFormat.forValues(_))
  }

  private def identityCols(len: Int): List[ColumnRef] = (0 until len).map({ i =>
    ColumnRef(CPath(CPathIndex(i)), CLong)
  })(scala.collection.breakOut)

  "IdentitiesRowFormat" should {
    "round-trip CLongs" in {
      prop { id: List[Long] =>
        val rowFormat = RowFormat.IdentitiesRowFormatV1(identityCols(id.size))
        val cId: List[CValue] = id map (CLong(_))
        rowFormat.decode(rowFormat.encode(cId)) must_== cId
      }
    }

    "encodeIdentities matches encode format" in {
      prop { id: List[Long] =>
        val rowFormat = RowFormat.IdentitiesRowFormatV1(identityCols(id.size))
        val cId: List[CValue] = id map (CLong(_))
        rowFormat.decode(rowFormat.encodeIdentities(id.toArray)) must_== cId
      }
    }

    "round-trip CLongs -> Column -> CLongs" in {
      prop { id: List[Long] =>
        val columns = arrayColumnsFor(1, identityCols(id.size))
        val rowFormat = RowFormat.IdentitiesRowFormatV1(identityCols(id.size))
        val columnDecoder = rowFormat.ColumnDecoder(columns)
        val columnEncoder = rowFormat.ColumnEncoder(columns)

        val cId: List[CValue] = id map (CLong(_))
        columnDecoder.decodeToRow(0, rowFormat.encode(cId))

        verify(cId :: Nil, columns)

        rowFormat.decode(columnEncoder.encodeFromRow(0)) must_== cId
      }
    }
  }

  "SortingKeyRowFormat" should {
    checkRoundTrips(RowFormat.forSortingKey(_))

    "sort encoded as ValueFormat does" in {
      prop { refs: List[ColumnRef] =>
        val valueRowFormat = RowFormat.forValues(refs)
        val sortingKeyRowFormat = RowFormat.forSortingKey(refs)
        implicit val arbRows: Arbitrary[List[List[CValue]]] =
          Arbitrary(Gen.listOfN(10, genCValuesForColumnRefs(refs)))

        prop { (vals: List[List[CValue]]) =>
          val valueEncoded = vals map (valueRowFormat.encode(_))
          val sortEncoded = vals map (sortingKeyRowFormat.encode(_))

          val sortedA = valueEncoded.sorted(new Ordering[Array[Byte]] {
            def compare(a: Array[Byte], b: Array[Byte]) = valueRowFormat.compare(a, b)
          }) map (valueRowFormat.decode(_))
          val sortedB = sortEncoded.sorted(new Ordering[Array[Byte]] {
            def compare(a: Array[Byte], b: Array[Byte]) = sortingKeyRowFormat.compare(a, b)
          }) map (sortingKeyRowFormat.decode(_))

          sortedA must_== sortedB
        }
      }
    }.set(minTestsOk = 500, maxDiscardRatio = 5).pendingUntilFixed
  }

  def checkRoundTrips(toRowFormat: List[ColumnRef] => RowFormat) = {
    "survive round-trip from CValue -> Array[Byte] -> CValue" in {
      prop { (refs: List[ColumnRef]) =>
        val rowFormat = toRowFormat(refs)
        implicit val arbColumnValues: Arbitrary[List[CValue]] = Arbitrary(genCValuesForColumnRefs(refs))

        prop { (vals: List[CValue]) =>
          assert(refs.size == vals.size)
          rowFormat.decode(rowFormat.encode(vals)) must_== vals
        }
      }
    }

    "survive rountrip from CValue -> Array[Byte] -> Column -> Array[Byte] -> CValue" in {
      val size = 10

      prop { (refs: List[ColumnRef]) =>
        val rowFormat = toRowFormat(refs)
        implicit val arbRows: Arbitrary[List[List[CValue]]] =
          Arbitrary(Gen.listOfN(size, genCValuesForColumnRefs(refs)))

        prop { (rows: List[List[CValue]]) =>
          val columns = arrayColumnsFor(size, refs)
          val columnDecoder = rowFormat.ColumnDecoder(columns)
          val columnEncoder = rowFormat.ColumnEncoder(columns)

          // Fill up the columns with the values from the rows.
          rows.zipWithIndex foreach { case (vals, row) =>
            columnDecoder.decodeToRow(row, rowFormat.encode(vals))
          }

          verify(rows, columns)

          rows.zipWithIndex forall { case (vals, row) =>
            rowFormat.decode(columnEncoder.encodeFromRow(row)) must_== vals
          }
        }
      }
    }
  }
}
