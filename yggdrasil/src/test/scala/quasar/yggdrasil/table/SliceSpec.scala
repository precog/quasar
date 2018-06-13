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

package quasar.yggdrasil.table

import quasar.RCValueGenerators
import quasar.blueeyes._
import quasar.blueeyes.json._
import quasar.fp.ski.ι
import quasar.precog.BitSet
import quasar.precog.TestSupport._
import quasar.precog.common._
import quasar.precog.util._
import quasar.time.DateGenerators
import quasar.yggdrasil.TableModule.SortDescending

import scala.util.Random

import org.scalacheck.{Arbitrary, Gen}, Gen.listOfN
import org.specs2.matcher.Matcher

class SliceSpec extends Specification with ScalaCheck with RCValueGenerators {
  import ArbitrarySlice._

  implicit def cValueOrdering: Ordering[CValue] = CValue.CValueOrder.toScalaOrdering
  implicit def listOrdering[A](implicit ord0: Ordering[A]) = new Ordering[List[A]] {
    def compare(a: List[A], b: List[A]): Int =
      (a zip b) map ((ord0.compare _).tupled) find (_ != 0) getOrElse (a.length - b.length)
  }

  def extractCValues(colGroups: List[List[Column]], row: Int): List[CValue] = {
    colGroups map { _ find (_.isDefinedAt(row)) map (_.cValue(row)) getOrElse CUndefined }
  }

  def columnsByCPath(slice: Slice): Map[CPath, List[Column]] = {
    val byCPath = slice.columns.groupBy(_._1.selector)
    byCPath.mapValues(_.map(_._2).toList)
  }

  def sortableCValues(slice: Slice, cpaths: Vector[CPath]): List[(List[CValue], List[CValue])] = {
    val byCPath = columnsByCPath(slice)
    (0 until slice.size).map({ row =>
      (extractCValues(cpaths.map(byCPath).toList, row), extractCValues(byCPath.values.toList, row))
    })(collection.breakOut)
  }

  def toCValues(slice: Slice): List[List[CValue]] = sortableCValues(slice, Vector.empty) map (_._2)

  def fakeSort(slice: Slice, sortKey: Vector[CPath]) =
    sortableCValues(slice, sortKey).sortBy(_._1).map(_._2)

  def fakeConcat(slices: List[Slice]) = {
    slices.foldLeft(List.empty[List[CValue]]) { (acc, slice) =>
      acc ++ toCValues(slice)
    }
  }

  def stripUndefineds(cvals: List[CValue]): Set[CValue] =
    (cvals filter (_ != CUndefined)).toSet

  def assertSlices(values: Vector[CValue], slices: Vector[Slice], expectedSliceSize: Matcher[Int]) = {
    if (values.isEmpty) slices.size must be_==(0)
    else slices.size must expectedSliceSize

    slices.map(s => toCValues(s)).foldLeft(List.empty[CValue])(_ ++ _.flatten) must_==(values.toList)
    slices.map(s => toCValues(s).isEmpty).exists(ι) must_==(false)
  }

  def valueCalcs(values: Vector[CValue]): (Int, Int, Int) = {
    val totalRows = values.size

    val columnsPerValue: Vector[Vector[ColumnRef]] = values.map(_.flattenWithPath.map { case (p, v) => ColumnRef(p, v.cType) })
    val totalColumns = columnsPerValue.flatten.toSet.size
    val nrColumnsBiggestValue = columnsPerValue.map(_.size).foldLeft(0)(Math.max(_, _))

    (totalRows, nrColumnsBiggestValue, totalColumns)
  }

  def testFromRValuesMaxSliceColumnsEqualsBiggestValue(values: Vector[CValue]) = {
    val (totalRows, nrColumnsBiggestValue, totalColumns) = valueCalcs(values)

    val slices = Slice.fromRValues(values, maxRows = Math.max(totalRows, 1), maxColumns = nrColumnsBiggestValue).toVector
    assertSlices(values, slices, be_>(0))
  }

  def testFromRValuesMaxSliceColumnsLowerThanBiggestValue(values: Vector[CValue]) = {
    val (totalRows, nrColumnsBiggestValue, totalColumns) = valueCalcs(values)

    val slices = Slice.fromRValues(values, maxRows = Math.max(totalRows, 1), maxColumns = nrColumnsBiggestValue - 1).toVector
    assertSlices(values, slices, be_>(0))
  }

  def testFromRValuesMaxSliceRowsOverflow(values: Vector[CValue]) = {
    val (totalRows, _, totalColumns) = valueCalcs(values)
    val maxSliceRows = Math.max(1, Math.ceil(totalRows.toDouble / 3).toInt)
    val expectedNrSlices = Math.min(Math.ceil(totalRows.toDouble / maxSliceRows).toInt, 3)

    val slices = Slice.fromRValues(values, maxRows = maxSliceRows, maxColumns = totalColumns).toVector
    assertSlices(values, slices, be_==(expectedNrSlices))
  }

  def testFromRValuesMaxSliceRows1(values: Vector[CValue]) = {
    val (totalRows, _, totalColumns) = valueCalcs(values)
    val maxSliceRows = 1

    val slices = Slice.fromRValues(values, maxRows = maxSliceRows, maxColumns = totalColumns).toVector
    assertSlices(values, slices, be_==(totalRows))
  }

  def testFromRValuesFittingIn1Slice(values: Vector[CValue]) = {
    val (totalRows, _, totalColumns) = valueCalcs(values)

    // test with a slice that's just big enough to hold the values
    val slices = Slice.fromRValues(values, maxRows = Math.max(totalRows, 1), maxColumns = totalColumns).toVector
    assertSlices(values, slices, be_==(1))
  }


  def testFromRValuesTemplate(input: JValue, maxRows: Int, maxCols: Int, expectedNrRows: Stream[Int], expectedNrCols: Stream[Int]) = {
    val data: Vector[RValue] = input match {
      case JArray(rows) => rows.toVector.flatMap(RValue.fromJValue)
      case _ => ???
    }

    val result: Stream[Slice] = Slice.fromRValues(data, maxRows, maxCols)

    result.map(s => toCValues(s)).foldLeft(List.empty[CValue])(_ ++ _.flatten).filter(_ != CUndefined) mustEqual(
      data.toList.map(_.flattenWithPath.map(_._2)).flatten)
    result.map(_.size) mustEqual expectedNrRows
    result.map(_.columns.size) mustEqual expectedNrCols
  }

  "fromRValues" should {

    val v = Vector(CString("x"), CNum(42))

    "construct slices from a simple vector" in {
      "fits in 1 slice" >> testFromRValuesFittingIn1Slice(v)
      "maxSliceRows < nrRows" >> testFromRValuesMaxSliceRowsOverflow(v)
      "maxSliceRows = 1" >> testFromRValuesMaxSliceRows1(v)
      "maxSliceColumns = nrColumns of biggest value" >> testFromRValuesMaxSliceColumnsEqualsBiggestValue(v)
      "maxSliceColumns < nrColumns of biggest value" >> testFromRValuesMaxSliceColumnsLowerThanBiggestValue(v)
    }

    val v1 = Vector.tabulate(10000)(CNum(_))

    "construct slices from a big vector" in {
      "fits in 1 slice" >> testFromRValuesFittingIn1Slice(v1)
      "maxSliceRows < nrRows" >> testFromRValuesMaxSliceRowsOverflow(v1)
      "maxSliceRows = 1" >> testFromRValuesMaxSliceRows1(v1)
      "maxSliceColumns = nrColumns of biggest value" >> testFromRValuesMaxSliceColumnsEqualsBiggestValue(v1)
      "maxSliceColumns < nrColumns of biggest value" >> testFromRValuesMaxSliceColumnsLowerThanBiggestValue(v1)
    }

    "construct slices from arbitrary values" in Prop.forAll(genCValues){ values =>
      testFromRValuesFittingIn1Slice(values) and
      testFromRValuesMaxSliceRowsOverflow(values) and
      testFromRValuesMaxSliceRows1(values) and
      testFromRValuesMaxSliceColumnsEqualsBiggestValue(values) and
      testFromRValuesMaxSliceColumnsLowerThanBiggestValue(values)
    }

    "construct slices with various bounds" in {

      val testInput1: JValue = JParser.parseUnsafe("""[
          {"foo":1, "bar1": 1},
          {"foo":2, "bar2": 2}
        ]""".stripMargin)

      "rows just fits in 1 slice (simple)" >>
        testFromRValuesTemplate(testInput1, 2, 10000, Stream(2), Stream(3))

      "columns just fits in 1 slice (simple)" >>
        testFromRValuesTemplate(testInput1, 1000, 3, Stream(2), Stream(3))

      "columns and rows just fits in 1 slice (simple)" >>
        testFromRValuesTemplate(testInput1, 2, 3, Stream(2), Stream(3))

      "columns boundary hit (simple)" >>
        testFromRValuesTemplate(testInput1, 1000, 2, Stream(1, 1), Stream(2, 2))

      "rows boundary hit (simple)" >>
        testFromRValuesTemplate(testInput1, 1, 1000, Stream(1, 1), Stream(2, 2))

      "columns boundary exceeded in 1 row (simple)" >>
        testFromRValuesTemplate(testInput1, 1000, 1, Stream(1, 1), Stream(2, 2))

      val testInput2: JValue = JParser.parseUnsafe("""[
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
        ]""".stripMargin)

      "fits in 1 slice" >>
        testFromRValuesTemplate(testInput2, 1000, 1000, Stream(14), Stream(10))

      "columns just fits in 1 slice" >>
        testFromRValuesTemplate(testInput2, 1000, 11, Stream(14), Stream(10))

      "rows just fits in 1 slice" >>
        testFromRValuesTemplate(testInput2, 14, 1000, Stream(14), Stream(10))

      "rows and columns just fits in 1 slice" >>
        testFromRValuesTemplate(testInput2, 14, 10, Stream(14), Stream(10))

      "columns boundary hit" >>
        testFromRValuesTemplate(testInput2, 1000, 9, Stream(13, 1), Stream(9, 2))

      "rows boundary hit" >>
        testFromRValuesTemplate(testInput2, 13, 1000, Stream(13, 1), Stream(9, 2))

      "columns boundary exceeded in 1 row" >>
        testFromRValuesTemplate(testInput2, 1000, 3, Stream(2, 1, 1, 2, 1, 6, 1), Stream(2, 3, 3, 2, 4, 3, 2))

      "columns boundary exceeded in 1 row multiple times" >>
        testFromRValuesTemplate(testInput2, 1000, 2, Stream(2, 1, 1, 2, 1, 1, 2, 3, 1), Stream(2, 3, 3, 2, 4, 2, 2, 2, 2))

      "column and row boundary hit" >>
        testFromRValuesTemplate(testInput2, 3, 4, Stream(3, 3, 2, 3, 3), Stream(4, 4, 4, 3, 3))
    }
  }

  "sortBy" should {
    "stably sort a slice by a projection" in {
      val array = JParser.parseUnsafe("""[
        { "city": "ANEHEIM", "state": "CA" },
        { "city": "LOPEZ", "state": "WA" },
        { "city": "SPOKANE", "state": "WA" },
        { "city": "WASCO", "state": "CA" }
        ]""".stripMargin)

      val data = array match {
        case JArray(rows) => rows.toVector
        case _ => ???
      }

      val target = Slice.fromJValues(data)

      // Note the monotonically decreasing sequence
      // associated with the keys, due to repeated
      // states being sorted in descending order.
      val keyArray = JParser.parseUnsafe("""[
          [ "CA", 0 ],
          [ "WA", -1 ],
          [ "WA", -2 ],
          [ "CA", -3 ]
        ]""".stripMargin)

      val keyData = keyArray match {
        case JArray(rows) => rows.toVector
        case _ => ???
      }

      val key = {
        val slices = Slice.fromJValues(keyData)
        slices.size mustEqual 1
        slices(0)
      }

      val result = target.map(_.sortWith(key, SortDescending)._1.toJsonElements.toVector)

      val expectedArray = JParser.parseUnsafe("""[
        { "city": "LOPEZ", "state": "WA" },
        { "city": "SPOKANE", "state": "WA" },
        { "city": "ANEHEIM", "state": "CA" },
        { "city": "WASCO", "state": "CA" }
        ]""".stripMargin)

      val expected = expectedArray match {
        case JArray(rows) => rows.toVector
        case _ => ???
      }

      result mustEqual Vector(expected)
    }

      // Commented out for now. sortWith is correct semantically, but it ruins
      // the semantics of sortBy (which uses sortWith). Need to add a global ID.
//    "sort a trivial slice" in {
//      val slice = new Slice {
//        val size = 5
//        val columns = Map(
//          ColumnRef(CPath("a"), CLong) -> new LongColumn {
//            def isDefinedAt(row: Int) = true
//            def apply(row: Int) = -row.toLong
//          },
//          ColumnRef(CPath("b"), CLong) -> new LongColumn {
//            def isDefinedAt(row: Int) = true
//            def apply(row: Int) = row / 2
//          })
//      }
//      val sortKey = VectorCase(CPath("a"))
//
//      fakeSort(slice, sortKey) must_== toCValues(slice.sortBy(sortKey))
//    }

//    "sort arbitrary slices" in { check { badSize: Int =>
//      val path = Path("/")
//      val auth = Authorities(Set())
//      val paths = Vector(
//        CPath("0") -> CLong,
//        CPath("1") -> CBoolean,
//        CPath("2") -> CString,
//        CPath("3") -> CDouble,
//        CPath("4") -> CNum,
//        CPath("5") -> CEmptyObject,
//        CPath("6") -> CEmptyArray,
//        CPath("7") -> CNum)
//      val pd = ProjectionDescriptor(0, paths.toList map { case (cpath, ctype) =>
//        ColumnRef(path, cpath, ctype, auth)
//      })

//      val size = scala.math.abs(badSize % 100).toInt
//      implicit def arbSlice = Arbitrary(genSlice(pd, size))
//
//      check { slice: Slice =>
//        for (i <- 0 to 7; j <- 0 to 7) {
//          val sortKey = if (i == j) {
//            VectorCase(paths(i)._1)
//          } else {
//            VectorCase(paths(i)._1, paths(j)._1)
//          }
//          fakeSort(slice, sortKey) must_== toCValues(slice.sortBy(sortKey))
//        }
//      }
//    } }
  }

  private def concatProjDesc = Seq(
    ColumnRef(CPath("0"), CLong),
    ColumnRef(CPath("1"), CBoolean),
    ColumnRef(CPath("2"), CString),
    ColumnRef(CPath("3"), CDouble),
    ColumnRef(CPath("4"), CNum),
    ColumnRef(CPath("5"), CEmptyObject),
    ColumnRef(CPath("6"), CEmptyArray),
    ColumnRef(CPath("7"), CNum)
  )

  "concat" should {
    "concat arbitrary slices together" in {
      implicit def arbSlice = Arbitrary(genSlice(concatProjDesc, 23))

      prop { slices: List[Slice] =>
        val slice = Slice.concat(slices)
        toCValues(slice) must_== fakeConcat(slices)
      }
    }

    "concat small singleton together" in {
      implicit def arbSlice = Arbitrary(genSlice(concatProjDesc, 1))

      prop { slices: List[Slice] =>
        val slice = Slice.concat(slices)
        toCValues(slice) must_== fakeConcat(slices)
      }
    }

    val emptySlice = new Slice {
      val size = 0
      val columns: Map[ColumnRef, Column] = Map.empty
    }

    "concat empty slices correctly" in {
      implicit def arbSlice = Arbitrary(genSlice(concatProjDesc, 23))

      prop { fullSlices: List[Slice] =>
        val slices = fullSlices collect {
          case slice if Random.nextBoolean => slice
          case _ => emptySlice
        }
        val slice = Slice.concat(slices)
        toCValues(slice) must_== fakeConcat(slices)
      }
    }

    "concat heterogeneous slices" in {
      val pds = List.fill(25)(concatProjDesc filter (_ => Random.nextBoolean))
      val g1 :: g2 :: gs = pds.map(genSlice(_, 17))

      implicit val arbSlice = Arbitrary(Gen.oneOf(g1, g2, gs: _*))

      prop { slices: List[Slice] =>
        val slice = Slice.concat(slices)
        // This is terrible, but there isn't an immediately easy way to test
        // without duplicating concat.
        toCValues(slice).map(stripUndefineds) must_== fakeConcat(slices).map(stripUndefineds)
      }
    }
  }
}


object ArbitrarySlice extends RCValueGenerators with DateGenerators {
  private def genBitSet(size: Int): Gen[BitSet] = listOfN(size, genBool) ^^ (BitsetColumn bitset _)

  // TODO remove duplication with `SegmentFormatSupport#genForCType`
  def genColumn(col: ColumnRef, size: Int): Gen[Column] = {
    def bs = BitSetUtil.range(0, size)
    col.ctype match {
      case CString         => arrayOfN(size, genString) ^^ (ArrayStrColumn(bs, _))
      case CBoolean        => arrayOfN(size, genBool) ^^ (ArrayBoolColumn(bs, _))
      case CLong           => arrayOfN(size, genLong) ^^ (ArrayLongColumn(bs, _))
      case CDouble         => arrayOfN(size, genDouble) ^^ (ArrayDoubleColumn(bs, _))
      case COffsetDateTime => arrayOfN(size, genOffsetDateTime) ^^ (ArrayOffsetDateTimeColumn(bs, _))
      case COffsetTime     => arrayOfN(size, genOffsetTime) ^^ (ArrayOffsetTimeColumn(bs, _))
      case COffsetDate     => arrayOfN(size, genOffsetDate) ^^ (ArrayOffsetDateColumn(bs, _))
      case CLocalDateTime  => arrayOfN(size, genLocalDateTime) ^^ (ArrayLocalDateTimeColumn(bs, _))
      case CLocalTime      => arrayOfN(size, genLocalTime) ^^ (ArrayLocalTimeColumn(bs, _))
      case CLocalDate      => arrayOfN(size, genLocalDate) ^^ (ArrayLocalDateColumn(bs, _))
      case CInterval       => arrayOfN(size, genInterval) ^^ (ArrayIntervalColumn(bs, _))
      case CNum            => arrayOfN(size, genDouble) ^^ (ns => ArrayNumColumn(bs, ns map (v => BigDecimal(v))))
      case CNull           => genBitSet(size) ^^ (s => new BitsetColumn(s) with NullColumn)
      case CEmptyObject    => genBitSet(size) ^^ (s => new BitsetColumn(s) with EmptyObjectColumn)
      case CEmptyArray     => genBitSet(size) ^^ (s => new BitsetColumn(s) with EmptyArrayColumn)
      case CUndefined      => UndefinedColumn.raw
      case CArrayType(_)   => abort("undefined")
    }
  }

  def genSlice(refs: Seq[ColumnRef], sz: Int): Gen[Slice] = {
    val zero    = Nil: Gen[List[(ColumnRef, Column)]]
    val gs      = refs map (cr => genColumn(cr, sz) ^^ (cr -> _))
    val genData = gs.foldLeft(zero)((res, g) => res >> (r => g ^^ (_ :: r)))

    genData ^^ (data => Slice(data.toMap, sz))
  }
}
