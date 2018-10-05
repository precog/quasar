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
import quasar.precog.BitSet
import quasar.pkg.tests._
import quasar.precog.common._
import quasar.precog.util._
import qdata.time.TimeGenerators
import quasar.yggdrasil.TableModule.SortDescending

import scala.util.Random
import org.scalacheck.{Arbitrary, Gen}
import org.specs2.execute.Result
import org.specs2.matcher.Matcher
import Gen.listOfN

import fs2.{Chunk, Stream}

class SliceSpec extends Specification with ScalaCheck {
  import ArbitrarySlice._
  import SliceIngest.ChunkSliced

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

  def toCValues(slice: Slice): List[List[CValue]] =
    sortableCValues(slice, Vector.empty) map (_._2) map stripUndefineds

  def fakeSort(slice: Slice, sortKey: Vector[CPath]): List[List[CValue]] =
    sortableCValues(slice, sortKey).sortBy(_._1).map(_._2)

  def fakeConcat(slices: List[Slice]): List[List[CValue]] = {
    slices.foldLeft(List.empty[List[CValue]]) { (acc, slice) =>
      acc ++ toCValues(slice)
    }
  }

  def stripUndefineds(cvals: List[CValue]): List[CValue] =
    cvals.filter(_ != CUndefined)

  def assertSlices(values: List[RValue], slices: List[Slice], expectedSliceSize: Matcher[Int]) = {
    if (values.isEmpty) slices.size must_===(0)
    else slices.size must expectedSliceSize

    val sliceValues: List[List[List[CValue]]] = slices.map(s => toCValues(s))
    val allSliceValues: List[CValue] = sliceValues.foldLeft(List.empty[CValue])(_ ++ _.flatten)
    allSliceValues must_===(values.flatMap(_.flattenWithPath.map(_._2)))
    sliceValues.exists(_.isEmpty) must_===(false)
  }

  def sliceEqualityAtDefinedRows(slice1: Slice, slice2: Slice): Result = {
    def colValues(col: ArrayColumn[_]): Iterator[Option[CValue]] = {
      col match {
        case ac: ArrayLongColumn           =>
          (0 until ac.values.length).iterator.map(i => if (ac.defined(i)) Some(CLong(ac.values(i))) else None)
        case ac: ArrayNumColumn            =>
          (0 until ac.values.length).iterator.map(i => if (ac.defined(i)) Some(CNum(ac.values(i))) else None)
        case ac: ArrayDoubleColumn         =>
          (0 until ac.values.length).iterator.map(i => if (ac.defined(i)) Some(CDouble(ac.values(i))) else None)
        case ac: ArrayStrColumn            =>
          (0 until ac.values.length).iterator.map(i => if (ac.defined(i)) Some(CString(ac.values(i))) else None)
        case ac: ArrayOffsetDateTimeColumn =>
          (0 until ac.values.length).iterator.map(i => if (ac.defined(i)) Some(COffsetDateTime(ac.values(i))) else None)
        case ac: ArrayOffsetTimeColumn     =>
          (0 until ac.values.length).iterator.map(i => if (ac.defined(i)) Some(COffsetTime(ac.values(i))) else None)
        case ac: ArrayOffsetDateColumn     =>
          (0 until ac.values.length).iterator.map(i => if (ac.defined(i)) Some(COffsetDate(ac.values(i))) else None)
        case ac: ArrayLocalDateTimeColumn  =>
          (0 until ac.values.length).iterator.map(i => if (ac.defined(i)) Some(CLocalDateTime(ac.values(i))) else None)
        case ac: ArrayLocalTimeColumn      =>
          (0 until ac.values.length).iterator.map(i => if (ac.defined(i)) Some(CLocalTime(ac.values(i))) else None)
        case ac: ArrayLocalDateColumn      =>
          (0 until ac.values.length).iterator.map(i => if (ac.defined(i)) Some(CLocalDate(ac.values(i))) else None)
        case ac: ArrayIntervalColumn       =>
          (0 until ac.values.length).iterator.map(i => if (ac.defined(i)) Some(CInterval(ac.values(i))) else None)
        case ac: MutableEmptyArrayColumn   =>
          (0 until (ac.defined.length << 6)).iterator.map(i => if (ac.defined(i)) Some(CEmptyArray) else None)
        case ac: MutableEmptyObjectColumn  =>
          (0 until (ac.defined.length << 6)).iterator.map(i => if (ac.defined(i)) Some(CEmptyObject) else None)
        case ac: MutableNullColumn         =>
          (0 until (ac.defined.length << 6)).iterator.map(i => if (ac.defined(i)) Some(CNull) else None)
      }
    }
    def equalIterators[A](xs: Iterator[A], ys: Iterator[A]): Result = {
      Result.foreach(xs.toStream.zip(ys.toStream)) {
        case (x, y) =>
          x must_== y
      }
    }
    val colList1 = slice1.materialized.columns.toList.sortBy(_._1)
    val colList2 = slice2.materialized.columns.toList.sortBy(_._1)
    colList1.size must_== colList2.size
    Result.foreach(colList1.zip(colList2)) {
      case ((cpath1, col1), (cpath2, col2)) =>
        cpath1 must_== cpath2
        equalIterators(colValues(col1.asInstanceOf[ArrayColumn[_]]), colValues(col2.asInstanceOf[ArrayColumn[_]]))
    }
  }

  def valueCalcs(values: List[RValue]): (Int, Int, Int) = {
    val totalRows = values.size

    val columnsPerValue: List[List[ColumnRef]] = values.map(_.flattenWithPath.map { case (p, v) => ColumnRef(p, v.cType) })
    val totalColumns = columnsPerValue.flatten.toSet.size
    val nrColumnsBiggestValue = columnsPerValue.map(_.size).foldLeft(0)(Math.max(_, _))

    (totalRows, nrColumnsBiggestValue, totalColumns)
  }

  def testFromQDataMaxSliceColumnsEqualsBiggestValue(values: List[CValue]) = {
    val (totalRows, nrColumnsBiggestValue, totalColumns) = valueCalcs(values)

    val slices = Slice.allFromQData(Stream.emits(values: List[RValue]), maxRows = Some(Math.max(totalRows, 1)), maxColumns = Some(nrColumnsBiggestValue)).toList
    assertSlices(values, slices, be_>(0))
  }

  def testFromQDataMaxSliceColumnsLowerThanBiggestValue(values: List[CValue]) = {
    val (totalRows, nrColumnsBiggestValue, totalColumns) = valueCalcs(values)

    val slices = Slice.allFromQData(Stream.emits(values: List[RValue]), maxRows = Some(Math.max(totalRows, 1)), maxColumns = Some(nrColumnsBiggestValue - 1)).toList
    assertSlices(values, slices, be_>(0))
  }

  def testFromQDataMaxSliceRowsOverflow(values: List[CValue]) = {
    val (totalRows, _, totalColumns) = valueCalcs(values)
    val maxSliceRows = Math.max(1, Math.ceil(totalRows.toDouble / 3).toInt)
    val expectedNrSlices = Math.min(Math.ceil(totalRows.toDouble / maxSliceRows).toInt, 3)
    val slices = Slice.allFromQData(Stream.emits(values: List[RValue]), maxRows = Some(maxSliceRows), maxColumns = Some(totalColumns)).toList
    assertSlices(values, slices, be_==(expectedNrSlices))
  }

  def testFromQDataMaxSliceRows1(values: List[CValue]) = {
    val (totalRows, _, totalColumns) = valueCalcs(values)
    val maxSliceRows = 1

    val slices = Slice.allFromQData(Stream.emits(values: List[RValue]), maxRows = Some(maxSliceRows), maxColumns = Some(totalColumns)).toList
    assertSlices(values, slices, be_==(totalRows))
  }

  def testFromQDataFittingIn1Slice(values: List[RValue]) = {
    val (totalRows, _, totalColumns) = valueCalcs(values)

    // test with a slice that's just big enough to hold the values
    val slices = Slice.allFromQData(Stream.emits(values), maxRows = Some(Math.max(totalRows, 1)), maxColumns = Some(totalColumns + 1)).toList
    assertSlices(values, slices, be_==(1))
  }


  def testFromQDataTemplate(input: JValue, maxRows: Int, maxCols: Int, expectedNrRows: List[Int], expectedNrCols: List[Int]) = {
    val data: List[RValue] = input match {
      case JArray(rows) => rows.toList.flatMap(RValue.fromJValue)
      case _ => ???
    }

    val result: List[Slice] = Slice.allFromQData(Stream.emits(data), Some(maxRows), Some(maxCols)).toList

    result.map(s => toCValues(s))
      .foldLeft(List.empty[CValue])(_ ++ _.flatten)
      .filter(_ != CUndefined) mustEqual(
        data.toList.flatMap(_.flattenWithPath.map(_._2)))
    result.map(_.size) mustEqual expectedNrRows
    result.map(_.columns.size) mustEqual expectedNrCols
  }

  "allFromQData" should {

    val v = List(CString("x"), CNum(42))

    "construct slices from a simple vector" in {
      "fits in 1 slice" >> testFromQDataFittingIn1Slice(v)
      "maxSliceRows < nrRows" >> testFromQDataMaxSliceRowsOverflow(v)
      "maxSliceRows = 1" >> testFromQDataMaxSliceRows1(v)
      "maxSliceColumns = nrColumns of biggest value" >> testFromQDataMaxSliceColumnsEqualsBiggestValue(v)
      "maxSliceColumns < nrColumns of biggest value" >> testFromQDataMaxSliceColumnsLowerThanBiggestValue(v)
    }

    val v1 = List.tabulate(10000)(CNum(_))

    "construct slices from a big vector" in {
      "fits in 1 slice" >> testFromQDataFittingIn1Slice(v1)
      "maxSliceRows < nrRows" >> testFromQDataMaxSliceRowsOverflow(v1)
      "maxSliceRows = 1" >> testFromQDataMaxSliceRows1(v1)
      "maxSliceColumns = nrColumns of biggest value" >> testFromQDataMaxSliceColumnsEqualsBiggestValue(v1)
      "maxSliceColumns < nrColumns of biggest value" >> testFromQDataMaxSliceColumnsLowerThanBiggestValue(v1)
    }

    val v2 = List.tabulate(2)(i => RArray(v))

    "construct slices from a vector of arrays" in {
      testFromQDataFittingIn1Slice(v2)
    }

    import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
    import qdata.time.{DateTimeInterval, OffsetDate}

    val pathological =
      List(CTrue, CEmptyObject, COffsetTime(OffsetTime.parse("16:30:38.540182417-17:24:37")), CLong(-2090824460258735860L),
        CString("醜ଯ㲙단蠒๨풥⏽★豫᲋⑻鹜컉膌洟ϛ쭪藋냎陇뜰䏧馢藳譓ᱽ鍙掹柔ꠅ"), CLong(-3221228079739133408L), CLocalDate(LocalDate.parse("-8368805-07-09")), CTrue,
        CEmptyObject, COffsetDate(OffsetDate.parse("-7773852-04-07+04:21:13")),
        CNum(BigDecimal("-1.0997856332307004858646483628256030000000000000000000000000000000000000000000000000000000000000000000")),
        CLocalDateTime(LocalDateTime.parse("+9106220-03-03T16:32:18.556437525")), CNull, CEmptyArray,
        COffsetDateTime(OffsetDateTime.parse("-7216369-10-14T13:59:13.655329534-11:51:09")),
        CLocalDate(LocalDate.parse("-6941142-08-11")), CNull, CEmptyObject, CNum(BigDecimal("0E-100")),
        CString("蟙荗ⶔ㪷읮ꑑۧ兹楀墉廸⡳灯懲獼␬ꂅ숬䫗⭜쿠暘Թ"), CNull, CInterval(DateTimeInterval.parse("PT568792H5M22.07172908S").get), CDouble(-2.5553705287306513E199),
        CLocalTime(LocalTime.parse("23:43:47.888386818")), COffsetTime(OffsetTime.parse("09:52:36.954641932+13:34:32")),
        CLocalDateTime(LocalDateTime.parse("-5205533-11-05T23:40:19.080489826")), CFalse, CLocalDate(LocalDate.parse("+1751043-09-25")),
        CEmptyArray, CLong(8180114872082381267L), COffsetDate(OffsetDate.parse("-3678224-01-01+15:25:19")),
        COffsetDateTime(OffsetDateTime.parse("+28785-12-16T09:45:14.815967029+16:18:49")), CNull,
        CInterval(DateTimeInterval.parse("P-9036285Y44M-411DT584566H4M1.912409135S").get), CDouble(-2.3703641119979175E233), CLocalDate(LocalDate.parse("+4396657-12-11")),
        CNull, CNull, CEmptyObject, CInterval(DateTimeInterval.parse("PT550632H50M7.334061745S").get), CLocalTime(LocalTime.parse("17:00:02.808168924")),
        CLong(-2051368316550135838L), COffsetDateTime(OffsetDateTime.parse("-9897897-05-10T00:02:09.498265366+04:48:25")), CDouble(1.0859578724304194E-147),
        CNull, CTrue, CLong(2512754280505120381L), CLocalDateTime(LocalDateTime.parse("-7951878-05-18T11:03:35.812478165")), CEmptyArray, CEmptyObject)

    "construct slices from pathological example" in {
      // a previous (broken) implementation didn't manage the column limit correctly.
      // instead of holding off on adding an RValue unless we're sure it won't put us over
      // the column limit, it would stop adding RValues *after* it hit the limit.
      // this data hits that edge case.
      testFromQDataFittingIn1Slice(pathological) and
      testFromQDataMaxSliceRowsOverflow(pathological) and
      testFromQDataMaxSliceRows1(pathological) and
      testFromQDataMaxSliceColumnsEqualsBiggestValue(pathological) and
      testFromQDataMaxSliceColumnsLowerThanBiggestValue(pathological)
    }


    "construct slices from arbitrary values" in Prop.forAll(genCValues){ values =>
      testFromQDataFittingIn1Slice(values)
      testFromQDataMaxSliceRowsOverflow(values) and
      testFromQDataMaxSliceRows1(values) and
      testFromQDataMaxSliceColumnsEqualsBiggestValue(values) and
      testFromQDataMaxSliceColumnsLowerThanBiggestValue(values)
    }

    "construct slices with various bounds" in {

      val testInput1: JValue = JParser.parseUnsafe("""[
          {"foo":1, "bar1": 1},
          {"foo":2, "bar2": 2}
        ]""")

      "rows just fits in 1 slice (simple)" >>
        testFromQDataTemplate(testInput1, 2, 10000, List(2), List(3))

      "columns just fits in 1 slice (simple)" >>
        testFromQDataTemplate(testInput1, 1000, 3, List(2), List(3))

      "columns and rows just fits in 1 slice (simple)" >>
        testFromQDataTemplate(testInput1, 2, 3, List(2), List(3))

      "columns boundary hit (simple)" >>
        testFromQDataTemplate(testInput1, 1000, 2, List(1, 1), List(2, 2))

      "rows boundary hit (simple)" >>
        testFromQDataTemplate(testInput1, 1, 1000, List(1, 1), List(2, 2))

      "columns boundary exceeded in 1 row (simple)" >>
        testFromQDataTemplate(testInput1, 1000, 1, List(1, 1), List(2, 2))

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
        ]""")

      "fits in 1 slice" >>
        testFromQDataTemplate(testInput2, 1000, 1000, List(14), List(10))

      "columns just fits in 1 slice" >>
        testFromQDataTemplate(testInput2, 1000, 11, List(14), List(10))

      "rows just fits in 1 slice" >>
        testFromQDataTemplate(testInput2, 14, 1000, List(14), List(10))

      "rows and columns just fits in 1 slice" >>
        testFromQDataTemplate(testInput2, 14, 10, List(14), List(10))

      "columns boundary hit" >>
        testFromQDataTemplate(testInput2, 1000, 9, List(13, 1), List(9, 2))

      "rows boundary hit" >>
        testFromQDataTemplate(testInput2, 13, 1000, List(13, 1), List(9, 2))

      "columns boundary exceeded in 1 row" >>
        testFromQDataTemplate(testInput2, 1000, 3, List(2, 1, 1, 2, 1, 6, 1), List(2, 3, 3, 2, 4, 3, 2))

      "columns boundary exceeded in 1 row multiple times" >>
        testFromQDataTemplate(testInput2, 1000, 2, List(2, 1, 1, 2, 1, 1, 2, 3, 1), List(2, 3, 3, 2, 4, 2, 2, 2, 2))

      "column and row boundary hit" >>
        testFromQDataTemplate(testInput2, 3, 4, List(3, 3, 2, 3, 3), List(4, 4, 4, 3, 3))
    }
  }

  "fromQDataStep" should {
    import SliceIngest.fromQDataStep

    "emit an empty slice given no data" >> {
      val (actualSlice, actualRemaining) = fromQDataStep(ChunkSliced.empty[RValue], 10, 10, 32)
      sliceEqualityAtDefinedRows(actualSlice, Slice.empty)
      actualRemaining mustEqual ChunkSliced.empty
    }

    "increase slice size to the next power of two when slice size cannot be predicted" >> {
      val data = ChunkSliced.fromChunk(Chunk.array(
        Array.fill[RValue](64)(CLong(1)) ++ Array(RArray(List(CLong(1))))))
      val columnSize = fromQDataStep(data, 64, 3, 2)
        ._1.columns(ColumnRef(CPath.Identity, CLong)).asInstanceOf[ArrayLongColumn].values.length
      columnSize must_== 64
    }

    "increase slice size to the next power of two when slice size can be predicted" >> {
      val data = ChunkSliced(Chunk.array(Array.fill[RValue](65)(CLong(1))), 0, 64)
      val columnSize = fromQDataStep(data, 64, 3, 2)
        ._1.columns(ColumnRef(CPath.Identity, CLong)).asInstanceOf[ArrayLongColumn].values.length
      columnSize must_== 64
    }

    "stop increasing slice size at maxRows" >> {
      val data = ChunkSliced.fromChunk(Chunk.array(
        Array[RValue](CLong(1), CLong(2), CLong(3), CLong(4), CLong(5))))
      val (slice, rest) = fromQDataStep(data, 4, 1, 2)
      val columnSize =
        slice.columns(ColumnRef(CPath.Identity, CLong)).asInstanceOf[ArrayLongColumn].values.length
      columnSize must_== 4
      rest.size must_== 1
    }

    "not add values that overflow the column limit" >> {
      val data = ChunkSliced.fromChunk(Chunk.array(Array[RValue](
        CLong(1),
        CLong(2),
        RArray(List(CLong(1), CLong(2), CLong(3))))))
      val expectedDefined = new BitSet(1)
      expectedDefined.set(0, 2)
      val expectedSlice = Slice(2,
        Map(
          ColumnRef(CPath.Identity, CLong) ->
            new ArrayLongColumn(expectedDefined, Array(1L, 2L))))
      val expectedRemaining = ChunkSliced(data.chunk, 2, 1)
      val (actualSlice, actualRemaining) = fromQDataStep(data, 3, 2, 32)
      sliceEqualityAtDefinedRows(actualSlice, expectedSlice)
      actualRemaining mustEqual expectedRemaining
    }

    "add a value that overflows the column limit, if otherwise the slice would be empty" >> {
      val data = ChunkSliced(Chunk.array(Array[RValue](RArray(List(CLong(1), CLong(2), CLong(3))))), 0, 1)
      val expectedDefined = new BitSet(1)
      expectedDefined.set(0)
      val expectedSlice = {
        def size = 1
        def columns = Map(
          ColumnRef(CPath.Identity \ 0, CLong) -> new ArrayLongColumn(expectedDefined, Array(1)),
          ColumnRef(CPath.Identity \ 1, CLong) -> new ArrayLongColumn(expectedDefined, Array(2)),
          ColumnRef(CPath.Identity \ 2, CLong) -> new ArrayLongColumn(expectedDefined, Array(3))
        )
        Slice(size, columns)
      }
      val expectedRemaining = ChunkSliced.empty[RValue]
      val (actualSlice, actualRemaining) = fromQDataStep(data, 1, 2, 32)
      sliceEqualityAtDefinedRows(actualSlice, expectedSlice)
      actualRemaining mustEqual expectedRemaining
    }

    "grow slices to maxRows given only scalars" >> {
      val data = Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      val chunkSliced = ChunkSliced.fromChunk(Chunk.array(data.map[RValue, Array[RValue]](CLong(_))))
      val expectedDefined = new BitSet(8)
      expectedDefined.set(0, 8)
      val expectedSlice = {
        def size = 8
        val columns = Map(
          ColumnRef(CPath.Identity, CLong) ->
            new ArrayLongColumn(expectedDefined, data))
        Slice(size, columns)
      }
      val expectedRemaining = ChunkSliced(chunkSliced.chunk, 8, 2)
      val (actualSlice, actualRemaining) = fromQDataStep(chunkSliced, 8, 10, 32)
      sliceEqualityAtDefinedRows(actualSlice, expectedSlice)
      actualRemaining mustEqual expectedRemaining
    }

  }

  "sortBy" should {
    "stably sort a slice by a projection" in {
      val array = JParser.parseUnsafe("""[
        { "city": "ANEHEIM", "state": "CA" },
        { "city": "LOPEZ", "state": "WA" },
        { "city": "SPOKANE", "state": "WA" },
        { "city": "WASCO", "state": "CA" }
        ]""")

      val data = array match {
        case JArray(rows) => rows.toStream
        case _ => ???
      }

      val target = Slice.fromJValues(data)

      // Note the monitonically decreasing sequence
      // associated with the keys, due to having repeated
      // states being sorted in descending order.
      val keyArray = JParser.parseUnsafe("""[
          [ "CA", 0 ],
          [ "WA", -1 ],
          [ "WA", -2 ],
          [ "CA", -3 ]
        ]""")

      val keyData = keyArray match {
        case JArray(rows) => rows.toStream
        case _ => ???
      }

      val key = Slice.fromJValues(keyData)

      val result = target.sortWith(key, SortDescending)._1.toJsonElements.toVector

      val expectedArray = JParser.parseUnsafe("""[
        { "city": "LOPEZ", "state": "WA" },
        { "city": "SPOKANE", "state": "WA" },
        { "city": "ANEHEIM", "state": "CA" },
        { "city": "WASCO", "state": "CA" }
        ]""")

      val expected = expectedArray match {
        case JArray(rows) => rows.toVector
        case _ => ???
      }

      result mustEqual expected
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

    val emptySlice = Slice.empty

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
        toCValues(slice).map(_.toSet) must_== fakeConcat(slices).map(_.toSet)
      }
    }
  }

  "ifUndefined" should {
    "merge homogeneous slices with holes" in {
      val sourceArray = JParser.parseUnsafe("""[
        { "a": 1 },
        { "a": 2 },
        { "b": "three" },
        { "a": 4 }
        ]""")

      val sourceData = sourceArray match {
        case JArray(rows) => rows.toStream
        case _ => ???
      }

      val source = Slice.fromJValues(sourceData).deref(CPathField("a"))

      val defaultArray = JParser.parseUnsafe("""[
        { "b": "five" },
        { "a": 3 },
        { "a": 3 },
        { "b": "six" }
        ]""")

      val defaultData = defaultArray match {
        case JArray(rows) => rows.toStream
        case _ => ???
      }

      val default = Slice.fromJValues(defaultData).deref(CPathField("a"))

      val result = source.ifUndefined(default)
      val values = result.toJsonElements

      val expectedArray = JParser.parseUnsafe("""[1, 2, 3, 4]""")

      val expectedData = expectedArray match {
        case JArray(rows) => rows.toVector
        case _ => ???
      }

      values mustEqual expectedData
    }

    "merge heterogeneous slices with holes" in {
      val sourceArray = JParser.parseUnsafe("""[
        { "a": true },
        { "a": 2 },
        { "b": 3 },
        { "a": null }
        ]""")

      val sourceData = sourceArray match {
        case JArray(rows) => rows.toStream
        case _ => ???
      }

      val source = Slice.fromJValues(sourceData).deref(CPathField("a"))

      val defaultArray = JParser.parseUnsafe("""[
        { "b": "five" },
        { "a": 3 },
        { "a": "three" },
        { "b": 6 }
        ]""")

      val defaultData = defaultArray match {
        case JArray(rows) => rows.toStream
        case _ => ???
      }

      val default = Slice.fromJValues(defaultData).deref(CPathField("a"))

      val result = source.ifUndefined(default)
      val values = result.toJsonElements

      val expectedArray = JParser.parseUnsafe("""[true, 2, "three", null]""")

      val expectedData = expectedArray match {
        case JArray(rows) => rows.toVector
        case _ => ???
      }

      values mustEqual expectedData
    }
  }
}


object ArbitrarySlice extends RCValueGenerators {
  import TimeGenerators._

  private def genBitSet(size: Int): Gen[BitSet] =
    listOfN(size, genBool) ^^ (BitsetColumn bitset _)

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

    genData ^^ (data => Slice(sz, ctrie.CTrie(data :_*)))
  }
}
