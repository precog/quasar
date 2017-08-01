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

package quasar.yggdrasil
package table

import quasar.blueeyes._, json._
import quasar.precog.common._, security._
import quasar.precog.TestSupport._, Gen._
import quasar.yggdrasil.bytecode.JType

import org.slf4j.LoggerFactory

import scalaz._, Scalaz._

import TableModule._
import SampleData._

import java.nio.CharBuffer

trait TestColumnarTableModule[M[+_]] extends ColumnarTableModuleTestSupport[M] {
  type GroupId = Int
  import trans._

  private val groupId = new java.util.concurrent.atomic.AtomicInteger
  def newGroupId = groupId.getAndIncrement

  def addThree: CFN = CFNP("addThree") {
    case List(x: LongColumn, y: LongColumn, z: LongColumn) =>
      new LongColumn {
        def apply(row: Int) = x(row) + y(row) + z(row)

        def isDefinedAt(row: Int) =
          x.isDefinedAt(row) && y.isDefinedAt(row) && z.isDefinedAt(row)
      }
  }

  class Table(slices: StreamT[M, Slice], size: TableSize) extends ColumnarTable(slices, size) {
    import trans._
    def load(apiKey: APIKey, jtpe: JType) = sys.error("todo")
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean = false) = M.point(this)
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = false): M[Seq[Table]] = sys.error("todo")
  }

  trait TableCompanion extends ColumnarTableCompanion {
    def apply(slices: StreamT[M, Slice], size: TableSize) = new Table(slices, size)

    def singleton(slice: Slice) = new Table(slice :: StreamT.empty[M, Slice], ExactSize(1))

    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): M[(Table, Table)] =
      sys.error("not implemented here")
  }

  object Table extends TableCompanion
}

trait ColumnarTableModuleSpec[M[+_]] extends TestColumnarTableModule[M]
    with TableModuleSpec[M]
    with CogroupSpec[M]
    with CrossSpec[M]
    with LeftShiftSpec[M]
    with TransformSpec[M]
    with CompactSpec[M]
    with TakeRangeSpec[M]
    with CanonicalizeSpec[M]
    with PartitionMergeSpec[M]
    with ToArraySpec[M]
    with SampleSpec[M]
    with DistinctSpec[M]
    with SchemasSpec[M]
    { spec =>

  import trans._

  def testConcat = {
    val json1 = """{ "a": 1, "b": "x", "c": null }"""
    val json2 = """[4, "foo", null, true]"""

    val data1: Stream[JValue] = Stream.fill(25)(JParser.parse(json1))
    val data2: Stream[JValue] = Stream.fill(35)(JParser.parse(json2))

    val table1 = fromSample(SampleData(data1), Some(10))
    val table2 = fromSample(SampleData(data2), Some(10))

    val results = toJson(table1.concat(table2))
    val expected = data1 ++ data2

    results.copoint must_== expected
  }

  lazy val xlogger = LoggerFactory.getLogger("quasar.yggdrasil.table.ColumnarTableModuleSpec")

  def streamToString(stream: StreamT[M, CharBuffer]): String = {
    def loop(stream: StreamT[M, CharBuffer], sb: StringBuilder): M[String] =
      stream.uncons.flatMap {
        case None =>
          M.point(sb.toString)
        case Some((cb, tail)) =>
          sb.append(cb)
          loop(tail, sb)
      }
    loop(stream, new StringBuilder).copoint
  }


  def testRenderCsv(json: String, maxSliceSize: Option[Int] = None): String = {
    val es    = JParser.parseManyFromString(json).valueOr(throw _)
    val table = fromJson(es.toStream, maxSliceSize)
    streamToString(table.renderCsv())
  }

  def testRenderJson(seq: Seq[JValue]) = {
    def minimizeItem(t: (String, JValue)) = minimize(t._2).map((t._1, _))

    def minimize(value: JValue): Option[JValue] = {
      value match {
        case JObject(fields) => Some(JObject(fields.flatMap(minimizeItem)))

        case JArray(Nil) => Some(JArray(Nil))

        case JArray(elements) =>
          val elements2 = elements.flatMap(minimize)
          if (elements2.isEmpty) None else Some(JArray(elements2))

        case JUndefined => None

        case v => Some(v)
      }
    }

    val table = fromJson(seq.toStream)

    val expected = JArray(seq.toList)

    val arrayM = table.renderJson("[", ",", "]").foldLeft("")(_ + _.toString).map(JParser.parseUnsafe)

    val minimized = minimize(expected) getOrElse JArray(Nil)
    arrayM.copoint mustEqual minimized
  }

  def renderLotsToCsv(lots: Int, maxSliceSize: Option[Int] = None) = {
    val event    = "{\"x\":123,\"y\":\"foobar\",\"z\":{\"xx\":1.0,\"yy\":2.0}}"
    val events   = event * lots
    val csv      = testRenderCsv(events, maxSliceSize)
    val expected = ".x,.y,.z.xx,.z.yy\r\n" + ("123,foobar,1,2\r\n" * lots)
    csv must_== expected
  }

  "a table dataset" should {
    "verify bijection from static JSON" in {
      val sample: List[JValue] = List(
        JObject(
          JField("key", JArray(JNum(-1L), JNum(0L))),
          JField("value", JNull)
        ),
        JObject(
          JField("key", JArray(JNum(-3090012080927607325l), JNum(2875286661755661474l))),
          JField("value", JObject(
            JField("q8b", JArray(
              JNum(6.615224799778253E307d),
              JArray(JBool(false), JNull, JNum(-8.988465674311579E307d), JNum(-3.536399224770604E307d))
            )),
            JField("lwu",JNum(-5.121099465699862E307d))
          ))
        ),
        JObject(
          JField("key", JArray(JNum(-3918416808128018609l), JNum(-1L))),
          JField("value", JNum(-1.0))
        )
      )

      val dataset = fromJson(sample.toStream)
      val results = dataset.toJson
      results.copoint.toList must_== sample
    }

    "verify bijection from JSON" in checkMappings(this)

    "verify renderJson round tripping" in {
      implicit val gen = sample(schema)

      prop { data: SampleData =>
        testRenderJson(data.data)
      }.set(minTestsOk = 20000, workers = Runtime.getRuntime.availableProcessors)
    }

    "handle special cases of renderJson" >> {
      "undefined at beginning of array" >> {
        testRenderJson(JArray(
          JUndefined ::
          JNum(1) ::
          JNum(2) :: Nil) :: Nil)
      }

      "undefined in middle of array" >> {
        testRenderJson(JArray(
          JNum(1) ::
          JUndefined ::
          JNum(2) :: Nil) :: Nil)
      }

      "fully undefined array" >> {
        testRenderJson(JArray(
          JUndefined ::
          JUndefined ::
          JUndefined :: Nil) :: Nil)
      }

      "undefined at beginning of object" >> {
        testRenderJson(JObject(
          JField("foo", JUndefined) ::
          JField("bar", JNum(1)) ::
          JField("baz", JNum(2)) :: Nil) :: Nil)
      }

      "undefined in middle of object" >> {
        testRenderJson(JObject(
          JField("foo", JNum(1)) ::
          JField("bar", JUndefined) ::
          JField("baz", JNum(2)) :: Nil) :: Nil)
      }

      "fully undefined object" >> {
        testRenderJson(JObject() :: Nil)
      }

      "undefined row" >> {
        testRenderJson(
          JObject(Nil) ::
          JNum(42) :: Nil)
      }

      "check utf-8 encoding" in prop { str: String =>
        val s = str.toList.map((c: Char) => if (c < ' ') ' ' else c).mkString
        testRenderJson(JString(s) :: Nil)
      }.set(minTestsOk = 20000, workers = Runtime.getRuntime.availableProcessors)

      "check long encoding" in prop { ln: Long =>
        testRenderJson(JNum(ln) :: Nil)
      }.set(minTestsOk = 20000, workers = Runtime.getRuntime.availableProcessors)
    }

    "in cogroup" >> {
      "perform a trivial cogroup" in testTrivialCogroup(identity[Table])
      "perform a trivial no-record cogroup" in testTrivialNoRecordCogroup(identity[Table])
      "perform a simple cogroup" in testSimpleCogroup(identity[Table])
      "perform another simple cogroup" in testAnotherSimpleCogroup
      "cogroup for unions" in testUnionCogroup
      "perform yet another simple cogroup" in testAnotherSimpleCogroupSwitched
      "cogroup across slice boundaries" in testCogroupSliceBoundaries
      "error on unsorted inputs" in testUnsortedInputs
      "cogroup partially defined inputs properly" in testPartialUndefinedCogroup

      "survive pathology 1" in testCogroupPathology1
      "survive pathology 2" in testCogroupPathology2
      "survive pathology 3" in testCogroupPathology3

      "not truncate cogroup when right side has long equal spans" in testLongEqualSpansOnRight
      "not truncate cogroup when left side has long equal spans" in testLongEqualSpansOnLeft
      "not truncate cogroup when both sides have long equal spans" in testLongEqualSpansOnBoth
      "not truncate cogroup when left side is long span and right is increasing" in testLongLeftSpanWithIncreasingRight

      "survive scalacheck" in {
        prop { cogroupData: (SampleData, SampleData) => testCogroup(cogroupData._1, cogroupData._2) }
      }
    }

    "in cross" >> {
      "perform a simple cartesian" in testSimpleCross

      "split a cross that would exceed maxSliceSize boundaries" in {
        val sample: List[JValue] = List(
          JObject(
            JField("key", JArray(JNum(-1L) :: JNum(0L) :: Nil)) ::
            JField("value", JNull) :: Nil
          ),
          JObject(
            JField("key", JArray(JNum(-3090012080927607325l) :: JNum(2875286661755661474l) :: Nil)) ::
            JField("value", JObject(List(
              JField("q8b", JArray(List(
                JNum(6.615224799778253E307d),
                JArray(List(JBool(false), JNull, JNum(-8.988465674311579E307d))), JNum(-3.536399224770604E307d)))),
              JField("lwu",JNum(-5.121099465699862E307d))))
            ) :: Nil
          ),
          JObject(
            JField("key", JArray(JNum(-3918416808128018609l) :: JNum(-1L) :: Nil)) ::
            JField("value", JNum(-1.0)) :: Nil
          ),
          JObject(
            JField("key", JArray(JNum(-3918416898128018609l) :: JNum(-2L) :: Nil)) ::
            JField("value", JNum(-1.0)) :: Nil
          ),
          JObject(
            JField("key", JArray(JNum(-3918426808128018609l) :: JNum(-3L) :: Nil)) ::
            JField("value", JNum(-1.0)) :: Nil
          )
        )

        val dataset1 = fromJson(sample.toStream, Some(3))
        val dataset2 = fromJson(sample.toStream, Some(3))

        dataset1.cross(dataset1)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight))).slices.uncons.copoint must beLike {
          case Some((head, _)) => head.size must beLessThanOrEqualTo(yggConfig.maxSliceSize)
        }
      }

      "cross across slice boundaries on one side" in testCrossSingles
      "survive scalacheck" in {
        prop { cogroupData: (SampleData, SampleData) => testCross(cogroupData._1, cogroupData._2) }
      }
    }

    "in leftShift" >> {
      "shift a simple array" in testTrivialArrayLeftShift
      "shift a simple object" in testTrivialObjectLeftShift
      "shift a mixture of objects and arrays" in testTrivialObjectArrayLeftShift
      "shift a set of arrays" in testSetArrayLeftShift
      "shift a heterogeneous array" in testHeteroArrayLeftShift
      "shift a simple array with an inner object" in testTrivialArrayLeftShiftWithInnerObject
    }

    "in transform" >> {
      "perform the identity transform" in checkTransformLeaf

      "perform a trivial map1" in testMap1IntLeaf
      "perform deepmap1 using numeric coercion" in testDeepMap1CoerceToDouble
      "perform map1 using numeric coercion" in testMap1CoerceToDouble
      "fail to map1 into array and object" in testMap1ArrayObject
      "perform a less trivial map1" in checkMap1.pendingUntilFixed

      //"give the identity transform for the trivial filter" in checkTrivialFilter
      "give the identity transform for the trivial 'true' filter" in checkTrueFilter
      "give the identity transform for a nontrivial filter" in checkFilter.pendingUntilFixed
      "give a transformation for a big decimal and a long" in testMod2Filter.pendingUntilFixed

      "perform an object dereference" in checkObjectDeref
      "perform an array dereference" in checkArrayDeref
      "perform metadata dereference on data without metadata" in checkMetaDeref

      "perform a trivial map2 add" in checkMap2Add.pendingUntilFixed
      "perform a trivial map2 eq" in checkMap2Eq
      "perform a map2 add over but not into arrays and objects" in testMap2ArrayObject

      "perform a mapN add over arrays and objects" in testMapNAddThree

      "perform a trivial equality check" in checkEqualSelf
      "perform a trivial equality check on an array" in checkEqualSelfArray
      "perform a slightly less trivial equality check" in checkEqual
      "test a failing equality example" in testEqual1
      "perform a simple equality check" in testSimpleEqual
      "perform another simple equality check" in testAnotherSimpleEqual
      "perform yet another simple equality check" in testYetAnotherSimpleEqual
      "perform a simple not-equal check" in testASimpleNonEqual

      "perform a equal-literal check" in checkEqualLiteral
      "perform a not-equal-literal check" in checkNotEqualLiteral

      "wrap the results of a transform in an object as the specified field" in checkWrapObject
      "give the identity transform for self-object concatenation" in checkObjectConcatSelf
      "use a right-biased overwrite strategy in object concat conflicts" in checkObjectConcatOverwrite
      "use a right-biased overwrite strategy in object concat conflicts (with differing types)" in checkObjectConcatOverwriteDifferingTypes
      "test inner object concat with a single boolean" in testObjectConcatSingletonNonObject
      "test inner object concat with a boolean and an empty object" in testObjectConcatTrivial
      "concatenate dissimilar objects" in checkObjectConcat
      "test inner object concat join semantics" in testInnerObjectConcatJoinSemantics
      "test inner object concat with empty objects" in testInnerObjectConcatEmptyObject
      "test outer object concat with empty objects" in testOuterObjectConcatEmptyObject
      "test inner object concat with undefined" in testInnerObjectConcatUndefined
      "test outer object concat with undefined" in testOuterObjectConcatUndefined
      "test inner object concat with empty" in testInnerObjectConcatLeftEmpty
      "test outer object concat with empty" in testOuterObjectConcatLeftEmpty

      "concatenate dissimilar arrays" in checkArrayConcat
      "inner concatenate arrays with undefineds" in testInnerArrayConcatUndefined
      "outer concatenate arrays with undefineds" in testOuterArrayConcatUndefined
      "inner concatenate arrays with empty arrays" in testInnerArrayConcatEmptyArray
      "outer concatenate arrays with empty arrays" in testOuterArrayConcatEmptyArray
      "inner array concatenate when one side is not an array" in testInnerArrayConcatLeftEmpty
      "outer array concatenate when one side is not an array" in testOuterArrayConcatLeftEmpty

      "delete elements according to a JType" in checkObjectDelete
      "delete only field in object without removing from array" in {
        val JArray(elements) = JParser.parseUnsafe("""[
          {"foo": 4, "bar": 12},
          {"foo": 5},
          {"bar": 45},
          {},
          {"foo": 7, "bar" :23, "baz": 24}
        ]""")

        val sample = SampleData(elements.toStream)
        val table = fromSample(sample)

        val spec = ObjectDelete(Leaf(Source), Set(CPathField("foo")))
        val results = toJson(table.transform(spec))
        val JArray(expected) = JParser.parseUnsafe("""[
          {"bar": 12},
          {},
          {"bar": 45},
          {},
          {"bar" :23, "baz": 24}
        ]""")

        results.copoint mustEqual expected.toStream
      }

      "perform a basic IsType transformation" in testIsTypeTrivial
      "perform an IsType transformation on numerics" in testIsTypeNumeric
      "perform an IsType transformation on trivial union" in testIsTypeUnionTrivial
      "perform an IsType transformation on union" in testIsTypeUnion
      "perform an IsType transformation on nested unfixed types" in testIsTypeUnfixed
      "perform an IsType transformation on objects" in testIsTypeObject
      "perform an IsType transformation on unfixed objects" in testIsTypeObjectUnfixed
      "perform an IsType transformation on unfixed arrays" in testIsTypeArrayUnfixed
      "perform an IsType transformation on empty objects" in testIsTypeObjectEmpty
      "perform an IsType transformation on empty arrays" in testIsTypeArrayEmpty
      "perform a check on IsType" in checkIsType

      "perform a trivial type-based filter" in checkTypedTrivial
      "perform a less trivial type-based filter" in checkTyped
      "perform a type-based filter across slice boundaries" in testTypedAtSliceBoundary
      "perform a trivial heterogeneous type-based filter" in testTypedHeterogeneous
      "perform a trivial object type-based filter" in testTypedObject
      "retain all object members when typed to unfixed object" in testTypedObjectUnfixed
      "perform another trivial object type-based filter" in testTypedObject2
      "perform a trivial array type-based filter" in testTypedArray
      "perform another trivial array type-based filter" in testTypedArray2
      "perform yet another trivial array type-based filter" in testTypedArray3
      "perform a fourth trivial array type-based filter" in testTypedArray4
      "perform a trivial number type-based filter" in testTypedNumber
      "perform another trivial number type-based filter" in testTypedNumber2
      "perform a filter returning the empty set" in testTypedEmpty

      "perform a summation scan case 1" in testTrivialScan
      "perform a summation scan of heterogeneous data" in testHetScan
      "perform a summation scan" in checkScan
      "perform dynamic object deref" in testDerefObjectDynamic
      "perform an array swap" in checkArraySwap
      "replace defined rows with a constant" in checkConst

      "check cond" in checkCond.pendingUntilFixed
    }

    "in compact" >> {
      "be the identity on fully defined tables"  in testCompactIdentity
      "preserve all defined rows"                in testCompactPreserve
      "have no undefined rows"                   in testCompactRows
      "have no empty slices"                     in testCompactSlices
      "preserve all defined key rows"            in testCompactPreserveKey
      "have no undefined key rows"               in testCompactRowsKey
      "have no empty key slices"                 in testCompactSlicesKey
    }

    "in distinct" >> {
      "be the identity on tables with no duplicate rows" in testDistinctIdentity
      "peform properly when the same row appears in two different slices" in testDistinctAcrossSlices
      "peform properly again when the same row appears in two different slices" in testDistinctAcrossSlices2
      "have no duplicate rows" in testDistinct
    }

    "in takeRange" >> {
      "select the correct rows in a trivial case" in testTakeRange
      "select the correct rows when we take past the end of the table" in testTakeRangeLarger
      "select the correct rows when we start at an index larger than the size of the table" in testTakeRangeEmpty
      "select the correct rows across slice boundary" in testTakeRangeAcrossSlices
      "select the correct rows only in second slice" in testTakeRangeSecondSlice
      "select the first slice" in testTakeRangeFirstSliceOnly
      "select nothing with a negative starting index" in testTakeRangeNegStart
      "select nothing with a negative number to take" in testTakeRangeNegNumber
      "select the correct rows using scalacheck" in checkTakeRange
    }

    "in toArray" >> {
      "create a single column given two single columns" in testToArrayHomogeneous
      "create a single column given heterogeneous data" in testToArrayHeterogeneous
    }

    "in concat" >> {
      "concat two tables" in testConcat
    }

    "in canonicalize" >> {
      "return the correct slice sizes using scalacheck" in checkCanonicalize
      "return the slice size in correct bound using scalacheck with range" in checkBoundedCanonicalize
      "return the correct slice sizes in a trivial case" in testCanonicalize
      "return the correct slice sizes given length zero" in testCanonicalizeZero
      "return the correct slice sizes along slice boundaries" in testCanonicalizeBoundary
      "return the correct slice sizes greater than slice boundaries" in testCanonicalizeOverBoundary
      "return empty table when given empty table" in testCanonicalizeEmpty
      "remove slices of size zero" in testCanonicalizeEmptySlices
    }

    "in schemas" >> {
      "find a schema in single-schema table" in testSingleSchema
      "find a schema in homogeneous array table" in testHomogeneousArraySchema
      "find schemas separated by slice boundary" in testCrossSliceSchema
      "extract intervleaved schemas" in testIntervleavedSchema
      "don't include undefineds in schema" in testUndefinedsInSchema
      "deal with most expected types" in testAllTypesInSchema
    }

    "in sample" >> {
       "sample from a dataset" in testSample
       "return no samples given empty sequence of transspecs" in testSampleEmpty
       "sample from a dataset given non-identity transspecs" in testSampleTransSpecs
       "return full set when sample size larger than dataset" in testLargeSampleSize
       "resurn empty table when sample size is 0" in test0SampleSize
    }
  }

  "partitionMerge" should {
    "concatenate reductions of subsequences" in testPartitionMerge
  }

  "logging" should {
    "run" in {
      testSimpleCogroup(t => t.logged(xlogger, "test-logging", "start stream", "end stream") {
        slice => "size: " + slice.size
      })
    }
  }

  "track table metrics" in {
    "single traversal" >> {
      implicit val gen = sample(objectSchema(_, 3))
      prop { (sample: SampleData) =>
        val expectedSlices = (sample.data.size.toDouble / defaultSliceSize).ceil

        val table = fromSample(sample)
        val t0 = table.transform(TransSpec1.Id)
        t0.toJson.copoint must_== sample.data

        table.metrics.startCount must_== 1
        table.metrics.sliceTraversedCount must_== expectedSlices
        t0.metrics.startCount must_== 1
        t0.metrics.sliceTraversedCount must_== expectedSlices
      }
    }

    "multiple transforms" >> {
      implicit val gen = sample(objectSchema(_, 3))
      prop { (sample: SampleData) =>
        val expectedSlices = (sample.data.size.toDouble / defaultSliceSize).ceil

        val table = fromSample(sample)
        val t0 = table.transform(TransSpec1.Id).transform(TransSpec1.Id).transform(TransSpec1.Id)
        t0.toJson.copoint must_== sample.data

        table.metrics.startCount must_== 1
        table.metrics.sliceTraversedCount must_== expectedSlices
        t0.metrics.startCount must_== 1
        t0.metrics.sliceTraversedCount must_== expectedSlices
      }
    }

    "multiple forcing calls" >> {
      implicit val gen = sample(objectSchema(_, 3))
      prop { (sample: SampleData) =>
        val expectedSlices = (sample.data.size.toDouble / defaultSliceSize).ceil

        val table = fromSample(sample)
        val t0 = table.compact(TransSpec1.Id).compact(TransSpec1.Id).compact(TransSpec1.Id)
        table.toJson.copoint must_== sample.data
        t0.toJson.copoint must_== sample.data

        table.metrics.startCount must_== 2
        table.metrics.sliceTraversedCount must_== (expectedSlices * 2)
        t0.metrics.startCount must_== 1
        t0.metrics.sliceTraversedCount must_== expectedSlices
      }
    }

    "render to CSV in a simple case" in {
      val events = """
{"a": 1, "b": {"bc": 999, "bd": "foooooo", "be": true, "bf": null, "bg": false}, "c": [1.999], "d": "dog"}
{"a": 2, "b": {"bc": 998, "bd": "fooooo", "be": null, "bf": false, "bg": true}, "c": [2.999], "d": "dogg"}
{"a": 3, "b": {"bc": 997, "bd": "foooo", "be": false, "bf": true, "bg": null}, "c": [3.999], "d": "doggg"}
{"a": 4, "b": {"bc": 996, "bd": "fooo", "be": true, "bf": null, "bg": false}, "c": [4.999], "d": "dogggg"}
""".trim

      val expected = "" +
      ".a,.b.bc,.b.bd,.b.be,.b.bf,.b.bg,.c[0],.d\r\n" +
      "1,999,foooooo,true,null,false,1.999,dog\r\n" +
      "2,998,fooooo,null,false,true,2.999,dogg\r\n" +
      "3,997,foooo,false,true,null,3.999,doggg\r\n" +
      "4,996,fooo,true,null,false,4.999,dogggg\r\n"

      testRenderCsv(events) must_== expected
    }

    "test rendering uniform tables of varying sizes" in {
      renderLotsToCsv(100)
      renderLotsToCsv(1000)
      renderLotsToCsv(10000)
      renderLotsToCsv(100000)
    }

    "test string escaping" in {
      val csv = testRenderCsv("{\"s\":\"a\\\"b\",\"t\":\",\",\"u\":\"aa\\nbb\",\"v\":\"a,b\\\"c\\r\\nd\"}")

      val expected = "" +
".s,.t,.u,.v\r\n" +
"\"a\"\"b\",\",\",\"aa\n" +
"bb\",\"a,b\"\"c\r\n" +
"d\"\r\n"

      csv must_== expected
    }

    "test mixed rows" in {

      val input = """
{"a": 1}
{"b": 99.1}
{"a": true}
{"c": "jgeiwgjewigjewige"}
{"b": "foo", "d": 999}
{"e": null}
{"f": {"aaa": 9}}
{"c": 100, "g": 934}
""".trim

      val expected = "" +
".a,.b,.c,.d,.e,.f.aaa,.g\r\n" +
"1,,,,,,\r\n" +
",99.1,,,,,\r\n" +
"true,,,,,,\r\n" +
",,jgeiwgjewigjewige,,,,\r\n" +
",foo,,999,,,\r\n" +
",,,,null,,\r\n" +
",,,,,9,\r\n" +
",,100,,,,934\r\n"

      testRenderCsv(input) must_== expected

      val expected2 = "" +
".a,.b\r\n1,\r\n,99.1\r\n\r\n" +
".a,.c\r\ntrue,\r\n,jgeiwgjewigjewige\r\n\r\n" +
".b,.d,.e\r\nfoo,999,\r\n,,null\r\n\r\n" +
".c,.f.aaa,.g\r\n,9,\r\n100,,934\r\n"

      testRenderCsv(input, Some(2)) must_== expected2
    }
  }
}

object ColumnarTableModuleSpec extends ColumnarTableModuleSpec[Need] {
  implicit def M = Need.need
}
