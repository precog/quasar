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
import quasar.precog.common._
import quasar.yggdrasil.bytecode.JType

import cats.effect.IO
import scalaz.StreamT
import shims._

// TODO: mix in a trait rather than defining Table directly

trait IndicesSpec extends ColumnarTableModuleTestSupport with TableModuleSpec with IndicesModule { self =>
  type GroupId = Int

  import TableModule._
  import trans._

  private val groupId = new java.util.concurrent.atomic.AtomicInteger
  def newGroupId = groupId.getAndIncrement

  class Table(slices: StreamT[IO, Slice], size: TableSize) extends ColumnarTable(slices, size) {
    import trans._
    def load(jtpe: JType) = sys.error("todo")
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean = false) = sys.error("todo")
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = false): IO[Seq[Table]] = sys.error("todo")
  }

  trait TableCompanion extends ColumnarTableCompanion {
    def apply(slices: StreamT[IO, Slice], size: TableSize) = new Table(slices, size)

    def singleton(slice: Slice) = new Table(slice :: StreamT.empty[IO, Slice], ExactSize(1))

    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1):
        IO[(Table, Table)] = sys.error("not implemented here")
  }

  object Table extends TableCompanion

  def groupkey(s: String) = DerefObjectStatic(Leaf(Source), CPathField(s))
  def valuekey(s: String) = DerefObjectStatic(Leaf(Source), CPathField(s))

  "a table index" should {
    "handle empty tables" in {
      val table = fromJson(Stream.empty[JValue])

      val keySpecs = Array(groupkey("a"), groupkey("b"))
      val valSpec = valuekey("c")

      val index: TableIndex = TableIndex.createFromTable(table, keySpecs, valSpec).unsafeRunSync

      index.getUniqueKeys(0).size must_== 0
      index.getSubTable(Array(0), Array(CString("a"))).size == ExactSize(0)
    }

    val json = """
      {"a": 1, "b": 2, "c": 3}
      {"a": 1, "b": 2, "c": 999, "d": "foo"}
      {"a": 1, "b": 2, "c": "cat"}
      {"a": 1, "b": 2}
      {"a": 2, "b": 2, "c": 3, "d": 1248}
      {"a": 2, "b": 2, "c": 13}
      {"a": "foo", "b": "bar", "c": 3}
      {"a": 3, "b": "", "c": 333}
      {"a": 3, "b": 2, "c": [1,2,3,4]}
      {"a": 1, "b": 2, "c": {"cat": 13, "dog": 12}}
      {"a": "foo", "b": 999}
      {"b": 2, "c": 9876}
      {"a": 1, "c": [666]}
      """

    val table             = fromJson(JParser.parseManyFromString(json).valueOr(throw _).toStream)
    val keySpecs          = Array(groupkey("a"), groupkey("b"))
    val valSpec           = valuekey("c")
    val index: TableIndex = TableIndex.createFromTable(table, keySpecs, valSpec).unsafeRunSync

    "determine unique groupkey values" in {
      index.getUniqueKeys(0) must_== Set[RValue](CNum(1), CNum(2), CNum(3), CString("foo"))
      index.getUniqueKeys(1) must_== Set[RValue](CNum(2), CNum(999), CString("bar"), CString(""))
    }

    "determine unique groupkey sets" in {
      index.getUniqueKeys() must_== Set[Seq[RValue]](
        Array(CNum(1), CNum(2)),
        Array(CNum(2), CNum(2)),
        Array(CString("foo"), CString("bar")),
        Array(CNum(3), CString("")),
        Array(CNum(3), CNum(2)),
        Array(CString("foo"), CNum(999))
      )
    }

    def subtableSet(index: TableIndex, ids: Seq[Int], vs: Seq[RValue]): Set[RValue] =
      index.getSubTable(ids, vs).toJson.unsafeRunSync.toSet

    def test(vs: Seq[RValue], result: Set[RValue]) =
      subtableSet(index, Array(0, 1), vs) must_== result

    "generate subtables based on groupkeys" in {
      def empty = Set.empty[RValue]

      test(Array(CLong(1), CNum(1)), empty)

      test(Array(CNum(1), CNum(2)), s1)
      def s1 = Set[RValue](
        CNum(3),
        CNum(999),
        CString("cat"),
        RObject(Map("cat" -> CNum(13), "dog" -> CNum(12)))
      )

      test(Array(CNum(2), CNum(2)), s2)
      def s2 = Set[RValue](CNum(3), CNum(13))

      test(Array(CString("foo"), CString("bar")), s3)
      def s3 = Set[RValue](CNum(3))

      test(Array(CNum(3), CString("")), s4)
      def s4 = Set[RValue](CNum(333))

      test(Array(CNum(3), CNum(2)), s5)
      def s5 = Set[RValue](RArray(CNum(1), CNum(2), CNum(3), CNum(4)))

      test(Array(CString("foo"), CNum(999)), empty)
    }

    val index1 = TableIndex.createFromTable(
      table, Array(groupkey("a")), valuekey("c")
    ).unsafeRunSync

    val index2 = TableIndex.createFromTable(
      table, Array(groupkey("b")), valuekey("c")
    ).unsafeRunSync

    "efficiently combine to produce unions" in {

      def tryit(tpls: (TableIndex, Seq[Int], Seq[RValue])*)(expected: RValue*) = {
        val table = TableIndex.joinSubTables(tpls.toList)
        table.toJson.unsafeRunSync.toSet must_== expected.toSet
      }

      // both disjunctions have data
      tryit(
        (index1, Seq(0), Seq(CNum(1))),
        (index2, Seq(0), Seq(CNum(2)))
      )(
        CNum(3),
        CNum(999),
        CNum(9876),
        CString("cat"),
        CNum(13),
        RArray(List(CNum(1), CNum(2), CNum(3), CNum(4))),
        RArray(List(CNum(666))),
        RObject(Map("cat" -> CNum(13), "dog" -> CNum(12)))
      )

      // only first disjunction has data
      tryit(
        (index1, Seq(0), Seq(CNum(1))),
        (index2, Seq(0), Seq(CNum(1234567)))
      )(
        CNum(3),
        CNum(999),
        CString("cat"),
        RArray(List(CNum(666))),
        RObject(Map("cat" -> CNum(13), "dog" -> CNum(12)))
      )

      // only second disjunction has data
      tryit(
        (index1, Seq(0), Seq(CNum(-8000))),
        (index2, Seq(0), Seq(CNum(2)))
      )(
        CNum(3),
        CNum(999),
        CNum(9876),
        CString("cat"),
        CNum(13),
        RArray(List(CNum(1), CNum(2), CNum(3), CNum(4))),
        RObject(Map("cat" -> CNum(13), "dog" -> CNum(12)))
      )

      // neither disjunction has data
      tryit(
        (index1, Seq(0), Seq(CNum(-8000))),
        (index2, Seq(0), Seq(CNum(1234567)))
      )()
    }
  }
}

object IndicesSpec extends IndicesSpec
