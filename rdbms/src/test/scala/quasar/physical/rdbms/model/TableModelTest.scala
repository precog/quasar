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

package quasar.physical.rdbms.model

import slamdata.Predef._
import quasar.{Data, DataCodec}
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import org.scalacheck.ScalacheckShapeless._
import org.specs2.scalacheck.Parameters
import org.specs2.scalaz.{ScalazMatchers, Spec}
import scala.collection.immutable.TreeSet
import TableModel._

import scala.Predef.implicitly

import scalaz._
import scalaz.scalacheck.{ScalazProperties => propz}
import Scalaz._

class TableModelTest extends Spec  with ScalazMatchers {

  implicit val codec: DataCodec = DataCodec.Precise
  implicit val params: Parameters = Parameters(minTestsOk = 1000)


  def data(str: String): Data =
    DataCodec.parse(str).valueOr(err => scala.sys.error(err.shows))
  
  def permutations[T](v: Vector[T]): Gen[Vector[T]] = {
    val perms = v.permutations.toList
    val permStream = Stream.continually(perms).flatMap(_.toStream).toIterator
    Gen.delay(permStream.next())
  }

  val arbCt: Arbitrary[ColumnType] = implicitly[Arbitrary[ColumnType]]

  val columnDescsGen: Gen[Set[ColumnDesc]] = Gen.nonEmptyListOf {
    for {
      name <- Gen.alphaNumStr.suchThat(_.nonEmpty)
      tpe <- arbCt.arbitrary
    }
      yield ColumnDesc(name, tpe)
  }.map(_.toSet)

  implicit val arbColDesc: Arbitrary[Set[ColumnDesc]] = Arbitrary(columnDescsGen)

  checkAll(propz.equal.laws[TableModel])
  checkAll(propz.monoid.laws[TableModel])

  "Table Model Monoid must" >> {
    "be commutative" >> {
      prop { (x: TableModel, y: TableModel) =>
        (x ⊹ y) must equal(y ⊹ x)
      }
    }

    "fail for empty vector" >> {
      TableModel.fromData(Vector.empty) must beLeftDisjunction
    }

    "devise columnar schema for a single row" >> {
      val row = data("""{"name":"John","surname":"Smith","birthYear":1980}""")
      TableModel.fromData(Vector(row)) must beRightDisjunction(
        ColumnarTable(
          TreeSet(ColumnDesc("name", StringCol),
            ColumnDesc("birthYear", IntCol),
            ColumnDesc("surname", StringCol))))
    }

    "devise columnar schema for homogenous data" >> {
      val row = data("""{"age": 25,"id":"748abf","keys": [4, 5, 6, 7]}""")
      TableModel.fromData(Vector(row, row, row, row, row, row)) must beRightDisjunction(
        ColumnarTable(
          TreeSet(ColumnDesc("age", IntCol),
            ColumnDesc("keys", JsonCol),
            ColumnDesc("id", StringCol))))
    }

    "devise columnar schema when extra columns appear" >> {
        val row = data("""{"age": 25,"id":"748abf","keys": [4, 5, 6, 7]}""")
        val updatedRow1 =
          data("""{"age": 35,"id":"a748abf","keys": [], "extra-col" : "v1"}""")
        val updatedRow2 = data(
          """{"age": 45, "another-extra-col": "v2", "id":"11","keys": [222]}""")

        forAll(permutations(Vector(row,
          row,
          updatedRow1,
          row,
          updatedRow2,
          row,
          row))) { rows =>

        TableModel.fromData(rows) must beRightDisjunction(
          ColumnarTable(TreeSet(
            ColumnDesc("age", IntCol),
            ColumnDesc("keys", JsonCol),
            ColumnDesc("id", StringCol),
            ColumnDesc("extra-col", StringCol),
            ColumnDesc("another-extra-col", StringCol)
          )))
      }
    }

    "devise columnar schema when some columns get nulled" >> {
        val row = data("""{"age": 25,"id":"748abf","keys": [4, 5, 6, 7]}""")
        val updatedRow1 = data("""{"id":"a748abf","keys": []}""")

        forAll(permutations(
          Vector(row, row, updatedRow1, row, row))) { rows =>

        TableModel.fromData(rows) must beRightDisjunction(
          ColumnarTable(
            TreeSet(ColumnDesc("age", IntCol),
              ColumnDesc("keys", JsonCol),
              ColumnDesc("id", StringCol))))
      }
    }

    "devise columnar schema when some columns get nulled and some get added" >> {
        val row = data("""{"age": 25,"id":"748abf","keys": [4, 5, 6, 7]}""")
        val updatedRow1 = data("""{"id":"a748abf","keys": []}""")
        val updatedRow2 = data("""{"extra": {"nested1": "val"}, "id":"a748abf","keys": []}""")

        forAll(permutations(Vector(row,
          row,
          updatedRow1,
          row,
          row,
          updatedRow2))) { rows =>

        TableModel.fromData(rows) must beRightDisjunction(
          ColumnarTable(
            TreeSet(ColumnDesc("age", IntCol),
                ColumnDesc("keys", JsonCol),
                ColumnDesc("id", StringCol),
                ColumnDesc("extra", JsonCol))))
      }
    }

    "devise columnar schema when all columns are nulled and some row has only new columns" >> {
        val row = data("""{"age": 25,"id":"748abf","keys": [4, 5, 6, 7]}""")
        val updatedRow1 = data("""{}""")
        val updatedRow2 = data("""{"extra": 35, "extra2": "str value"}""")

      forAll(permutations(Vector(row,
        row,
        updatedRow1,
        row,
        row,
        updatedRow2))) { rows =>

        TableModel.fromData(rows) must beRightDisjunction(
          ColumnarTable(
            TreeSet(ColumnDesc("age", IntCol),
                ColumnDesc("keys", JsonCol),
                ColumnDesc("id", StringCol),
                ColumnDesc("extra", IntCol),
                ColumnDesc("extra2", StringCol))))
      }
    }

    "devise json schema on encountering incompatible data types" >> {
      val row1 = data("""{"age": 25,"id":"748abf","keys": [4, 5, 6, 7]}""")
      val row2 = data("""{"age": "25","id":"648abf","keys": [4, 5, 6, 7]}""")

      forAll(permutations(Vector(row1, row2, row2, row2, row1))) { rows =>

        TableModel.fromData(rows) must beRightDisjunction(JsonTable)
      }
    }

    "treat nulls as 'always compatible' column types" >> {
      val row1 = data("""{"age": 25,"id":"748abf", "name": "Bob"}""")
      val row2 = data("""{"age": 35,"id":"648abf", "name": null}""")

      forAll(permutations(Vector(row1, row2, row2, row2, row1))) { rows =>

        TableModel.fromData(rows) must beRightDisjunction(
          ColumnarTable(
            TreeSet(ColumnDesc("age", IntCol),
                ColumnDesc("id", StringCol),
                ColumnDesc("name", StringCol))))
      }
    }
  }

  "Table Model alter must" >> {

    "return no updates if initial model is json-based" >> {
      TableModel.alter(JsonTable, JsonTable) must beRightDisjunction(Set.empty)
      TableModel.alter(JsonTable, ColumnarTable(Set(ColumnDesc("l", IntCol)))) must beRightDisjunction(Set.empty)
    }

    "fail if updating from columnar to json-based single column model" >> {
      TableModel.alter(ColumnarTable(Set(ColumnDesc("l", IntCol))), JsonTable) must beLeftDisjunction
    }

    "return no updates if both initial and new model are the same" >> {
      val model = ColumnarTable(Set(ColumnDesc("l", IntCol), ColumnDesc("s", StringCol)))
      TableModel.alter(model, model) must beRightDisjunction(Set.empty)
    }

    "return information about new columns" >> {
      val initial = ColumnarTable(Set(ColumnDesc("c1", IntCol), ColumnDesc("c2", StringCol)))
      val newModel = ColumnarTable(Set(ColumnDesc("c3", IntCol), ColumnDesc("c4", StringCol), ColumnDesc("c1", IntCol)))
      TableModel.alter(initial, newModel) must beRightDisjunction(
        Set(AddColumn("c3", IntCol), AddColumn("c4", StringCol)))
    }

    "return information about updated columns" >> {
      val initial = ColumnarTable(Set(ColumnDesc("c1", IntCol), ColumnDesc("c2", NullCol), ColumnDesc("c3", NullCol)))
      val newModel = ColumnarTable(Set(ColumnDesc("c2", StringCol), ColumnDesc("c3", IntCol)))
      TableModel.alter(initial, newModel) must beRightDisjunction(
        Set(ModifyColumn("c2", StringCol), ModifyColumn("c3", IntCol)))
    }

    "return information about updated and added columns" >> {
      val initial = ColumnarTable(Set(ColumnDesc("c1", IntCol), ColumnDesc("c2", NullCol)))
      val newModel = ColumnarTable(Set(ColumnDesc("c2", StringCol), ColumnDesc("c3", IntCol)))
      TableModel.alter(initial, newModel) must beRightDisjunction(
        Set(ModifyColumn("c2", StringCol), AddColumn("c3", IntCol)))
    }
  }
}
