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
import quasar.Data
import quasar.fs.FileSystemError
import quasar.fs.FileSystemError._

import scala.collection.immutable.TreeSet
import scala.math.Ordering

import scalaz._
import Scalaz._

sealed trait ColumnType
case object JsonCol extends ColumnType
case object StringCol extends ColumnType
case object IntCol extends ColumnType
case object DecCol extends ColumnType
case object NullCol extends ColumnType

final case class ColumnDesc(name: String, tpe: ColumnType) {

  override def equals(other: scala.Any): Boolean = {
    other match {
      case other@ColumnDesc(_, _) => TableModel.columnDescEq.equal(this, other)
      case _                      => false
    }
  }

  override def hashCode: Int = name.hashCode
}

sealed trait TableModel

case object JsonTable extends TableModel

final case class ColumnarTable(columns: Set[ColumnDesc])
  extends TableModel

sealed trait AlterColumn

final case class AddColumn(name: String, tpe: ColumnType) extends AlterColumn
final case class ModifyColumn(name: String, tpe: ColumnType) extends AlterColumn


object ColumnarTable {
  import TableModel._

  def fromColumns(cs: List[ColumnDesc]): ColumnarTable = ColumnarTable(TreeSet.empty ++ cs)
}

object TableModel {

  def alter(initial: TableModel, newModel: TableModel): FileSystemError \/ Set[AlterColumn] = {
    initial match {
      case JsonTable => Set.empty.right
      case ColumnarTable(initialCols) =>
        val result = initial ⊹ newModel
        result match {
          case JsonTable =>
            unsupportedOperation(s"Cannot update columnar model $initial to single-column json model.").left
          case ColumnarTable(resultCols) =>
            val newCols = resultCols -- initialCols
            val updatedCols = (resultCols -- newCols).filter { rc =>
              initialCols.exists {
                pc => pc.name === rc.name && (pc.tpe ≠ rc.tpe)
              }
            }.map(c => ModifyColumn(c.name, c.tpe))
            (newCols.map(c => AddColumn(c.name, c.tpe): AlterColumn) ++ updatedCols).right
        }
    }
  }

  implicit val columnDescEq: Equal[ColumnDesc] = Equal.equalBy(_.name)

  implicit val columnDescOrdering: Ordering[ColumnDesc] =
    Ordering.fromLessThan[ColumnDesc](_.name < _.name)

  implicit val columnTypeEq: Equal[ColumnType] = Equal.equalA

  implicit val tableModelEq: Equal[TableModel] =  Equal.equal {
    case (JsonTable, JsonTable) => true
    case (ColumnarTable(c1), ColumnarTable(c2)) => c1.toList === c2.toList
    case _ => false
  }

  implicit val tableModelShow: Show[TableModel] = Show.showFromToString[TableModel]
  /**
    * When updating current table model with new model, the resulting model
    * can be either extended by new columns, or just a JsonTable. The latter
    * means that current and new models cannot produce a new definition which
    * reconciles data types for both (data is heterogenous).
    */
  implicit val tableModelMonoid: Monoid[TableModel] = new Monoid[TableModel] {
    override def zero: TableModel = ColumnarTable(TreeSet.empty)
    def empty: TableModel = zero

    override def append(s1: TableModel, s2: => TableModel): TableModel = {

      def updateCols(cols: Set[ColumnDesc],
                     newCols: Set[ColumnDesc]): TableModel = {
        cols.toList
          .traverse { c =>
            newCols
              .find(_.name === c.name)
              .map { newC =>
                if (c.tpe === NullCol)
                  newC.some
                else if (c.tpe === newC.tpe || newC.tpe === NullCol)
                  c.some
                else if (c.tpe === DecCol && newC.tpe === IntCol)
                  c.some
                else if (c.tpe === IntCol && newC.tpe === DecCol)
                  newC.some
                else
                  none
              }
              .getOrElse(c.some)
          }
          .map(cs =>
            ColumnarTable.fromColumns(cs ++ newCols.diff(cols)): TableModel)
          .getOrElse(JsonTable)
      }

      s1 match {
        case JsonTable                           => JsonTable
        case ColumnarTable(cols) if cols.isEmpty => s2
        case ColumnarTable(cols) =>
          s2 match {
            case JsonTable                                 => JsonTable
            case ColumnarTable(newCols) if newCols.isEmpty => s1
            case ColumnarTable(newCols)                    =>
              updateCols(cols, newCols)
          }
      }
    }
  }

  def simpleColumnType(data: Data): ColumnType = {
    data match {
      case Data.Null   => NullCol
      case Data.Str(_) => StringCol
      case Data.Dec(_) => DecCol
      case Data.Int(_) => IntCol
      case _           => NullCol // TODO support all types
    }
  }

  def columnType(data: Data): ColumnType = {
    data match {
      case Data.Obj(_) => JsonCol
      case Data.Arr(_) => JsonCol
      case _           => simpleColumnType(data)
    }
  }

  def rowModel(row: Data): TableModel = {
    row match {
      case Data.Obj(fields) =>
        ColumnarTable.fromColumns(fields.map {
          case (label, value) => ColumnDesc(label, columnType(value))
        }.toList)
      case _ => JsonTable
    }
  }

  def fromData(ds: Vector[Data]): FileSystemError \/ TableModel = {
    ds match {
      case _ :+ _ =>
        ds.foldMap(rowModel).right
      case _ =>
        unsupportedOperation("Cannot map empty data row to a RDBMS table model.").left
    }
  }
}
