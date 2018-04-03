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

package quasar.physical.rdbms.model

import slamdata.Predef._
import quasar.{Data, Type}
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
case object BoolCol extends ColumnType

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

    val widenings = Map[Set[ColumnType], ColumnType](
      Set[ColumnType](IntCol, DecCol) -> DecCol,
      Set[ColumnType](DecCol, StringCol) -> StringCol,
      Set[ColumnType](IntCol, StringCol) -> StringCol,
      Set[ColumnType](BoolCol, StringCol) -> StringCol,
      Set[ColumnType](BoolCol, DecCol) -> StringCol,
      Set[ColumnType](BoolCol, IntCol) -> StringCol,
      Set[ColumnType](StringCol, JsonCol) -> JsonCol,
      Set[ColumnType](BoolCol, JsonCol) -> JsonCol,
      Set[ColumnType](IntCol, JsonCol) -> JsonCol,
      Set[ColumnType](DecCol, JsonCol) -> JsonCol,
    )

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
                else
                  widenings.get(Set(c.tpe, newC.tpe)).map(ColumnDesc(c.name, _))
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

  def simpleColumnType(data: Type): ColumnType = {
    import quasar.{Type => t}
    data match {
      case t.Null    => NullCol
      case t.Str     => StringCol
      case t.Int     => IntCol
      case t.Bool    => BoolCol
      case t.Dec     => DecCol
      case t.OffsetDateTime => JsonCol
      case t.OffsetDate => JsonCol
      case t.OffsetTime => JsonCol
      case t.LocalDateTime => JsonCol
      case t.LocalDate => JsonCol
      case t.LocalTime => JsonCol
      case t.Id      => JsonCol
      case _         => NullCol // TODO support all types
    }
  }

  def columnType(dataType: Type): ColumnType = {
    import quasar.{Type => t}
    dataType match {
      case t.Arr(_)    => JsonCol
      case t.Obj(_, _) => JsonCol
      case _           => simpleColumnType(dataType)
    }
  }

  def rowModel(row: Data): TableModel = {
    row match {
      case Data.Obj(fields) =>
        ColumnarTable.fromColumns(fields.map {
          case (label, value) => ColumnDesc(label, columnType(value.dataType))
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
