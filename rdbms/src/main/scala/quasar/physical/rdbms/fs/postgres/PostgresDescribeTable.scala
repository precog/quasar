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

package quasar.physical.rdbms.fs.postgres

import slamdata.Predef._
import quasar.physical.rdbms.fs.RdbmsDescribeTable
import quasar.physical.rdbms.common._
import quasar.physical.rdbms.common.TablePath._
import doobie.imports._
import quasar.physical.rdbms.model._
import quasar.physical.rdbms.model.TableModel._

import scalaz._
import Scalaz._

trait PostgresDescribeTable extends RdbmsDescribeTable {

  private def descQuery[F[_], T](
      whereClause: Fragment,
      mapResult: List[(String, String)] => T): ConnectionIO[T] = {
    (fr"SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS"
      ++ whereClause)
      .query[(String, String)]
      .list
      .map(mapResult)
  }

  private def whereSchemaAndTable(tablePath: TablePath): Fragment = {
    fr"WHERE TABLE_SCHEMA =" ++
    Fragment.const("'" + tablePath.schema.shows + "'") ++
    fr"AND TABLE_NAME =" ++
    Fragment.const("'" + tablePath.table.shows + "'")
  }

  private def whereSchema(schema: Schema): Fragment =
    if (schema.isRoot)
      fr""
  else
    fr"WHERE TABLE_SCHEMA =" ++
      Fragment.const("'" + schema.shows + "'")


  override def findChildTables(schema: Schema): ConnectionIO[Vector[TableName]] = {
    if (schema.isRoot)
      Vector.empty.point[ConnectionIO]
    else
    (fr"select TABLE_NAME from information_schema.tables" ++ whereSchema(schema))
      .query[String]
      .vector
      .map(_.map(TableName.apply))
  }

  override def findChildSchemas(parent: Schema): ConnectionIO[Vector[Schema]] = {
    val whereClause = if (parent.isRoot)
        fr""
      else
        fr"WHERE SCHEMA_NAME LIKE" ++ Fragment.const("'" + parent.shows + Separator + "%'")

    (fr"SELECT SCHEMA_NAME FROM information_schema.schemata" ++ whereClause)
      .query[String]
      .vector
      .map(_.map(Schema.apply))
  }


  override def schemaExists(schema: Schema): ConnectionIO[Boolean] =
    if (schema.isRoot) true.point[ConnectionIO]
    else
      sql"""SELECT 1 FROM information_schema.schemata WHERE SCHEMA_NAME=${schema.shows}"""
      .query[Int]
      .list
      .map(_.nonEmpty)

  override def tableExists(
      tablePath: TablePath): ConnectionIO[Boolean] =
    descQuery(whereSchemaAndTable(tablePath), _.nonEmpty)

  def tableModel(tablePath: TablePath): ConnectionIO[Option[TableModel]] = {
    val cols = descQuery(whereSchemaAndTable(tablePath), _.map {
      case (colName, colTypeStr) =>
        ColumnDesc(colName, colTypeStr.mapToColumnType)
    })

    cols.map {
      case Nil => None
      case c :: Nil if c.tpe === JsonCol => Some(JsonTable)
      case multipleCols => Some(ColumnarTable.fromColumns(multipleCols))
    }

  }

}
