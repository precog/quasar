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
import quasar.physical.rdbms.common.{CustomSchema, DefaultSchema, Schema, TableName, TablePath}
import quasar.physical.rdbms.fs.RdbmsDescribeTable
import quasar.physical.rdbms.common.TablePath._

import doobie.imports._
import scalaz.syntax.show._
import scalaz.syntax.applicative._

trait PostgresDescribeTable extends RdbmsDescribeTable {

  private def descQuery[F[_], T](
      whereClause: Fragment,
      mapResult: List[(String, String)] => T): ConnectionIO[T] = {
    (fr"SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS"
      ++ whereClause).query[(String, String)].list.map(mapResult)
  }

  private def whereSchemaAndTable(tablePath: TablePath): Fragment = {
    fr"WHERE TABLE_SCHEMA =" ++
      Fragment.const("'" + tablePath.schema.shows + "'") ++
      fr"AND TABLE_NAME =" ++
      Fragment.const("'" + tablePath.table.shows + "'")
  }

  private def whereSchema(schemaName: String): Fragment =
    fr"WHERE TABLE_SCHEMA =" ++
      Fragment.const("'" + schemaName + "'")

  override def findChildTables(schema: Schema): ConnectionIO[Vector[TableName]] = {
    val whereClause = schema match {
      case DefaultSchema => fr""
      case c: CustomSchema => fr"WHERE TABLE_SCHEMA = ${c.shows}"
    }

    (fr"select TABLE_NAME from information_schema.tables" ++ whereClause)
      .query[String]
      .vector
      .map(_.map(TableName.apply))
  }

  override def findChildSchemas(parent: Schema): ConnectionIO[Vector[CustomSchema]] = {
    val whereClause = parent match {
      case DefaultSchema => fr""
      case c: CustomSchema =>
        fr"WHERE SCHEMA_NAME LIKE" ++ Fragment.const("'" + c.shows + Separator + "%'")
    }
    (fr"SELECT SCHEMA_NAME FROM information_schema.schemata" ++ whereClause)
      .query[String]
      .vector
      .map(_.map(CustomSchema.apply))
  }

  override def schemaExists(schema: Schema): ConnectionIO[Boolean] =
    schema match {
      case DefaultSchema => true.point[ConnectionIO]
      case c: CustomSchema =>
        descQuery(whereSchema(c.shows), _.nonEmpty)
    }

  override def tableExists(tablePath: TablePath): ConnectionIO[Boolean] =
    descQuery(whereSchemaAndTable(tablePath), _.nonEmpty)

}
