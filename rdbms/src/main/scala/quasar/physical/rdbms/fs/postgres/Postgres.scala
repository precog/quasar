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
import planner.PostgresFlatRenderQuery
import quasar.connector.EnvironmentError
import quasar.Data
import quasar.fs.FileSystemType
import quasar.fs.mount.BackendDef.DefinitionError
import quasar.fs.mount.ConnectionUri
import quasar.physical.rdbms.Rdbms
import quasar.physical.rdbms.fs._
import quasar.physical.rdbms.jdbc.JdbcConnectionInfo
import quasar.physical.rdbms.planner.sql.RenderQuery
import java.net.URI

import doobie.util.composite.Composite
import doobie.util.meta.Meta
import quasar.physical.rdbms.fs.postgres.mapping.DataComposite

import scalaz.{-\/, NonEmptyList, \/, \/-}
import scalaz.syntax.either._

object Postgres
    extends Rdbms
    with PostgresInsert
    with PostgresDescribeTable
    with PostgresCreate
    with PostgresMove {

  override val Type = FileSystemType("postgres")

  val driverClass = "org.postgresql.Driver"
  val formatHint =
    "jdbc:postgresql://host:port/db_name?user=username(&password=pw)"
  val jdbcPrefixLength = "jdbc".length

  private def parsingErr(uri: ConnectionUri) =
    -\/(NonEmptyList(
      s"Cannot extract credentials from URI [${uri.value}]. Expected format: $formatHint"))

  override def parseConnectionUri(
      uri: ConnectionUri): \/[DefinitionError, JdbcConnectionInfo] = {

    val jUri = new URI(uri.value.substring(jdbcPrefixLength + 1))
    val connectionInfo = (Option(jUri.getScheme),
                          Option(jUri.getAuthority),
                          Option(jUri.getPath),
                          Option(jUri.getQuery)) match {
      case (Some("postgresql"), Some(authority), Some(db), Some(query)) =>
        val url = s"jdbc:postgresql://$authority$db"
        val userPass = query.split("&").flatMap(_.split("=")).toList match {
          case "user" :: u :: "password" :: p :: _ => \/-((u, Some(p)))
          case "user" :: u :: _                    => \/-((u, None))
          case _                                   => parsingErr(uri)
        }
        userPass.map {
          case (u, p) => JdbcConnectionInfo(driverClass, url, u, p)
        }
      case _ => parsingErr(uri)
    }

    connectionInfo.leftMap(_.left[EnvironmentError])
  }

  override lazy val dataMeta: Meta[Data] = postgres.mapping.JsonDataMeta
  override lazy val renderQuery: RenderQuery = PostgresFlatRenderQuery // TODO or JSON
  override lazy val createComposite: Repr => Composite[Data] = new DataComposite(_)
}
