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

package quasar.physical.sparkcore.fs.cassandra

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.fp.ski._
import quasar.{Data, DataCodec}

import pathy.Path._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import org.apache.spark.SparkContext
import org.apache.spark.rdd._

sealed trait CassandraDDL[A]
final case class KeyspaceExists(keyspace: String) extends CassandraDDL[Boolean]
final case class DropKeyspace(keyspace: String) extends CassandraDDL[Unit]
final case class CreateKeyspace(keyspace: String) extends CassandraDDL[Unit]

final case class TableExists(keyspace: String, table: String) extends CassandraDDL[Boolean]
final case class DropTable(keyspace: String, table: String) extends CassandraDDL[Unit]
final case class CreateTable(keyspace: String, table: String) extends CassandraDDL[Unit]

final case class MoveTable(fromKs: String, fromTable: String, toKs: String, toTable: String) extends CassandraDDL[Unit]
final case class ListTables(keyspace: String) extends CassandraDDL[Set[String]]
final case class ListKeyspaces(startWith: String) extends CassandraDDL[Set[String]]

final case class ReadTable(keyspace: String, table: String) extends CassandraDDL[RDD[Data]]
final case class InsertData(keyspace: String, table: String, data: String) extends CassandraDDL[Unit]

object CassandraDDL {


  class Ops[S[_]](implicit s0: CassandraDDL :<: S) {
    def keyspaceExists(keyspace: String): Free[S, Boolean] = Free.liftF(s0.inj(KeyspaceExists(keyspace)))
    def tableExists(keyspace: String, table: String): Free[S, Boolean] = Free.liftF(s0.inj(TableExists(keyspace, table)))
    def dropKeyspace(keyspace: String): Free[S, Unit] = Free.liftF(s0.inj(DropKeyspace(keyspace)))
    def dropTable(keyspace: String, table: String): Free[S, Unit] = Free.liftF(s0.inj(DropTable(keyspace, table)))
    def createTable(keyspace: String, table: String): Free[S, Unit] = Free.liftF(s0.inj(CreateTable(keyspace, table)))
    def createKeyspace(keyspace: String): Free[S, Unit] = Free.liftF(s0.inj(CreateKeyspace(keyspace)))
    def moveTable(fromK: String, fromT: String, toK: String, toT: String): Free[S, Unit] = Free.liftF(s0.inj(MoveTable(fromK, fromT, toK, toT)))
    def listTables(keyspace: String): Free[S, Set[String]] = Free.liftF(s0.inj(ListTables(keyspace)))
    def listKeyspaces(startWith: String): Free[S, Set[String]] = Free.liftF(s0.inj(ListKeyspaces(startWith)))
    def readTable(keyspace: String, table: String): Free[S, RDD[Data]] = Free.liftF(s0.inj(ReadTable(keyspace, table)))
    def insertData(keyspace: String, table: String, data: String): Free[S, Unit] = Free.liftF(s0.inj(InsertData(keyspace, table, data)))
  }

  object Ops {

    implicit def apply[S[_]](implicit s0: CassandraDDL :<: S): Ops[S] = new Ops[S]

  }

  def interpreter[S[_]](implicit sc: SparkContext) = new (CassandraDDL ~> Task) {
    def apply[A](from: CassandraDDL[A]) =
      from match {
        case KeyspaceExists(keyspace) =>
          keyspaceExists(keyspace)
        case TableExists(keyspace, table) =>
          tableExists(keyspace, table)
        case DropKeyspace(keyspace) =>
          dropKeyspace(keyspace)
        case DropTable(keyspace, table) =>
          dropTable(keyspace, table)
        case CreateTable(keyspace, table) =>
          createTable(keyspace, table)
        case CreateKeyspace(keyspace) =>
          createKeyspace(keyspace)
        case MoveTable(fromK, fromT, toK, toT) =>
          moveTable(fromK, fromT, toK, toT)
        case ListTables(keyspace) =>
          listTables(keyspace)
        case ListKeyspaces(nameStartWith) =>
          listKeyspaces(nameStartWith)
        case ReadTable(keyspace, table) =>
          readTable(keyspace, table)
        case InsertData(keyspace, table, data) =>
          insertData(keyspace, table, data)
      }
  }

  def keyspaceExists[S[_]](keyspace: String)(implicit sc: SparkContext) = Task.delay {
    if (keyspace.length > 0) {
      CassandraConnector(sc.getConf).withSessionDo { implicit session =>
        val stmt = session.prepare("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?;")
        session.execute(stmt.bind(keyspace)).all().size() > 0
      }
    } else false
  }

  def tableExists[S[_]](keyspace: String, table: String)(implicit sc: SparkContext) = Task.delay {
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      val stmt = session.prepare("SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?;")
      session.execute(stmt.bind(keyspace, table)).all().size() > 0
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def dropKeyspace[S[_]](keyspace: String)(implicit sc: SparkContext) = Task.delay {
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      session.execute(s"DROP KEYSPACE IF EXISTS $keyspace;")
      ()
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def dropTable[S[_]](keyspace: String, table: String)(implicit sc: SparkContext) = Task.delay {
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      session.execute(s"DROP TABLE $keyspace.$table;")
      ()
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def createTable[S[_]](keyspace: String, table: String)(implicit sc: SparkContext) = Task.delay {
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      session.execute(s"CREATE TABLE $keyspace.$table (id timeuuid PRIMARY KEY, data text);")
      ()
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def createKeyspace[S[_]](keyspace: String)(implicit sc: SparkContext) = Task.delay {
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      session.execute(s"CREATE KEYSPACE $keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
      ()
    }
  }

  def moveTable[S[_]](fromK: String, fromT: String, toK: String, toT: String)(implicit sc: SparkContext) = Task.delay {
    val rdd = sc.cassandraTable(fromK, fromT)
    rdd.saveAsCassandraTableEx(rdd.tableDef.copy(keyspaceName = toK, tableName = toT))
  }

  def listTables[S[_]](keyspace: String)(implicit sc: SparkContext) = Task.delay {
    sc.cassandraTable[String]("system_schema", "tables")
      .select("table_name")
      .where("keyspace_name = ?", keyspace)
      .collect.toSet
  }

  def listKeyspaces[S[_]](nameStartWith: String)(implicit sc: SparkContext) = Task.delay {
    sc.cassandraTable[String]("system_schema","keyspaces")
      .select("keyspace_name")
      .filter(_.startsWith(nameStartWith))
      .collect.toSet
  }

  def readTable[S[_]](keyspace: String, table: String)(implicit sc: SparkContext) = Task.delay {
    sc.cassandraTable[String](keyspace, table)
      .select("data")
      .map { raw =>
        DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι)
      }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def insertData[S[_]](keyspace: String, table: String, data: String)(implicit sc: SparkContext) = Task.delay {
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      val stmt = session.prepare(s"INSERT INTO $keyspace.$table (id, data) VALUES (now(),  ?);")
      session.execute(stmt.bind(data))
      ()
    }
  }

}

object common {

  def keyspace(dir: ADir) =
    posixCodec.printPath(dir).substring(1).replace("/", "_").toLowerCase

  def tableName(file: AFile) =
    posixCodec.printPath(file).split("/").reverse(0).toLowerCase

}
