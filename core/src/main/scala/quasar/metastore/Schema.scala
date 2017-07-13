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

package quasar.metastore

import slamdata.Predef._
import quasar.db

import scala.collection.immutable.SortedMap

import doobie.imports._
import scalaz._, Scalaz._

object Schema {
  val schema = db.Schema[Int](
    MetaStoreAccess.tableExists("quasar_properties").flatMap {
      case true  => sql"SELECT schema_version FROM quasar_properties".query[Int].unique.map(_.some)
      case false => none[Int].point[ConnectionIO]
    },
    ver => sql"UPDATE quasar_properties SET schema_version = $ver".update.run.void,
    SortedMap(
      0 ->
        sql"""DROP TABLE IF EXISTS properties""".update.run.void                           *>
        sql"""CREATE TABLE IF NOT EXISTS quasar_properties (
              schema_version INT NOT NULL
              )""".update.run.void                                                         *>
        sql"""INSERT INTO quasar_properties (schema_version) VALUES (0)""".update.run.void *>
        sql"""CREATE TABLE IF NOT EXISTS mounts (
              path          VARCHAR  NOT NULL  PRIMARY KEY,
              type          VARCHAR  NOT NULL,
              connectionUri VARCHAR  NOT NULL
              )""".update.run.void                                                         *>
        sql"""CREATE INDEX IF NOT EXISTS mounts_type ON mounts (type)""".update.run.void

        /*
        Important! Once a new schema version is added to this sequence and
        released, the particular update should never be changed. Instead, when
        a schema change is needed, a new update should be added that modifies
        the schema created in earlier versions, and migrates data that is
        already present in the DB.

        Remember, these updates may be applied to a DB in _any_ previously-
        deployed version, and they always need to produce a DB having the final
        expected schema.

        Exceptions should be considered if, for example:
        - You are _certain_ that a particular schema version has never been
        deployed to a "production" system. This probably applies only to
        changing the latest update during development.
        - The change is merely syntactic, and doesn't affect the behavior of
        any query _or_ any subsequent update.
        */
      ))
}
