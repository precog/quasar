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

package quasar.db

import slamdata.Predef._

import argonaut._
import doobie.imports._
import scalaz.{:+: => _, _}, Scalaz._
import scalaz.concurrent.Task

trait PostgresTxFixture {
  // TODO: Interim
  def postgresConfigStr: OptionT[Task, String]

  /** Connects to a freshly-created PostgreSQL test DB.
    */
  def postgreSqlTransactor(dbName: String): OptionT[Task, Transactor[Task]] = {
    def recreateDb(dbName: String): ConnectionIO[Unit] =
      Update0(s"DROP DATABASE IF EXISTS $dbName", None).run.void *>
        Update0(s"CREATE DATABASE $dbName", None).run.void

    postgresConfigStr.flatMapF(cfgStr =>
      for {
        cfgJson <- Parse.parse(cfgStr).fold(
                    err => Task.fail(new RuntimeException(err)),
                    json => Task.now(Json("postgresql" -> json)))
        mainCfg <- cfgJson.as[DbConnectionConfig].result.fold(
                    err => Task.fail(new RuntimeException(err.toString)),
                    Task.now)

        // Reset the test DB:
        interp  =  DbUtil.noTxInterp(DbConnectionConfig.connectionInfo(mainCfg))
        _       <- interp(recreateDb(dbName))

        // Connect to the fresh test DB:
        testCfg =  mainCfg.asInstanceOf[DbConnectionConfig.PostgreSql].copy(database = dbName.some)
      } yield simpleTransactor(DbConnectionConfig.connectionInfo(testCfg)))
  }
}
