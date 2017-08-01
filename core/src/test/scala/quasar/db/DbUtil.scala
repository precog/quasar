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

import doobie.imports._
import scalaz.{:+: => _, _}, Scalaz._
import scalaz.concurrent.Task

object DbUtil {
  /** Configuration for an in-memory DB that persists only as long
    * as the process is running. The same db can be accessed by connecting
    * multiple times with the same name.
    * @param name Should not contain `;`.
    */
  def inMemoryConfig(name: String): DbConnectionConfig =
    // LOCK_TIMEOUT: Some tests were hitting the default time limit
    //               of 1 second, 10 seconds seems like a reasonable
    //               value even if this is eventually used for an
    //               ephemeral production database.
    DbConnectionConfig.H2(s"mem:$name;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000")

  /** Transactor that does not use a connection pool, so doesn't require any cleanup. */
  def simpleTransactor(cxn: ConnectionInfo): Transactor[Task] =
    DriverManagerTransactor[Task](
      cxn.driverClassName,
      cxn.url,
      cxn.userName,
      cxn.password)

  /** Interpreter that runs a doobie program outside of any transaction. */
  def noTxInterp(info: ConnectionInfo): ConnectionIO ~> Task = {
    // NB: When not using one of the provided Transactors, we have to make sure
    // the JDBC driver is loaded. Believe it or not, this is the standard way
    // to load a driver for JDBC.
    val loadDriver = HDM.delay(java.lang.Class.forName(info.driverClassName))

    def interp[A](fa: ConnectionIO[A]) =
      HDM.getConnection(info.url, info.userName, info.password)(fa)

    λ[ConnectionIO ~> Task] { fa =>
      (loadDriver *> interp(fa)).trans[Task]
    }
  }
}
