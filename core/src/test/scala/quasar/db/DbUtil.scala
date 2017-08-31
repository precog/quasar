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

import doobie.free.connection.ConnectionIO
import doobie.imports.DriverManagerTransactor
import scalaz.~>
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

  /** Interpreter that runs a doobie program outside of any transaction. */
  def noTxInterp(info: ConnectionInfo): ConnectionIO ~> Task = {
    val xa = DriverManagerTransactor[Task](
      info.driverClassName,
      info.url,
      info.userName,
      info.password
    )

    xa.rawTrans
  }

}
