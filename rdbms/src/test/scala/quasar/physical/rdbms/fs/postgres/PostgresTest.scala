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
import quasar.fs.mount.ConnectionUri
import quasar.physical.rdbms.jdbc.JdbcConnectionInfo
import quasar.Qspec

import scalaz.{-\/, \/-, NonEmptyList}

class PostgresTest extends Qspec {

  private def parse(uri: String) =
    Postgres.parseConnectionUri(ConnectionUri(uri))

  private def expectParseResult(
      forConnectionUri: String,
      url: String,
      user: String,
      password: Option[String]) = {
    Postgres.parseConnectionUri(ConnectionUri(forConnectionUri)) must_===
      \/-(JdbcConnectionInfo(Postgres.driverClass, url, user, password))
  }

  private def expectParsingError(forConnectionUri: String) = {
    Postgres.parseConnectionUri(ConnectionUri(forConnectionUri)) must_===
      -\/(
        -\/(NonEmptyList(s"Cannot extract credentials from URI [$forConnectionUri]. " +
          s"Expected format: jdbc:postgresql://host:port/db_name?user=username(&password=pw)")))
  }

  "Postgres" should {

    "parse URI with user name and empty password" in {
      expectParseResult(
        forConnectionUri = "jdbc:postgresql://HOST:123/DATABASE?user=pgadmin",
        url = "jdbc:postgresql://HOST:123/DATABASE",
        user = "pgadmin",
        password = None)
    }

    "parse URI with user name and password" in {
      expectParseResult(
        forConnectionUri =
          "jdbc:postgresql://HOST2:1234/DATABASE_2?user=pgadmin_2&password=secret",
        url = "jdbc:postgresql://HOST2:1234/DATABASE_2",
        user = "pgadmin_2",
        password = Some("secret")
      )
    }

    "parse URI without port" in {
      expectParseResult(
        forConnectionUri = "jdbc:postgresql://HOST3/DATABASE_3?user=john",
        url = "jdbc:postgresql://HOST3/DATABASE_3",
        user = "john",
        password = None)
    }

    "fail on malformed URI" in {
      expectParsingError("jdbc:postgresql://HOST2:1234/DATABASE_2?password=secret")
      expectParsingError("jdbc:postgresql:/HOST:123/DATABASE?user=some_user")
      expectParsingError("jdbc:postgresql:/HOST:123/?user=some_user")
      expectParsingError("jdbc:postgresql:/HOST:123/DATABASE")
    }
  }
}
