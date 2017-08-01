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
import quasar.contrib.pathy.AFile

import scala.{Either, Left, Right}

import argonaut._, Argonaut._
import org.specs2.mutable
import org.specs2.ScalaCheck
import pathy.Path._
import pathy.argonaut.PosixCodecJson._
import pathy.scalacheck.PathyArbitrary._

class DbConnectionConfigSpec extends mutable.Specification with ScalaCheck with DbConnectionConfigArbitrary {
  import DbConnectionConfig._

  def decode(json: Json): Either[String, ConnectionInfo] =
    json.as[DbConnectionConfig].fold(
      { case (msg, _) => Left(msg) },
      cfg => Right(connectionInfo(cfg)))


  "H2" should {
    "decode" >> prop { (f: AFile) =>
      val cfg =
        Json("h2" ->
          Json("location" := f))
      decode(cfg) must beRight.which { info =>
          (info.driverClassName must_== "org.h2.Driver") and
            (info.url must_== "jdbc:h2:" + posixCodec.printPath(f))
        }
    }
    "decode old format" >> prop { (f: AFile) =>
      val cfg =
        Json("h2" ->
          Json("file" := f))
      decode(cfg) must beRight.which { info =>
        (info.driverClassName must_== "org.h2.Driver") and
          (info.url must_== "jdbc:h2:" + posixCodec.printPath(f))
      }
    }
  }

  "PostgreSQL" should {
    "decode minimal" in {
      val cfg =
        Json("postgresql" ->
          Json(
            "userName" := "sa",
            "password" := "pw"))
      decode(cfg) must beRight.which { info =>
        (info.driverClassName must_== "org.postgresql.Driver") and
          (info.url must_== "jdbc:postgresql:/") and
          (info.userName must_== "sa") and
          (info.password must_== "pw")
      }
    }

    "decode all" in {
      val cfg =
        Json("postgresql" ->
          Json(
            "host" := "localhost",
            "port" := 5432,
            "database" := "quasarmeta",
            "userName" := "sa",
            "password" := "pw",
            "parameters" -> Json(
              "ssl" := "true",
              "loglevel" := 1)))
      decode(cfg) must beRight.which { info =>
        (info.url must_== "jdbc:postgresql://localhost:5432/quasarmeta?ssl=true&loglevel=1") and
          (info.userName must_== "sa") and
          (info.password must_== "pw")
      }
    }

    "require host if port is specified" in {
      val cfg =
        Json("postgresql" ->
          Json(
            "port" := 5432,
            "userName" := "sa",
            "password" := "pw"))
      decode(cfg) must beLeft("host required when port specified")
    }
  }

  "lawful json codec" >> prop { (cfg: DbConnectionConfig) =>
    CodecJson.codecLaw(CodecJson.derived[DbConnectionConfig])(cfg)
  }
}
