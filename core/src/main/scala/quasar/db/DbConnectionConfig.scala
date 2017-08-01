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
import quasar.config._

import argonaut._, Argonaut._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

sealed trait DbConnectionConfig {
  def isInMemory: Boolean
}

object DbConnectionConfig {

  final case class H2(url: String) extends DbConnectionConfig {
    def isInMemory: Boolean = url.startsWith("mem:")
  }

  final case class HostInfo(name: String, port: Option[Int])

  final case class PostgreSql(
    host: Option[HostInfo],
    database: Option[String],
    userName: String,
    password: String,
    parameters: Map[String, String]) extends DbConnectionConfig {
    def isInMemory: Boolean = false
  }

  implicit val encodeJson: EncodeJson[DbConnectionConfig] =
    EncodeJson {
      case H2(file) => Json("h2" -> Json("location" := file))
      case PostgreSql(host, database, userName, password, parameters) =>
        Json("postgresql" -> (
          ("host"       :=? host.map(_.name)) ->?:
          ("port"       :=? host.flatMap(_.port)) ->?:
          ("database"   :=? database) ->?:
          ("userName"   :=  userName) ->:
          ("password"   :=  password) ->:
          ("parameters" :=? parameters.nonEmpty.option(parameters)) ->?:
            jEmptyObject))
    }

  implicit val decodeJson: DecodeJson[DbConnectionConfig] = {
    final case class ParamValue(value: String)
    implicit val decodeParamValue: DecodeJson[ParamValue] =
      DecodeJson(cur =>
        (cur.as[String] |||
          cur.as[Int].map(_.toString) |||
          cur.as[Boolean].map(_.toString)).map(ParamValue(_)))

    DecodeJson(cur =>
      cur.fields match {
        case Some("h2" :: Nil) =>
          // "file" field is for backwards compatibility for older config files
          ((cur --\ "h2" --\ "location").as[String] ||| (cur --\ "h2" --\ "file").as[String]).map(H2(_))
        case Some("postgresql" :: Nil) =>
          val pg = cur --\ "postgresql"
          for {
            host     <- (pg --\ "host").as[Option[String]]
            port     <- (pg --\ "port").as[Option[Int]]
            _ <- if (host.empty && port.nonEmpty)
                  DecodeResult.fail("host required when port specified", cur.history)
                 else DecodeResult.ok(())
            database <- (pg --\ "database").as[Option[String]]
            userName <- (pg --\ "userName").as[String]
            password <- (pg --\ "password").as[String]
            params   <- (pg --\ "parameters").as[Option[Map[String, ParamValue]]]
          } yield PostgreSql(
            host.map(HostInfo(_, port)),
            database,
            userName,
            password,
            params.cata(_.mapValues(_.value), ListMap.empty))
        case Some(fields) =>
          DecodeResult.fail(s"""unrecognized metastore type: ${fields.mkString(", ")}; expected 'h2' or 'postgresql'""", cur.history)
        case None =>
          DecodeResult.fail(s"""expected metastore""", cur.history)
      })
  }

  def connectionInfo(config: DbConnectionConfig): ConnectionInfo = config match {
    case H2(url) =>
      ConnectionInfo(
        "org.h2.Driver",
        "jdbc:h2:" + url,
        "sa",
        "")
    case cfg @ PostgreSql(_, _, _, _, _) =>
      ConnectionInfo(
        "org.postgresql.Driver",
        "jdbc:postgresql:"              ⊹
          cfg.host.cata(
            {
              case HostInfo(name, Some(port)) => s"//$name:$port/"
              case HostInfo(name, None)       => s"//$name/"
            },
            "")                         ⊹
          cfg.database.getOrElse("/")   ⊹
          (if (cfg.parameters.nonEmpty)
            "?" + cfg.parameters.map {
              case (k, v) => k + "=" + v  // TODO: URL-encode keys and values
            }.mkString("&")
          else ""),
        cfg.userName,
        cfg.password)
  }

  val defaultConnectionConfig: Task[DbConnectionConfig] =
    for {
      os <- OS.currentOS
      p  <- ConfigOps.defaultPathForOS(dir("quasar") </> file("quasar-metastore.db"))(os)
    } yield DbConnectionConfig.H2(FsPath.printFsPath(FsPath.codecForOS(os), p))

}
