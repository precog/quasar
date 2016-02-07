/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.config

import quasar.Predef._
import quasar._, Evaluator._
import quasar.Planner.CompilationError.CSemanticError
import quasar.fp._
import quasar.fs.{Path => QPath}

import java.io.{File => JFile}

import argonaut._, Argonaut._
import com.mongodb.ConnectionString
import scalaz.{Lens => _, _}, Scalaz._
import scalaz.concurrent.Task

sealed trait MountConfig {
  def validate(path: QPath): EnvironmentError \/ Unit
}
final case class MongoDbConfig(uri: ConnectionString) extends MountConfig {
  def validate(path: QPath) =
    if (path.relative) -\/(InvalidConfig("Not an absolute path: " + path))
    else if (!path.pureDir) -\/(InvalidConfig("Not a directory path: " + path))
    else \/-(())
}
object MongoConnectionString {
  def parse(uri: String): String \/ ConnectionString =
    \/.fromTryCatchNonFatal(new ConnectionString(uri)).leftMap(_.toString)

  def decode(uri: String): DecodeResult[ConnectionString] = {
    DecodeResult(parse(uri).leftMap(κ((s"invalid connection URI: $uri", CursorHistory(Nil)))))
  }

  implicit val connectionStringCodecJson: CodecJson[ConnectionString] =
    CodecJson[ConnectionString](
      c => jString(c.getConnectionString),
      _.as[String].flatMap(decode))
}
object MongoDbConfig {
  import MongoConnectionString._
  implicit def Codec: CodecJson[MongoDbConfig] =
    casecodec1(MongoDbConfig.apply, MongoDbConfig.unapply)("connectionUri")
}

final case class ViewConfig(query: sql.Expr, variables: Variables) extends MountConfig {
  def validate(path: QPath) = for {
    _ <- if (path.relative) -\/(InvalidConfig("Not an absolute path: " + path))
          else if (path.pureDir) -\/(InvalidConfig("Not a file path: " + path))
          else \/-(())
    _ <- Variables.substVars(query, variables).leftMap(e => EnvCompError(CSemanticError(e)))
  } yield ()
}
object ViewConfig {
  private val VarPrefix = "var."

  private def fromUri(uri: String): String \/ ViewConfig = {
    import org.http4s._, util._

    for {
      parsed <- Uri.fromString(uri).leftMap(_.sanitized)
      _      <- parsed.scheme.fold[String \/ Unit](-\/("missing URI scheme: " + parsed))(
                  scheme => if (scheme == CaseInsensitiveString("sql2")) \/-(()) else -\/("unrecognized scheme: " + scheme))
      queryStr <- parsed.params.get("q") \/> ("missing query: " + uri)
      query <- new sql.SQLParser().parse(sql.Query(queryStr)).leftMap(_.message)
      vars = Variables(parsed.multiParams.collect {
              case (n, vs) if n.startsWith(VarPrefix) =>
                VarName(n.substring(VarPrefix.length)) -> VarValue(vs.lastOption.getOrElse(""))
            })
    } yield ViewConfig(query, vars)
  }

  private def toUri(cfg: ViewConfig): String = {
    import org.http4s._, util._

    // NB: host and path are specified here just to force the URI to have
    // all three slashes, as the documentation shows it. The current parser
    // will accept any number of slashes, actually, since we're ignoring
    // the host and path for now.
    Uri(
      scheme = Some(CaseInsensitiveString("sql2")),
      authority = Some(Uri.Authority(host = Uri.RegName(CaseInsensitiveString("")))),
      path = "/",
      query = Query.fromMap(
        Map("q" -> List(sql.pprint(cfg.query))) ++
          cfg.variables.value.map { case (n, v) => (VarPrefix + n.value) -> List(v.value) })).renderString
  }

  implicit def Codec = CodecJson[ViewConfig](
    cfg => Json("connectionUri" := toUri(cfg)),
    c => {
      val uriC = (c --\ "connectionUri")
      for {
        uri <- uriC.as[String]
        cfg <- DecodeResult(fromUri(uri).leftMap(e => (e.toString, uriC.history)))
      } yield cfg
    })

  private def nonEmpty(strOrNull: String) =
    if (strOrNull == "") None else Option(strOrNull)
}

object MountConfig {
  implicit val Codec = CodecJson[MountConfig](
    encoder = _ match {
      case x @ MongoDbConfig(_) => ("mongodb", MongoDbConfig.Codec.encode(x)) ->: jEmptyObject
      case x @ ViewConfig(_, _) => ("view", ViewConfig.Codec.encode(x)) ->: jEmptyObject
    },
    decoder = c => (c.fields match {
      case Some("mongodb" :: Nil) => c.get[MongoDbConfig]("mongodb")
      case Some("view" :: Nil)    => c.get[ViewConfig]("view")
      case Some(t :: Nil)         => DecodeResult.fail("unrecognized mount type: " + t, c.history)
      case _                      => DecodeResult.fail("invalid mount: " + c.focus, c.history)
    }).map(v => v: MountConfig))
}
