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

package quasar.fs.mount

import quasar.Predef._
import quasar.{Variables, VarName, VarValue}
import quasar.fp.prism._
import quasar.fs.FileSystemType
import quasar.sql, sql.{Expr}

import argonaut._, Argonaut._
import monocle.Prism
import scalaz._, Scalaz._

/** Configuration for a mount, currently either a view or a filesystem. */
sealed trait MountConfig

object MountConfig {
  final case class ViewConfig private[mount] (query: Expr, vars: Variables)
    extends MountConfig

  final case class FileSystemConfig private[mount] (typ: FileSystemType, uri: ConnectionUri)
    extends MountConfig

  val viewConfig: Prism[MountConfig, (Expr, Variables)] =
    Prism[MountConfig, (Expr, Variables)] {
      case ViewConfig(query, vars) => Some((query, vars))
      case _                       => None
    } ((ViewConfig(_, _)).tupled)

  val viewConfigUri: Prism[String, (Expr, Variables)] =
    Prism((viewCfgFromUri _) andThen (_.toOption))((viewCfgAsUri _).tupled)

  val fileSystemConfig: Prism[MountConfig, (FileSystemType, ConnectionUri)] =
    Prism[MountConfig, (FileSystemType, ConnectionUri)] {
      case FileSystemConfig(typ, uri) => Some((typ, uri))
      case _                          => None
    } ((FileSystemConfig(_, _)).tupled)

  implicit val mountConfigShow: Show[MountConfig] =
    Show.shows {
      case ViewConfig(expr, vars) =>
        viewConfigUri.reverseGet((expr, vars))
      case FileSystemConfig(typ, uri) =>
        s"[${typ.value}] ${uri.value}"
    }

/** TODO: Equal[sql.Expr]
  implicit val mountConfigEqual: Equal[MountConfig] =
    Equal.equalBy[MountConfig, Expr \/ (FileSystemType, Json)] {
      case ViewConfig(query)           => query.left
      case FileSystemConfig(typ, json) => (typ, json).right
    }
*/

  val toConfigPair: MountConfig => (String, ConnectionUri) = {
    case ViewConfig(query, vars) =>
      "view" -> ConnectionUri(viewCfgAsUri(query, vars))
    case FileSystemConfig(typ, uri) =>
      typ.value -> uri
  }

  val fromConfigPair: (String, ConnectionUri) => String \/ MountConfig = {
    case ("view", uri) =>
      viewCfgFromUri(uri.value).map(viewConfig(_))
    case (typ, uri) =>
      fileSystemConfig(FileSystemType(typ), uri).right
  }

  implicit val mountConfigCodecJson: CodecJson[MountConfig] =
    CodecJson({ cfg =>
      val (key, uri) = toConfigPair(cfg)
      Json(key := Json("connectionUri" := uri.value))
    },
    json => json.fields match {
      case Some(key :: Nil) =>
        val uriCur = json --\ key --\ "connectionUri"
        uriCur.as[ConnectionUri].flatMap(uri => DecodeResult(
          fromConfigPair(key, uri).leftMap((_, uriCur.history)).toEither))
      case _ =>
        DecodeResult.fail(s"invalid config: ${json.focus}", json.history)
    })

  ////

  private val VarPrefix = "var."

  private def viewCfgFromUri(uri: String): String \/ (Expr, Variables) = {
    import org.http4s._, util._, CaseInsensitiveString._

    for {
      parsed   <- Uri.fromString(uri).leftMap(_.sanitized)
      scheme   <- parsed.scheme \/> s"missing URI scheme: $parsed"
      _        <- (scheme == "sql2".ci) either (()) or s"unrecognized scheme: $scheme"
      queryStr <- parsed.params.get("q") \/> s"missing query: $uri"
      query    <- sql.parse(sql.Query(queryStr)).leftMap(_.message)
      vars     =  Variables(parsed.multiParams collect {
                    case (n, vs) if n.startsWith(VarPrefix) => (
                      VarName(n.substring(VarPrefix.length)),
                      VarValue(vs.lastOption.getOrElse(""))
                    )
                  })
    } yield (query, vars)
  }

  private def viewCfgAsUri(query: Expr, vars: Variables): String = {
    import org.http4s._, util._, CaseInsensitiveString._

    val qryMap = vars.value.foldLeft(Map("q" -> List(sql.pprint(query)))) {
      case (qm, (n, v)) => qm + ((VarPrefix + n.value, List(v.value)))
    }

    /** NB: host and path are specified here just to force the URI to have
      * all three slashes, as the documentation shows it. The current parser
      * will accept any number of slashes, actually, since we're ignoring
      * the host and path for now.
      */
    Uri(
      scheme    = Some("sql2".ci),
      authority = Some(Uri.Authority(host = Uri.RegName("".ci))),
      path      = "/",
      query     = Query.fromMap(qryMap)
    ).renderString
  }
}
