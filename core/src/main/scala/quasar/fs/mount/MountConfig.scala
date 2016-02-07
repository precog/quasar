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
import quasar.sql, sql.{Expr, SQLParser}

import argonaut._, Argonaut._
import monocle.Prism
import scalaz._
import scalaz.syntax.std.option._
import scalaz.syntax.std.boolean._

/** Configuration for a mount, currently either a view or a filesystem. */
sealed trait MountConfig2

object MountConfig2 {
  final case class ViewConfig private[mount] (query: Expr, vars: Variables)
    extends MountConfig2

  final case class FileSystemConfig private[mount] (typ: FileSystemType, uri: ConnectionUri)
    extends MountConfig2

  val viewConfig: Prism[MountConfig2, (Expr, Variables)] =
    Prism[MountConfig2, (Expr, Variables)] {
      case ViewConfig(query, vars) => Some((query, vars))
      case _                       => None
    } ((ViewConfig(_, _)).tupled)

  val viewConfigUri: Prism[String, (Expr, Variables)] =
    Prism((viewCfgFromUri _) andThen (_.toOption))((viewCfgAsUri _).tupled)

  val fileSystemConfig: Prism[MountConfig2, (FileSystemType, ConnectionUri)] =
    Prism[MountConfig2, (FileSystemType, ConnectionUri)] {
      case FileSystemConfig(typ, uri) => Some((typ, uri))
      case _                          => None
    } ((FileSystemConfig(_, _)).tupled)

  implicit val mountConfigShow: Show[MountConfig2] =
    Show.shows {
      case ViewConfig(expr, vars) =>
        viewConfigUri.reverseGet((expr, vars))
      case FileSystemConfig(typ, uri) =>
        s"[${typ.value}] ${uri.value}"
    }

/** TODO: Equal[sql.Expr]
  implicit val mountConfigEqual: Equal[MountConfig2] =
    Equal.equalBy[MountConfig2, Expr \/ (FileSystemType, Json)] {
      case ViewConfig(query)           => query.left
      case FileSystemConfig(typ, json) => (typ, json).right
    }
*/

  implicit val mountConfigCodecJson: CodecJson[MountConfig2] =
    CodecJson({
      case ViewConfig(query, vars) =>
        Json("view" := Json("connectionUri" := viewCfgAsUri(query, vars)))

      case FileSystemConfig(typ, uri) =>
        Json(typ.value := Json("connectionUri" := uri))
    }, c => c.fields match {
      case Some("view" :: Nil) =>
        val uriCur = (c --\ "view" --\ "connectionUri")
        uriCur.as[String].flatMap(uri => DecodeResult(
          viewCfgFromUri(uri).bimap(
            e => (e.toString, uriCur.history),
            viewConfig(_))
        ))

      case Some(t :: Nil) =>
        (c --\ t --\ "connectionUri").as[ConnectionUri]
          .map(fileSystemConfig(FileSystemType(t), _))

      case _ =>
        DecodeResult.fail(s"invalid config: ${c.focus}", c.history)
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
      query    <- new SQLParser().parse(sql.Query(queryStr)).leftMap(_.message)
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

    // TODO: Workaround for https://github.com/http4s/http4s/issues/510
    val urlEncode: String => String =
      UrlCodingUtils.urlEncode(_, spaceIsPlus = false, toSkip = UrlFormCodec.urlUnreserved)

    val qryMap = vars.value.foldLeft(Map("q" -> List(sql.pprint(query)))) {
      case (qm, (n, v)) => qm + ((urlEncode(VarPrefix + n.value), List(v.value)))
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
