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

package quasar.fs.mount

import slamdata.Predef._
import quasar.{Variables, VarName, VarValue}
import quasar.fs.FileSystemType
import quasar.sql, sql._

import argonaut._, Argonaut._
import matryoshka.data.Fix
import monocle.Prism
import org.http4s.Uri
import scalaz._, Scalaz._

/** Configuration for a mount, currently either a view or a filesystem. */
sealed abstract class MountConfig

object MountConfig {
  final case class ModuleConfig private[mount] (statements: List[Statement[Fix[Sql]]])
    extends MountConfig
  {
    def declarations: List[FunctionDecl[Fix[Sql]]] =
      statements.decls
    def imports: List[Import[Fix[Sql]]] =
      statements.imports
  }

  final case class ViewConfig private[mount] (query: ScopedExpr[Fix[Sql]], vars: Variables)
    extends MountConfig

  final case class FileSystemConfig private[mount] (typ: FileSystemType, uri: ConnectionUri)
    extends MountConfig

  val moduleConfig = Prism.partial[MountConfig, List[Statement[Fix[Sql]]]] {
    case ModuleConfig(statements) => statements
  } (ModuleConfig)

  def viewConfig0(scopedExpr: ScopedExpr[Fix[Sql]], vars: (String, String)*): MountConfig = {
    val vars0 = Variables(Map(vars.map { case (n, v) => quasar.VarName(n) -> quasar.VarValue(v) }: _*))
    viewConfig.apply(scopedExpr, vars0)
  }

  val viewConfig = Prism.partial[MountConfig, (ScopedExpr[Fix[Sql]], Variables)] {
    case ViewConfig(query, vars) => (query, vars)
  } ((ViewConfig(_, _)).tupled)

  val viewConfigUri: Prism[String, (ScopedExpr[Fix[Sql]], Variables)] =
    Prism((viewCfgFromUriStr _) andThen (_.toOption))((viewCfgAsUriStr _).tupled)

  val fileSystemConfig =
    Prism.partial[MountConfig, (FileSystemType, ConnectionUri)] {
      case FileSystemConfig(typ, uri) => (typ, uri)
    } ((FileSystemConfig(_, _)).tupled)

  implicit val show: Show[MountConfig] =
    Show.shows {
      case ModuleConfig(statements) =>
        "Module Config"  // TODO: Perhaps make this more descriptive
      case ViewConfig(expr, vars) =>
        viewConfigUri.reverseGet((expr, vars))
      case FileSystemConfig(typ, uri) =>
        s"[${typ.value}] ${uri.value}"
    }

  implicit def equal: Equal[MountConfig] =
    Equal.equalBy(m => (viewConfig.getOption(m), fileSystemConfig.getOption(m), moduleConfig.getOption(m)))

  implicit val mountConfigCodecJson: CodecJson[MountConfig] =
    CodecJson({
      case ModuleConfig(statements)   =>
        Json("module" := statements.pprint)
      case ViewConfig(query, vars)    =>
        Json("view" := Json("connectionUri" := viewCfgAsUriStr(query, vars)))
      case FileSystemConfig(typ, uri) =>
        Json(typ.value := Json("connectionUri" := uri.value))
    },
    json => json.fields match {
      case Some(key :: Nil) => key match {
        case "module" =>
          (json --\ key).as[String].flatMap(stmtsStr =>
            DecodeResult(stmtsStrAsSql(stmtsStr).bimap(e => (e.message, (json --\ key).history), moduleConfig(_)).toEither))
        case "view" =>
          (json --\ key --\ "connectionUri").as[ConnectionUri].flatMap(uri =>
            DecodeResult(viewCfgFromUriStr(uri.value).bimap((_, (json --\ key).history), viewConfig(_)).toEither))
        case fsType =>
          (json --\ key --\ "connectionUri").as[ConnectionUri].map(uri =>
            fileSystemConfig(FileSystemType(fsType), uri))
      }
      case _ =>
        DecodeResult.fail(s"invalid mount config, expected only one field describing mount type, but found: ${json.focus}", json.history)
    })

  def viewCfgFromUri(uri: ConnectionUri): String \/ (ScopedExpr[Fix[Sql]], Variables) = viewCfgFromUriStr(uri.value)

  def viewCfgAsUri(query: ScopedExpr[Fix[Sql]], vars: Variables): ConnectionUri = ConnectionUri(viewCfgAsUriStr(query, vars))

  def stmtsAsSqlUri(stmts: List[Statement[Fix[Sql]]]): ConnectionUri = ConnectionUri(stmtsAsSqlStr(stmts))

  ////

  private val VarPrefix = "var."

  // FIXME
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def viewCfgFromUriStr(uri: String): String \/ (ScopedExpr[Fix[Sql]], Variables) = {
    import org.http4s.{parser => _, _}, util._, CaseInsensitiveString._

    for {
      parsed   <- Uri.fromString(uri).leftMap(_.sanitized)
      scheme   <- parsed.scheme \/> s"missing URI scheme: $parsed"
      _        <- (scheme == "sql2".ci) either (()) or s"unrecognized scheme: $scheme"
      queryStr <- parsed.params.get("q") \/> s"missing query: $uri"
      sExpr    <- sql.fixParser.parseScopedExpr(queryStr).leftMap(_.message)
      vars     =  Variables(parsed.multiParams collect {
                    case (n, vs) if n.startsWith(VarPrefix) => (
                      VarName(n.substring(VarPrefix.length)),
                      VarValue(vs.lastOption.getOrElse(""))
                    )
                  })
    } yield (sExpr, vars)
  }

  private def viewCfgAsUriStr(scopedExpr: ScopedExpr[Fix[Sql]], vars: Variables): String = {
    import org.http4s._, util._, CaseInsensitiveString._

    val qryMap = vars.value.foldLeft(Map("q" -> List(scopedExpr.pprint))) {
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

  private def stmtsAsSqlStr(stmts: List[Statement[Fix[Sql]]]) =
    stmts.map(st => st.map(sql.pprint[Fix[Sql]]).pprint).mkString(";\n")

  private def stmtsStrAsSql(stmts: String): sql.ParsingError \/ List[Statement[Fix[Sql]]] =
    sql.fixParser.parseModule(stmts)
}
