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

package quasar.physical.marklogic.fs

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.physical.marklogic.{DocType, MonadErrMsgs}

import java.net.URI

import pathy.Path.{rootDir => rtDir, _}
import monocle.macros.Lenses
import scalaz._, Scalaz._

@Lenses
final case class MarkLogicConfig(xccUri: URI, rootDir: ADir, docType: DocType) {
  def asUriString: String =
    s"${xccUri}${posixCodec.printPath(rootDir)}?format=${DocType.name(docType)}"
}

object MarkLogicConfig {
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def fromUriString[F[_]: MonadErrMsgs](str: String): F[MarkLogicConfig] = {
    def ensureScheme(u: URI): ValidationNel[String, Unit] =
      Option(u.getScheme).exists(_ === "xcc")
        .unlessM("Missing or unrecognized scheme, expected 'xcc'.".failureNel)

    def ensureHost(u: URI): ValidationNel[String, Unit] =
      Option(u.getHost).isDefined.unlessM("Missing host".failureNel)

    def ensurePort(u: URI): ValidationNel[String, Unit] =
      Option(u.getPort).filter(_ > 0).isDefined
        .unlessM("Missing port".failureNel)

    def validations(u: URI): ValidationNel[String, Unit] =
      ensureScheme(u) *> ensureHost(u) *> ensurePort(u)

    def dbAndRest(u: URI): ValidationNel[String, (String, ADir)] =
      Option(u.getPath).flatMap(posixCodec.parseAbsAsDir).map(unsafeSandboxAbs).flatMap { d =>
        firstSegmentName(d) map (_.bimap(_.value, _.value).merge) map { db =>
          (db, stripPrefixA(rtDir </> dir(db))(d))
        }
      } toSuccessNel "No database specified."

    def xccUriAndRoot(u: URI): ValidationNel[String, (URI, ADir)] =
      validations(u) *> dbAndRest(u) map { case (db, dir) =>
        (new URI(u.getScheme, u.getUserInfo, u.getHost, u.getPort, "/" + db, null, null), dir)
      }

    def docType(u: URI): ValidationNel[String, DocType] =
      Option(u.getQuery).toList
        .flatMap(_ split '&')
        .flatMap(_ split '=' match {
          case Array(n, v) => List((n, v))
          case other       => List()
        })
        .find(_._1 === "format")
        .fold(DocType.xml.successNel[String]) { case (_, fmt) =>
          DocType.name.getOption(fmt)
            .toSuccessNel(s"Unsupported document format: $fmt")
        }

    \/.fromTryCatchNonFatal(new URI(str))
      .leftMap(_.getMessage.wrapNel)
      .flatMap(u => (xccUriAndRoot(u) |@| docType(u))((a, t) => MarkLogicConfig(a._1, a._2, t)).disjunction)
      .fold(_.raiseError[F, MarkLogicConfig], _.point[F])
  }

  implicit val equal: Equal[MarkLogicConfig] = {
    implicit val uriEq: Equal[URI] = Equal.equalA[URI]
    Equal.equalBy(c => (c.xccUri, c.rootDir, c.docType))
  }

  implicit val show: Show[MarkLogicConfig] =
    Show.showFromToString[MarkLogicConfig]
}
