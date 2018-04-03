/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.api

import slamdata.Predef._

import org.http4s.{Header, HttpService, Response}
import org.http4s.headers.`Content-Disposition`
import org.http4s.server._
import scalaz.syntax.equal._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

// [#1861] Workaround to support a small slice of RFC 5987 for this isolated case
object RFC5987ContentDispositionRender extends HttpMiddleware {
  // NonUnitStatements due to http4s's Writer
  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  final case class ContentDisposition(dispositionType: String, parameters: Map[String, String])
    extends Header.Parsed {
    import org.http4s.util.Writer
    override def key = `Content-Disposition`
    override lazy val value = super.value
    override def renderValue(writer: Writer): writer.type = {
      writer.append(dispositionType)
      parameters.foreach(p =>
        p._1.endsWith("*").fold(
          writer << "; " << p._1 << "=" << p._2,
          writer << "; " << p._1 << "=\"" << p._2 << '"'))
      writer
    }
  }

  def apply(service: HttpService): HttpService =
    service.map {
      case resp: Response =>
        resp.headers.get(`Content-Disposition`).cata(
          i => resp.copy(headers = resp.headers
            .filter(_.name ≠ `Content-Disposition`.name)
            .put(ContentDisposition(i.dispositionType, i.parameters))),
          resp)
      case pass => pass
    }
}
