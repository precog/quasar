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

import argonaut.{DecodeResult => _, _}, Argonaut._
import org.http4s.{Header, Headers, HttpService, Request, Service}
import org.http4s.argonaut._
import org.http4s.dsl._
import org.http4s.server._
import org.http4s.util.CaseInsensitiveString
import scalaz.{\/, -\/, \/-}
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.bifunctor._
import scalaz.syntax.traverse._
import scalaz.syntax.std.either._
import scalaz.syntax.std.option._

object HeaderParam extends HttpMiddleware {
  type HeaderValues = Map[CaseInsensitiveString, List[String]]

  def parse(param: String): String \/ HeaderValues = {
    def strings(json: Json): String \/ List[String] =
      json.string.map(str => \/-(str :: Nil)).getOrElse(
        json.array.map { vs =>
          vs.traverse(v => v.string \/> (s"expected string in array; found: $v"))
        }.getOrElse(-\/(s"expected a string or array of strings; found: $json")))

    for {
      json <- Parse.parse(param).leftMap("parse error (" + _ + ")").disjunction
      obj <- json.obj \/> (s"expected a JSON object; found: $json")
      values <- obj.toList.traverse { case (k, v) =>
        strings(v).map(CaseInsensitiveString(k) -> _)
      }
    } yield Map(values: _*)
  }

  def rewrite(headers: Headers, param: HeaderValues): Headers =
    Headers(
      param.toList.flatMap {
        case (k, vs) => vs.map(v => Header.Raw(CaseInsensitiveString(k), v))
      } ++
      headers.toList.filterNot(h => param contains h.name))

  def apply(service: HttpService): HttpService =
    Service.lift { req =>
      (req.params.get("request-headers").fold[String \/ Request](\/-(req)) { v =>
        parse(v).map(hv => req.withHeaders(rewrite(req.headers, hv)))
      }).fold(
        err => BadRequest(Json("error" := "invalid request-headers: " + err)),
        service.run)
    }
}
