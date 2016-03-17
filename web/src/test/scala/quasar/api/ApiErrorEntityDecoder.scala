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

package quasar.api

import quasar.Predef._

import argonaut._, Argonaut._
import org.http4s.{DecodeResult => _, _}
import org.http4s.argonaut._
import scalaz.EitherT
import scalaz.concurrent.Task
import scalaz.syntax.applicative._

object ApiErrorEntityDecoder {
  implicit val apiErrorEntityDecoder: EntityDecoder[ApiError] =
    EntityDecoder.decodeBy(MediaType.`application/json`) {
      case res @ Response(status, _, _, _, _) =>
        res.attemptAs[Json] flatMap { json =>
          EitherT.fromDisjunction[Task](fromJson(status, json.hcursor).toDisjunction)
            .leftMap { case (msg, _) => ParseFailure(
              "Failed to decode JSON as an ApiError",
              s"JSON: $json, reason: $msg")
            }
        }

      case Request(_, _, _, _, _, _) =>
        EitherT.left(ParseFailure("ApiError is only decodable from a Response.", "").point[Task])
    }

  private def fromJson(status: Status, hc: HCursor): DecodeResult[ApiError] =
    (hc --\ "error" --\ "detail").as[Option[Json]]
      .map(_.flatMap(_.obj) getOrElse JsonObject.empty)
      .tuple((hc --\ "error" --\ "status").as[String])
      .map { case (o, s) => ApiError(status withReason s, o) }
}
