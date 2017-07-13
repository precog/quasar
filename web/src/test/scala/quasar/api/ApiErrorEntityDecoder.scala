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

package quasar.api

import slamdata.Predef._

import argonaut._, Argonaut._
import argonaut.ArgonautScalaz._
import org.http4s.{DecodeResult => _, _}
import org.http4s.argonaut._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object ApiErrorEntityDecoder {
  implicit val apiErrorEntityDecoder: EntityDecoder[ApiError] =
    EntityDecoder.decodeBy(MediaType.`application/json`) {
      case res @ Response(status, _, _, _, _) =>
        res.attemptAs[Json] flatMap { json =>
          EitherT.fromDisjunction[Task](fromJson(status, json.hcursor).toEither.disjunction)
            .leftMap { case (msg, _) => InvalidMessageBodyFailure(
              s"Failed to decode JSON as an ApiError. JSON: $json, reason: $msg")
            }
        }

      case Request(_, _, _, _, _, _) =>
        EitherT.left(MalformedMessageBodyFailure("ApiError is only decodable from a Response.").point[Task])
    }

  private def fromJson(status: Status, hc: HCursor): DecodeResult[ApiError] =
    (hc --\ "error" --\ "detail").as[Option[Json]]
      .map(_.flatMap(_.obj) getOrElse JsonObject.empty)
      .tuple((hc --\ "error" --\ "status").as[String])
      .map { case (o, s) => ApiError(status withReason s, o) }
}
