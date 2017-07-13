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
import org.http4s.Status, Status._
import org.specs2.matcher._
import org.specs2.scalaz._
import scalaz.std.anyVal._
import scalaz.std.string._

object matchers {
  import MustMatchers._
  import ScalazMatchers._
  import ApiError.apiError

  def beApiErrorLike[A](a: A)(implicit A: ToApiError[A]): Matcher[ApiError] =
    equal(A.toApiError(a))

  def beApiErrorWithMessage(status: Status, otherFields: JsonAssoc*): Matcher[ApiError] =
    (haveSameCodeAndReason(status) ^^ { (_: ApiError).status }) and
    (equal(JsonObject.fromTraversableOnce(otherFields.toList)) ^^ { (_: ApiError).detail - "message" }) and
    (haveFields("message" :: otherFields.map(_._1).toList : _*) ^^ { (_: ApiError).detail })

  def beHeaderMissingError(name: String): Matcher[ApiError] =
    equal(apiError(
      BadRequest withReason s"'$name' header missing.",
      "headerName" := name))

  private def haveSameCodeAndReason(status: Status): Matcher[Status] =
    (equal(status.code) ^^ { (_: Status).code }) and
    (equal(status.reason) ^^ { (_: Status).reason })

  private def haveFields(fields: JsonField*): Matcher[JsonObject] =
    contain(exactly(fields : _*)) ^^ { (_: JsonObject).fields }
}
