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
import argonaut.JsonObjectScalaz._
import org.http4s.Status
import scalaz.{Equal, Show}
import scalaz.std.anyVal._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.std.boolean._

final case class ApiError(status: Status, detail: JsonObject) {
  def +: (jassoc: JsonAssoc): ApiError =
    copy(detail = jassoc +: detail)

  def :+ (jassoc: JsonAssoc): ApiError =
    copy(detail = detail + (jassoc._1, jassoc._2))

  def +?: (maybeAssoc: Option[JsonAssoc]): ApiError =
    maybeAssoc.fold(this)(_ +: this)

  def :?+ (maybeAssoc: Option[JsonAssoc]): ApiError =
    maybeAssoc.fold(this)(this :+ _)
}

object ApiError {
  def apiError(status: Status, details: JsonAssoc*): ApiError =
    apply(status, JsonObject.fromTraversableOnce(details.toList))

  def fromStatus(status: Status): ApiError =
    apiError(status)

  /** Convenience for adding a detailed message in a standardized way, the
    * resulting response will include a `message` field in the `detail` object
    * refering to the contents of `msg`.
    */
  def fromMsg(status: Status, msg: String, addlDetails: JsonAssoc*): ApiError =
    ("message" := msg) +: apiError(status, addlDetails: _*)

  def fromMsg_(status: Status, msg: String): ApiError =
    fromMsg(status, msg)

  implicit val encodeJson: EncodeJson[ApiError] =
    EncodeJson(err =>
      ("status" :=  err.status.reason) ->:
      ("detail" :?= err.detail.isNotEmpty.option(jObject(err.detail))) ->?:
      jEmptyObject)

  implicit val equal: Equal[ApiError] =
    Equal.equalBy(ae => (ae.status.code, ae.status.reason, ae.detail))

  implicit val show: Show[ApiError] =
    Show.showFromToString
}
