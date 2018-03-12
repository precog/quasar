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

package quasar.ejson

import slamdata.Predef.String

import monocle.Iso
import scalaz.{Order, Show}
import scalaz.std.string._

final case class TypeTag(value: String) extends scala.AnyVal

object TypeTag {
  val Binary    = TypeTag("_ejson.binary")
  val Date      = TypeTag("_ejson.date")
  val Interval  = TypeTag("_ejson.interval")
  val Time      = TypeTag("_ejson.time")
  val Timestamp = TypeTag("_ejson.timestamp")

  val stringIso: Iso[TypeTag, String] =
    Iso[TypeTag, String](_.value)(TypeTag(_))

  implicit val decodeEJson: DecodeEJson[TypeTag] =
    DecodeEJson[String].map(TypeTag(_))

  implicit val encodeEJson: EncodeEJson[TypeTag] =
    EncodeEJson[String].contramap(_.value)

  implicit val order: Order[TypeTag] =
    Order.orderBy(_.value)

  implicit val show: Show[TypeTag] =
    Show.showFromToString
}
