/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.connector

import slamdata.Predef.{Boolean, Int, Product, Serializable, String}

import monocle.Prism
import scalaz.{Enum, Equal, Show}
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.order._

sealed trait ParsableType extends Product with Serializable

object ParsableType extends ParsableTypeInstances {
  final case class Json(variant: JsonVariant, isPrecise: Boolean) extends ParsableType

  sealed trait JsonVariant extends Product with Serializable

  object JsonVariant {
    case object ArrayWrapped extends JsonVariant
    case object LineDelimited extends JsonVariant

    val stringP: Prism[String, JsonVariant] =
      Prism.partial[String, JsonVariant] {
        case "json" => ArrayWrapped
        case "ldjson" => LineDelimited
      } {
        case ArrayWrapped => "json"
        case LineDelimited => "ldjson"
      }

    implicit val enum: Enum[JsonVariant] =
      new Enum[JsonVariant] {
        def order(x: JsonVariant, y: JsonVariant) =
          toInt(x) ?|? toInt(y)

        def pred(jv: JsonVariant): JsonVariant =
          jv match {
            case ArrayWrapped => LineDelimited
            case LineDelimited => ArrayWrapped
          }

        def succ(jv: JsonVariant): JsonVariant =
          jv match {
            case ArrayWrapped => LineDelimited
            case LineDelimited => ArrayWrapped
          }

        def toInt(jv: JsonVariant): Int =
          jv match {
            case ArrayWrapped => 0
            case LineDelimited => 1
          }
      }

    implicit val show: Show[JsonVariant] =
      Show.showFromToString
  }

  val json: Prism[ParsableType, (JsonVariant, Boolean)] =
    Prism.partial[ParsableType, (JsonVariant, Boolean)] {
      case Json(v, p) => (v, p)
    } ((Json(_, _)).tupled)
}

sealed abstract class ParsableTypeInstances {
  implicit val equal: Equal[ParsableType] =
    Equal.equalBy(ParsableType.json.getOption(_))

  implicit val show: Show[ParsableType] =
    Show.showFromToString
}
