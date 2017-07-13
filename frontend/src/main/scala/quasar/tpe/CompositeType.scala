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

package quasar.tpe

import slamdata.Predef._

import monocle.Prism
import scalaz._
import scalaz.std.anyVal._
import scalaz.syntax.order._

/** Primary composite types. */
sealed abstract class CompositeType extends Product with Serializable

object CompositeType {
  // NB: Defined in order of their total ordering.
  final case object Arr extends CompositeType
  final case object Map extends CompositeType

  val name: Prism[String, CompositeType] =
    Prism.partial[String, CompositeType] {
      case "array" => Arr
      case "map"   => Map
    } {
      case Arr     => "array"
      case Map     => "map"
    }

  implicit val enum: Enum[CompositeType] =
    new Enum[CompositeType] {
      def order(x: CompositeType, y: CompositeType): Ordering =
        toInt(x) ?|? toInt(y)

      def pred(ct: CompositeType): CompositeType = ct match {
        case Arr => Map
        case Map => Arr
      }

      def succ(ct: CompositeType): CompositeType = ct match {
        case Arr => Map
        case Map => Arr
      }

      override val min: Option[CompositeType] =
        Some(Arr)

      override val max: Option[CompositeType] =
        Some(Map)

      ////

      private val toInt: CompositeType => Int = {
        case Arr => 0
        case Map => 1
      }
    }

  implicit val show: Show[CompositeType] =
    Show.shows(name(_))
}
