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

package quasar.tpe

import slamdata.Predef._

import monocle.Prism
import scalaz._
import scalaz.std.anyVal._
import scalaz.syntax.order._

/** Primary, non-composite types. */
sealed abstract class SimpleType extends Product with Serializable

object SimpleType {
  // NB: Defined in order of their total ordering.
  final case object Null extends SimpleType
  final case object Bool extends SimpleType
  final case object Byte extends SimpleType
  final case object Char extends SimpleType
  final case object Str  extends SimpleType
  final case object Int  extends SimpleType
  final case object Dec  extends SimpleType

  val name: Prism[String, SimpleType] =
    Prism.partial[String, SimpleType] {
      case "null"      => Null
      case "boolean"   => Bool
      case "byte"      => Byte
      case "character" => Char
      case "string"    => Str
      case "integer"   => Int
      case "decimal"   => Dec
    } {
      case Null        => "null"
      case Bool        => "boolean"
      case Byte        => "byte"
      case Char        => "character"
      case Str         => "string"
      case Int         => "integer"
      case Dec         => "decimal"
    }

  implicit val enum: Enum[SimpleType] =
    new Enum[SimpleType] {
      def order(x: SimpleType, y: SimpleType): Ordering =
        toInt(x) ?|? toInt(y)

      def pred(st: SimpleType): SimpleType = st match {
        case Null => Dec
        case Bool => Null
        case Byte => Bool
        case Char => Byte
        case Str  => Char
        case Int  => Str
        case Dec  => Int
      }

      def succ(st: SimpleType): SimpleType = st match {
        case Null => Bool
        case Bool => Byte
        case Byte => Char
        case Char => Str
        case Str  => Int
        case Int  => Dec
        case Dec  => Null
      }

      override val min: Option[SimpleType] =
        Some(Null)

      override val max: Option[SimpleType] =
        Some(Dec)

      ////

      private val toInt: SimpleType => Int = {
        case Null => 0
        case Bool => 1
        case Byte => 2
        case Char => 3
        case Str  => 4
        case Int  => 5
        case Dec  => 6
      }
    }

  implicit val show: Show[SimpleType] =
    Show.shows(name(_))
}
