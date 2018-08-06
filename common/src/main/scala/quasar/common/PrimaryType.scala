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

package quasar.common

import slamdata.Predef._

import monocle.Prism
import scalaz._, Scalaz._

/** An enumeration of types used as part of a [[Pattern]]. */
sealed abstract class PrimaryType

// NB: These are defined in order of their total ordering
final case object Null extends PrimaryType
final case object Bool extends PrimaryType
final case object Char extends PrimaryType
final case object Int extends PrimaryType
final case object Dec extends PrimaryType
final case object Arr extends PrimaryType
final case object Map extends PrimaryType

object PrimaryType {
  def name = Prism[String, PrimaryType] {
    case "null"      => Null.some
    case "boolean"   => Bool.some
    case "character" => Char.some
    case "integer"   => Int.some
    case "decimal"   => Dec.some
    case "array"     => Arr.some
    case "map"       => Map.some
    case _           => none
  } {
    case Null => "null"
    case Bool => "boolean"
    case Char => "character"
    case Int  => "integer"
    case Dec  => "decimal"
    case Arr  => "array"
    case Map  => "map"
  }

  implicit val equal: Equal[PrimaryType] = Equal.equalA
}
