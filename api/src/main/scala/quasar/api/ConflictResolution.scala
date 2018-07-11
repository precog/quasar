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

import monocle.Prism
import scalaz.{Enum, Show}
import scalaz.std.{anyVal, option}, anyVal._, option._
import scalaz.syntax.order._
import scalaz.syntax.std.option._

sealed trait ConflictResolution extends Product with Serializable

object ConflictResolution extends ConflictResolutionInstances {
  case object Preserve extends ConflictResolution
  case object Replace extends ConflictResolution

  val preserve: ConflictResolution =
    Preserve

  val replace: ConflictResolution =
    Replace

  def string = Prism[String, ConflictResolution] {
    case "preserve" => Preserve.some
    case "replace" => Replace.some
    case _ => none
  } {
    case Preserve => "preserve"
    case Replace => "replace"
  }
}

sealed abstract class ConflictResolutionInstances {
  implicit val enum: Enum[ConflictResolution] =
    new Enum[ConflictResolution] {
      def order(x: ConflictResolution, y: ConflictResolution) =
        toInt(x) ?|? toInt(y)

      def pred(cr: ConflictResolution) =
        cr match {
          case ConflictResolution.Preserve => ConflictResolution.Replace
          case ConflictResolution.Replace  => ConflictResolution.Preserve
        }

      def succ(cr: ConflictResolution) =
        cr match {
          case ConflictResolution.Preserve => ConflictResolution.Replace
          case ConflictResolution.Replace  => ConflictResolution.Preserve
        }

      override val min = Some(ConflictResolution.Preserve)

      override val max = Some(ConflictResolution.Replace)

      private def toInt(cr: ConflictResolution): Int =
        cr match {
          case ConflictResolution.Preserve => 0
          case ConflictResolution.Replace  => 1
        }
    }

  implicit val show: Show[ConflictResolution] =
    Show.showFromToString
}
