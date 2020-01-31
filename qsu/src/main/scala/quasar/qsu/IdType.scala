/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.qsu

import slamdata.Predef.Int

import scalaz.{Order, Show}
import scalaz.std.anyVal._

sealed trait IdType

object IdType {
  import QScriptUniform.Rotation

  final case object Dataset extends IdType
  final case object Array extends IdType
  final case object Map extends IdType
  final case object Expr extends IdType

  val fromRotation: Rotation => IdType = {
    case Rotation.FlattenArray | Rotation.ShiftArray => Array
    case Rotation.FlattenMap | Rotation.ShiftMap => Map
  }

  implicit val order: Order[IdType] =
    Order[Int] contramap {
      case Dataset => 0
      case Array => 1
      case Map => 2
      case Expr => 3
    }

  implicit val show: Show[IdType] =
    Show.showFromToString
}
