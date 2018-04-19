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

package quasar.qscript

import quasar.RenderTree

import scala.{Product, Serializable}
import scalaz.{Equal, Show}

sealed trait ShiftType extends Product with Serializable

object ShiftType {
  case object Array extends ShiftType
  case object Map extends ShiftType

  implicit def equal: Equal[ShiftType] = Equal.equalRef

  implicit def renderTree: RenderTree[ShiftType] = RenderTree.fromShow("ShiftType")

  implicit def show: Show[ShiftType] = Show.showFromToString
}
