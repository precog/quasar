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

import scalaz._

sealed abstract class DeadEnd

/** The top level of a filesystem. During compilation this represents `/`, but
  * in the structure a backend sees, it represents the mount point.
  */
final case object Root extends DeadEnd

object DeadEnd {
  implicit def equal: Equal[DeadEnd] = Equal.equalRef
  implicit def show: Show[DeadEnd] = Show.showFromToString
  implicit def renderTree: RenderTree[DeadEnd] = RenderTree.fromShow("DeadEnd")
}
