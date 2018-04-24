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

import scalaz._

sealed abstract class SelectionOp

/** Drops the first `count` elements from a dataset. */
final case object Drop extends SelectionOp

/** Drops all elements after the first `count` elements from a dataset. */
final case object Take extends SelectionOp

/** Similar to Take, but keeps a random sampling of elements. */
final case object Sample extends SelectionOp

object SelectionOp {
  implicit val equal: Equal[SelectionOp] = Equal.equalRef
  implicit val show: Show[SelectionOp] = Show.showFromToString
}
