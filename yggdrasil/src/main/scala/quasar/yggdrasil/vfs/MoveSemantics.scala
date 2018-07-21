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

package quasar.yggdrasil.vfs

import slamdata.Predef.{Boolean, Unit}
import quasar.fp.ski._

import monocle.Prism
import scalaz.Show

sealed abstract class MoveSemantics {
  import MoveSemantics._

  /**
   * Given an existence check on the target, return whether or not
   * the operation is valid.
   */
  final def apply(check: Boolean): Boolean = this match {
    case Overwrite => true
    case FailIfExists => !check
    case FailIfMissing => check
  }
}

/**
 * NB: Certain write operations' consistency is affected by faithful support
 *     of these semantics, thus their consistency/atomicity is as good as the
 *     support of these semantics by the interpreter.
 *
 *     Currently, this allows us to implement all the write scenarios in terms
 *     of append and move, however if this proves too difficult to support by
 *     backends, we may want to relax the move semantics and instead add
 *     additional primitive operations for the conditional write operations.
 */
object MoveSemantics {

  /**
   * Indicates the move operation should overwrite anything at the
   * destination, creating it if it doesn't exist.
   */
  case object Overwrite extends MoveSemantics

  /**
   * Indicates the move should (atomically, if possible) fail if the
   * destination exists.
   */
  case object FailIfExists extends MoveSemantics

  /**
   * Indicates the move should (atomically, if possible) fail unless
   * the destination exists, overwriting it otherwise.
   */
  case object FailIfMissing extends MoveSemantics

  val overwrite = Prism.partial[MoveSemantics, Unit] {
    case Overwrite => ()
  } (κ(Overwrite))

  val failIfExists = Prism.partial[MoveSemantics, Unit] {
    case FailIfExists => ()
  } (κ(FailIfExists))

  val failIfMissing = Prism.partial[MoveSemantics, Unit] {
    case FailIfMissing => ()
  } (κ(FailIfMissing))

  implicit val show: Show[MoveSemantics] = Show.showFromToString
}
