/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.fp._

import matryoshka._
import scalaz._, Scalaz._

sealed abstract class DeadEnd

/** The top level of a filesystem. During compilation this represents `/`, but
  * in the structure a backend sees, it represents the mount point.
  */
final case object Root extends DeadEnd
final case object Empty extends DeadEnd

object DeadEnd {
  implicit def equal: Equal[DeadEnd] = Equal.equalRef
  implicit def show: Show[DeadEnd] = Show.showFromToString

  implicit def mergeable[T[_[_]]]: Mergeable.Aux[T, DeadEnd] =
    new Mergeable[DeadEnd] {
      type IT[F[_]] = T[F]

      def mergeSrcs(
        left: FreeMap[IT],
        right: FreeMap[IT],
        p1: DeadEnd,
        p2: DeadEnd) =
        OptionT(state(
          if (p1 ≟ p2)
            SrcMerge[DeadEnd, FreeMap[IT]](p1, UnitF, UnitF).some
          else
            None))
    }
}
