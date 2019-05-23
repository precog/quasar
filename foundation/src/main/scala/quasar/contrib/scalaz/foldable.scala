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

package quasar.contrib.scalaz

import slamdata.Predef._

import scalaz._, Scalaz._

final class FoldableOps[F[_], A] private[scalaz] (self: F[A])(implicit F0: Foldable[F]) {

  /** Returns whether this `Foldable` is equal to `other` when viewed as sets.
    *
    * NB: Has O(n^2) complexity.
    */
  def equalsAsSets(other: F[A])(implicit A: Equal[A]): Boolean =
    self.all(other element _) && other.all(self element _)
}

trait ToFoldableOps {
  implicit def toFoldableOps[F[_]: Foldable, A](self: F[A]): FoldableOps[F, A] =
    new FoldableOps(self)
}

object foldable extends ToFoldableOps
