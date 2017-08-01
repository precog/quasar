/*
 * Copyright 2014–2017 SlamData Inc.
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

import scalaz._, Scalaz._
import scalaz.stream._

final class FoldableOps[F[_], A] private[scalaz] (self: F[A])(implicit F0: Foldable[F]) {
  /** The pure `Process` of the values in this `Foldable`. */
  final def toProcess: Process0[A] =
    self.foldRight[Process0[A]](Process.halt)((a, p) => Process.emit(a) ++ p)
}

trait ToFoldableOps {
  implicit def toFoldableOps[F[_]: Foldable, A](self: F[A]): FoldableOps[F, A] =
    new FoldableOps(self)
}

object foldable extends ToFoldableOps
