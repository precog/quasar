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

package quasar

import java.lang.RuntimeException

import scalaz._
import scalaz.syntax.show._
import scalaz.syntax.monad._

object rethrow {
  /** "Throw" the lhs of an `EitherT` given a `Catchable` base type and the
    * ability to represent `E` as a `String`.
    */
  def apply[F[_]: Catchable : Monad, E: Show]: EitherT[F, E, ?] ~> F =
    new (EitherT[F, E, ?] ~> F) {
      def apply[A](et: EitherT[F, E, A]) =
        et.fold(
          e => Catchable[F].fail[A](new RuntimeException(e.shows)),
          _.point[F]
        ).join
    }
}
