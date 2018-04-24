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

package quasar.fs

import slamdata.Predef._
import quasar._
import quasar.fp._

import scalaz._

final case class TempFilePrefix(s: String) extends AnyVal

object TempFilePrefix {

  implicit val showTempFilePrefix: Show[TempFilePrefix] =
    Show.shows(_.s)

  implicit val renderTreeTempFilePrefix: RenderTree[TempFilePrefix] =
    RenderTree.fromShow("TempFilePrefix")

  implicit val monoidTempFilePrefix: Monoid[TempFilePrefix] =
    new Monoid[TempFilePrefix] {
      val zero = TempFilePrefix("")

      def append(p1: TempFilePrefix, p2: => TempFilePrefix) =
        TempFilePrefix(p1.s + p2.s)
    }

}
