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

package quasar.fs

import java.lang.Exception

import monocle.macros.Lenses
import monocle.Prism
import scalaz._

sealed abstract class PhysicalError {
  val cause: Exception
}
@Lenses final case class UnhandledFSError(cause: Exception)
    extends PhysicalError

object PhysicalError {
  implicit val show: Show[PhysicalError] = Show.shows(_.cause.getMessage)
}

abstract class PhysicalErrorPrisms {
  val unhandledFSError = Prism.partial[PhysicalError, Exception] {
    case UnhandledFSError(ex) => ex
  } (UnhandledFSError(_))
}
