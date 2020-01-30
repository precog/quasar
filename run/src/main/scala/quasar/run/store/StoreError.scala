/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.run.store

import slamdata.Predef._

import monocle.Prism
import scalaz.Show

sealed abstract trait StoreError extends Product with Serializable {
  def detail: String
}

object StoreError {
  final case class Corrupt(detail: String) extends StoreError

  val corrupt: Prism[StoreError, String] =
    Prism.partial[StoreError, String] {
      case Corrupt(details) => details
    } (Corrupt(_))

  val throwableP: Prism[Throwable, StoreError] =
    Prism.partial[Throwable, StoreError] {
      case StoreErrorException(re) => re
    } (StoreErrorException(_))

  implicit val show: Show[StoreError] = Show.shows {
    case Corrupt(detail) => "StoreError.Corrupt(" + detail + ")"
  }

  ////

  private final case class StoreErrorException(err: StoreError)
      extends Exception(err.detail)
}
