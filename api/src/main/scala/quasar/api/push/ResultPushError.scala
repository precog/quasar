/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.api.push

import slamdata.Predef._

sealed trait ResultPushError[+T, +D] extends Product with Serializable

object ResultPushError {
  sealed trait ExistentialError[+T, +D] extends ResultPushError[T, D]

  final case class DestinationNotFound[D](destinationId: D) extends ExistentialError[Nothing, D]

  final case class TableNotFound[T](tableId: T) extends ExistentialError[T, Nothing]

  final case class FormatNotSupported[D](destinationId: D, format: String) extends ResultPushError[Nothing, D]

  final case class PushAlreadyRunning[T, D](tableId: T, destinationId: D) extends ResultPushError[T, D]
}
