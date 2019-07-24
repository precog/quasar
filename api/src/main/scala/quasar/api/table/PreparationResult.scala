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

package quasar.api.table

import slamdata.Predef.{Product, Serializable}

sealed trait PreparationResult[I, A] extends Product with Serializable

object PreparationResult {
  final case class Available[I, A](tableId: I, value: A) extends PreparationResult[I, A]
  final case class Unavailable[I, A](tableId: I) extends PreparationResult[I, A]
}
