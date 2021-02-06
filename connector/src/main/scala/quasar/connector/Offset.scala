/*
 * Copyright 2020 Precog Data
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

package quasar.connector

import scala.{Product, Serializable}
import quasar.api.push.{OffsetKey, OffsetPath, InternalKey}

import skolems.∃

sealed trait Offset extends Product with Serializable {
  val value: ∃[OffsetKey.Actual]
}

object Offset {
  final case class Internal(path: OffsetPath, value: ∃[InternalKey.Actual])
      extends Offset

  final case class External(value: ∃[OffsetKey.Actual])
      extends Offset
}
