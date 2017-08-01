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

package quasar.precog.common

import quasar.blueeyes.json.serialization.{Decomposer, Extractor}
import quasar.blueeyes.json.serialization.DefaultDecomposers._
import quasar.blueeyes.json.serialization.DefaultExtractors._

import java.util.UUID

object JavaSerialization {
  implicit val uuidDecomposer: Decomposer[UUID] = implicitly[Decomposer[String]].contramap((_: UUID).toString)
  implicit val uuidExtractor: Extractor[UUID] = implicitly[Extractor[String]].map(UUID.fromString)
}
