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

package quasar.yggdrasil.vfs

import quasar.blueeyes.json.serialization.Versioned._
import quasar.blueeyes.json.serialization.{Decomposer, Extractor}

import java.time.Instant

import java.util.UUID

import shapeless._

import scalaz._

case class VersionEntry(id: UUID, typeName: PathData.DataType, timestamp: Instant)

object VersionEntry {
  //implicit val versionEntryIso: Iso.hlist(VersionEntry.apply _, VersionEntry.unapply _)

  val schemaV1 = "id" :: "typeName" :: "timestamp" :: HNil

  // FIXME
  implicit val Decomposer: Decomposer[VersionEntry] = ??? //decomposerV(schemaV1, Some("1.0".v))
  implicit val Extractor: Extractor[VersionEntry] = ??? //extractorV(schemaV1, Some("1.0".v))
}
