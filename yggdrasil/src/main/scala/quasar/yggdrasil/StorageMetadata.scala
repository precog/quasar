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

package quasar.yggdrasil

import quasar.precog.MimeType
import quasar.precog.common._

case class PathMetadata(path: Path, pathType: PathMetadata.PathType)

object PathMetadata {
  sealed trait PathType
  case class DataDir(contentType: MimeType)  extends PathType
  case class DataOnly(contentType: MimeType) extends PathType
  case object PathOnly extends PathType
}

case class PathStructure(types: Map[CType, Long], children: Set[CPath])

object PathStructure {
  val Empty = PathStructure(Map.empty, Set.empty)
}
