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

package quasar.niflheim

import quasar.precog.common._
import quasar.precog.util._

trait StorageReader {
  def snapshot(pathConstraints: Option[Set[CPath]]): Block
  def snapshotRef(refConstraints: Option[Set[ColumnRef]]): Block
  def structure: Iterable[ColumnRef]

  def isStable: Boolean

  def id: Long

  /**
   * Returns the total length of the block.
   */
  def length: Int

  override def toString = "StorageReader: id = %d, length = %d, structure = %s".format(id, length, structure)
}
