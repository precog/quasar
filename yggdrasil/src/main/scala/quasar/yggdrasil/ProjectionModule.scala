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

package quasar.yggdrasil

import quasar.precog.common._

import cats.effect.IO
import scalaz._, Scalaz._
import shims._

trait ProjectionModule[Block] {
  type Projection <: ProjectionLike[Block]
  type ProjectionCompanion <: ProjectionCompanionLike

  def Projection: ProjectionCompanion

  trait ProjectionCompanionLike { self =>
    def apply(path: Path): IO[Option[Projection]]
  }
}

case class BlockProjectionData[Key, Block](minKey: Key, maxKey: Key, data: Block)

trait ProjectionLike[Block] {
  type Key

  def structure: Set[ColumnRef]
  def length: Long

  /**
    * Get a block of data beginning with the first record with a key greater than
    * the specified key. If id.isEmpty, return a block starting with the minimum
    * key. Each resulting block should contain only the columns specified in the
    * column set; if the set of columns is empty, return all columns.
    */
  def getBlockAfter(id: Option[Key], columns: Option[Set[ColumnRef]] = None): IO[Option[BlockProjectionData[Key, Block]]]

  def getBlockStream(columns: Option[Set[ColumnRef]]): StreamT[IO, Block] = {
    StreamT.unfoldM[IO, Block, Option[Key]](none) { key =>
      getBlockAfter(key, columns) map {
        _ map { case BlockProjectionData(_, maxKey, block) => (block, Some(maxKey)) }
      }
    }
  }
}
