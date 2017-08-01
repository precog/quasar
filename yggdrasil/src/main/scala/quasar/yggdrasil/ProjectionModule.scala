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

import quasar.precog.common._
import scalaz._, Scalaz._

trait ProjectionModule[M[+ _], Block] {
  type Projection <: ProjectionLike[M, Block]
  type ProjectionCompanion <: ProjectionCompanionLike[M]

  def Projection: ProjectionCompanion

  trait ProjectionCompanionLike[M0[+ _]] { self =>
    def apply(path: Path): M0[Option[Projection]]

    def liftM[T[_ [+ _], + _]](implicit T: Hoist[T], M0: Monad[M0]) = new ProjectionCompanionLike[({ type λ[+α] = T[M0, α] })#λ] {
      def apply(path: Path) = self.apply(path).liftM[T]
    }
  }
}

case class BlockProjectionData[Key, Block](minKey: Key, maxKey: Key, data: Block)

trait ProjectionLike[M[+ _], Block] {
  type Key

  def structure(implicit M: Monad[M]): M[Set[ColumnRef]]
  def length: Long

  /**
    * Get a block of data beginning with the first record with a key greater than
    * the specified key. If id.isEmpty, return a block starting with the minimum
    * key. Each resulting block should contain only the columns specified in the
    * column set; if the set of columns is empty, return all columns.
    */
  def getBlockAfter(id: Option[Key], columns: Option[Set[ColumnRef]] = None)(implicit M: Monad[M]): M[Option[BlockProjectionData[Key, Block]]]

  def getBlockStream(columns: Option[Set[ColumnRef]])(implicit M: Monad[M]): StreamT[M, Block] = {
    StreamT.unfoldM[M, Block, Option[Key]](None) { key =>
      getBlockAfter(key, columns) map {
        _ map { case BlockProjectionData(_, maxKey, block) => (block, Some(maxKey)) }
      }
    }
  }
}
