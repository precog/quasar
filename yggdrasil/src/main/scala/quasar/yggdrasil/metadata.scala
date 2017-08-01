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
import scalaz._

package object metadata {
  type MetadataMap = Map[MetadataType, Metadata]

  type ColumnMetadata = Map[ColumnRef, MetadataMap]

  object ColumnMetadata {
    val Empty = Map.empty[ColumnRef, MetadataMap]

    implicit object monoid extends Monoid[ColumnMetadata] {
      val zero = Empty

      def append(m1: ColumnMetadata, m2: => ColumnMetadata): ColumnMetadata = {
        m1.foldLeft(m2) {
          case (acc, (descriptor, mmap)) =>
            val currentMmap: MetadataMap = acc.getOrElse(descriptor, Map.empty[MetadataType, Metadata])
            val newMmap: MetadataMap = mmap.foldLeft(currentMmap) {
              case (macc, (mtype, m)) =>
                macc + (mtype -> macc.get(mtype).flatMap(_.merge(m)).getOrElse(m))
            }

            acc + (descriptor -> newMmap)
        }
      }
    }
  }
}
