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

package quasar.yggdrasil.util

import quasar.precog.util._
import quasar.precog.common._
import quasar.yggdrasil.table._
import quasar.yggdrasil.{Config, FreshAtomicIdSource}

trait IdSourceScannerModule {
  val idSource = new FreshAtomicIdSource
  def freshIdScanner = new CScanner {
    type A = Long
    def init = 0
    val id = Config.idSource.nextId()

    def scan(pos: Long, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
      val rawCols = cols.values.toArray
      val defined = BitSetUtil.filteredRange(range.start, range.end) { i =>
        Column.isDefinedAt(rawCols, i)
      }

      val seqCol = new LongColumn {
        def isDefinedAt(row: Int) = defined(row)
        def apply(row: Int): Long = pos + row
      }

      (pos + range.end, Map(ColumnRef(CPath(CPathIndex(0)), CLong) -> seqCol))
    }
  }
}
