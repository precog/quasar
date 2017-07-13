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
package jdbm3

import quasar.blueeyes._
import quasar.precog.common._
import org.slf4j.LoggerFactory
import quasar.yggdrasil.table._

object JDBMSlice {
  private lazy val log = LoggerFactory.getLogger("quasar.yggdrasil.jdbm3.JDBMSlice")

  def load(size: Int,
           source: () => Iterator[java.util.Map.Entry[Array[Byte], Array[Byte]]],
           keyDecoder: ColumnDecoder,
           valDecoder: ColumnDecoder): (Array[Byte], Array[Byte], Int) = {
    var firstKey: Array[Byte] = null.asInstanceOf[Array[Byte]]
    var lastKey: Array[Byte]  = null.asInstanceOf[Array[Byte]]

    @tailrec
    def consumeRows(source: Iterator[java.util.Map.Entry[Array[Byte], Array[Byte]]], row: Int): Int = {
      if (source.hasNext) {
        val entry  = source.next
        val rowKey = entry.getKey
        if (row == 0) { firstKey = rowKey }
        lastKey = rowKey

        keyDecoder.decodeToRow(row, rowKey)
        valDecoder.decodeToRow(row, entry.getValue)

        consumeRows(source, row + 1)
      } else {
        row
      }
    }

    val rows = {
      // FIXME: Looping here is a blatantly poor way to work around ConcurrentModificationExceptions
      // From the Javadoc for CME, the exception is an indication of a bug
      var finalCount = -1
      var tries      = 0
      while (tries < JDBMProjection.MAX_SPINS && finalCount == -1) {
        try {
          finalCount = consumeRows(source().take(size), 0)
        } catch {
          case t: Throwable =>
            log.warn("Error during block read, retrying")
            Thread.sleep(50)
        }
        tries += 1
      }
      if (finalCount == -1) {
        throw new VicciniException("Block read failed with too many concurrent mods.")
      } else {
        finalCount
      }
    }

    (firstKey, lastKey, rows)
  }

  def columnFor(prefix: CPath, sliceSize: Int)(ref: ColumnRef): (ColumnRef, ArrayColumn[_]) =
    (ref.copy(selector = (prefix \ ref.selector)), ref.ctype match {
      case CString              => ArrayStrColumn.empty(sliceSize)
      case CBoolean             => ArrayBoolColumn.empty()
      case CLong                => ArrayLongColumn.empty(sliceSize)
      case CDouble              => ArrayDoubleColumn.empty(sliceSize)
      case CNum                 => ArrayNumColumn.empty(sliceSize)
      case CDate                => ArrayDateColumn.empty(sliceSize)
      case CPeriod              => ArrayPeriodColumn.empty(sliceSize)
      case CNull                => MutableNullColumn.empty()
      case CEmptyObject         => MutableEmptyObjectColumn.empty()
      case CEmptyArray          => MutableEmptyArrayColumn.empty()
      case CArrayType(elemType) => ArrayHomogeneousArrayColumn.empty(sliceSize)(elemType)
      case CUndefined           => sys.error("CUndefined cannot be serialized")
    })
}
