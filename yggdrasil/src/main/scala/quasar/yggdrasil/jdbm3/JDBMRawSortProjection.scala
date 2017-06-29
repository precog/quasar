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

import quasar.precog.common._
import quasar.yggdrasil.table._
import quasar.yggdrasil.TableModule._

import org.apache.jdbm._

import org.slf4s.Logging

import java.io.File
import java.util.SortedMap

import scalaz._

import scala.collection.JavaConverters._

/**
  * A Projection wrapping a raw JDBM TreeMap index used for sorting. It's assumed that
  * the index has been created and filled prior to creating this wrapper.
  */
class JDBMRawSortProjection[M[+ _]] private[yggdrasil] (dbFile: File,
                                                        indexName: String,
                                                        sortKeyRefs: Seq[ColumnRef],
                                                        valRefs: Seq[ColumnRef],
                                                        sortOrder: DesiredSortOrder,
                                                        sliceSize: Int,
                                                        val length: Long)
    extends ProjectionLike[M, Slice]
    with Logging {
  import JDBMProjection._
  type Key = Array[Byte]

  def structure(implicit M: Monad[M]) = M.point((sortKeyRefs ++ valRefs).toSet)

  def foreach(f: java.util.Map.Entry[Array[Byte], Array[Byte]] => Unit) {
    val DB                                         = DBMaker.openFile(dbFile.getCanonicalPath).make()
    val index: SortedMap[Array[Byte], Array[Byte]] = DB.getTreeMap(indexName)

    index.entrySet().iterator().asScala.foreach(f)

    DB.close()
  }

  val rowFormat = RowFormat.forValues(valRefs)
  val keyFormat = RowFormat.forSortingKey(sortKeyRefs)

  override def getBlockAfter(id: Option[Array[Byte]], columns: Option[Set[ColumnRef]])(
      implicit M: Monad[M]): M[Option[BlockProjectionData[Array[Byte], Slice]]] = M.point {
    // TODO: Make this far, far less ugly
    if (columns.nonEmpty) {
      throw new IllegalArgumentException("JDBM Sort Projections may not be constrained by column descriptor")
    }

    // At this point we have completed all valid writes, so we open readonly + no locks, allowing for concurrent use of sorted data
    //println("opening: " + dbFile.getCanonicalPath)
    val db = DBMaker.openFile(dbFile.getCanonicalPath).readonly().disableLocking().make()
    try {
      val index: SortedMap[Array[Byte], Array[Byte]] = db.getTreeMap(indexName)

      if (index == null) {
        throw new IllegalArgumentException("No such index in DB: %s:%s".format(dbFile, indexName))
      }

      val constrainedMap = id.map { idKey =>
        index.tailMap(idKey)
      }.getOrElse(index)
      val iteratorSetup = () => {
        val rawIterator = constrainedMap.entrySet.iterator.asScala
        // Since our key to retrieve after was the last key we retrieved, we know it exists,
        // so we can safely discard it
        if (id.isDefined && rawIterator.hasNext) rawIterator.next();
        rawIterator
      }

      // FIXME: this is brokenness in JDBM somewhere
      val iterator = {
        var initial: Iterator[java.util.Map.Entry[Array[Byte], Array[Byte]]] = null
        var tries                                                            = 0
        while (tries < MAX_SPINS && initial == null) {
          try {
            initial = iteratorSetup()
          } catch {
            case t: Throwable => log.warn("Failure on load iterator initialization")
          }
          tries += 1
        }
        if (initial == null) {
          throw new VicciniException("Initial drop failed with too many concurrent mods.")
        } else {
          initial
        }
      }

      if (iterator.isEmpty) {
        None
      } else {
        val keyColumns = sortKeyRefs.map(JDBMSlice.columnFor(CPath("[0]"), sliceSize))
        val valColumns = valRefs.map(JDBMSlice.columnFor(CPath("[1]"), sliceSize))

        val keyColumnDecoder = keyFormat.ColumnDecoder(keyColumns.map(_._2)(collection.breakOut))
        val valColumnDecoder = rowFormat.ColumnDecoder(valColumns.map(_._2)(collection.breakOut))

        val (firstKey, lastKey, rows) = JDBMSlice.load(sliceSize, iteratorSetup, keyColumnDecoder, valColumnDecoder)

        val slice = new Slice {
          val size    = rows
          val columns = keyColumns.toMap ++ valColumns
        }

        Some(BlockProjectionData(firstKey, lastKey, slice))
      }
    } finally {
      db.close() // creating the slice should have already read contents into memory
    }
  }
}
