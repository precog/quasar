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

package quasar.yggdrasil.nihdb

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal

import quasar.precog.common._
import quasar.precog.common.security.Authorities
import quasar.niflheim._
import quasar.yggdrasil._
import quasar.yggdrasil.table.Slice

import org.slf4s.Logging

import scalaz.Monad

final class NIHDBProjection(snapshot: NIHDBSnapshot, val authorities: Authorities, projectionId: Int) extends ProjectionLike[Future, Slice] with Logging {
  type Key = Long

  private[this] val readers = snapshot.readers

  val length = readers.map(_.length.toLong).sum

  override def toString = "NIHDBProjection(id = %d, len = %d, authorities = %s)".format(projectionId, length, authorities)

  def structure(implicit M: Monad[Future]) = M.point(readers.flatMap(_.structure)(collection.breakOut): Set[ColumnRef])

  def getBlockAfter(id0: Option[Long], columns: Option[Set[ColumnRef]])(implicit MP: Monad[Future]): Future[Option[BlockProjectionData[Long, Slice]]] = MP.point {
    val id = id0.map(_ + 1)
    val index = id getOrElse 0L
    getSnapshotBlock(id, columns.map(_.map(_.selector))) map {
      case Block(_, segments, _) =>
        val slice = SegmentsWrapper(segments, projectionId, index)
        BlockProjectionData(index, index, slice)
    }
  }

  def reduce[A](reduction: Reduction[A], path: CPath): Map[CType, A] = {
    readers.foldLeft(Map.empty[CType, A]) { (acc, reader) =>
      reader.snapshot(Some(Set(path))).segments.foldLeft(acc) { (acc, segment) =>
        reduction.reduce(segment, None) map { a =>
          val key = segment.ctype
          val value = acc.get(key).map(reduction.semigroup.append(_, a)).getOrElse(a)
          acc + (key -> value)
        } getOrElse acc
      }
    }
  }

  private def getSnapshotBlock(id: Option[Long], columns: Option[Set[CPath]]): Option[Block] = {
    try {
      // We're limiting ourselves to 2 billion blocks total here
      val index = id.map(_.toInt).getOrElse(0)
      if (index >= readers.length) {
        None
      } else {
        Some(Block(index, readers(index).snapshot(columns).segments, true))
      }
    } catch {
      case NonFatal(e) =>
        // Difficult to do anything else here other than bail
        log.warn("Error during block read", e)
        None
    }
  }
}

object NIHDBProjection {
  def wrap(nihdb: NIHDB): Future[NIHDBProjection] = nihdb.getSnapshot map { snap =>
    new NIHDBProjection(snap, nihdb.authorities, nihdb.projectionId)
  }
}
