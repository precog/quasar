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

package quasar.yggdrasil.table

import quasar.precog.common._
import quasar.yggdrasil.Config
import quasar.yggdrasil.table.ctrie._

import java.math.MathContext
import scala.annotation.tailrec
import scala.sys

import fs2.{Chunk, Pull, Stream}
import qdata.{QDataDecode, QType}

private[table] object SliceIngest {

  final case class ChunkSliced[A](chunk: Chunk[A], start: Int, size: Int) {
    def isEmpty: Boolean = size == 0
    def head: A = chunk(start)
    def tail: ChunkSliced[A] = ChunkSliced(chunk, start + 1, size - 1)
  }

  object ChunkSliced {
    private val empty_ = ChunkSliced[Nothing](Chunk.empty, 0, 0)

    def empty[A]: ChunkSliced[A] =
      empty_.asInstanceOf[ChunkSliced[A]]

    def fromChunk[A](chunk: Chunk[A]): ChunkSliced[A] =
      ChunkSliced(chunk, 0, chunk.size)
  }

  def allFromQData[F[_], A: QDataDecode](
      values: Stream[F, A],
      maxRows: Option[Int],
      maxColumns: Option[Int])
      : Stream[F, Slice] = {

    val maxRowsC = maxRows.getOrElse(Config.maxSliceRows)
    val maxColumnsC = maxColumns.getOrElse(Config.maxSliceColumns)
    // println(s"maxRows: $maxRowsC, maxCols: $maxColumnsC")

    def rec(next: ChunkSliced[A], values: Stream[F, A]): Pull[F, Slice, Unit] =
      if (next.isEmpty) {
        values.pull.unconsChunk flatMap {
          case Some((chunk, next)) =>
            rec(ChunkSliced.fromChunk(chunk), next)
          case None =>
            // println("Finished top-level loop")
            Pull.done
        }
      } else {
        // println(s"extracting slice from data with size ${next.size}")
        val (nextSlice, remainingData) =
          fromQDataStep(next, maxRowsC, maxColumnsC, Config.defaultMinRows)

        Pull.output1(nextSlice) >> rec(remainingData, values)
      }

    rec(ChunkSliced.empty, values).stream
  }

  def fromQDataStep[A: QDataDecode](
    values: ChunkSliced[A],
    maxRows: Int,
    maxColumns: Int,
    startingSize: Int)
    : (Slice, ChunkSliced[A]) = {

    @tailrec
    def inner(
        next: ChunkSliced[A], rows: Int, colsOverflowed: Boolean,
        acc: Map[ColumnRef, ArrayColumn[_]], allocatedColSize: Int)
        : (Slice, ChunkSliced[A]) = {
      if (next.isEmpty) {
        // there's no more data to make slices of.
        // we'll have to emit whatever data we have already as a slice.
        if (acc.isEmpty) {
          // println("no more data to use, emitting slice with no data")
          (Slice.empty, ChunkSliced.empty)
        } else {
          // println("no more data to use, emitting slice with data")
          (Slice(rows, acc), ChunkSliced.empty)
        }
      } else {
        // we have some data.
        if (rows >= maxRows) {
          // we'd have more rows than `maxRows` if we added
          // this row to the slice. so we have to emit the
          // data we have already as a slice, and pass
          // the row back to the caller.
          // println(s"we have the maximum ($maxRows) rows, cutting slice early")
          (Slice(rows, acc), next)
        } else if (rows >= allocatedColSize && !colsOverflowed) {
          // we'd have more rows than `allocatedColSize` if we added this
          // row to the slice. `allocatedColSize` is the size in rows of
          // all of the columns we've accumulated, so we'll
          // have to resize those columns to add further data.
          // `allocatedColSize` should always be a power of two, so we multiply
          // by two.
          val newSize =
            if (allocatedColSize == 2 && startingSize != 2) startingSize
            else allocatedColSize * 2
          // println(s"we're resizing the columns from $allocatedColSize to $newSize because we have too many rows")
          inner(next, rows, false, acc.mapValues(_.resize(newSize)), newSize)
        } else {
          // we already have too many columns, so there's no need to check
          // if the next row would put us over the limit.
          if (colsOverflowed) {
          // println(s"we already have too many columns (${acc.size}), cutting slice early at $rows (allocated size $allocatedColSize)")
            (Slice(rows, acc), next)
          } else {
            // we *may* have enough space in this slice for the
            // next value. we're going to attempt adding the value
            // to find out how many columns we would have
            // if we added that value to the current slice.
            // if we have no other rows, we'll start with a two-row
            // slice, just in case we have too many columns immediately.
            val newAcc = updateRefs(next.head, acc, rows, allocatedColSize)
            val newCols = newAcc.size
            if (newCols > maxColumns && rows > 0) {
              // we would have too many columns in this slice if we added this
              // value. so we're going to pass it back to the caller and
              // return a slice with the data we have already. we don't have to
              // clear the new value's data out of the accumulated data,
              // because we've already made sure to set `size` correctly.
              // println(s"we would have too many columns with this new value ($newCols), cutting slice early at $rows")
              (Slice(rows, acc), next)
            } else {
              // we're okay with adding this value to the slice!
              // we already have the slice's data including value in `newAcc`,
              // so we pass that on and advance the data cursor.
              // println("we're adding a value to the slice")
              inner(next.tail, rows + 1, newCols > maxColumns, newAcc, allocatedColSize)
            }
          }
        }
      }
    }

    if (values.isEmpty) {
      (Slice.empty, ChunkSliced.empty)
    } else {
      val size = Math.min(maxRows, startingSize)
      inner(values, 0, false, CTrie.empty[ArrayColumn[_]], size)
    }
  }

  sealed trait QKind
  final case object QArray extends QKind
  final case object QObject extends QKind
  final case object QUnknown extends QKind

  def foldScalars[A, B](a: A)(z: B)(
    emptyArray: (B, CPath) => B,
    emptyObject: (B, CPath) => B,
    value: (B, CPath, A) => B)(
    implicit A: QDataDecode[A])
    : B = {

    var b = z
    var as: List[(QKind, CPath, A)] = List((QUnknown, CPath.Identity, a))

    def foldUnknown(p: CPath, v: A): Unit =
      A.tpe(v) match {
        case QType.QMeta =>
          as = (QUnknown, p, A.getMetaValue(v)) :: as

        case QType.QArray =>
          as = (QArray, p, v) :: as

        case QType.QObject =>
          as = (QObject, p, v) :: as

        case _ =>
          b = value(b, p, v)
      }

    def foldArray(p: CPath, v: A): Unit = {
      var cur = A.getArrayCursor(v)
      var idx = 0

      if (A.hasNextArray(cur))
        while (A.hasNextArray(cur)) {
          foldUnknown(p \ idx, A.getArrayAt(cur))
          cur = A.stepArray(cur)
          idx = idx + 1
        }
      else
        b = emptyArray(b, p)
    }

    def foldObject(p: CPath, v: A): Unit = {
      var cur = A.getObjectCursor(v)

      if (A.hasNextObject(cur))
        while (A.hasNextObject(cur)) {
          foldUnknown(p \ A.getObjectKeyAt(cur), A.getObjectValueAt(cur))
          cur = A.stepObject(cur)
        }
      else
        b = emptyObject(b, p)
    }

    while (as.nonEmpty) {
      val h = as.head
      as = as.tail

      h match {
        case (QUnknown, p, v) => foldUnknown(p, v)
        case (QArray, p, v) => foldArray(p, v)
        case (QObject, p, v) => foldObject(p, v)
      }
    }

    b
  }

  def scalarType[A](a: A)(implicit A: QDataDecode[A]): CType =
    A.tpe(a) match {
      case QType.QLong => CLong
      case QType.QDouble => CDouble
      case QType.QReal => CNum
      case QType.QString => CString
      case QType.QNull => CNull
      case QType.QBoolean => CBoolean
      case QType.QLocalDateTime => CLocalDateTime
      case QType.QLocalDate => CLocalDate
      case QType.QLocalTime => CLocalTime
      case QType.QOffsetDateTime => COffsetDateTime
      case QType.QOffsetDate => COffsetDate
      case QType.QOffsetTime => COffsetTime
      case QType.QInterval => CInterval
      case composite => sys.error(s"Not a scalar type: $composite")
    }

  def updateRefs[A](
      from: A,
      into: Map[ColumnRef, ArrayColumn[_]],
      sliceIndex: Int,
      sliceSize: Int)(
      implicit A: QDataDecode[A])
      : Map[ColumnRef, ArrayColumn[_]] = {

    def updateEmptyArray(m: Map[ColumnRef, ArrayColumn[_]], p: CPath): Map[ColumnRef, ArrayColumn[_]] = {
      val ref = ColumnRef(p, CEmptyArray)
      val c = m.getOrElse(ref, MutableEmptyArrayColumn.empty()).asInstanceOf[MutableEmptyArrayColumn]
      c.update(sliceIndex, true)
      m.updated(ref, c)
    }

    def updateEmptyObject(m: Map[ColumnRef, ArrayColumn[_]], p: CPath): Map[ColumnRef, ArrayColumn[_]] = {
      val ref = ColumnRef(p, CEmptyObject)
      val c = m.getOrElse(ref, MutableEmptyObjectColumn.empty()).asInstanceOf[MutableEmptyObjectColumn]
      c.update(sliceIndex, true)
      m.updated(ref, c)
    }

    def updateScalarValue(m: Map[ColumnRef, ArrayColumn[_]], p: CPath, a: A): Map[ColumnRef, ArrayColumn[_]] = {
      val ref = ColumnRef(p, scalarType(a))

      val updatedColumn: ArrayColumn[_] = A.tpe(a) match {
        case QType.QBoolean =>
          val c = m.getOrElse(ref, ArrayBoolColumn.empty(sliceSize)).asInstanceOf[ArrayBoolColumn]
          c.update(sliceIndex, A.getBoolean(a))

        case QType.QLong =>
          val c = m.getOrElse(ref, ArrayLongColumn.empty(sliceSize)).asInstanceOf[ArrayLongColumn]
          c.update(sliceIndex, A.getLong(a))

        case QType.QDouble =>
          val c = m.getOrElse(ref, ArrayDoubleColumn.empty(sliceSize)).asInstanceOf[ArrayDoubleColumn]
          c.update(sliceIndex, A.getDouble(a))

        case QType.QReal =>
          val c = m.getOrElse(ref, ArrayNumColumn.empty(sliceSize)).asInstanceOf[ArrayNumColumn]
          c.update(sliceIndex, A.getReal(a).toRational.toBigDecimal(MathContext.UNLIMITED))

        case QType.QString =>
          val c = m.getOrElse(ref, ArrayStrColumn.empty(sliceSize)).asInstanceOf[ArrayStrColumn]
          c.update(sliceIndex, A.getString(a))

        case QType.QOffsetDateTime =>
          val c = m.getOrElse(ref, ArrayOffsetDateTimeColumn.empty(sliceSize)).asInstanceOf[ArrayOffsetDateTimeColumn]
          c.update(sliceIndex, A.getOffsetDateTime(a))

        case QType.QOffsetTime =>
          val c = m.getOrElse(ref, ArrayOffsetTimeColumn.empty(sliceSize)).asInstanceOf[ArrayOffsetTimeColumn]
          c.update(sliceIndex, A.getOffsetTime(a))

        case QType.QOffsetDate =>
          val c = m.getOrElse(ref, ArrayOffsetDateColumn.empty(sliceSize)).asInstanceOf[ArrayOffsetDateColumn]
          c.update(sliceIndex, A.getOffsetDate(a))

        case QType.QLocalDateTime =>
          val c = m.getOrElse(ref, ArrayLocalDateTimeColumn.empty(sliceSize)).asInstanceOf[ArrayLocalDateTimeColumn]
          c.update(sliceIndex, A.getLocalDateTime(a))

        case QType.QLocalTime =>
          val c = m.getOrElse(ref, ArrayLocalTimeColumn.empty(sliceSize)).asInstanceOf[ArrayLocalTimeColumn]
          c.update(sliceIndex, A.getLocalTime(a))

        case QType.QLocalDate =>
          val c = m.getOrElse(ref, ArrayLocalDateColumn.empty(sliceSize)).asInstanceOf[ArrayLocalDateColumn]
          c.update(sliceIndex, A.getLocalDate(a))

        case QType.QInterval =>
          val c = m.getOrElse(ref, ArrayIntervalColumn.empty(sliceSize)).asInstanceOf[ArrayIntervalColumn]
          c.update(sliceIndex, A.getInterval(a))

        case QType.QNull =>
          val c = m.getOrElse(ref, MutableNullColumn.empty()).asInstanceOf[MutableNullColumn]
          c.update(sliceIndex, true)

        case composite => sys.error(s"Not a scalar type: $composite")
      }

      m.updated(ref, updatedColumn)
    }

    foldScalars(from)(into)(updateEmptyArray, updateEmptyObject, updateScalarValue)
  }
}
