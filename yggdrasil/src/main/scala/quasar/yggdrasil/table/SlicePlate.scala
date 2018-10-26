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
package table

import qdata.json.PreciseKeys

import quasar.common.{CPath, CPathField, CPathNode}
import quasar.precog.common._
import quasar.time.DateTimeUtil

import tectonic.{Plate, Signal}

import scala.collection.mutable

private[table] abstract class EmptyFinishRowPlate[A] extends Plate[A] {
  def finishRow() = ()
}

private[table] final class SlicePlate(precise: Boolean)
    extends EmptyFinishRowPlate[List[Slice]]    // <3 Scala
    with ContinuingNestPlate[List[Slice]]
    with CPathPlate[List[Slice]] {

  import PreciseKeys._

  private val PreciseKeySet = Set[CPathNode](
    CPathField(LocalDateTimeKey),
    CPathField(LocalDateKey),
    CPathField(LocalTimeKey),
    CPathField(OffsetDateTimeKey),
    CPathField(OffsetDateKey),
    CPathField(OffsetTimeKey),
    CPathField(IntervalKey))

  private val Nil = scala.Nil

  private var nextThreshold = Config.defaultMinRows

  private var size = 0    // rows in process don't count toward size
  private val columns = mutable.Map[ColumnRef, ArrayColumn[_]]()

  private val completed = mutable.ListBuffer[Slice]()

  def nul(): Signal = {
    growArrays()

    val col = checkGet(ColumnRef(CPath(cursor.reverse), CNull)).asInstanceOf[MutableNullColumn]
    col(size) = true

    Signal.Continue
  }

  def fls(): Signal = {
    growArrays()

    val col = checkGet(ColumnRef(CPath(cursor.reverse), CBoolean)).asInstanceOf[ArrayBoolColumn]
    col(size) = false

    Signal.Continue
  }

  def tru(): Signal = {
    growArrays()

    val col = checkGet(ColumnRef(CPath(cursor.reverse), CBoolean)).asInstanceOf[ArrayBoolColumn]
    col(size) = true

    Signal.Continue
  }

  def map(): Signal = {
    growArrays()

    val col = checkGet(ColumnRef(CPath(cursor.reverse), CEmptyObject)).asInstanceOf[MutableEmptyObjectColumn]
    col(size) = true

    Signal.Continue
  }

  def arr(): Signal = {
    growArrays()

    val col = checkGet(ColumnRef(CPath(cursor.reverse), CEmptyArray)).asInstanceOf[MutableEmptyArrayColumn]
    col(size) = true

    Signal.Continue
  }

  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = {
    import tectonic.util.{parseLong, parseLongUnsafe, InvalidLong, MaxSafeLongLength}

    growArrays()

    if (decIdx < 0 && expIdx < 0 && s.length < MaxSafeLongLength) {
      val col = checkGet(ColumnRef(CPath(cursor.reverse), CLong)).asInstanceOf[ArrayLongColumn]
      col(size) = parseLongUnsafe(s)
    } else if (decIdx < 0) {
      try {
        val ln = parseLong(s)
        val col = checkGet(ColumnRef(CPath(cursor.reverse), CLong)).asInstanceOf[ArrayLongColumn]
        col(size) = ln
      } catch {
        case _: InvalidLong =>
          val col = checkGet(ColumnRef(CPath(cursor.reverse), CNum)).asInstanceOf[ArrayNumColumn]
          col(size) = BigDecimal(s.toString)
      }
    } else {
      val num = BigDecimal(s.toString)

      if (num.isDecimalDouble) {
        val col = checkGet(ColumnRef(CPath(cursor.reverse), CDouble)).asInstanceOf[ArrayDoubleColumn]
        col(size) = num.doubleValue
      } else {
        val col = checkGet(ColumnRef(CPath(cursor.reverse), CNum)).asInstanceOf[ArrayNumColumn]
        col(size) = num
      }
    }

    Signal.Continue
  }

  def str(s: CharSequence): Signal = {
    growArrays()

    val str = s.toString
    var value: AnyRef = null

    val ref = if (precise && !(cursor eq Nil) && PreciseKeySet.contains(cursor.head)) {
      import DateTimeUtil._

      val tpe = cursor.head match {
        case CPathField(LocalDateTimeKey) =>
          value = parseLocalDateTime(str)
          CLocalDateTime

        case CPathField(LocalDateKey) =>
          value = parseLocalDate(str)
          CLocalDate

        case CPathField(LocalTimeKey) =>
          value = parseLocalTime(str)
          CLocalTime

        case CPathField(OffsetDateTimeKey) =>
          value = parseOffsetDateTime(str)
          COffsetDateTime

        case CPathField(OffsetDateKey) =>
          value = parseOffsetDate(str)
          COffsetDate

        case CPathField(OffsetTimeKey) =>
          value = parseOffsetTime(str)
          COffsetTime

        case CPathField(IntervalKey) =>
          value = parseInterval(str)
          CInterval

        case _ =>
          sys.error("impossible")
      }

      ColumnRef(CPath(cursor.tail.reverse), tpe)
    } else {
      value = str
      ColumnRef(CPath(cursor.reverse), CString)
    }

    val col = checkGet(ref).asInstanceOf[ArrayColumn[AnyRef]]
    col(size) = value

    Signal.Continue
  }

  final override def finishRow(): Unit = {
    super.finishRow()
    size += 1

    if (size > Config.maxSliceRows || columns.size > Config.maxSliceColumns) {
      finishSlice()
    }
  }

  def finishBatch(terminal: Boolean): List[Slice] = {
    if (terminal) {
      finishSlice()
    }

    val back = completed.toList
    completed.clear()

    back
  }

  private def checkGet(ref: ColumnRef): ArrayColumn[_] = {
    var back = columns.getOrElse(ref, null)

    if (back == null) {
      back = ref.ctype match {
        case CBoolean => ArrayBoolColumn.empty(nextThreshold)
        case CLong => ArrayLongColumn.empty(nextThreshold)
        case CDouble => ArrayDoubleColumn.empty(nextThreshold)
        case CNum => ArrayNumColumn.empty(nextThreshold)
        case CString => ArrayStrColumn.empty(nextThreshold)
        case CNull => MutableNullColumn.empty()
        case CEmptyArray => MutableEmptyArrayColumn.empty()
        case CEmptyObject => MutableEmptyObjectColumn.empty()

        // precise types
        case CLocalDateTime => ArrayLocalDateTimeColumn.empty(nextThreshold)
        case CLocalDate => ArrayLocalDateColumn.empty(nextThreshold)
        case CLocalTime => ArrayLocalTimeColumn.empty(nextThreshold)
        case COffsetDateTime => ArrayOffsetDateTimeColumn.empty(nextThreshold)
        case COffsetDate => ArrayOffsetDateColumn.empty(nextThreshold)
        case COffsetTime => ArrayOffsetTimeColumn.empty(nextThreshold)
        case CInterval => ArrayIntervalColumn.empty(nextThreshold)

        case tpe => sys.error(s"should be impossible to create a column of $tpe")
      }

      columns(ref) = back
    }

    back
  }

  private def growArrays(): Unit = {
    if (size >= nextThreshold) {
      nextThreshold *= 2

      for ((ref, col) <- columns) {
        columns(ref) = col.resize(nextThreshold)
      }
    }
  }

  private def finishSlice(): Unit = {
    if (size > 0) {
      val slice = Slice(size, columns.to[λ[α => Map[ColumnRef, Column]]])
      completed += slice

      nextThreshold = Config.defaultMinRows
      size = 0
      columns.clear()
    }
  }
}
