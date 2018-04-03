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

import quasar.time.{DateTimeInterval, OffsetDate}

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

class MemoBoolColumn(c: BoolColumn) extends BoolColumn {
  private[this] var row0          = -1
  private[this] var memo: Boolean = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoLongColumn(c: LongColumn) extends LongColumn {
  private[this] var row0       = -1
  private[this] var memo: Long = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoDoubleColumn(c: DoubleColumn) extends DoubleColumn {
  private[this] var row0         = -1
  private[this] var memo: Double = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoNumColumn(c: NumColumn) extends NumColumn {
  private[this] var row0             = -1
  private[this] var memo: BigDecimal = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoStrColumn(c: StrColumn) extends StrColumn {
  private[this] var row0         = -1
  private[this] var memo: String = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoIntervalColumn(c: IntervalColumn) extends IntervalColumn {
  private[this] var row0                   = -1
  private[this] var memo: DateTimeInterval = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoOffsetDateTimeColumn(c: OffsetDateTimeColumn) extends OffsetDateTimeColumn {
  private[this] var row0                 = -1
  private[this] var memo: OffsetDateTime = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoOffsetTimeColumn(c: OffsetTimeColumn) extends OffsetTimeColumn {
  private[this] var row0             = -1
  private[this] var memo: OffsetTime = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoOffsetDateColumn(c: OffsetDateColumn) extends OffsetDateColumn {
  private[this] var row0             = -1
  private[this] var memo: OffsetDate = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoLocalDateTimeColumn(c: LocalDateTimeColumn) extends LocalDateTimeColumn {
  private[this] var row0                = -1
  private[this] var memo: LocalDateTime = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoLocalTimeColumn(c: LocalTimeColumn) extends LocalTimeColumn {
  private[this] var row0            = -1
  private[this] var memo: LocalTime = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoLocalDateColumn(c: LocalDateColumn) extends LocalDateColumn {
  private[this] var row0           = -1
  private[this] var memo: LocalDate = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}
