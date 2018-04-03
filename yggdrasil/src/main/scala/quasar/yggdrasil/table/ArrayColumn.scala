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

import quasar.precog._
import quasar.precog.common._
import quasar.precog.util._
import quasar.time.{DateTimeInterval, OffsetDate}

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

import scala.reflect.ClassTag
import scala.specialized

trait DefinedAtIndex {
  private[table] val defined: BitSet
  def isDefinedAt(row: Int) = defined(row)
}

trait ArrayColumn[@specialized(Boolean, Long, Double) A] extends DefinedAtIndex with ExtensibleColumn {
  def update(row: Int, value: A): Unit
}

class ArrayHomogeneousArrayColumn[@specialized(Boolean, Long, Double) A](val defined: BitSet, values: Array[Array[A]])(val tpe: CArrayType[A])
    extends HomogeneousArrayColumn[A]
    with ArrayColumn[Array[A]] {
  def apply(row: Int) = values(row)

  def update(row: Int, value: Array[A]) {
    defined.set(row)
    values(row) = value
  }
}

object ArrayHomogeneousArrayColumn {
  def apply[@specialized(Boolean, Long, Double) A: CValueType](values: Array[Array[A]]) =
    new ArrayHomogeneousArrayColumn(BitSetUtil.range(0, values.length), values)(CArrayType(CValueType[A]))
  def apply[@specialized(Boolean, Long, Double) A: CValueType](defined: BitSet, values: Array[Array[A]]) =
    new ArrayHomogeneousArrayColumn(defined.copy, values)(CArrayType(CValueType[A]))
  def empty[@specialized(Boolean, Long, Double) A](size: Int)(implicit elemType: CValueType[A]): ArrayHomogeneousArrayColumn[A] = {
    implicit val m: ClassTag[A] = elemType.classTag

    new ArrayHomogeneousArrayColumn(new BitSet, new Array[Array[A]](size))(CArrayType(elemType))
  }
}

class ArrayBoolColumn(val defined: BitSet, val values: BitSet) extends ArrayColumn[Boolean] with BoolColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: Boolean) = {
    defined.set(row)
    if (value) values.set(row) else values.clear(row)
  }
}

object ArrayBoolColumn {
  def apply(defined: BitSet, values: BitSet) =
    new ArrayBoolColumn(defined.copy, values.copy)
  def apply(defined: BitSet, values: Array[Boolean]) =
    new ArrayBoolColumn(defined.copy, BitSetUtil.filteredRange(0, values.length)(values))
  def apply(values: Array[Boolean]) = {
    val d = BitSetUtil.range(0, values.length)
    val v = BitSetUtil.filteredRange(0, values.length)(values)
    new ArrayBoolColumn(d, v)
  }

  def empty(): ArrayBoolColumn =
    new ArrayBoolColumn(new BitSet, new BitSet)
}

class ArrayLongColumn(val defined: BitSet, val values: Array[Long]) extends ArrayColumn[Long] with LongColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: Long) = {
    defined.set(row)
    values(row) = value
  }
}

object ArrayLongColumn {
  def apply(values: Array[Long]) =
    new ArrayLongColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[Long]) =
    new ArrayLongColumn(defined.copy, values)
  def empty(size: Int): ArrayLongColumn =
    new ArrayLongColumn(new BitSet, new Array[Long](size))
}

class ArrayDoubleColumn(val defined: BitSet, values: Array[Double]) extends ArrayColumn[Double] with DoubleColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: Double) = {
    defined.set(row)
    values(row) = value
  }
}

object ArrayDoubleColumn {
  def apply(values: Array[Double]) =
    new ArrayDoubleColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[Double]) =
    new ArrayDoubleColumn(defined.copy, values)
  def empty(size: Int): ArrayDoubleColumn =
    new ArrayDoubleColumn(new BitSet, new Array[Double](size))
}

class ArrayNumColumn(val defined: BitSet, val values: Array[BigDecimal]) extends ArrayColumn[BigDecimal] with NumColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: BigDecimal) = {
    defined.set(row)
    values(row) = value
  }
}

object ArrayNumColumn {
  def apply(values: Array[BigDecimal]) =
    new ArrayNumColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[BigDecimal]) =
    new ArrayNumColumn(defined.copy, values)
  def empty(size: Int): ArrayNumColumn =
    new ArrayNumColumn(new BitSet, new Array[BigDecimal](size))
}

class ArrayStrColumn(val defined: BitSet, values: Array[String]) extends ArrayColumn[String] with StrColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: String) = {
    defined.set(row)
    values(row) = value
  }
}

object ArrayStrColumn {
  def apply(values: Array[String]) =
    new ArrayStrColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[String]) =
    new ArrayStrColumn(defined.copy, values)
  def empty(size: Int): ArrayStrColumn =
    new ArrayStrColumn(new BitSet, new Array[String](size))
}

class ArrayOffsetDateTimeColumn(val defined: BitSet, values: Array[OffsetDateTime]) extends ArrayColumn[OffsetDateTime] with OffsetDateTimeColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: OffsetDateTime) = {
    defined.set(row)
    values(row) = value
  }
}

object ArrayOffsetDateTimeColumn {
  def apply(values: Array[OffsetDateTime]) =
    new ArrayOffsetDateTimeColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[OffsetDateTime]) =
    new ArrayOffsetDateTimeColumn(defined.copy, values)
  def empty(size: Int): ArrayOffsetDateTimeColumn =
    new ArrayOffsetDateTimeColumn(new BitSet, new Array[OffsetDateTime](size))
}

class ArrayOffsetTimeColumn(val defined: BitSet, values: Array[OffsetTime]) extends ArrayColumn[OffsetTime] with OffsetTimeColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: OffsetTime) = {
    defined.set(row)
    values(row) = value
  }
}

object ArrayOffsetTimeColumn {
  def apply(values: Array[OffsetTime]) =
    new ArrayOffsetTimeColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[OffsetTime]) =
    new ArrayOffsetTimeColumn(defined.copy, values)
  def empty(size: Int): ArrayOffsetTimeColumn =
    new ArrayOffsetTimeColumn(new BitSet, new Array[OffsetTime](size))
}

class ArrayOffsetDateColumn(val defined: BitSet, values: Array[OffsetDate]) extends ArrayColumn[OffsetDate] with OffsetDateColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: OffsetDate) = {
    defined.set(row)
    values(row) = value
  }
}

object ArrayOffsetDateColumn {
  def apply(values: Array[OffsetDate]) =
    new ArrayOffsetDateColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[OffsetDate]) =
    new ArrayOffsetDateColumn(defined.copy, values)
  def empty(size: Int): ArrayOffsetDateColumn =
    new ArrayOffsetDateColumn(new BitSet, new Array[OffsetDate](size))
}

class ArrayLocalDateTimeColumn(val defined: BitSet, values: Array[LocalDateTime]) extends ArrayColumn[LocalDateTime] with LocalDateTimeColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: LocalDateTime) = {
    defined.set(row)
    values(row) = value
  }
}

object ArrayLocalDateTimeColumn {
  def apply(values: Array[LocalDateTime]) =
    new ArrayLocalDateTimeColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[LocalDateTime]) =
    new ArrayLocalDateTimeColumn(defined.copy, values)
  def empty(size: Int): ArrayLocalDateTimeColumn =
    new ArrayLocalDateTimeColumn(new BitSet, new Array[LocalDateTime](size))
}

class ArrayLocalTimeColumn(val defined: BitSet, values: Array[LocalTime]) extends ArrayColumn[LocalTime] with LocalTimeColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: LocalTime) = {
    defined.set(row)
    values(row) = value
  }
}

object ArrayLocalTimeColumn {
  def apply(values: Array[LocalTime]) =
    new ArrayLocalTimeColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[LocalTime]) =
    new ArrayLocalTimeColumn(defined.copy, values)
  def empty(size: Int): ArrayLocalTimeColumn =
    new ArrayLocalTimeColumn(new BitSet, new Array[LocalTime](size))
}

class ArrayLocalDateColumn(val defined: BitSet, values: Array[LocalDate]) extends ArrayColumn[LocalDate] with LocalDateColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: LocalDate) = {
    defined.set(row)
    values(row) = value
  }
}

object ArrayLocalDateColumn {
  def apply(values: Array[LocalDate]) =
    new ArrayLocalDateColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[LocalDate]) =
    new ArrayLocalDateColumn(defined.copy, values)
  def empty(size: Int): ArrayLocalDateColumn =
    new ArrayLocalDateColumn(new BitSet, new Array[LocalDate](size))
}

class ArrayIntervalColumn(val defined: BitSet, values: Array[DateTimeInterval]) extends ArrayColumn[DateTimeInterval] with IntervalColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: DateTimeInterval) = {
    defined.set(row)
    values(row) = value
  }
}

object ArrayIntervalColumn {
  def apply(values: Array[DateTimeInterval]) =
    new ArrayIntervalColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[DateTimeInterval]) =
    new ArrayIntervalColumn(defined.copy, values)
  def empty(size: Int): ArrayIntervalColumn =
    new ArrayIntervalColumn(new BitSet, new Array[DateTimeInterval](size))
}

class MutableEmptyArrayColumn(val defined: BitSet) extends ArrayColumn[Boolean] with EmptyArrayColumn {
  def update(row: Int, value: Boolean) = {
    if (value) defined.set(row) else defined.clear(row)
  }
}

object MutableEmptyArrayColumn {
  def empty(): MutableEmptyArrayColumn = new MutableEmptyArrayColumn(new BitSet)
}

class MutableEmptyObjectColumn(val defined: BitSet) extends ArrayColumn[Boolean] with EmptyObjectColumn {
  def update(row: Int, value: Boolean) = {
    if (value) defined.set(row) else defined.clear(row)
  }
}

object MutableEmptyObjectColumn {
  def empty(): MutableEmptyObjectColumn = new MutableEmptyObjectColumn(new BitSet)
}

class MutableNullColumn(val defined: BitSet) extends ArrayColumn[Boolean] with NullColumn {
  def update(row: Int, value: Boolean) = {
    if (value) defined.set(row) else defined.clear(row)
  }
}

object MutableNullColumn {
  def empty(): MutableNullColumn = new MutableNullColumn(new BitSet)
}

/* help for ctags
type ArrayColumn */
