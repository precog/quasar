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

package quasar.yggdrasil.table.cf

import quasar.blueeyes._
import quasar.precog._
import quasar.yggdrasil._
import quasar.yggdrasil.table._

import scala.reflect.ClassTag

object util {

  /**
    * Right-biased column union
    */
  val UnionRight = CF2P("builtin::ct::unionRight") {
    case (c1: BoolColumn, c2: BoolColumn) =>
      new UnionColumn(c1, c2) with BoolColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: LongColumn, c2: LongColumn) =>
      new UnionColumn(c1, c2) with LongColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: DoubleColumn, c2: DoubleColumn) =>
      new UnionColumn(c1, c2) with DoubleColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: NumColumn, c2: NumColumn) =>
      new UnionColumn(c1, c2) with NumColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: StrColumn, c2: StrColumn) =>
      new UnionColumn(c1, c2) with StrColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: OffsetDateTimeColumn, c2: OffsetDateTimeColumn) =>
      new UnionColumn(c1, c2) with OffsetDateTimeColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: OffsetTimeColumn, c2: OffsetTimeColumn) =>
      new UnionColumn(c1, c2) with OffsetTimeColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: OffsetDateColumn, c2: OffsetDateColumn) =>
      new UnionColumn(c1, c2) with OffsetDateColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: LocalDateTimeColumn, c2: LocalDateTimeColumn) =>
      new UnionColumn(c1, c2) with LocalDateTimeColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: LocalTimeColumn, c2: LocalTimeColumn) =>
      new UnionColumn(c1, c2) with LocalTimeColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: LocalDateColumn, c2: LocalDateColumn) =>
      new UnionColumn(c1, c2) with LocalDateColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: IntervalColumn, c2: IntervalColumn) =>
      new UnionColumn(c1, c2) with IntervalColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: HomogeneousArrayColumn[a], _c2: HomogeneousArrayColumn[_]) if c1.tpe == _c2.tpe =>
      val c2 = _c2.asInstanceOf[HomogeneousArrayColumn[a]]
      new UnionColumn(c1, c2) with HomogeneousArrayColumn[a] {
        val tpe = c1.tpe
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: EmptyArrayColumn, c2: EmptyArrayColumn)   => new UnionColumn(c1, c2) with EmptyArrayColumn
    case (c1: EmptyObjectColumn, c2: EmptyObjectColumn) => new UnionColumn(c1, c2) with EmptyObjectColumn
    case (c1: NullColumn, c2: NullColumn)               => new UnionColumn(c1, c2) with NullColumn
  }

  case object NConcat {

    // Closest thing we can get to casting an array. This is completely unsafe.
    private def copyCastArray[A: ClassTag](as: Array[_]): Array[A] = {
      val bs = new Array[A](as.length)
      System.arraycopy(as, 0, bs, 0, as.length)
      bs
    }

    def apply(cols: List[(Int, Column)]) = {
      val sortedCols             = cols.sortBy(_._1)
      val offsets: Array[Int]    = sortedCols.map(_._1)(collection.breakOut)
      val columns: Array[Column] = sortedCols.map(_._2)(collection.breakOut)

      cols match {
        case (_, _: BoolColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[BoolColumn]) =>
          val boolColumns = copyCastArray[BoolColumn](columns)
          Some(new NConcatColumn(offsets, boolColumns) with BoolColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              boolColumns(i)(row - offsets(i))
            }
          })

        case (_, _: LongColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[LongColumn]) =>
          val longColumns = copyCastArray[LongColumn](columns)
          Some(new NConcatColumn(offsets, longColumns) with LongColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              longColumns(i)(row - offsets(i))
            }
          })

        case (_, _: DoubleColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[DoubleColumn]) =>
          val doubleColumns = copyCastArray[DoubleColumn](columns)
          Some(new NConcatColumn(offsets, doubleColumns) with DoubleColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              doubleColumns(i)(row - offsets(i))
            }
          })

        case (_, _: NumColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[NumColumn]) =>
          val numColumns = copyCastArray[NumColumn](columns)
          Some(new NConcatColumn(offsets, numColumns) with NumColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              numColumns(i)(row - offsets(i))
            }
          })

        case (_, _: StrColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[StrColumn]) =>
          val strColumns = copyCastArray[StrColumn](columns)
          Some(new NConcatColumn(offsets, strColumns) with StrColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              strColumns(i)(row - offsets(i))
            }
          })

        case (_, _: OffsetDateTimeColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[OffsetDateTimeColumn]) =>
          val dateTimeColumns = copyCastArray[OffsetDateTimeColumn](columns)
          Some(new NConcatColumn(offsets, dateTimeColumns) with OffsetDateTimeColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              dateTimeColumns(i)(row - offsets(i))
            }
          })

        case (_, _: OffsetTimeColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[OffsetTimeColumn]) =>
          val timeColumns = copyCastArray[OffsetTimeColumn](columns)
          Some(new NConcatColumn(offsets, timeColumns) with OffsetTimeColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              timeColumns(i)(row - offsets(i))
            }
          })

        case (_, _: OffsetDateColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[OffsetDateColumn]) =>
          val dateColumns = copyCastArray[OffsetDateColumn](columns)
          Some(new NConcatColumn(offsets, dateColumns) with OffsetDateColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              dateColumns(i)(row - offsets(i))
            }
          })

        case (_, _: LocalDateTimeColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[LocalDateTimeColumn]) =>
          val dateTimeColumns = copyCastArray[LocalDateTimeColumn](columns)
          Some(new NConcatColumn(offsets, dateTimeColumns) with LocalDateTimeColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              dateTimeColumns(i)(row - offsets(i))
            }
          })

        case (_, _: LocalTimeColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[LocalTimeColumn]) =>
          val timeColumns = copyCastArray[LocalTimeColumn](columns)
          Some(new NConcatColumn(offsets, timeColumns) with LocalTimeColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              timeColumns(i)(row - offsets(i))
            }
          })

        case (_, _: LocalDateColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[LocalDateColumn]) =>
          val dateColumns = copyCastArray[LocalDateColumn](columns)
          Some(new NConcatColumn(offsets, dateColumns) with LocalDateColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              dateColumns(i)(row - offsets(i))
            }
          })

        case (_, _: IntervalColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[IntervalColumn]) =>
          val periodColumns = copyCastArray[IntervalColumn](columns)
          Some(new NConcatColumn(offsets, periodColumns) with IntervalColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              periodColumns(i)(row - offsets(i))
            }
          })

        case (_, _: EmptyArrayColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[EmptyArrayColumn]) =>
          val emptyArrayColumns = copyCastArray[EmptyArrayColumn](columns)
          Some(new NConcatColumn(offsets, emptyArrayColumns) with EmptyArrayColumn)

        case (_, _: EmptyObjectColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[EmptyObjectColumn]) =>
          val emptyObjectColumns = copyCastArray[EmptyObjectColumn](columns)
          Some(new NConcatColumn(offsets, emptyObjectColumns) with EmptyObjectColumn)

        case (_, _: NullColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[NullColumn]) =>
          val nullColumns = copyCastArray[NullColumn](columns)
          Some(new NConcatColumn(offsets, nullColumns) with NullColumn)

        case _ => None
      }
    }
  }

  //it would be nice to generalize these to `CoerceTo[A]`
  def CoerceToDouble = CF1P("builtin:ct:coerceToDouble") {
    case (c: DoubleColumn) => c

    case (c: LongColumn) =>
      new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = c(row).toDouble
      }

    case (c: NumColumn) =>
      new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = c(row).toDouble
      }
  }

  def CoerceToOffsetDateTime = CF1P("builtin:ct:coerceToOffsetDateTime") {
    case (c: OffsetDateTimeColumn) => c

    case (c: StrColumn) => new OffsetDateTimeColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && DateTimeUtil.isValidOffsetDateTime(c(row))
      def apply(row: Int) = DateTimeUtil.parseOffsetDateTime(c(row))
    }
  }

  def CoerceToOffsetTime = CF1P("builtin:ct:coerceToOffsetTime") {
    case (c: OffsetTimeColumn) => c

    case (c: StrColumn) => new OffsetTimeColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && DateTimeUtil.isValidOffsetTime(c(row))
      def apply(row: Int) = DateTimeUtil.parseOffsetTime(c(row))
    }
  }

  def CoerceToOffsetDate = CF1P("builtin:ct:coerceToOffsetDate") {
    case (c: OffsetDateColumn) => c

    case (c: StrColumn) => new OffsetDateColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && DateTimeUtil.isValidOffsetDate(c(row))
      def apply(row: Int) = DateTimeUtil.parseOffsetDate(c(row))
    }
  }

  def CoerceToLocalDateTime = CF1P("builtin:ct:coerceToLocalDateTime") {
    case (c: LocalDateTimeColumn) => c

    case (c: StrColumn) => new LocalDateTimeColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && DateTimeUtil.isValidLocalDateTime(c(row))
      def apply(row: Int) = DateTimeUtil.parseLocalDateTime(c(row))
    }
  }

  def CoerceToLocalTime = CF1P("builtin:ct:coerceToLocalTime") {
    case (c: LocalTimeColumn) => c

    case (c: StrColumn) => new LocalTimeColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && DateTimeUtil.isValidLocalTime(c(row))
      def apply(row: Int) = DateTimeUtil.parseLocalTime(c(row))
    }
  }

  def CoerceToLocalDate = CF1P("builtin:ct:coerceToLocalDate") {
    case (c: LocalDateColumn) => c

    case (c: StrColumn) => new LocalDateColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && DateTimeUtil.isValidLocalDate(c(row))
      def apply(row: Int) = DateTimeUtil.parseLocalDate(c(row))
    }
  }

  def Shift(by: Int) = CF1P("builtin::ct::shift") {
    case c: BoolColumn =>
      new ShiftColumn(by, c) with BoolColumn {
        def apply(row: Int) = c(row - by)
      }

    case c: LongColumn =>
      new ShiftColumn(by, c) with LongColumn {
        def apply(row: Int) = c(row - by)
      }

    case c: DoubleColumn =>
      new ShiftColumn(by, c) with DoubleColumn {
        def apply(row: Int) = c(row - by)
      }

    case c: NumColumn =>
      new ShiftColumn(by, c) with NumColumn {
        def apply(row: Int) = c(row - by)
      }

    case c: StrColumn =>
      new ShiftColumn(by, c) with StrColumn {
        def apply(row: Int) = c(row - by)
      }

    case c: OffsetDateTimeColumn =>
      new ShiftColumn(by, c) with OffsetDateTimeColumn {
        def apply(row: Int) = c(row - by)
      }

    case c: OffsetTimeColumn =>
      new ShiftColumn(by, c) with OffsetTimeColumn {
        def apply(row: Int) = c(row - by)
      }

    case c: OffsetDateColumn =>
      new ShiftColumn(by, c) with OffsetDateColumn {
        def apply(row: Int) = c(row - by)
      }

    case c: LocalDateTimeColumn =>
      new ShiftColumn(by, c) with LocalDateTimeColumn {
        def apply(row: Int) = c(row - by)
      }

    case c: LocalTimeColumn =>
      new ShiftColumn(by, c) with LocalTimeColumn {
        def apply(row: Int) = c(row - by)
      }

    case c: LocalDateColumn =>
      new ShiftColumn(by, c) with LocalDateColumn {
        def apply(row: Int) = c(row - by)
      }

    case c: IntervalColumn =>
      new ShiftColumn(by, c) with IntervalColumn {
        def apply(row: Int) = c(row - by)
      }

    case c: HomogeneousArrayColumn[a] =>
      new ShiftColumn(by, c) with HomogeneousArrayColumn[a] {
        val tpe = c.tpe
        def apply(row: Int) = c(row - by)
      }

    case c: EmptyArrayColumn  => new ShiftColumn(by, c) with EmptyArrayColumn
    case c: EmptyObjectColumn => new ShiftColumn(by, c) with EmptyObjectColumn
    case c: NullColumn        => new ShiftColumn(by, c) with NullColumn
  }

  def Sparsen(idx: Array[Int], toSize: Int) = CF1P("builtin::ct::sparsen") {
    case c: BoolColumn           => new SparsenColumn(c, idx, toSize) with BoolColumn { def apply(row: Int)     =       c(remap(row)) }
    case c: LongColumn           => new SparsenColumn(c, idx, toSize) with LongColumn { def apply(row: Int)     =       c(remap(row)) }
    case c: DoubleColumn         => new SparsenColumn(c, idx, toSize) with DoubleColumn { def apply(row: Int)   =       c(remap(row)) }
    case c: NumColumn            => new SparsenColumn(c, idx, toSize) with NumColumn { def apply(row: Int)      =       c(remap(row)) }
    case c: StrColumn            => new SparsenColumn(c, idx, toSize) with StrColumn { def apply(row: Int)      =       c(remap(row)) }
    case c: OffsetDateTimeColumn => new SparsenColumn(c, idx, toSize) with OffsetDateTimeColumn { def apply(row: Int) = c(remap(row)) }
    case c: OffsetTimeColumn     => new SparsenColumn(c, idx, toSize) with OffsetTimeColumn { def apply(row: Int) =     c(remap(row)) }
    case c: OffsetDateColumn     => new SparsenColumn(c, idx, toSize) with OffsetDateColumn { def apply(row: Int) =     c(remap(row)) }
    case c: LocalDateTimeColumn  => new SparsenColumn(c, idx, toSize) with LocalDateTimeColumn { def apply(row: Int) =  c(remap(row)) }
    case c: LocalTimeColumn      => new SparsenColumn(c, idx, toSize) with LocalTimeColumn { def apply(row: Int) =      c(remap(row)) }
    case c: LocalDateColumn      => new SparsenColumn(c, idx, toSize) with LocalDateColumn { def apply(row: Int) =      c(remap(row)) }
    case c: IntervalColumn       => new SparsenColumn(c, idx, toSize) with IntervalColumn { def apply(row: Int) =       c(remap(row)) }
    case c: HomogeneousArrayColumn[a] =>
      new SparsenColumn(c, idx, toSize) with HomogeneousArrayColumn[a] {
        val tpe = c.tpe
        def apply(row: Int) = c(remap(row))
      }

    case c: EmptyArrayColumn  => new SparsenColumn(c, idx, toSize) with EmptyArrayColumn
    case c: EmptyObjectColumn => new SparsenColumn(c, idx, toSize) with EmptyObjectColumn
    case c: NullColumn        => new SparsenColumn(c, idx, toSize) with NullColumn
  }

  val Empty = CF1P("builtin::ct::empty") {
    case c: BoolColumn          => new EmptyColumn[BoolColumn] with BoolColumn
    case c: LongColumn          => new EmptyColumn[LongColumn] with LongColumn
    case c: DoubleColumn        => new EmptyColumn[DoubleColumn] with DoubleColumn
    case c: NumColumn           => new EmptyColumn[NumColumn] with NumColumn
    case c: StrColumn           => new EmptyColumn[StrColumn] with StrColumn
    case c: LocalDateTimeColumn => new EmptyColumn[LocalDateTimeColumn] with LocalDateTimeColumn
    case c: LocalTimeColumn     => new EmptyColumn[LocalTimeColumn] with LocalTimeColumn
    case c: LocalDateColumn     => new EmptyColumn[LocalDateColumn] with LocalDateColumn
    case c: IntervalColumn      => new EmptyColumn[IntervalColumn] with IntervalColumn
    case c: HomogeneousArrayColumn[a] =>
      new EmptyColumn[HomogeneousArrayColumn[a]] with HomogeneousArrayColumn[a] {
        val tpe = c.tpe
      }
    case c: EmptyArrayColumn  => new EmptyColumn[EmptyArrayColumn] with EmptyArrayColumn
    case c: EmptyObjectColumn => new EmptyColumn[EmptyObjectColumn] with EmptyObjectColumn
    case c: NullColumn        => new EmptyColumn[NullColumn] with NullColumn
  }

  val Undefined = CF1P("builtin::ct::undefined") {
    case c: BoolColumn           => UndefinedColumn.raw
    case c: LongColumn           => UndefinedColumn.raw
    case c: DoubleColumn         => UndefinedColumn.raw
    case c: NumColumn            => UndefinedColumn.raw
    case c: StrColumn            => UndefinedColumn.raw
    case c: OffsetDateTimeColumn => UndefinedColumn.raw
    case c: OffsetTimeColumn     => UndefinedColumn.raw
    case c: OffsetDateColumn     => UndefinedColumn.raw
    case c: LocalDateTimeColumn  => UndefinedColumn.raw
    case c: LocalTimeColumn      => UndefinedColumn.raw
    case c: LocalDateColumn      => UndefinedColumn.raw
    case c: IntervalColumn       => UndefinedColumn.raw
    case c: HomogeneousArrayColumn[_] => UndefinedColumn.raw
    case c: EmptyArrayColumn  => UndefinedColumn.raw
    case c: EmptyObjectColumn => UndefinedColumn.raw
    case c: NullColumn        => UndefinedColumn.raw
  }

  def Remap(f: Int => Int) = CF1P("builtin::ct::remap") {
    case c: BoolColumn           => new RemapColumn(c, f) with BoolColumn { def apply(row: Int)           = c(f(row)) }
    case c: LongColumn           => new RemapColumn(c, f) with LongColumn { def apply(row: Int)           = c(f(row)) }
    case c: DoubleColumn         => new RemapColumn(c, f) with DoubleColumn { def apply(row: Int)         = c(f(row)) }
    case c: NumColumn            => new RemapColumn(c, f) with NumColumn { def apply(row: Int)            = c(f(row)) }
    case c: StrColumn            => new RemapColumn(c, f) with StrColumn { def apply(row: Int)            = c(f(row)) }
    case c: OffsetDateTimeColumn => new RemapColumn(c, f) with OffsetDateTimeColumn { def apply(row: Int) = c(f(row)) }
    case c: OffsetTimeColumn     => new RemapColumn(c, f) with OffsetTimeColumn { def apply(row: Int)     = c(f(row)) }
    case c: OffsetDateColumn     => new RemapColumn(c, f) with OffsetDateColumn { def apply(row: Int)     = c(f(row)) }
    case c: LocalDateTimeColumn  => new RemapColumn(c, f) with LocalDateTimeColumn { def apply(row: Int)  = c(f(row)) }
    case c: LocalTimeColumn      => new RemapColumn(c, f) with LocalTimeColumn { def apply(row: Int)      = c(f(row)) }
    case c: LocalDateColumn      => new RemapColumn(c, f) with LocalDateColumn { def apply(row: Int)      = c(f(row)) }
    case c: IntervalColumn => new RemapColumn(c, f) with IntervalColumn { def apply(row: Int)             = c(f(row)) }
    case c: HomogeneousArrayColumn[a] =>
      new RemapColumn(c, f) with HomogeneousArrayColumn[a] {
        val tpe = c.tpe
        def apply(row: Int) = c(f(row))
      }
    case c: EmptyArrayColumn  => new RemapColumn(c, f) with EmptyArrayColumn
    case c: EmptyObjectColumn => new RemapColumn(c, f) with EmptyObjectColumn
    case c: NullColumn        => new RemapColumn(c, f) with NullColumn
  }

  def RemapFilter(filter: Int => Boolean, offset: Int) = CF1P("builtin::ct::remapFilter") {
    case c: BoolColumn     => new RemapFilterColumn(c, filter, offset) with BoolColumn { def apply(row: Int)                 = c(row + offset) }
    case c: LongColumn     => new RemapFilterColumn(c, filter, offset) with LongColumn { def apply(row: Int)                 = c(row + offset) }
    case c: DoubleColumn   => new RemapFilterColumn(c, filter, offset) with DoubleColumn { def apply(row: Int)               = c(row + offset) }
    case c: NumColumn      => new RemapFilterColumn(c, filter, offset) with NumColumn { def apply(row: Int)                  = c(row + offset) }
    case c: StrColumn      => new RemapFilterColumn(c, filter, offset) with StrColumn { def apply(row: Int)                  = c(row + offset) }
    case c: OffsetDateTimeColumn => new RemapFilterColumn(c, filter, offset) with OffsetDateTimeColumn { def apply(row: Int) = c(row + offset) }
    case c: OffsetTimeColumn     => new RemapFilterColumn(c, filter, offset) with OffsetTimeColumn { def apply(row: Int)     = c(row + offset) }
    case c: OffsetDateColumn     => new RemapFilterColumn(c, filter, offset) with OffsetDateColumn { def apply(row: Int)     = c(row + offset) }
    case c: LocalDateTimeColumn => new RemapFilterColumn(c, filter, offset) with LocalDateTimeColumn { def apply(row: Int)   = c(row + offset) }
    case c: LocalTimeColumn     => new RemapFilterColumn(c, filter, offset) with LocalTimeColumn { def apply(row: Int)       = c(row + offset) }
    case c: LocalDateColumn     => new RemapFilterColumn(c, filter, offset) with LocalDateColumn { def apply(row: Int)       = c(row + offset) }
    case c: IntervalColumn => new RemapFilterColumn(c, filter, offset) with IntervalColumn { def apply(row: Int)             = c(row + offset) }
    case c: HomogeneousArrayColumn[a] =>
      new RemapFilterColumn(c, filter, offset) with HomogeneousArrayColumn[a] {
        val tpe = c.tpe
        def apply(row: Int) = c(row + offset)
      }
    case c: EmptyArrayColumn  => new RemapFilterColumn(c, filter, offset) with EmptyArrayColumn
    case c: EmptyObjectColumn => new RemapFilterColumn(c, filter, offset) with EmptyObjectColumn
    case c: NullColumn        => new RemapFilterColumn(c, filter, offset) with NullColumn
  }

  def RemapIndices(indices: ArrayIntList) = CF1P("builtin::ct::remapIndices") {
    case c: BoolColumn           => new RemapIndicesColumn(c, indices) with BoolColumn { def apply(row: Int)           = c(indices.get(row)) }
    case c: LongColumn           => new RemapIndicesColumn(c, indices) with LongColumn { def apply(row: Int)           = c(indices.get(row)) }
    case c: DoubleColumn         => new RemapIndicesColumn(c, indices) with DoubleColumn { def apply(row: Int)         = c(indices.get(row)) }
    case c: NumColumn            => new RemapIndicesColumn(c, indices) with NumColumn { def apply(row: Int)            = c(indices.get(row)) }
    case c: StrColumn            => new RemapIndicesColumn(c, indices) with StrColumn { def apply(row: Int)            = c(indices.get(row)) }
    case c: OffsetDateTimeColumn => new RemapIndicesColumn(c, indices) with OffsetDateTimeColumn { def apply(row: Int) = c(indices.get(row)) }
    case c: OffsetTimeColumn     => new RemapIndicesColumn(c, indices) with OffsetTimeColumn { def apply(row: Int)     = c(indices.get(row)) }
    case c: OffsetDateColumn     => new RemapIndicesColumn(c, indices) with OffsetDateColumn { def apply(row: Int)     = c(indices.get(row)) }
    case c: LocalDateTimeColumn  => new RemapIndicesColumn(c, indices) with LocalDateTimeColumn { def apply(row: Int)  = c(indices.get(row)) }
    case c: LocalTimeColumn      => new RemapIndicesColumn(c, indices) with LocalTimeColumn { def apply(row: Int)      = c(indices.get(row)) }
    case c: LocalDateColumn      => new RemapIndicesColumn(c, indices) with LocalDateColumn { def apply(row: Int)      = c(indices.get(row)) }
    case c: IntervalColumn       => new RemapIndicesColumn(c, indices) with IntervalColumn { def apply(row: Int)       = c(indices.get(row)) }
    case c: HomogeneousArrayColumn[a] =>
      new RemapIndicesColumn(c, indices) with HomogeneousArrayColumn[a] {
        val tpe = c.tpe
        def apply(row: Int) = c(indices.get(row))
      }
    case c: EmptyArrayColumn  => new RemapIndicesColumn(c, indices) with EmptyArrayColumn
    case c: EmptyObjectColumn => new RemapIndicesColumn(c, indices) with EmptyObjectColumn
    case c: NullColumn        => new RemapIndicesColumn(c, indices) with NullColumn
  }

  def filter(from: Int, to: Int, definedAt: BitSet) = CF1P("builtin::ct::filter") {
    case c: BoolColumn           => new BitsetColumn(definedAt & c.definedAt(from, to)) with BoolColumn { def apply(row: Int)           = c(row) }
    case c: LongColumn           => new BitsetColumn(definedAt & c.definedAt(from, to)) with LongColumn { def apply(row: Int)           = c(row) }
    case c: DoubleColumn         => new BitsetColumn(definedAt & c.definedAt(from, to)) with DoubleColumn { def apply(row: Int)         = c(row) }
    case c: NumColumn            => new BitsetColumn(definedAt & c.definedAt(from, to)) with NumColumn { def apply(row: Int)            = c(row) }
    case c: StrColumn            => new BitsetColumn(definedAt & c.definedAt(from, to)) with StrColumn { def apply(row: Int)            = c(row) }
    case c: OffsetDateTimeColumn => new BitsetColumn(definedAt & c.definedAt(from, to)) with OffsetDateTimeColumn { def apply(row: Int) = c(row) }
    case c: OffsetTimeColumn     => new BitsetColumn(definedAt & c.definedAt(from, to)) with OffsetTimeColumn { def apply(row: Int)     = c(row) }
    case c: OffsetDateColumn     => new BitsetColumn(definedAt & c.definedAt(from, to)) with OffsetDateColumn { def apply(row: Int)     = c(row) }
    case c: LocalDateTimeColumn  => new BitsetColumn(definedAt & c.definedAt(from, to)) with LocalDateTimeColumn { def apply(row: Int)  = c(row) }
    case c: LocalTimeColumn      => new BitsetColumn(definedAt & c.definedAt(from, to)) with LocalTimeColumn { def apply(row: Int)      = c(row) }
    case c: LocalDateColumn      => new BitsetColumn(definedAt & c.definedAt(from, to)) with LocalDateColumn { def apply(row: Int)      = c(row) }
    case c: IntervalColumn => new BitsetColumn(definedAt & c.definedAt(from, to)) with IntervalColumn { def apply(row: Int)             = c(row) }
    case c: HomogeneousArrayColumn[a] =>
      new BitsetColumn(definedAt & c.definedAt(from, to)) with HomogeneousArrayColumn[a] {
        val tpe = c.tpe
        def apply(row: Int) = c(row)
      }
    case c: EmptyArrayColumn  => new BitsetColumn(definedAt & c.definedAt(from, to)) with EmptyArrayColumn
    case c: EmptyObjectColumn => new BitsetColumn(definedAt & c.definedAt(from, to)) with EmptyObjectColumn
    case c: NullColumn        => new BitsetColumn(definedAt & c.definedAt(from, to)) with NullColumn
  }

  def filterBy(p: Int => Boolean) = CF1P("builtin::ct::filterBy") {
    case c: BoolColumn           => new BoolColumn { def apply(row: Int)                   = c(row); def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
    case c: LongColumn           => new LongColumn { def apply(row: Int)                   = c(row); def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
    case c: DoubleColumn         => new DoubleColumn { def apply(row: Int)                 = c(row); def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
    case c: NumColumn            => new NumColumn { def apply(row: Int)                    = c(row); def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
    case c: StrColumn            => new StrColumn { def apply(row: Int)                    = c(row); def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
    case c: OffsetDateTimeColumn => new OffsetDateTimeColumn { def apply(row: Int) = c(row); def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
    case c: OffsetTimeColumn     => new OffsetTimeColumn { def apply(row: Int)         = c(row); def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
    case c: OffsetDateColumn     => new OffsetDateColumn { def apply(row: Int)         = c(row); def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
    case c: LocalDateTimeColumn  => new LocalDateTimeColumn { def apply(row: Int)   = c(row); def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
    case c: LocalTimeColumn      => new LocalTimeColumn { def apply(row: Int)           = c(row); def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
    case c: LocalDateColumn      => new LocalDateColumn { def apply(row: Int)           = c(row); def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
    case c: IntervalColumn       => new IntervalColumn { def apply(row: Int)             = c(row); def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
    case c: HomogeneousArrayColumn[a] =>
      new HomogeneousArrayColumn[a] {
        val tpe = c.tpe
        def apply(row: Int) = c(row)
        def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row)
      }
    case c: EmptyArrayColumn  => new EmptyArrayColumn { def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
    case c: EmptyObjectColumn => new EmptyObjectColumn { def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
    case c: NullColumn        => new NullColumn { def isDefinedAt(row: Int) = c.isDefinedAt(row) && p(row) }
  }

  val isSatisfied = CF1P("builtin::ct::isSatisfied") {
    case c: BoolColumn =>
      new BoolColumn {
        def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row)
        def apply(row: Int)       = isDefinedAt(row)
      }
  }

  def MaskedUnion(leftMask: BitSet) = CF2P("builtin::ct::maskedUnion") {
    case (left: BoolColumn, right: BoolColumn) =>
      new UnionColumn(left, right) with BoolColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: LongColumn, right: LongColumn) =>
      new UnionColumn(left, right) with LongColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: DoubleColumn, right: DoubleColumn) =>
      new UnionColumn(left, right) with DoubleColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: NumColumn, right: NumColumn) =>
      new UnionColumn(left, right) with NumColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: StrColumn, right: StrColumn) =>
      new UnionColumn(left, right) with StrColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: OffsetDateTimeColumn, right: OffsetDateTimeColumn) =>
      new UnionColumn(left, right) with OffsetDateTimeColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: OffsetTimeColumn, right: OffsetTimeColumn) =>
      new UnionColumn(left, right) with OffsetTimeColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: OffsetDateColumn, right: OffsetDateColumn) =>
      new UnionColumn(left, right) with OffsetDateColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: LocalDateTimeColumn, right: LocalDateTimeColumn) =>
      new UnionColumn(left, right) with LocalDateTimeColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: LocalTimeColumn, right: LocalTimeColumn) =>
      new UnionColumn(left, right) with LocalTimeColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: LocalDateColumn, right: LocalDateColumn) =>
      new UnionColumn(left, right) with LocalDateColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: IntervalColumn, right: IntervalColumn) =>
      new UnionColumn(left, right) with IntervalColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: HomogeneousArrayColumn[a], right: HomogeneousArrayColumn[b]) if left.tpe == right.tpe =>
      new UnionColumn(left, right) with HomogeneousArrayColumn[a] {
        val tpe = left.tpe
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row).asInstanceOf[Array[a]]
      }

    case (left: EmptyArrayColumn, right: EmptyArrayColumn)   => new UnionColumn(left, right) with EmptyArrayColumn
    case (left: EmptyObjectColumn, right: EmptyObjectColumn) => new UnionColumn(left, right) with EmptyObjectColumn
    case (left: NullColumn, right: NullColumn)               => new UnionColumn(left, right) with NullColumn
  }
}
