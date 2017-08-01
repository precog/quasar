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

package quasar.mimir

import quasar.precog.util.NumericComparisons
import quasar.precog.util.DateTimeUtil

import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.table._

import java.time.{Instant, Period, ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

trait TimeLibModule[M[+_]] extends ColumnarTableLibModule[M] {
  trait TimeLib extends ColumnarTableLib {
    import StdLib.StrAndDateT

    val TimeNamespace = Vector("std", "time")

    override def _lib1 = super._lib1 ++ Set(
      GetMillis,
      Date,
      Time,
      TimeZone,
      Season,

      Year,
      QuarterOfYear,
      MonthOfYear,
      WeekOfMonth,
      DayOfYear,
      DayOfMonth,
      DayOfWeek,
      HourOfDay,
      MinuteOfHour,
      SecondOfMinute,
      NanoOfSecond,

      ParsePeriod)

    override def _lib2 = super._lib2 ++ Set(
      YearsPlus,
      MonthsPlus,
      WeeksPlus,
      DaysPlus,
      HoursPlus,
      MinutesPlus,
      SecondsPlus,
      NanosPlus,

      YearsBetween,
      MonthsBetween,
      WeeksBetween,
      DaysBetween,
      HoursBetween,
      MinutesBetween,
      SecondsBetween,

      MillisToISO,
      ChangeTimeZone,
      ParseDateTime,

      MinTimeOf,
      MaxTimeOf)

    object ParseDateTime extends Op2F2(TimeNamespace, "parseDateTime") {
      val tpe = BinaryOperationType(StrAndDateT, JTextT, JDateT)
      def f2: F2 = CF2P("builtin::time::parseDateTime") {
        case (c1: DateColumn, c2: StrColumn) => c1

        case (c1: StrColumn, c2: StrColumn) => new DateColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && DateTimeUtil.isValidFormat(c1(row), c2(row))

          def apply(row: Int): ZonedDateTime = {
            val time = c1(row)
            val fmt = c2(row)

            ZonedDateTime.parse(time, DateTimeFormatter.ofPattern(fmt))
          }
        }
      }
    }

    object ParsePeriod extends Op1F1(TimeNamespace, "parsePeriod") {
      val tpe = UnaryOperationType(JTextT, JPeriodT)
      def f1: F1 = CF1P("builtin::time::parsePeriod") {
        case (c: StrColumn) => new PeriodColumn {
          def isDefinedAt(row: Int) = c.isDefinedAt(row) && DateTimeUtil.isValidPeriod(c(row))
          def apply(row: Int): Period = Period.parse(c(row))
        }
      }
    }

    //ok to `get` because CoerceToDate is defined for StrColumn
    def createDateCol(c: StrColumn) =
      cf.util.CoerceToDate(c) collect { case (dc: DateColumn) => dc } get

    object ChangeTimeZone extends Op2F2(TimeNamespace, "changeTimeZone") {
      val tpe = BinaryOperationType(StrAndDateT, JTextT, JDateT)

      def f2: F2 = CF2P("builtin::time::changeTimeZone") {
        case (c1: DateColumn, c2: StrColumn) => newColumn(c1, c2)
        case (c1: StrColumn, c2: StrColumn) => newColumn(createDateCol(c1), c2)
      }

      def newColumn(c1: DateColumn, c2: StrColumn): DateColumn = new DateColumn {
        def isDefinedAt(row: Int) =
          c1.isDefinedAt(row) && c2.isDefinedAt(row) && DateTimeUtil.isValidTimeZone(c2(row))

        def apply(row: Int) = {
          val time = c1(row)
          val tz = c2(row)
          time.withZoneSameLocal(ZoneId.of(tz))
        }
      }
    }

    trait ExtremeTime extends Op2F2 {
      val tpe = BinaryOperationType(StrAndDateT, StrAndDateT, JDateT)
      def f2: F2 = CF2P("builtin::time::extremeTime") {
        case (c1: StrColumn, c2: StrColumn) =>
          val dateCol1 = createDateCol(c1)
          val dateCol2 = createDateCol(c2)

          new Map2Column(dateCol1, dateCol2) with DateColumn {
            def apply(row: Int) = computeExtreme(dateCol1(row), dateCol2(row))
          }

        case (c1: DateColumn, c2: StrColumn) =>
          val dateCol2 = createDateCol(c2)

          new Map2Column(c1, dateCol2) with DateColumn {
            def apply(row: Int) = computeExtreme(c1(row), dateCol2(row))
          }

        case (c1: StrColumn, c2: DateColumn) =>
          val dateCol1 = createDateCol(c1)

          new Map2Column(dateCol1, c2) with DateColumn {
            def apply(row: Int) = computeExtreme(dateCol1(row), c2(row))
          }

        case (c1: DateColumn, c2: DateColumn) => new Map2Column(c1, c2) with DateColumn {
          def apply(row: Int) = computeExtreme(c1(row), c2(row))
        }
      }

      def computeExtreme(t1: ZonedDateTime, t2: ZonedDateTime): ZonedDateTime
    }

    object MinTimeOf extends Op2F2(TimeNamespace, "minTimeOf") with ExtremeTime { 
      def computeExtreme(t1: ZonedDateTime, t2: ZonedDateTime): ZonedDateTime = {
        val res: Int = NumericComparisons.compare(t1, t2)
        if (res < 0) t1
        else t2
      }
    }

    object MaxTimeOf extends Op2F2(TimeNamespace, "maxTimeOf") with ExtremeTime { 
      def computeExtreme(t1: ZonedDateTime, t2: ZonedDateTime): ZonedDateTime = {
        val res: Int = NumericComparisons.compare(t1, t2)
        if (res > 0) t1
        else t2
      }
    }

    trait TimePlus extends Op2F2 {
      val tpe = BinaryOperationType(StrAndDateT, JNumberT, JDateT)
      def f2: F2 = CF2P("builtin::time::timePlus") {
        case (c1: StrColumn, c2: LongColumn) =>
          val dateCol1 = createDateCol(c1)

          new Map2Column(dateCol1, c2) with DateColumn {
            def apply(row: Int) = plus(dateCol1(row), c2(row).toInt)
          }      

        case (c1: StrColumn, c2: NumColumn) =>
          val dateCol1 = createDateCol(c1)

          new Map2Column(dateCol1, c2) with DateColumn {
            def apply(row: Int) = plus(dateCol1(row), c2(row).toInt)
          }      

        case (c1: StrColumn, c2: DoubleColumn) =>
          val dateCol1 = createDateCol(c1)

          new Map2Column(dateCol1, c2) with DateColumn {
            def apply(row: Int) = plus(dateCol1(row), c2(row).toInt)
          }      

        case (c1: DateColumn, c2: LongColumn) => new Map2Column(c1, c2) with DateColumn {
          def apply(row: Int) = plus(c1(row), c2(row).toInt)
        }      

        case (c1: DateColumn, c2: NumColumn) => new Map2Column(c1, c2) with DateColumn {
          def apply(row: Int) = plus(c1(row), c2(row).toInt)
        }      

        case (c1: DateColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DateColumn {
          def apply(row: Int) = plus(c1(row), c2(row).toInt)
        }
      }

      def plus(d: ZonedDateTime, i: Int): ZonedDateTime
    }

    object YearsPlus extends Op2F2(TimeNamespace, "yearsPlus") with TimePlus {
      def plus(d: ZonedDateTime, i: Int) = d.plusYears(i)
    }

    object MonthsPlus extends Op2F2(TimeNamespace, "monthsPlus") with TimePlus {
      def plus(d: ZonedDateTime, i: Int) = d.plusMonths(i)
    }

    object WeeksPlus extends Op2F2(TimeNamespace, "weeksPlus") with TimePlus {
      def plus(d: ZonedDateTime, i: Int) = d.plusWeeks(i)
    }

    object DaysPlus extends Op2F2(TimeNamespace, "daysPlus") with TimePlus {
      def plus(d: ZonedDateTime, i: Int) = d.plusDays(i)
    }

    object HoursPlus extends Op2F2(TimeNamespace, "hoursPlus") with TimePlus {
      def plus(d: ZonedDateTime, i: Int) = d.plusHours(i)
    }

    object MinutesPlus extends Op2F2(TimeNamespace, "minutesPlus") with TimePlus {
      def plus(d: ZonedDateTime, i: Int) = d.plusMinutes(i)
    }

    object SecondsPlus extends Op2F2(TimeNamespace, "secondsPlus") with TimePlus {
      def plus(d: ZonedDateTime, i: Int) = d.plusSeconds(i)
    }

    object NanosPlus extends Op2F2(TimeNamespace, "nanosPlus") with TimePlus{
      def plus(d: ZonedDateTime, i: Int) = d.plusNanos(i)
    }
    
    trait TimeBetween extends Op2F2 {
      val tpe = BinaryOperationType(StrAndDateT, StrAndDateT, JNumberT)
      def f2: F2 = CF2P("builtin::time::timeBetween") {
        case (c1: StrColumn, c2: StrColumn) =>
          val dateCol1 = createDateCol(c1)
          val dateCol2 = createDateCol(c2)

        new Map2Column(dateCol1, dateCol2) with LongColumn {
          def apply(row: Int) = between(dateCol1(row), dateCol2(row))
        }

        case (c1: DateColumn, c2: StrColumn) =>
          val dateCol2 = createDateCol(c2)

        new Map2Column(c1, dateCol2) with LongColumn {
          def apply(row: Int) = between(c1(row), dateCol2(row))
        }

        case (c1: StrColumn, c2: DateColumn) =>
          val dateCol1 = createDateCol(c1)

        new Map2Column(dateCol1, c2) with LongColumn {
          def apply(row: Int) = between(dateCol1(row), c2(row))
        }

        case (c1: DateColumn, c2: DateColumn) => new Map2Column(c1, c2) with LongColumn {
          def apply(row: Int) = between(c1(row), c2(row))
        }
      }

      def between(d1: ZonedDateTime, d2: ZonedDateTime): Long
    }

    object YearsBetween extends Op2F2(TimeNamespace, "yearsBetween") with TimeBetween {
      def between(d1: ZonedDateTime, d2: ZonedDateTime) = ChronoUnit.YEARS.between(d1, d2)
    }

    object MonthsBetween extends Op2F2(TimeNamespace, "monthsBetween") with TimeBetween {
      def between(d1: ZonedDateTime, d2: ZonedDateTime) = ChronoUnit.MONTHS.between(d1, d2)
    }

    object WeeksBetween extends Op2F2(TimeNamespace, "weeksBetween") with TimeBetween {
      def between(d1: ZonedDateTime, d2: ZonedDateTime) = ChronoUnit.WEEKS.between(d1, d2)
     }

    object DaysBetween extends Op2F2(TimeNamespace, "daysBetween") with TimeBetween {
      def between(d1: ZonedDateTime, d2: ZonedDateTime) = ChronoUnit.DAYS.between(d1, d2)
    }

    object HoursBetween extends Op2F2(TimeNamespace, "hoursBetween") with TimeBetween {
      def between(d1: ZonedDateTime, d2: ZonedDateTime) = ChronoUnit.HOURS.between(d1, d2)
    }

    object MinutesBetween extends Op2F2(TimeNamespace, "minutesBetween") with TimeBetween {
      def between(d1: ZonedDateTime, d2: ZonedDateTime) = ChronoUnit.MINUTES.between(d1, d2)
    }

    object SecondsBetween extends Op2F2(TimeNamespace, "secondsBetween") with TimeBetween {
      def between(d1: ZonedDateTime, d2: ZonedDateTime) = ChronoUnit.SECONDS.between(d1, d2)
    }

    object MillisToISO extends Op2F2(TimeNamespace, "millisToISO") {
      def checkDefined(c1: Column, c2: StrColumn, row: Int) =
        c1.isDefinedAt(row) && c2.isDefinedAt(row) && DateTimeUtil.isValidTimeZone(c2(row))

      val tpe = BinaryOperationType(JNumberT, JTextT, JDateT)
      def f2: F2 = CF2P("builtin::time::millisToIso") {
        case (c1: LongColumn, c2: StrColumn) => new DateColumn {
          def isDefinedAt(row: Int) = checkDefined(c1, c2, row)

          def apply(row: Int) = { 
            val time = c1(row)
            val tz = c2(row)
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.of(tz))
          }
        }      

        case (c1: NumColumn, c2: StrColumn) => new DateColumn {
          def isDefinedAt(row: Int) = checkDefined(c1, c2, row) && c1(row) >= Long.MinValue && c1(row) <= Long.MaxValue

          def apply(row: Int) = { 
            val time = c1(row)
            val tz = c2(row)
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(time.toLong), ZoneId.of(tz))
          }
        }

        case (c1: DoubleColumn, c2: StrColumn) => new DateColumn {
          def isDefinedAt(row: Int) = checkDefined(c1, c2, row) && c1(row) >= Long.MinValue && c1(row) <= Long.MaxValue

          def apply(row: Int) = { 
            val time = c1(row)
            val tz = c2(row)
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(time.toLong), ZoneId.of(tz))
          }
        }
      }
    }

    object GetMillis extends Op1F1(TimeNamespace, "getMillis") {
      val tpe = UnaryOperationType(StrAndDateT, JNumberT)
      def f1: F1 = CF1P("builtin::time::getMillis") {
        case (c: StrColumn) =>  
          val dateCol = createDateCol(c)

          new Map1Column(dateCol) with LongColumn {
            def apply(row: Int) = dateCol(row).toInstant.toEpochMilli()
        }
        case (c: DateColumn) => new Map1Column(c) with LongColumn {
          def apply(row: Int) = c(row).toInstant.toEpochMilli()
        }
      }
    }

    object TimeZone extends Op1F1(TimeNamespace, "timeZone") {
      val tpe = UnaryOperationType(StrAndDateT, JTextT)
      def f1: F1 = CF1P("builtin::time::timeZone") {
        case (c: StrColumn) =>
          val dateCol = createDateCol(c)

          new Map1Column(dateCol) with StrColumn {
            def apply(row: Int) = dateCol(row).getZone().getId()
          }
        case (c: DateColumn) => new Map1Column(c) with StrColumn {
          def apply(row: Int) = c(row).getZone().getId()
        }
      }
    }

    object Season extends Op1F1(TimeNamespace, "season") {
      def determineSeason(time: ZonedDateTime): String = { 
        val day = time.getDayOfYear()
        
        if (day >= 79 & day < 171) "spring"
        else if (day >= 171 & day < 265) "summer"
        else if (day >= 265 & day < 355) "fall"
        else "winter"
      }
    
      val tpe = UnaryOperationType(StrAndDateT, JTextT)
      def f1: F1 = CF1P("builtin::time::season") {
        case (c: StrColumn) =>
          val dateCol = createDateCol(c)

          new Map1Column(dateCol) with StrColumn {
            def apply(row: Int) = determineSeason(dateCol(row))
          }
        case (c: DateColumn) => new Map1Column(c) with StrColumn {
          def apply(row: Int) = determineSeason(c(row))
        }
      }
    } 

    trait TimeFraction extends Op1F1 {
      val tpe = UnaryOperationType(StrAndDateT, JNumberT)
      def f1: F1 = CF1P("builtin::time::timeFraction") {
        case (c: StrColumn) =>
          val dateCol = createDateCol(c)

          new Map1Column(dateCol) with LongColumn {
            def apply(row: Int) = fraction(dateCol(row))
          }
        case (c: DateColumn) => new Map1Column(c) with LongColumn {
          def apply(row: Int) = fraction(c(row))
        }
      }

      def fraction(d: ZonedDateTime): Int
    }

    object Year extends Op1F1(TimeNamespace, "year") with TimeFraction {
      def fraction(d: ZonedDateTime) = d.getYear()
    }

    object QuarterOfYear extends Op1F1(TimeNamespace, "quarter") with TimeFraction {
      def fraction(d: ZonedDateTime) = ((d.getMonthValue() - 1) / 3) + 1
    }

    object MonthOfYear extends Op1F1(TimeNamespace, "monthOfYear") with TimeFraction {
      def fraction(d: ZonedDateTime) = d.getMonthValue()
    }

    object WeekOfMonth extends Op1F1(TimeNamespace, "weekOfMonth") with TimeFraction {
      def fraction(newTime: ZonedDateTime) = {
        val dayOfMonth = newTime.getDayOfMonth()
        val firstDate = newTime.withDayOfMonth(1)
        val firstDayOfWeek = firstDate.getDayOfWeek().getValue()
        val offset = firstDayOfWeek - 1
        ((dayOfMonth + offset) / 7) + 1
      } 
    }
   
    object DayOfYear extends Op1F1(TimeNamespace, "dayOfYear") with TimeFraction {
      def fraction(d: ZonedDateTime) = d.getDayOfYear()
    }

    object DayOfMonth extends Op1F1(TimeNamespace, "dayOfMonth") with TimeFraction {
      def fraction(d: ZonedDateTime) = d.getDayOfMonth()
    }

    object DayOfWeek extends Op1F1(TimeNamespace, "dayOfWeek") with TimeFraction {
      def fraction(d: ZonedDateTime) = d.getDayOfWeek().getValue() % 7 // java indexes 1-7 while quasar expects 0-6
    }

    object HourOfDay extends Op1F1(TimeNamespace, "hourOfDay") with TimeFraction {
      def fraction(d: ZonedDateTime) = d.getHour()
    }

    object MinuteOfHour extends Op1F1(TimeNamespace, "minuteOfHour") with TimeFraction {
      def fraction(d: ZonedDateTime) = d.getMinute()
    }

    object SecondOfMinute extends Op1F1(TimeNamespace, "secondOfMinute") with TimeFraction {
      def fraction(d: ZonedDateTime) = d.getSecond()
    }
      
    object NanoOfSecond extends Op1F1(TimeNamespace, "nanoOfSecond") with TimeFraction {
      def fraction(d: ZonedDateTime) = d.getNano()
    }

    trait TimeTruncation extends Op1F1 {
      val tpe = UnaryOperationType(StrAndDateT, JTextT)
      def f1: F1 = CF1P("builtin::time::truncation") {
        case (c: StrColumn) =>
          val dateCol = createDateCol(c)

          new Map1Column(dateCol) with StrColumn {
            def apply(row: Int) = dateCol(row).format(fmt)
          }
        case (c: DateColumn) => new Map1Column(c) with StrColumn {
          def apply(row: Int) = c(row).format(fmt)
        }
      }

      def fmt: DateTimeFormatter
    }

    object Date extends Op1F1(TimeNamespace, "date") with TimeTruncation {
      val fmt = DateTimeFormatter.ISO_DATE
    }

    object Time extends Op1F1(TimeNamespace, "time") with TimeTruncation {
      val fmt = DateTimeFormatter.ISO_LOCAL_TIME
    }
  }
}
