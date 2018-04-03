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

import quasar.time.OffsetDate
import quasar.yggdrasil.table._

import java.time._

object ColumnDateTimeExtractors {

  object AsTimeColumn {
    def unapply(col: Column): Option[LocalTimeColumn] = col match {
      case c: OffsetDateTimeColumn => Some(new LocalTimeColumn {
        def apply(row: Int): LocalTime = c(row).toLocalTime
        def isDefinedAt(row: Int): Boolean = c.isDefinedAt(row)
      })
      case c: OffsetTimeColumn => Some(new LocalTimeColumn {
        def apply(row: Int): LocalTime = c(row).toLocalTime
        def isDefinedAt(row: Int): Boolean = c.isDefinedAt(row)
      })
      case c: LocalDateTimeColumn => Some(new LocalTimeColumn {
        def apply(row: Int): LocalTime = c(row).toLocalTime
        def isDefinedAt(row: Int): Boolean = c.isDefinedAt(row)
      })
      case c: LocalTimeColumn => Some(c)
      case _ => None
    }
  }

  object AsDateColumn {
    def unapply(col: Column): Option[LocalDateColumn] = col match {
      case c: OffsetDateTimeColumn => Some(new LocalDateColumn {
        def apply(row: Int): LocalDate = c(row).toLocalDate
        def isDefinedAt(row: Int): Boolean = c.isDefinedAt(row)
      })
      case c: OffsetDateColumn => Some(new LocalDateColumn {
        def apply(row: Int): LocalDate = c(row).date
        def isDefinedAt(row: Int): Boolean = c.isDefinedAt(row)
      })
      case c: LocalDateTimeColumn => Some(new LocalDateColumn {
        def apply(row: Int): LocalDate = c(row).toLocalDate
        def isDefinedAt(row: Int): Boolean = c.isDefinedAt(row)
      })
      case c: LocalDateColumn => Some(c)
      case _ => None
    }
  }

  object CanAddTime {
    def unapply(col: Column): Option[LocalTime => Column] = col match {
      case c: LocalDateTimeColumn => Some(jlt => new LocalDateTimeColumn {
        def apply(row: Int): LocalDateTime = LocalDateTime.of(c(row).toLocalDate, jlt)
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
      })
      case c: LocalDateColumn => Some(jlt => new LocalDateTimeColumn {
        def apply(row: Int): LocalDateTime = LocalDateTime.of(c(row), jlt)
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
      })
      case c: OffsetDateTimeColumn => Some(jlt => new OffsetDateTimeColumn {
        def apply(row: Int): OffsetDateTime = {
          val v = c(row)
          OffsetDateTime.of(v.toLocalDate, jlt, v.getOffset)
        }
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
      })
      case c: OffsetDateColumn => Some(jlt => new OffsetDateTimeColumn {
        def apply(row: Int): OffsetDateTime = {
          val v = c(row)
          OffsetDateTime.of(v.date, jlt, v.offset)
        }
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
      })
      case _ => None
    }
  }

  object CanSetTimeZone {
    def unapply(col: Column): Option[ZoneOffset => Column] = col match {
      case c: OffsetDateTimeColumn => Some(zo => new OffsetDateTimeColumn {
        def apply(row: Int): OffsetDateTime = OffsetDateTime.of(c(row).toLocalDateTime, zo)
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
      })
      case c: OffsetDateColumn => Some(zo => new OffsetDateColumn {
        def apply(row: Int): OffsetDate = OffsetDate(c(row).date, zo)
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
      })
      case c: LocalDateTimeColumn => Some(zo => new OffsetDateTimeColumn {
        def apply(row: Int): OffsetDateTime = OffsetDateTime.of(c(row), zo)
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
      })
      case c: LocalDateColumn => Some(zo => new OffsetDateColumn {
        def apply(row: Int): OffsetDate = OffsetDate(c(row), zo)
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
      })
      case _ => None
    }
  }

  object CanRemoveTime {
    def unapply(col: Column): Option[Column] = col match {
      case c: LocalDateTimeColumn => Some(new LocalDateColumn {
        def apply(row: Int): LocalDate = c(row).toLocalDate
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
      })
      case c: OffsetDateTimeColumn => Some(new OffsetDateColumn {
        def apply(row: Int): OffsetDate = {
          val v = c(row)
          OffsetDate(v.toLocalDate, v.getOffset)
        }
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
      })
      case _ => None
    }
  }
}
