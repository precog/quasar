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

package quasar.common.data

import slamdata.Predef._

import Data.{
  LocalDate => DLocalDate,
  LocalDateTime => DLocalDateTime,
  LocalTime => DLocalTime,
  OffsetDate => DOffsetDate,
  OffsetDateTime => DOffsetDateTime,
  OffsetTime => DOffsetTime
}
import quasar.fp._
import quasar.time
import qdata.time.{OffsetDate => QOffsetDate}

import java.time.{
  LocalDate => JLocalDate,
  LocalDateTime => JLocalDateTime,
  LocalTime => JLocalTime,
  OffsetDateTime => JOffsetDateTime,
  OffsetTime => JOffsetTime,
  ZoneOffset
}

import scalaz._

object DataDateTimeExtractors {

  object CanAddTime {
    def unapply(data: Data): Option[JLocalTime => Data] = data match {
      case DLocalDateTime(v) => Some(
        jlt => DLocalDateTime(JLocalDateTime.of(v.toLocalDate, jlt)))
      case DLocalDate(v) => Some(
        jlt => DLocalDateTime(JLocalDateTime.of(v, jlt)))
      case DOffsetDateTime(v) => Some(
        jlt => DOffsetDateTime(JOffsetDateTime.of(v.toLocalDate, jlt, v.getOffset)))
      case DOffsetDate(v) => Some(
        jlt => DOffsetDateTime(JOffsetDateTime.of(v.date, jlt, v.offset)))
      case _ => None
    }
  }

  object CanLensTime {
    def unapply(data: Data): Option[Store[JLocalTime, Data]] = data match {
      case d @ DOffsetDateTime(_) => Some(time.lensTimeOffsetDateTime.store(d.value).map(DOffsetDateTime))
      case d @ DOffsetTime(_) => Some(time.lensTimeOffsetTime.store(d.value).map(DOffsetTime))
      case d @ DLocalDateTime(_) => Some(time.lensTimeLocalDateTime.store(d.value).map(DLocalDateTime))
      case d @ DLocalTime(_) => Some(Store(DLocalTime, d.value))
      case _ => None
    }
  }

  object CanLensDate {
    def unapply(data: Data): Option[Store[JLocalDate, Data]] = data match {
      case d @ DOffsetDateTime(_) => Some(time.lensDateOffsetDateTime.store(d.value).map(DOffsetDateTime))
      case d @ DOffsetDate(_) => Some(time.lensDateOffsetDate.store(d.value).map(DOffsetDate))
      case d @ DLocalDateTime(_) => Some(time.lensDateLocalDateTime.store(d.value).map(DLocalDateTime))
      case d @ DLocalDate(_) => Some(Store(DLocalDate, d.value))
      case _ => None
    }
  }

  object CanLensDateTime {
    def unapply(data: Data): Option[Store[JLocalDateTime, Data]] = data match {
      case d @ DOffsetDateTime(_) => Some(time.lensDateTimeOffsetDateTime.store(d.value).map(DOffsetDateTime))
      case d @ DLocalDateTime(_) => Some(time.lensDateTimeLocalDateTime.store(d.value).map(DLocalDateTime))
      case _ => None
    }
  }

  object CanLensTimeZone {
    def unapply(data: Data): Option[Store[ZoneOffset, Data]] = data match {
      case d @ DOffsetDateTime(_) => Some(time.lensTimeZoneOffsetDateTime.store(d.value).map(DOffsetDateTime))
      case d @ DOffsetDate(_) => Some(time.lensTimeZoneOffsetDate.store(d.value).map(DOffsetDate))
      case d @ DOffsetTime(_) => Some(time.lensTimeZoneOffsetTime.store(d.value).map(DOffsetTime))
      case _ => None
    }
  }

  object CanSetTimeZone {
    def unapply(data: Data): Option[ZoneOffset => Data] = data match {
      case d @ DOffsetDateTime(_) => Some(zo => DOffsetDateTime(JOffsetDateTime.of(d.value.toLocalDateTime, zo)))
      case d @ DOffsetDate(_) => Some(zo => DOffsetDate(QOffsetDate(d.value.date, zo)))
      case d @ DOffsetTime(_) => Some(zo => DOffsetTime(JOffsetTime.of(d.value.toLocalTime, zo)))
      case d @ DLocalDateTime(_) => Some(zo => DOffsetDateTime(JOffsetDateTime.of(d.value, zo)))
      case d @ DLocalDate(_) => Some(zo => DOffsetDate(QOffsetDate(d.value, zo)))
      case d @ DLocalTime(_) => Some(zo => DOffsetTime(JOffsetTime.of(d.value, zo)))
      case _ => None
    }
  }

  object CanRemoveTime {
    def unapply(data: Data): Option[Data] = data match {
      case DLocalDateTime(v) => Some(DLocalDate(v.toLocalDate))
      case DOffsetDateTime(v) => Some(DOffsetDate(QOffsetDate(v.toLocalDate, v.getOffset)))
      case _ => None
    }
  }
}
