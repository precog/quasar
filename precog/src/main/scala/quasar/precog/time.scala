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

package quasar.precog

import java.time.{Duration, Instant, Period, LocalDateTime}
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.ISO_DATE_TIME

trait PackageTime {

  implicit class QuasarDurationOps(private val x: Duration) {
    def getMillis: Long = x.toMillis
  }
  implicit class QuasarInstantOps(private val x: Instant) {
    def getMillis: Long           = x.toEpochMilli
    def -(y: Instant): Duration = java.time.Duration.between(x, y)
  }
  implicit class QuasarPeriodOps(private val x: Period) {
    def getMillis: Long       = toDuration.getMillis
    def toDuration: Duration = java.time.Duration from x
  }
  implicit class QuasarLocalDateTimeOps(private val x: LocalDateTime) {
    def until(end: LocalDateTime): Period = java.time.Period.between(x.toLocalDate, end.toLocalDate)
    def toUtcInstant: Instant        = x toInstant UTC
    def getMillis: Long              = toUtcInstant.toEpochMilli
  }

  object duration {
    def fromMillis(ms: Long): Duration = java.time.Duration ofMillis ms
  }
  object period {
    def fromMillis(ms: Long): Period = java.time.Period from (duration fromMillis ms)
  }
  object instant {
    def zero: Instant                             = fromMillis(0L)
    def ofEpoch(secs: Long, nanos: Long): Instant = java.time.Instant.ofEpochSecond(secs, nanos)
    def now(): Instant                            = java.time.Instant.now
    def fromMillis(ms: Long): Instant             = java.time.Instant.ofEpochMilli(ms)
    def apply(s: String): Instant                 = java.time.Instant parse s
  }
  object dateTime {
    def minimum: LocalDateTime              = java.time.LocalDateTime.MIN
    def maximum: LocalDateTime              = java.time.LocalDateTime.MAX
    def fromIso(s: String): LocalDateTime   = java.time.LocalDateTime.parse(s, ISO_DATE_TIME)
    def showIso(d: LocalDateTime): String   = d format ISO_DATE_TIME
    def zero: LocalDateTime                 = fromMillis(0)
    def now(): LocalDateTime                = java.time.LocalDateTime.now
    def fromMillis(ms: Long): LocalDateTime = java.time.LocalDateTime.ofInstant(instant fromMillis ms, UTC)
    def apply(s: String): LocalDateTime     = java.time.LocalDateTime.parse(s)
  }
}
