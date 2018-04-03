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

package quasar.time

import slamdata.Predef.{SuppressWarnings, _}
import java.time.format.DateTimeFormatter
import java.time.temporal._
import java.time.{LocalDate, Period, ZoneOffset}

import scalaz.std.anyVal._
import scalaz.syntax.equal._

// this doesn't exist in java.time but is still supported by several connectors
final case class OffsetDate(date: LocalDate, offset: ZoneOffset) extends TemporalAccessor {
  def compareTo(other: OffsetDate): Int = {
    val dateCompare = date.compareTo(other.date)

    if (dateCompare === 0)
      offset.compareTo(other.offset)
    else
      dateCompare
  }

  override def toString(): String =
    DateTimeFormatter.ISO_OFFSET_DATE.format(this)

  def plus(period: Period): OffsetDate =
    OffsetDate(date.plus(period), offset)

  def minus(period: Period): OffsetDate =
    OffsetDate(date.minus(period), offset)

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  override def isSupported(field: TemporalField) =
    (field eq ChronoField.OFFSET_SECONDS) || date.isSupported(field)

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  override def getLong(field: TemporalField) = {
    if (field eq ChronoField.OFFSET_SECONDS)
      offset.getTotalSeconds.toLong
    else
      date.getLong(field)
  }
}

object OffsetDate {
  def parse(str: String) =
    DateTimeFormatter.ISO_OFFSET_DATE.parse(str, query)

  val query: TemporalQuery[OffsetDate] = new TemporalQuery[OffsetDate] {
    override def queryFrom(temporal: TemporalAccessor): OffsetDate = {
      val day = temporal.getLong(ChronoField.EPOCH_DAY)
      val zoneOffset = temporal.get(ChronoField.OFFSET_SECONDS)
      OffsetDate(LocalDate.ofEpochDay(day), ZoneOffset.ofTotalSeconds(zoneOffset))
    }
  }
}
