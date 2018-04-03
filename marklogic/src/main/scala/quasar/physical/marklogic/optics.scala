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

package quasar.physical.marklogic

import slamdata.Predef._
import java.util.Base64

import monocle.Prism
import java.time._
import java.time.format._
import java.time.temporal.{TemporalAccessor, TemporalQuery}

import quasar.time.{DateTimeInterval, OffsetDate}

import scalaz._

object optics {
  val base64Bytes = Prism[String, ImmutableArray[Byte]](
    s => \/.fromTryCatchNonFatal(Base64.getDecoder.decode(s))
           .map(ImmutableArray.fromArray)
           .toOption
  )((Base64.getEncoder.encodeToString(_)) compose (_.toArray))

  val isoDuration = Prism[String, DateTimeInterval](
    DateTimeInterval.parse)(_.toString)

  val isoOffsetDateTime: Prism[String, OffsetDateTime] = temporal(OffsetDateTime from _, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  val isoOffsetDate: Prism[String, OffsetDate]         =
    Prism[String, OffsetDate](s => \/.fromTryCatchNonFatal(OffsetDate.parse(s)).toOption)(DateTimeFormatter.ISO_OFFSET_DATE.format)
  val isoOffsetTime: Prism[String, OffsetTime]         = temporal(OffsetTime     from _, DateTimeFormatter.ISO_OFFSET_TIME)
  val isoLocalDateTime: Prism[String, LocalDateTime]   = temporal(LocalDateTime  from _, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
  val isoLocalDate: Prism[String, LocalDate]           = temporal(LocalDate      from _, DateTimeFormatter.ISO_LOCAL_DATE)
  val isoLocalTime: Prism[String, LocalTime]           = temporal(LocalTime      from _, DateTimeFormatter.ISO_LOCAL_TIME)

  ////

  private def temporal[T <: TemporalAccessor](f: TemporalAccessor => T, fmt: DateTimeFormatter): Prism[String, T] = {
    val tq = new TemporalQuery[T] { def queryFrom(q: TemporalAccessor): T = f(q) }
    Prism[String, T](s => \/.fromTryCatchNonFatal(fmt.parse(s, tq)).toOption)(fmt.format)
  }
}
