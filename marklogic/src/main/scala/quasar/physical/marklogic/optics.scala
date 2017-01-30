/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._

import java.util.Base64

import monocle.Prism
import java.time._
import java.time.format._
import java.time.temporal.{TemporalAccessor, TemporalQuery}
import scalaz._, Scalaz._

object optics {
  val base64Bytes = Prism[String, ImmutableArray[Byte]](
    s => \/.fromTryCatchNonFatal(Base64.getDecoder.decode(s))
           .map(ImmutableArray.fromArray)
           .toOption
  )((Base64.getEncoder.encodeToString(_)) compose (_.toArray))

  val isoDuration = Prism[String, Duration](
    s => \/.fromTryCatchNonFatal(Duration.parse(s)).toOption)(
    d => (d.isNegative either d.negated or d).umap(_.toString).map("-" + _).merge)

  val isoInstant:   Prism[String, Instant]   = temporal(Instant   from _, DateTimeFormatter.ISO_INSTANT)
  val isoLocalDate: Prism[String, LocalDate] = temporal(LocalDate from _, DateTimeFormatter.ISO_DATE)
  val isoLocalTime: Prism[String, LocalTime] = temporal(LocalTime from _, DateTimeFormatter.ISO_TIME)

  ////

  private def temporal[T <: TemporalAccessor](f: TemporalAccessor => T, fmt: DateTimeFormatter): Prism[String, T] = {
    val tq = new TemporalQuery[T] { def queryFrom(q: TemporalAccessor): T = f(q) }
    Prism[String, T](s => \/.fromTryCatchNonFatal(fmt.parse(s, tq)).toOption)(fmt.format)
  }
}
