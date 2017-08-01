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

package quasar

import slamdata.Predef._
import quasar.pkg.tests._
import quasar.std.TemporalPart

import java.time.{Duration, Instant, LocalDate, LocalTime}

trait DateArbitrary {
  import TemporalPart._

  implicit val arbTemporalPart: Arbitrary[TemporalPart] = genTemporalPart
  implicit val arbInstant: Arbitrary[Instant]           = genInstant
  implicit val arbDuration: Arbitrary[Duration]         = genDuration
  implicit val arbDate: Arbitrary[LocalDate]            = genDate
  implicit val arbTime: Arbitrary[LocalTime]            = genTime

  implicit def genTemporalPart: Gen[TemporalPart] = Gen.oneOf(
    Century, Day, Decade, Hour, Microsecond, Millennium,
    Millisecond, Minute, Month, Quarter, Second, Week, Year)

  private def genSeconds: Gen[Long]     = genInt ^^ (_.toLong)
  private def genSecondOfDay: Gen[Long] = choose(0L, 24L * 60 * 60 - 1)
  private def genMillis: Gen[Long]      = choose(0L, 999L)
  private def genNanos: Gen[Long]       = genMillis ^^ (_ * 1000000)

  def genInstant: Gen[Instant]   = (genSeconds, genNanos) >> Instant.ofEpochSecond
  def genDuration: Gen[Duration] = (genSeconds, genNanos) >> Duration.ofSeconds
  def genDate: Gen[LocalDate]    = genSeconds ^^ LocalDate.ofEpochDay
  def genTime: Gen[LocalTime]    = genSecondOfDay ^^ LocalTime.ofSecondOfDay
}

object DateArbitrary extends DateArbitrary
