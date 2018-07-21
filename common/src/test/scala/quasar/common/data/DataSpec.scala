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
import quasar.Qspec
import quasar.contrib.iota.copkTraverse
import quasar.ejson.EJson
import qdata.time.DateTimeInterval

import matryoshka.implicits._
import matryoshka.patterns._

import scalaz._, Scalaz._

import java.time.{LocalDate, LocalTime}

class DataSpec extends Qspec with DataGenerators {

  def roundtrip(data: Data): Option[Data] =
    data.hyloM[Option, CoEnv[Data, EJson, ?], Data](
      interpretM[Option, EJson, Data, Data]({
        case d @ Data.NA => d.some // Data.NA does not roundtrip
        case _ => None
      },
        Data.fromEJson >>> (_.some)),
      Data.toEJson[EJson].apply(_).some)

  "round trip a date" >> {
    val data: Data = Data.LocalDate(LocalDate.of(1992, 6, 30))
    roundtrip(data) must_=== data.some
  }

  "round trip a time" >> {
    val data: Data = Data.LocalTime(LocalTime.of(7, 16, 30, 17))
    roundtrip(data) must_=== data.some
  }

  "round trip an interval" >> {
    val data: Data = Data.Interval(DateTimeInterval.make(1982, 4, 12, 18, 123456789))
    roundtrip(data) must_=== data.some
  }

  "round trip an interval to nano precision" >> {
    val data: Data = Data.Interval(DateTimeInterval.ofNanos(1811451749862000000L))
    roundtrip(data) must_=== data.some
  }

  "round trip Data => EJson => Data" >> prop { data: Data =>
    roundtrip(data) must_=== data.some
  }.set(minTestsOk = 1000)
}
