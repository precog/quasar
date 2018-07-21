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

import quasar.pkg.tests._
import qdata.time.TimeGenerators

case class RepresentableData(data: Data)

trait RepresentableDataGenerators {
  import Data._

  // See DataCodec.representable for what needs to be avoided in this generator
  val atomicData: Gen[Data] = Gen.oneOf[Data](
    Null,
    True,
    False,
    Gen.alphaStr                     ^^ Str,
    DataGenerators.defaultInt        ^^ Int,
    DataGenerators.defaultDec        ^^ Dec,
    TimeGenerators.genOffsetDateTime ^^ OffsetDateTime,
    TimeGenerators.genOffsetDate     ^^ Data.OffsetDate,
    TimeGenerators.genOffsetTime     ^^ OffsetTime,
    TimeGenerators.genLocalDateTime  ^^ LocalDateTime,
    TimeGenerators.genLocalDate      ^^ LocalDate,
    TimeGenerators.genLocalTime      ^^ LocalTime,
    TimeGenerators.genInterval       ^^ Interval)

  implicit val representableDataArbitrary: Arbitrary[RepresentableData] = Arbitrary(
    DataGenerators.genDataDefault(atomicData)) ^^ RepresentableData
}

object RepresentableDataGenerators extends RepresentableDataGenerators
