/*
 * Copyright 2014–2020 SlamData Inc.
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

trait CValueGenerators {
  import CValueGenerators._

  implicit val cValueArbitray: Arbitrary[CValue] = Arbitrary(genCValue)
}

object CValueGenerators {
  def genCValue: Gen[CValue] = Gen.oneOf[CValue](
    CTrue,
    CFalse,
    CNull,
    CEmptyObject,
    CEmptyArray,
    genString.map(v => CString(v)),
    genLong.map(v => CLong(v)),
    genDouble.map(v => CDouble(v)),
    genBigDecimal.map(v => CNum(v)),
    TimeGenerators.genLocalDateTime.map(v => CLocalDateTime(v)),
    TimeGenerators.genLocalDate.map(v => CLocalDate(v)),
    TimeGenerators.genLocalTime.map(v => CLocalTime(v)),
    TimeGenerators.genOffsetDateTime.map(v => COffsetDateTime(v)),
    TimeGenerators.genOffsetDate.map(v => COffsetDate(v)),
    TimeGenerators.genOffsetTime.map(v => COffsetTime(v)),
    TimeGenerators.genInterval.map(v => CInterval(v)),
    arrayOf(genByte).map(CBinary(_)))
}
