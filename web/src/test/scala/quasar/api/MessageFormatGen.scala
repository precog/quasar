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

package quasar.api

import quasar.Predef._

import org.scalacheck.{Arbitrary, Gen}
import quasar.api.MessageFormat.{JsonContentType, Csv}
import JsonPrecision.{Precise,Readable}
import JsonFormat.{LineDelimited,SingleArray}
import scalaz.scalacheck.ScalaCheckBinding._

import scalaz._, Scalaz._

object MessageFormatGen {
  implicit val arbCSV: Arbitrary[Csv] = Arbitrary(
    (Arbitrary.arbitrary[Char]   |@|
     Arbitrary.arbitrary[String] |@|
     Arbitrary.arbitrary[Char]   |@|
     Arbitrary.arbitrary[Char])(Csv.apply(_,_,_,_,None)))
  implicit val arbJsonContentType: Arbitrary[JsonContentType] = Arbitrary(
    Gen.oneOf(
      JsonContentType(Readable, LineDelimited),
      JsonContentType(Readable,SingleArray),
      JsonContentType(Precise,LineDelimited),
      JsonContentType(Precise,SingleArray)))
  implicit val arbMessageFormat: Arbitrary[MessageFormat] =
    Arbitrary(Gen.oneOf(arbCSV.arbitrary, arbJsonContentType.arbitrary))
}
