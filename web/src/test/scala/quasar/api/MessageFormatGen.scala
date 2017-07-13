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

package quasar.api

import slamdata.Predef._
import quasar.csv.CsvParser

import org.scalacheck.{Arbitrary, Gen}
import quasar.api.MessageFormat.{JsonContentType, Csv}
import JsonPrecision.{Precise,Readable}
import JsonFormat.{LineDelimited,SingleArray}
import scalaz.scalacheck.ScalaCheckBinding._

import scalaz._, Scalaz._

object MessageFormatGen {

  // The Content-Type spec specifies that control characters are not allowed which is
  // why we use alphaChar here
  // See https://github.com/tototoshi/scala-csv/issues/98 for why actually let's avoid alphaChar for now
  // and go with relatively "standard" csv formats
  implicit val arbFormat: Arbitrary[CsvParser.Format] = Arbitrary(
    for {
      del   <- Gen.oneOf(List(',', '\t', '|', ':', ';'))
      quote <- Gen.oneOf(List('"', '\''))
      esc   <- Gen.oneOf(quote, '\\')
      term  <- Gen.oneOf(List("\r\n")) // See https://github.com/tototoshi/scala-csv/issues/97 for why `lineTerminator` must be constant for now
    } yield CsvParser.Format(del,quote,esc,term))

  implicit val arbCSV: Arbitrary[Csv] = arbFormat.map(Csv.apply(_,None))

  implicit val arbJsonContentType: Arbitrary[JsonContentType] = Arbitrary(
    Gen.oneOf(
      JsonContentType(Readable, LineDelimited),
      JsonContentType(Readable,SingleArray),
      JsonContentType(Precise,LineDelimited),
      JsonContentType(Precise,SingleArray)))

  implicit val arbMessageFormat: Arbitrary[MessageFormat] =
    Arbitrary(Gen.oneOf(arbCSV.arbitrary, arbJsonContentType.arbitrary))
}
