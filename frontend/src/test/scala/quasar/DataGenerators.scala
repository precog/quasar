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

package quasar

import slamdata.Predef._
import quasar.pkg.tests._
import quasar.time.{DateGenerators, OffsetDate}

import java.time.{
  LocalDate => JLocalDate,
  LocalDateTime => JLocalDateTime,
  LocalTime => JLocalTime,
  OffsetDateTime => JOffsetDateTime,
  OffsetTime => JOffsetTime,
  ZoneOffset
}

trait DataGenerators {
  import DataGenerators._

  def genKey = Gen.alphaChar ^^ (_.toString)

  implicit val dataArbitrary: Arbitrary[Data] = Arbitrary(data)

  implicit def dataShrink(implicit l: Shrink[List[Data]], m: Shrink[ListMap[String, Data]])
      : Shrink[Data] =
    Shrink {
      case Data.Arr(value) => l.shrink(value).map(Data.Arr(_))
      case Data.Obj(value) => m.shrink(value).map(Data.Obj(_))
      case _               => Stream.empty
    }
}

object DataGenerators extends DataGenerators {
  // Too big for Long
  val LargeInt = BigInt(Long.MaxValue.toString + "0")

  /** Long value that can safely be represented in any possible backend
    * (including those using JavaScript.)
    */
  val SafeInt: Gen[Long] = choose(-1000L, 1000L)
  val SafeBigInt: Gen[BigInt] = SafeInt ^^ (x => BigInt(x))

  // NB: Decimals that look like ints, may need special handling
  val IntAsDouble: Gen[Double] = SafeInt ^^ (_.toDouble)

  val defaultInt: Gen[BigInt] = Gen.oneOf[BigInt](SafeBigInt, LargeInt)

  // NB: Decimals that look like ints, may need special handling
  val defaultDec: Gen[BigDecimal] =
    Gen.oneOf[Double](Gen.choose(-1000.0, 1000.0), IntAsDouble) ^^ (x => BigDecimal(x))

  // NB: a (nominally) valid MongoDB id, because we use this generator to test BSON conversion, too
  val defaultId: Gen[String] = Gen.oneOf[Char]("0123456789abcdef") * 24 ^^ (_.mkString)

  // TODO: make this very conservative so as likely to work with as many backends as possible
  val simpleData: Gen[Data] = genAtomicData(Gen.alphaStr, defaultInt, defaultDec, defaultId)

  val data: Gen[Data] =Gen.oneOf(
    simpleData,
    genNested(genKey, simpleData))

  def genNested(genKey: Gen[String], genAtomicData: Gen[Data]): Gen[Data] = Gen.oneOf[Data](
    (genKey, genAtomicData).zip.list ^^ (xs => Data.Obj(xs: _*)),
    genAtomicData.list ^^ Data.Arr)

  /** Generator of atomic Data (everything but Obj and Arr). */
  def genAtomicData(strSrc: Gen[String], intSrc: Gen[BigInt], decSrc: Gen[BigDecimal], idSrc: Gen[String])
      : Gen[Data] = {
    Gen.oneOf[Data](
      Data.Null,
      Data.True,
      Data.False,
      Data.NA,
      strSrc ^^ Data.Str,
      intSrc ^^ Data.Int,
      decSrc ^^ Data.Dec,
      DateGenerators.genInterval ^^ Data.Interval,
      DateGenerators.genOffsetDateTime ^^ Data.OffsetDateTime,
      DateGenerators.genOffsetDate ^^ Data.OffsetDate,
      DateGenerators.genOffsetTime ^^ Data.OffsetTime,
      DateGenerators.genLocalDateTime ^^ Data.LocalDateTime,
      DateGenerators.genLocalDate ^^ Data.LocalDate,
      DateGenerators.genLocalTime ^^ Data.LocalTime,
      arrayOf(genByte) ^^ Data.Binary.fromArray,
      idSrc ^^ Data.Id)
  }

  final case class Builder[-I, +O](f: I => O)

  implicit val genDateBuilder: Arbitrary[Builder[JLocalDate, Data]] = Gen.oneOf[Builder[JLocalDate, Data]](
    Builder[JLocalDate, Data](ld => Data.OffsetDate(OffsetDate(ld, ZoneOffset.UTC))),
    Builder[JLocalDate, Data](Data.LocalDate),
    Builder[JLocalDate, Data](ld => Data.LocalDateTime(JLocalDateTime.of(ld, JLocalTime.MIN))),
    Builder[JLocalDate, Data](ld => Data.OffsetDateTime(JOffsetDateTime.of(ld, JLocalTime.MIN, ZoneOffset.UTC))))

  implicit val genTimeBuilder: Arbitrary[Builder[JLocalTime, Data]] = Gen.oneOf[Builder[JLocalTime, Data]](
    Builder[JLocalTime, Data](lt => Data.OffsetTime(JOffsetTime.of(lt, ZoneOffset.UTC))),
    Builder[JLocalTime, Data](Data.LocalTime),
    Builder[JLocalTime, Data](lt => Data.LocalDateTime(JLocalDateTime.of(JLocalDate.MIN, lt))),
    Builder[JLocalTime, Data](lt => Data.OffsetDateTime(JOffsetDateTime.of(JLocalDate.MIN, lt, ZoneOffset.UTC))))

  implicit val genDateTimeBuilder: Arbitrary[Builder[JLocalDateTime, Data]] = Gen.oneOf[Builder[JLocalDateTime, Data]](
    Builder[JLocalDateTime, Data](Data.LocalDateTime),
    Builder[JLocalDateTime, Data](ldt => Data.OffsetDateTime(JOffsetDateTime.of(ldt, ZoneOffset.UTC))))
}
