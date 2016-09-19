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

package quasar

import quasar.Predef._

import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.threeten.bp._

trait DataArbitrary {
  import DataArbitrary._

  implicit val dataArbitrary: Arbitrary[Data] = Arbitrary {
    Gen.oneOf(
      simpleData,
      Gen.oneOf(
        Gen.listOf(for { c <- Gen.alphaChar; d <- simpleData } yield c.toString -> d).map(t => Data.Obj(ListMap(t: _*))),
        Gen.listOf(simpleData).map(Data.Arr(_)),
        // Tricky cases:
        Gen.const(Data.Obj(ListMap("$date" -> Data.Str("Jan 1")))),
        SafeInt.map(x =>
          Data.Obj(ListMap(
            "$obj" -> Data.Obj(ListMap(
              "$obj" -> Data.Int(x))))))))
  }

  implicit def dataShrink(implicit l: Shrink[List[Data]], m: Shrink[ListMap[String, Data]]): Shrink[Data] = Shrink {
    case Data.Arr(value) => l.shrink(value).map(Data.Arr(_))
    case Data.Obj(value) => m.shrink(value).map(Data.Obj(_))
    case _               => Stream.empty
  }
}

object DataArbitrary extends DataArbitrary {
  // Too big for Long
  val LargeInt = Data.Int(new java.math.BigInteger(Long.MaxValue.toString + "0"))

  /** Long value that can safely be represented in any possible backend
    * (including those using JavaScript.)
    */
  val SafeInt: Gen[Long] = Gen.choose(-1000L, 1000L)

  val simpleData: Gen[Data] =
    Gen.oneOf(
      Gen.const(Data.Null),
      Gen.const(Data.True),
      Gen.const(Data.False),
      Gen.alphaStr.map(Data.Str(_)),
      SafeInt.map(Data.Int(_)),
      Gen.choose(-1000.0, 1000.0).map(Data.Dec(_)),
      Gen.const(Data.Timestamp(Instant.now)),  // TODO
      SafeInt.map(ms => Data.Interval(Duration.ofMillis(ms))),
      Gen.const(Data.Date(LocalDate.now)),
      Gen.const(Data.Time(LocalTime.now)),
      Gen.listOf(Arbitrary.arbitrary[Byte]).map(bs => Data.Binary(Array[Byte](bs: _*))),
      // NB: a (nominally) valid MongoDB id, because we use this generator to test
      //     BSON conversion, too
      Gen.listOfN(24, Gen.oneOf(("0123456789abcdef": Seq[Char]))).map(ds => Data.Id(ds.mkString)),
      Gen.const(Data.NA),

      // Tricky cases:
      Gen.const(DataArbitrary.LargeInt),
      SafeInt.map(x => Data.Dec(x.toDouble))) // Looks like an Int, so needs special handling
}
