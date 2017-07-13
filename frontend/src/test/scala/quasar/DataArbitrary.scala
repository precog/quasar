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

trait DataArbitrary {
  import DataArbitrary._
  import Data.Obj

  def genKey = Gen.alphaChar ^^ (_.toString)

  implicit val dataArbitrary: Arbitrary[Data] = Arbitrary(
    Gen.oneOf(
      simpleData,
      genNested(genKey, simpleData),
      // Tricky cases: (TODO: These belong in MongoDB tests somewhere.)
      const(Obj("$date" -> Data.Str("Jan 1"))),
      SafeInt ^^ (x => Obj("$obj" -> Obj("$obj" -> Data.Int(x))))
    )
  )

  implicit def dataShrink(implicit l: Shrink[List[Data]], m: Shrink[ListMap[String, Data]]): Shrink[Data] = Shrink {
    case Data.Arr(value) => l.shrink(value).map(Data.Arr(_))
    case Data.Obj(value) => m.shrink(value).map(Data.Obj(_))
    case _               => Stream.empty
  }
}

object DataArbitrary extends DataArbitrary {
  import DateArbitrary._

  // Too big for Long
  val LargeInt = BigInt(Long.MaxValue.toString + "0")

  /** Long value that can safely be represented in any possible backend
    * (including those using JavaScript.)
    */
  val SafeInt: Gen[Long] = choose(-1000L, 1000L)
  val SafeBigInt         = SafeInt ^^ (x => BigInt(x))
  val IntAsDouble        = SafeInt ^^ (_.toDouble) // NB: Decimals that look like ints, may need special handling

  val defaultInt: Gen[BigInt] = Gen.oneOf[BigInt](SafeBigInt, LargeInt)

  // NB: Decimals that look like ints, may need special handling
  val defaultDec: Gen[BigDecimal] = Gen.oneOf[Double](Gen.choose(-1000.0, 1000.0), IntAsDouble) ^^ (x => BigDecimal(x))

  // NB: a (nominally) valid MongoDB id, because we use this generator to test BSON conversion, too
  val defaultId: Gen[String] = Gen.oneOf[Char]("0123456789abcdef") * 24 ^^ (_.mkString)

  // TODO: make this very conservative so as likely to work with as many backends as possible
  val simpleData: Gen[Data] = genAtomicData(Gen.alphaStr, defaultInt, defaultDec, defaultId)

  def genNested(genKey: Gen[String], genAtomicData: Gen[Data]): Gen[Data] = Gen.oneOf[Data](
    (genKey, genAtomicData).zip.list ^^ (xs => Data.Obj(xs: _*)),
    genAtomicData.list ^^ Data.Arr
  )

  /** Generator of atomic Data (everything but Obj and Arr). */
  def genAtomicData(strSrc: Gen[String], intSrc: Gen[BigInt], decSrc: Gen[BigDecimal], idSrc: Gen[String]): Gen[Data] = {
    import Data._
    Gen.oneOf[Data](
      Null,
      True,
      False,
      NA,
      strSrc           ^^ Str,
      intSrc           ^^ Int,
      decSrc           ^^ Dec,
      genInstant       ^^ Timestamp,
      genDuration      ^^ Interval,
      genDate          ^^ Date,
      genTime          ^^ Time,
      arrayOf(genByte) ^^ Binary.fromArray,
      idSrc            ^^ Id
    )
  }

}
