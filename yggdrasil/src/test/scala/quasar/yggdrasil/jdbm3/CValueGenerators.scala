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

package quasar.yggdrasil
package jdbm3

import quasar.blueeyes._
import quasar.precog.common._
import quasar.precog.TestSupport._, Gen._

import java.time.{Instant, ZoneOffset, ZonedDateTime}

trait CValueGenerators {
  def maxArrayDepth = 3

  def genColumn(size: Int, values: Gen[Array[CValue]]): Gen[List[Seq[CValue]]] = containerOfN[List,Seq[CValue]](size, values.map(_.toSeq))

  private def genNonArrayCValueType: Gen[CValueType[_]] = Gen.oneOf[CValueType[_]](CString, CBoolean, CLong, CDouble, CNum, CDate)

  def genCValueType(maxDepth: Int = maxArrayDepth, depth: Int = 0): Gen[CValueType[_]] = {
    if (depth >= maxDepth) genNonArrayCValueType else {
      frequency(0 -> (genCValueType(maxDepth, depth + 1) map (CArrayType(_))), 6 -> genNonArrayCValueType)
    }
  }

  def genCType: Gen[CType] = frequency(7 -> genCValueType(), 3 -> Gen.oneOf(CNull, CEmptyObject, CEmptyArray))

  // TODO remove duplication with `SegmentFormatSupport#genForCType`
  def genValueForCValueType[A](cType: CValueType[A]): Gen[CWrappedValue[A]] = cType match {
    case CString  => genString map (CString(_))
    case CBoolean => genBool map (CBoolean(_))
    case CLong    => genLong map (CLong(_))
    case CDouble  => genDouble map (CDouble(_))
    case CNum     => for {
      scale  <- genInt
      bigInt <- genBigInt
    } yield CNum(BigDecimal(new java.math.BigDecimal(bigInt.bigInteger, scale - 1), java.math.MathContext.UNLIMITED))
    case CDate =>
      genPosLong ^^ (n => CDate(ZonedDateTime.ofInstant(Instant.ofEpochSecond(n % Instant.MAX.getEpochSecond), ZoneOffset.UTC)))
    case CArrayType(elemType) =>
      vectorOf(genValueForCValueType(elemType) map (_.value)) map { xs =>
        CArray(xs.toArray(elemType.classTag), CArrayType(elemType))
      }
    case CPeriod => abort("undefined")
  }

  def genCValue(tpe: CType): Gen[CValue] = tpe match {
    case tpe: CValueType[_] => genValueForCValueType(tpe)
    case CNull              => Gen.const(CNull)
    case CEmptyObject       => Gen.const(CEmptyObject)
    case CEmptyArray        => Gen.const(CEmptyArray)
    case invalid            => sys.error("No values for type " + invalid)
  }
}


