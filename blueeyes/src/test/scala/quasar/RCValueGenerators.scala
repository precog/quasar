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

import quasar.precog.common._
import quasar.pkg.tests._, Gen._
import quasar.time.DateGenerators._

trait RCValueGenerators {
  def maxArrayDepth = 3

  def genColumn(size: Int, values: Gen[Array[CValue]]): Gen[List[Seq[CValue]]] = containerOfN[List,Seq[CValue]](size, values.map(_.toSeq))

  private def genNonArrayCValueType: Gen[CValueType[_]] =
    Gen.oneOf[CValueType[_]](CString, CBoolean, CLong, CDouble, CNum,
      COffsetDateTime, COffsetTime, COffsetDate,
      CLocalDateTime, CLocalTime, CLocalDate, CInterval)

  def genCValueType(maxDepth: Int = maxArrayDepth): Gen[CValueType[_]] = {
    if (maxDepth > 0) {
      genNonArrayCValueType
    } else {
      frequency(0 -> (genCValueType(maxDepth - 1) map (CArrayType(_))), 6 -> genNonArrayCValueType)
    }
  }

  def genCType: Gen[CType] = frequency(7 -> genCValueType(), 3 -> Gen.oneOf(CNull, CEmptyObject, CEmptyArray))

  def genValueForCValueType[A](cType: CValueType[A]): Gen[CWrappedValue[A]] = cType match {
    case CString  => genJSONString map (CString(_))
    case CBoolean => genBool map (CBoolean(_))
    case CLong    => genLong map (CLong(_))
    case CDouble  => genDouble map (CDouble(_))
    case CNum     => genSmallScaleBigDecimal.map(CNum(_))
    case CLocalDateTime => genLocalDateTime.map(CLocalDateTime(_))
    case CLocalTime => genLocalTime.map(CLocalTime(_))
    case CLocalDate => genLocalDate.map(CLocalDate(_))
    case COffsetDateTime => genOffsetDateTime.map(COffsetDateTime(_))
    case COffsetTime => genOffsetTime.map(COffsetTime(_))
    case COffsetDate => genOffsetDate.map(COffsetDate(_))
    case CInterval => genInterval.map(CInterval(_))
    case CArrayType(elemType) =>
      vectorOf(genValueForCValueType(elemType) map (_.value)) map { xs =>
        CArray(xs.toArray(elemType.classTag), CArrayType(elemType))
      }
  }

  def genCValueForType(tpe: CType): Gen[CValue] = tpe match {
    case tpe: CValueType[_] => genValueForCValueType(tpe)
    case CNull              => Gen.const(CNull)
    case CEmptyObject       => Gen.const(CEmptyObject)
    case CEmptyArray        => Gen.const(CEmptyArray)
    case invalid            => sys.error("No values for type " + invalid)
  }

  def genCValue: Gen[CValue] = for {
    tp <- genCType
    v <- genCValueForType(tp)
  } yield v

  def genCValues: Gen[Vector[CValue]] = for {
    n <- Gen.choose(0, 100)
    l <- Gen.containerOfN(n, genCValue)
  } yield l
}

object RCValueGenerators extends RCValueGenerators
