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

package quasar

import slamdata.Predef._
import quasar.common.data._
import quasar.pkg.tests._, Gen._

import qdata.time.TimeGenerators._

import scala.sys

trait RCValueGenerators {
  def maxArrayDepth = 3

  def genBytes: Gen[Array[Byte]] =
    arrayOf(genByte)

  def genCValueType: Gen[CValueType[_]] =
    Gen.oneOf[CValueType[_]](
      CString, CBoolean, CLong, CDouble, CNum,
      COffsetDateTime, COffsetTime, COffsetDate,
      CLocalDateTime, CLocalTime, CLocalDate, CInterval,
      CBinary)

  def genCType: Gen[CType] = frequency(7 -> genCValueType, 3 -> Gen.oneOf(CNull, CEmptyObject, CEmptyArray))

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
    case CBinary => genBytes.map(CBinary(_))
    case CArrayType(_) =>
      scala.sys.error("CArrayType not supported")
  }

  def genCValue: Gen[CValue] = for {
    tp <- genCType
    v <- genTypedCValue(tp)
  } yield v

  def genCValues: Gen[List[CValue]] = for {
    n <- Gen.choose(0, 100)
    l <- Gen.listOfN(n, genCValue)
  } yield l

  def genTypedCValue(tpe: CType): Gen[CValue] = tpe match {
    case tpe: CValueType[_] => genValueForCValueType(tpe)
    case CNull              => Gen.const(CNull)
    case CEmptyObject       => Gen.const(CEmptyObject)
    case CEmptyArray        => Gen.const(CEmptyArray)
    case invalid            => sys.error("No values for type " + invalid)
  }
}

object RCValueGenerators extends RCValueGenerators

