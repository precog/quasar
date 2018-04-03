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

package quasar.niflheim

import quasar.precog.common._

import scalaz.{ Validation, Success, Failure }

import scala.annotation.tailrec
import scala.collection.mutable

import java.io.IOException
import java.nio.ByteBuffer

object CTypeFlags {
  object Flags {
    val FBoolean: Byte = 1
    val FString: Byte = 2
    val FLong: Byte = 3
    val FDouble: Byte = 4
    val FBigDecimal: Byte = 5
    val FOffsetDateTime: Byte = 6
    val FOffsetTime: Byte = 7
    val FOffsetDate: Byte = 8
    val FLocalDateTime: Byte = 9
    val FLocalTime: Byte = 10
    val FLocalDate: Byte = 11
    val FDuration: Byte = 12
    val FArray: Byte = 13
    val FNull: Byte = 16
    val FEmptyArray: Byte = 17
    val FEmptyObject: Byte = 18
  }

  def getFlagFor(ctype: CType): Array[Byte] = {
    import Flags._

    val buffer = new mutable.ArrayBuffer[Byte]()

    def flagForCType(t: CType) {
      @tailrec
      def flagForCValueType(t: CValueType[_]) {
        t match {
          case CString => buffer += FString
          case CBoolean => buffer += FBoolean
          case CLong => buffer += FLong
          case CDouble => buffer += FDouble
          case CNum => buffer += FBigDecimal
          case COffsetDateTime => buffer += FOffsetDateTime
          case COffsetTime => buffer += FOffsetTime
          case COffsetDate => buffer += FOffsetDate
          case CLocalDateTime => buffer += FLocalDateTime
          case CLocalTime => buffer += FLocalTime
          case CLocalDate => buffer += FLocalDate
          case CArrayType(tpe) =>
            buffer += FArray
            flagForCValueType(tpe)
          case CInterval => buffer += FDuration
        }
      }

      t match {
        case t: CValueType[_] =>
          flagForCValueType(t)
        case CNull =>
          buffer += FNull
        case CEmptyArray =>
          buffer += FEmptyArray
        case CEmptyObject =>
          buffer += FEmptyObject
        case CUndefined =>
          sys.error("Unexpected CUndefined type. Undefined segments don't exist!")
      }
    }

    flagForCType(ctype)
    buffer.toArray
  }

  def cTypeForFlag(flag: Array[Byte]): CType =
    readCType(ByteBuffer.wrap(flag)).fold(throw _, identity)

  def readCType(buffer: ByteBuffer): Validation[IOException, CType] = {
    import Flags._

    def readCValueType(flag: Byte): Validation[IOException, CValueType[_]] = flag match {
      case FBoolean => Success(CBoolean)
      case FString => Success(CString)
      case FLong => Success(CLong)
      case FDouble => Success(CDouble)
      case FBigDecimal => Success(CNum)
      case FOffsetDateTime => Success(COffsetDateTime)
      case FOffsetTime => Success(COffsetTime)
      case FOffsetDate => Success(COffsetDate)
      case FLocalDateTime => Success(CLocalDateTime)
      case FLocalTime => Success(CLocalTime)
      case FLocalDate => Success(CLocalDate)
      case FDuration => Success(CInterval)
      case FArray => readCValueType(buffer.get()) map (CArrayType(_))
      case _ => Failure(new IOException("Unexpected segment type flag: %x" format flag))
    }

    buffer.get() match {
      case FNull => Success(CNull)
      case FEmptyArray => Success(CEmptyArray)
      case FEmptyObject => Success(CEmptyObject)
      case flag => readCValueType(flag)
    }
  }
}

