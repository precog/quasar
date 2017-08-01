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

import quasar.precog._
import quasar.blueeyes._
import quasar.precog.common._

import java.time.ZonedDateTime

/**
  * Defines a base set of codecs that are often used in `RowFormat`s.
  */
trait StdCodecs {
  implicit def LongCodec: Codec[Long]
  implicit def DoubleCodec: Codec[Double]
  implicit def BigDecimalCodec: Codec[BigDecimal]
  implicit def StringCodec: Codec[String]
  implicit def BooleanCodec: Codec[Boolean]
  implicit def DateTimeCodec: Codec[ZonedDateTime]
  implicit def PeriodCodec: Codec[Period]
  implicit def BitSetCodec: Codec[BitSet]
  implicit def RawBitSetCodec: Codec[Array[Int]]
  implicit def IndexedSeqCodec[A](implicit elemCodec: Codec[A]): Codec[IndexedSeq[A]]
  implicit def ArrayCodec[A](implicit elemCodec: Codec[A], m: CTag[A]): Codec[Array[A]]

  def codecForCValueType[A](cType: CValueType[A]): Codec[A] = cType match {
    case CBoolean             => BooleanCodec
    case CString              => StringCodec
    case CLong                => LongCodec
    case CDouble              => DoubleCodec
    case CNum                 => BigDecimalCodec
    case CDate                => DateTimeCodec
    case CPeriod              => PeriodCodec
    case CArrayType(elemType) => ArrayCodec(codecForCValueType(elemType), elemType.classTag)
  }
}

trait RowFormatCodecs extends StdCodecs { self: RowFormat =>
  implicit def LongCodec: Codec[Long]             = Codec.PackedLongCodec
  implicit def DoubleCodec: Codec[Double]         = Codec.DoubleCodec
  implicit def BigDecimalCodec: Codec[BigDecimal] = Codec.BigDecimalCodec
  implicit def StringCodec: Codec[String]         = Codec.Utf8Codec
  implicit def BooleanCodec: Codec[Boolean]       = Codec.BooleanCodec
  implicit def DateTimeCodec: Codec[ZonedDateTime] = Codec.ZonedDateTimeCodec
  implicit def PeriodCodec: Codec[Period]         = Codec.PeriodCodec
  // implicit def BitSetCodec: Codec[BitSet] = Codec.BitSetCodec
  //@transient implicit lazy val BitSetCodec: Codec[BitSet] = Codec.SparseBitSetCodec(columnRefs.size)
  @transient implicit lazy val BitSetCodec: Codec[BitSet]       = Codec.SparseBitSetCodec(columnRefs.size)
  @transient implicit lazy val RawBitSetCodec: Codec[Array[Int]] = Codec.SparseRawBitSetCodec(columnRefs.size)
  implicit def IndexedSeqCodec[A](implicit elemCodec: Codec[A]): Codec[IndexedSeq[A]]       = Codec.IndexedSeqCodec(elemCodec)
  implicit def ArrayCodec[A](implicit elemCodec: Codec[A], m: CTag[A]): Codec[Array[A]] = Codec.ArrayCodec(elemCodec)(m)
}
