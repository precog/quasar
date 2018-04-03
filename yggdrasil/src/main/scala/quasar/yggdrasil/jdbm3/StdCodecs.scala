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

package quasar.yggdrasil
package jdbm3

import java.time._

import quasar.precog._
import quasar.precog.common._
import quasar.time.{DateTimeInterval, OffsetDate}

import scala.reflect.ClassTag

/**
  * Defines a base set of codecs that are often used in `RowFormat`s.
  */
trait StdCodecs {
  implicit def LongCodec: Codec[Long]
  implicit def DoubleCodec: Codec[Double]
  implicit def BigDecimalCodec: Codec[BigDecimal]
  implicit def StringCodec: Codec[String]
  implicit def BooleanCodec: Codec[Boolean]
  implicit def OffsetDateTimeCodec: Codec[OffsetDateTime]
  implicit def OffsetTimeCodec: Codec[OffsetTime]
  implicit def OffsetDateCodec: Codec[OffsetDate]
  implicit def LocalDateTimeCodec: Codec[LocalDateTime]
  implicit def LocalTimeCodec: Codec[LocalTime]
  implicit def LocalDateCodec: Codec[LocalDate]
  implicit def IntervalCodec: Codec[DateTimeInterval]
  implicit def BitSetCodec: Codec[BitSet]
  implicit def RawBitSetCodec: Codec[Array[Int]]
  implicit def IndexedSeqCodec[A](implicit elemCodec: Codec[A]): Codec[IndexedSeq[A]]
  implicit def ArrayCodec[A](implicit elemCodec: Codec[A], m: ClassTag[A]): Codec[Array[A]]

  def codecForCValueType[A](cType: CValueType[A]): Codec[A] = cType match {
    case CBoolean             => BooleanCodec
    case CString              => StringCodec
    case CLong                => LongCodec
    case CDouble              => DoubleCodec
    case CNum                 => BigDecimalCodec
    case COffsetDateTime      => OffsetDateTimeCodec
    case COffsetTime          => OffsetTimeCodec
    case COffsetDate          => OffsetDateCodec
    case CLocalDateTime       => LocalDateTimeCodec
    case CLocalTime           => LocalTimeCodec
    case CLocalDate           => LocalDateCodec
    case CInterval            => IntervalCodec
    case CArrayType(elemType) => ArrayCodec(codecForCValueType(elemType), elemType.classTag)
  }
}

trait RowFormatCodecs extends StdCodecs { self: RowFormat =>
  implicit def LongCodec: Codec[Long]             = Codec.PackedLongCodec
  implicit def DoubleCodec: Codec[Double]         = Codec.DoubleCodec
  implicit def BigDecimalCodec: Codec[BigDecimal] = Codec.BigDecimalCodec
  implicit def StringCodec: Codec[String]         = Codec.Utf8Codec
  implicit def BooleanCodec: Codec[Boolean]       = Codec.BooleanCodec
  implicit def OffsetDateTimeCodec: Codec[OffsetDateTime] = Codec.OffsetDateTimeCodec
  implicit def OffsetTimeCodec: Codec[OffsetTime]         = Codec.OffsetTimeCodec
  implicit def OffsetDateCodec: Codec[OffsetDate]         = Codec.OffsetDateCodec
  implicit def LocalDateTimeCodec: Codec[LocalDateTime]   = Codec.LocalDateTimeCodec
  implicit def LocalTimeCodec: Codec[LocalTime]           = Codec.LocalTimeCodec
  implicit def LocalDateCodec: Codec[LocalDate]           = Codec.LocalDateCodec
  implicit def IntervalCodec: Codec[DateTimeInterval] = Codec.IntervalCodec
  // implicit def BitSetCodec: Codec[BitSet] = Codec.BitSetCodec
  //@transient implicit lazy val BitSetCodec: Codec[BitSet] = Codec.SparseBitSetCodec(columnRefs.size)
  @transient implicit lazy val BitSetCodec: Codec[BitSet]       = Codec.SparseBitSetCodec(columnRefs.size)
  @transient implicit lazy val RawBitSetCodec: Codec[Array[Int]] = Codec.SparseRawBitSetCodec(columnRefs.size)
  implicit def IndexedSeqCodec[A](implicit elemCodec: Codec[A]): Codec[IndexedSeq[A]]       = Codec.IndexedSeqCodec(elemCodec)
  implicit def ArrayCodec[A](implicit elemCodec: Codec[A], m: ClassTag[A]): Codec[Array[A]] = Codec.ArrayCodec(elemCodec)(m)
}
