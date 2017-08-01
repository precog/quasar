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

package quasar.precog.common

import quasar.blueeyes._, json._
import quasar.blueeyes.json.serialization.{ Extractor, Decomposer }
import quasar.blueeyes.json.serialization.DefaultSerialization._
import quasar.blueeyes.json.serialization.Extractor._
import quasar.blueeyes.json.serialization.Versioned._

import scalaz._
import scalaz.std.string._

import shapeless.HNil

sealed trait MetadataType

object MetadataType {
  def toName(metadataType: MetadataType): String = metadataType match {
    case BooleanValueStats    => "BooleanValueStats"
    case LongValueStats       => "LongValueStats"
    case DoubleValueStats     => "DoubleValueStats"
    case BigDecimalValueStats => "BigDecimalValueStats"
    case StringValueStats     => "StringValueStats"
  }

  def fromName(name: String): Option[MetadataType] = name match {
    case "BooleanValueStats"    => Option(BooleanValueStats)
    case "LongValueStats"       => Option(LongValueStats)
    case "DoubleValueStats"     => Option(DoubleValueStats)
    case "BigDecimalValueStats" => Option(BigDecimalValueStats)
    case "StringValueStats"     => Option(StringValueStats)
    case _                      => None
  }
}

sealed trait Metadata {
  def metadataType: MetadataType

  def fold[A](bf: BooleanValueStats => A, lf: LongValueStats => A, df: DoubleValueStats => A, bdf: BigDecimalValueStats => A, sf: StringValueStats => A): A

  def merge(that: Metadata): Option[Metadata]
}

object Metadata {
  implicit val MetadataDecomposer: Decomposer[Metadata] = new Decomposer[Metadata] {
    override def decompose(metadata: Metadata): JValue = {
      JObject(List(JField(MetadataType.toName(metadata.metadataType), metadata.fold(_.jv, _.jv, _.jv, _.jv, _.jv))))
    }
  }

  implicit val MetadataExtractor: Extractor[Metadata] = new Extractor[Metadata] {
    override def validated(obj: JValue): Validation[Error, Metadata] = obj match {
      case metadata @ JObject(entries) if entries.size == 1 => {
        val (key, value) = entries.head
        MetadataType.fromName(key).map {
          case BooleanValueStats    => value.validated[BooleanValueStats]
          case LongValueStats       => value.validated[LongValueStats]
          case DoubleValueStats     => value.validated[DoubleValueStats]
          case BigDecimalValueStats => value.validated[BigDecimalValueStats]
          case StringValueStats     => value.validated[StringValueStats]
        } getOrElse { Failure(Invalid("Unknown metadata type: " + key)) }
      }

      case _ => Failure(Invalid("Invalid metadata entry: " + obj))
    }
  }

  implicit val MetadataSemigroup = new Semigroup[Map[MetadataType, Metadata]] {
    def append(m1: Map[MetadataType, Metadata], m2: => Map[MetadataType, Metadata]) =
      m1.foldLeft(m2) { (acc, t) =>
        val (mtype, meta) = t
        acc + (mtype -> acc.get(mtype).map(combineMetadata(_, meta)).getOrElse(meta))
      }

    def combineMetadata(m1: Metadata, m2: Metadata) = m1.merge(m2).getOrElse(sys.error("Invalid attempt to combine incompatible metadata"))
  }
}

sealed trait MetadataStats extends Metadata {
  def count: Long
}

case class BooleanValueStats(count: Long, trueCount: Long) extends MetadataStats {
  def falseCount: Long    = count - trueCount
  def probability: Double = trueCount.toDouble / count

  def metadataType = BooleanValueStats

  def fold[A](bf: BooleanValueStats => A, lf: LongValueStats => A, df: DoubleValueStats => A, bdf: BigDecimalValueStats => A, sf: StringValueStats => A): A =
    bf(this)

  def merge(that: Metadata) = that match {
    case BooleanValueStats(count, trueCount) => Some(BooleanValueStats(this.count + count, this.trueCount + trueCount))
    case _                                   => None
  }
}

object BooleanValueStats extends MetadataType {
  val schemaV1                                             = "count" :: "trueCount" :: HNil
  implicit val decomposerV1: Decomposer[BooleanValueStats] = decomposerV[BooleanValueStats](schemaV1, Some("1.0".v))
  implicit val extractorV1: Extractor[BooleanValueStats]   = extractorV[BooleanValueStats](schemaV1, Some("1.0".v))
}

case class LongValueStats(count: Long, min: Long, max: Long) extends MetadataStats {
  def metadataType = LongValueStats

  def fold[A](bf: BooleanValueStats => A, lf: LongValueStats => A, df: DoubleValueStats => A, bdf: BigDecimalValueStats => A, sf: StringValueStats => A): A =
    lf(this)

  def merge(that: Metadata) = that match {
    case LongValueStats(count, min, max) => Some(LongValueStats(this.count + count, this.min.min(min), this.max.max(max)))
    case _                               => None
  }
}

object LongValueStats extends MetadataType {
  val schemaV1                                          = "count" :: "min" :: "max" :: HNil
  implicit val decomposerV1: Decomposer[LongValueStats] = decomposerV[LongValueStats](schemaV1, Some("1.0".v))
  implicit val extractorV1: Extractor[LongValueStats]   = extractorV[LongValueStats](schemaV1, Some("1.0".v))
}

case class DoubleValueStats(count: Long, min: Double, max: Double) extends MetadataStats {
  def metadataType = DoubleValueStats

  def fold[A](bf: BooleanValueStats => A, lf: LongValueStats => A, df: DoubleValueStats => A, bdf: BigDecimalValueStats => A, sf: StringValueStats => A): A =
    df(this)

  def merge(that: Metadata) = that match {
    case DoubleValueStats(count, min, max) => Some(DoubleValueStats(this.count + count, this.min min min, this.max max max))
    case _                                 => None
  }
}

object DoubleValueStats extends MetadataType {
  val schemaV1                                            = "count" :: "min" :: "max" :: HNil
  implicit val decomposerV1: Decomposer[DoubleValueStats] = decomposerV[DoubleValueStats](schemaV1, Some("1.0".v))
  implicit val extractorV1: Extractor[DoubleValueStats]   = extractorV[DoubleValueStats](schemaV1, Some("1.0".v))
}

case class BigDecimalValueStats(count: Long, min: BigDecimal, max: BigDecimal) extends MetadataStats {
  def metadataType = BigDecimalValueStats

  def fold[A](bf: BooleanValueStats => A, lf: LongValueStats => A, df: DoubleValueStats => A, bdf: BigDecimalValueStats => A, sf: StringValueStats => A): A =
    bdf(this)

  def merge(that: Metadata) = that match {
    case BigDecimalValueStats(count, min, max) => Some(BigDecimalValueStats(this.count + count, this.min min min, this.max max max))
    case _                                     => None
  }
}

object BigDecimalValueStats extends MetadataType {
  val schemaV1                                                = "count" :: "min" :: "max" :: HNil
  implicit val decomposerV1: Decomposer[BigDecimalValueStats] = decomposerV[BigDecimalValueStats](schemaV1, Some("1.0".v))
  implicit val extractorV1: Extractor[BigDecimalValueStats]   = extractorV[BigDecimalValueStats](schemaV1, Some("1.0".v))
}

case class StringValueStats(count: Long, min: String, max: String) extends MetadataStats {
  def metadataType = StringValueStats

  def fold[A](bf: BooleanValueStats => A, lf: LongValueStats => A, df: DoubleValueStats => A, bdf: BigDecimalValueStats => A, sf: StringValueStats => A): A =
    sf(this)

  def merge(that: Metadata) = that match {
    case StringValueStats(count, min, max) =>
      Some(StringValueStats(this.count + count, scalaz.Order[String].min(this.min, min), scalaz.Order[String].max(this.max, max)))
    case _ => None
  }
}

object StringValueStats extends MetadataType {
  val schemaV1                                            = "count" :: "min" :: "max" :: HNil
  implicit val decomposerV1: Decomposer[StringValueStats] = decomposerV[StringValueStats](schemaV1, Some("1.0".v))
  implicit val extractorV1: Extractor[StringValueStats]   = extractorV[StringValueStats](schemaV1, Some("1.0".v))
}
