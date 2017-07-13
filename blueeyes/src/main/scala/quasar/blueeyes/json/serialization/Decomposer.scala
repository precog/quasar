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

package quasar.blueeyes.json.serialization

import quasar.blueeyes._
import quasar.blueeyes.json._
import quasar.precog.{MimeType, MimeTypes}

import scalaz._, Scalaz._
import ExtractorDecomposer.by

import java.util.UUID
import java.time.LocalDateTime

/** Decomposes the value into a JSON object.
  */
trait Decomposer[A] { self =>
  def decompose(tvalue: A): JValue

  def contramap[B](f: B => A): Decomposer[B] = new Decomposer[B] {
    override def decompose(b: B) = self.decompose(f(b))
  }

  def apply(tvalue: A): JValue = decompose(tvalue)

  def unproject(jpath: JPath): Decomposer[A] = new Decomposer[A] {
    override def decompose(b: A) = JUndefined.unsafeInsert(jpath, self.decompose(b))
  }
}

object Decomposer {
  def apply[A](implicit d: Decomposer[A]): Decomposer[A] = d
}

class ExtractorDecomposer[A](toJson: A => JValue, fromJson: JValue => Validation[Extractor.Error, A]) extends Extractor[A] with Decomposer[A] {
  def decompose(x: A)      = toJson(x)
  def validated(x: JValue) = fromJson(x)
}
object ExtractorDecomposer {
  def makeOpt[A, B](fg: A => B, gf: B => Validation[Extractor.Error, A])(implicit ez: Extractor[B], dz: Decomposer[B]): ExtractorDecomposer[A] = {
    new ExtractorDecomposer[A](
      v => dz decompose fg(v),
      j => ez validated j flatMap gf
    )
  }

  def make[A, B](fg: A => B, gf: B => A)(implicit ez: Extractor[B], dz: Decomposer[B]): ExtractorDecomposer[A] = {
    new ExtractorDecomposer[A](
      v => dz decompose fg(v),
      j => ez validated j map gf
    )
  }

  def by[A] = new {
    def apply[B](fg: A => B)(gf: B => A)(implicit ez: Extractor[B], dz: Decomposer[B]): ExtractorDecomposer[A]                            = make[A, B](fg, gf)
    def opt[B](fg: A => B)(gf: B => Validation[Extractor.Error, A])(implicit ez: Extractor[B], dz: Decomposer[B]): ExtractorDecomposer[A] = makeOpt[A, B](fg, gf)
  }
}

trait MiscSerializers {
  import DefaultExtractors._, DefaultDecomposers._
  import SerializationImplicits._

  implicit val JPathExtractorDecomposer: ExtractorDecomposer[JPath] =
    ExtractorDecomposer.by[JPath].opt(x => JString(x.toString): JValue)(_.validated[String] map (JPath(_)))

  implicit val InstantExtractorDecomposer  = by[Instant](_.getMillis)(instant fromMillis _)
  implicit val DurationExtractorDecomposer = by[Duration](_.getMillis)(duration fromMillis _)
  implicit val UuidExtractorDecomposer     = by[UUID](_.toString)(uuid)
  implicit val MimeTypeExtractorDecomposer = by[MimeType].opt(x => JString(x.toString): JValue)(jv =>
    StringExtractor validated jv map (MimeTypes parseMimeTypes _ toList) flatMap {
      case Nil        => Failure(Extractor.Error.invalid("No mime types found in " + jv.renderCompact))
      case first :: _ => Success(first)
    }
  )
}

/** Serialization implicits allow a convenient syntax for serialization and
  * deserialization when implicit decomposers and extractors are in scope.
  * <p>
  * foo.serialize
  * <p>
  * jvalue.deserialize[Foo]
  */
trait SerializationImplicits extends MiscSerializers {
  case class DeserializableJValue(jvalue: JValue) {
    def deserialize[T](implicit e: Extractor[T]): T                            = e.extract(jvalue)
    def validated[T](implicit e: Extractor[T]): Validation[Extractor.Error, T] = e.validated(jvalue)
    def validated[T](jpath: JPath)(implicit e: Extractor[T])                   = e.validated(jvalue, jpath)
  }

  case class SerializableTValue[T](tvalue: T) {
    def serialize(implicit d: Decomposer[T]): JValue = d.decompose(tvalue)
    def jv(implicit d: Decomposer[T]): JValue        = d.decompose(tvalue)
  }

  implicit def JValueToTValue[T](jvalue: JValue): DeserializableJValue = DeserializableJValue(jvalue)
  implicit def TValueToJValue[T](tvalue: T): SerializableTValue[T]     = SerializableTValue[T](tvalue)
}

object SerializationImplicits extends SerializationImplicits

/** Bundles default extractors, default decomposers, and serialization
  * implicits for natural serialization of core supported types.
  */
object DefaultSerialization extends DefaultExtractors with DefaultDecomposers with SerializationImplicits {
  implicit val DateTimeExtractorDecomposer =
    by[LocalDateTime].opt(x => JNum(x.getMillis): JValue)(_.validated[Long] map (dateTime fromMillis _))
}

// when we want to serialize dates as ISO8601 not as numbers
object Iso8601Serialization extends DefaultExtractors with DefaultDecomposers with SerializationImplicits {
  import Extractor._
  implicit val TZDateTimeExtractorDecomposer =
    by[LocalDateTime].opt(d => JString(dateTime showIso d): JValue) {
      case JString(dt) => (Thrown.apply _) <-: Validation.fromTryCatchNonFatal(dateTime fromIso dt)
      case _           => Failure(Invalid("Date time must be represented as JSON string"))
    }
}
