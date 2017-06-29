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

import quasar.blueeyes._, json._
import IsoSerialization._
import Extractor._
import DefaultSerialization._

import shapeless._
import scalaz._, Scalaz._, Validation._, FlatMap._

case class Version(major: Int, minor: Int, micro: Option[Int] = None, classifier: Option[String] = None) {
  def isBackwardCompatible(other: Version) = this >= other && this.major == other.major
}

object SVersion {
  val VPattern = """(\d+)\.(\d+)(?:\.(\d+))?(?:-(.*))?""".r

  def unapply(s: String): Option[Version] = s match {
    case VPattern(major, minor, micro, classifier) => Some(Version(major.toInt, minor.toInt, Option(micro).map(_.toInt), Option(classifier)))
    case _                                         => None
  }
}

object Version {
  implicit val tc: Order[Version] with Show[Version] = new Order[Version] with Show[Version] {
    import scalaz.Ordering
    import scalaz.Ordering._

    def order(v1: Version, v2: Version): Ordering = {
      import scalaz.syntax.apply._
      (v1.major ?|? v2.major) |+|
        (v1.minor ?|? v2.minor) |+|
        (^(v1.micro, v2.micro) { _ ?|? _ } getOrElse EQ)
    }

    override def shows(v: Version): String = {
      val microS = v.micro.map("." + _).getOrElse("")
      val clS    = v.classifier.map("-" + _).getOrElse("")
      v.major + "." + v.minor + microS + clS
    }
  }

  implicit val serialization: Extractor[Version] with Decomposer[Version] = new Extractor[Version] with Decomposer[Version] {
    def decompose(v: Version) = JString(v.shows)

    def validated(jvalue: JValue) = jvalue match {
      case JString(SVersion(v)) => success(v)
      case _                    => Failure(Invalid("Version " + jvalue.renderCompact + " + did not match the expected pattern."))
    }
  }
}

object Versioned {
  val defaultVersionProperty = JPath(".schemaVersion")

  def extractorV[T]     = new MkExtractorV[T]
  def decomposerV[T]    = new MkDecomposerV[T]
  def serializationV[T] = new MkSerializationV[T]

  implicit def toToVersion(s: String): ToVersion = new ToVersion(s)
  class ToVersion(s: String) {
    def v: Version = (s: @unchecked) match { case SVersion(v) => v }
  }

  implicit def toVersionableExtractor[A](extractor: Extractor[A]): VersionableExtractor[A] = new VersionableExtractor(extractor)
  class VersionableExtractor[A](extractor: Extractor[A]) {
    def versioned(version: Option[Version], versionField: JPath = defaultVersionProperty): Extractor[A] = new Extractor[A] {
      def validated(jv: JValue) = {
        import scalaz.syntax.traverse._

        version.traverse[Validation[Error, ?], Version] { v =>
          jv.validated[Option[Version]]("schemaVersion") flatMap {
            case Some(vrec) =>
              if (v.isBackwardCompatible(vrec)) success(vrec)
              else failure(Invalid(versionField.path + " value " + vrec.shows + " was incompatible with desired version " + v.shows))
            case None =>
              failure(Invalid(versionField.path + " property missing for value " + jv.renderCompact + "; was expecting " + v.shows))
          }
        } flatMap { _: Option[Version] =>
          extractor.validated(jv)
        }
      }
    }
  }

  implicit def toVersionableDecomposer[A](decomposer: Decomposer[A]): VersionableDecomposer[A] = new VersionableDecomposer(decomposer)
  class VersionableDecomposer[A](decomposer: Decomposer[A]) {
    def versioned(version: Option[Version], versionField: JPath = defaultVersionProperty): Decomposer[A] = new Decomposer[A] {
      def decompose(a: A): JValue = {
        val baseResult = decomposer.decompose(a)
        version map { v =>
          if (baseResult.isInstanceOf[JObject]) {
            baseResult.unsafeInsert(versionField, v.jv)
          } else {
            sys.error("Cannot version primitive or array values!")
          }
        } getOrElse {
          baseResult
        }
      }
    }
  }

  class MkDecomposerV[T] {
    def apply[F <: HList, L <: HList](fields: F, version: Option[Version], versionProperty: JPath = defaultVersionProperty)(
        implicit iso: Generic.Aux[T, L],
        decomposer: DecomposerAux[F, L]): Decomposer[T] =
      new IsoDecomposer(fields, iso, decomposer).versioned(version, versionProperty)
  }

  class MkExtractorV[T] {
    def apply[F <: HList, L <: HList](fields: F, version: Option[Version], versionProperty: JPath = defaultVersionProperty)(
        implicit iso: Generic.Aux[T, L],
        extractor: ExtractorAux[F, L]): Extractor[T] =
      new IsoExtractor(fields, iso, extractor).versioned(version, versionProperty)
  }

  class MkSerializationV[T] {
    def apply[F <: HList, L <: HList](fields: F, version: Option[Version], versionProperty: JPath = defaultVersionProperty)(
        implicit iso: Generic.Aux[T, L],
        decomposer: DecomposerAux[F, L],
        extractor: ExtractorAux[F, L]): (Decomposer[T], Extractor[T]) =
      (new IsoDecomposer(fields, iso, decomposer).versioned(version, versionProperty),
       new IsoExtractor(fields, iso, extractor).versioned(version, versionProperty))
  }
}
