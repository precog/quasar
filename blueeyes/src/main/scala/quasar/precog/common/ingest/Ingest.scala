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

package quasar.precog.common
package ingest

import quasar.blueeyes._, json._, serialization._
import IsoSerialization._, Iso8601Serialization._, Versioned._
import Extractor._
import scalaz._, Scalaz._, Validation._

import shapeless.HNil

import java.util.UUID

import java.time.Instant

sealed trait Event {
  def fold[A](ingest: Ingest => A, archive: Archive => A): A
  def split(n: Int): List[Event]
  def length: Int
}

object Event {
  implicit val decomposer: Decomposer[Event] = new Decomposer[Event] {
    override def decompose(event: Event): JValue = {
      event.fold(_.serialize, _.serialize)
    }
  }
}

case class Ingest(path: Path, data: Seq[JValue], jobId: Option[JobId], timestamp: Instant, streamRef: StreamRef)
    extends Event {
  def fold[A](ingest: Ingest => A, archive: Archive => A): A = ingest(this)

  def split(n: Int): List[Event] = {
    val splitSize = (data.length / n) max 1
    val splitData = data.grouped(splitSize).toSeq
    (splitData zip streamRef.split(splitData.size)).map({
      case (d, ref) => this.copy(data = d, streamRef = ref)
    })(collection.breakOut)
  }

  def length = data.length
}

object Ingest {
  val schemaV1 = "path" :: "data" :: "jobId" :: "timestamp" :: "streamRef" :: HNil
  implicit def seqExtractor[A: Extractor]: Extractor[Seq[A]] = implicitly[Extractor[List[A]]].map(_.toSeq)

  val decomposerV1: Decomposer[Ingest] = decomposerV[Ingest](schemaV1, Some("1.1".v))
  val extractorV1: Extractor[Ingest] = extractorV[Ingest](schemaV1, Some("1.1".v))

  // A transitionary format similar to V1 structure, but lacks a version number and only carries a single data element
  val extractorV1a = new Extractor[Ingest] {
    def validated(obj: JValue): Validation[Error, Ingest] =
      obj.validated[Path]("path").map { path =>
        val jv = (obj \ "data")
        Ingest(
          path,
          if (jv == JUndefined) Vector() else Vector(jv),
          None,
          EventMessage.defaultTimestamp,
          StreamRef.Append)
      }
  }

  val extractorV0 = new Extractor[Ingest] {
    def validated(obj: JValue): Validation[Error, Ingest] = {
      obj.validated[Path]("path").map { path =>
        val jv = (obj \ "data")
        Ingest(path, if (jv == JUndefined) Vector() else Vector(jv), None, EventMessage.defaultTimestamp, StreamRef.Append)
      }
    }
  }

  implicit val decomposer: Decomposer[Ingest] = decomposerV1
  implicit val extractor: Extractor[Ingest] = extractorV1
}

case class Archive(path: Path, jobId: Option[JobId], timestamp: Instant) extends Event {
  def fold[A](ingest: Ingest => A, archive: Archive => A): A = archive(this)
  def split(n: Int) = List(this) // can't split an archive
  def length = 1
}

object Archive {
  val schemaV1 = "path" :: "jobId" :: ("timestamp" ||| EventMessage.defaultTimestamp) :: HNil

  val extractorV1: Extractor[Archive] = extractorV[Archive](schemaV1, None)
  val decomposerV1: Decomposer[Archive] = decomposerV[Archive](schemaV1, Some("1.0".v))

  implicit val decomposer: Decomposer[Archive] = decomposerV1
  implicit val extractor: Extractor[Archive] = extractorV1
}

sealed trait StreamRef {
  def terminal: Boolean
  def terminate: StreamRef
  def split(n: Int): Seq[StreamRef]
}

object StreamRef {
  case class Create(streamId: UUID, terminal: Boolean) extends StreamRef {
    def terminate = copy(terminal = true)
    def split(n: Int): Seq[StreamRef] = Vector.fill(n - 1) { copy(terminal = false) } :+ this
  }

  case class Replace(streamId: UUID, terminal: Boolean) extends StreamRef {
    def terminate = copy(terminal = true)
    def split(n: Int): Seq[StreamRef] = Vector.fill(n - 1) { copy(terminal = false) } :+ this
  }

  case object Append extends StreamRef {
    val terminal = false
    def terminate = this
    def split(n: Int): Seq[StreamRef] = Vector.fill(n) { this }
  }

  implicit val decomposer: Decomposer[StreamRef] = new Decomposer[StreamRef] {
    def decompose(streamRef: StreamRef) = streamRef match {
      case Create(uuid, terminal) => JObject("create" -> JObject("uuid" -> uuid.jv, "terminal" -> terminal.jv))
      case Replace(uuid, terminal) => JObject("replace" -> JObject("uuid" -> uuid.jv, "terminal" -> terminal.jv))
      case Append => JString("append")
    }
  }

  implicit val extractor: Extractor[StreamRef] = new Extractor[StreamRef] {
    def validated(jv: JValue) = jv match {
      case JString("append") => Success(Append)
      case other =>
        ((other \? "create") map { jv =>
              (jv, Create.apply _)
            }) orElse ((other \? "replace") map { jv =>
              (jv, Replace.apply _)
            }) map {
          case (jv, f) => (jv.validated[UUID]("uuid") |@| jv.validated[Boolean]("terminal")) { f }
        } getOrElse {
          Failure(Invalid("Storage mode %s not recogized.".format(other)))
        }
    }
  }
}
