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
package ingest

import quasar.blueeyes._
import security._

import quasar.blueeyes._, json._, serialization._
import IsoSerialization._, Iso8601Serialization._, Versioned._, Extractor._
import scalaz._, Scalaz._, Validation._

sealed trait EventMessage {
  def apiKey: APIKey
  def path: Path
  def jobId: Option[JobId]
  def timestamp: Instant
  def fold[A](im: IngestMessage => A, am: ArchiveMessage => A, sf: StoreFileMessage => A): A
}

object EventMessage {
  type EventMessageExtraction = (APIKey, Path, Authorities => EventMessage) \/ EventMessage

  // an instant that's close enough to the start of timestamping for our purposes
  val defaultTimestamp = instant fromMillis 1362465101979L

  implicit val decomposer: Decomposer[EventMessage] = new Decomposer[EventMessage] {
    override def decompose(eventMessage: EventMessage): JValue = {
      eventMessage.fold(IngestMessage.Decomposer.apply _, ArchiveMessage.Decomposer.apply _, StoreFileMessage.Decomposer.apply _)
    }
  }
}

case class EventId(producerId: Int, sequenceId: Int) { /** ProducerId and SequenceId used to be aliases to Int */
  val uid = (producerId.toLong << 32) | (sequenceId.toLong & 0xFFFFFFFFL)
}

object EventId {
  val schemaV1 = "producerId" :: "sequenceId" :: HNil

  implicit val (decomposerV1, extractorV1) = serializationV[EventId](schemaV1, Some("1.0".v))

  def fromLong(id: Long): EventId = EventId(producerId(id), sequenceId(id))

  def producerId(id: Long) = (id >> 32).toInt
  def sequenceId(id: Long) = id.toInt
}

case class IngestRecord(eventId: EventId, value: JValue)

object IngestRecord {
  val schemaV1 = "eventId" :: "jvalue" :: HNil

  implicit val (decomposerV1, extractorV1) = serializationV[IngestRecord](schemaV1, Some("1.0".v))
}

/**
  * ownerAccountId must be determined before the message is sent to the central queue; we have to
  * accept records for processing in the local queue.
  */
case class IngestMessage(apiKey: APIKey,
                         path: Path,
                         writeAs: Authorities,
                         data: Seq[IngestRecord],
                         jobId: Option[JobId],
                         timestamp: Instant,
                         streamRef: StreamRef)
    extends EventMessage {
  def fold[A](im: IngestMessage => A, am: ArchiveMessage => A, sf: StoreFileMessage => A): A = im(this)
  def split: Seq[IngestMessage] = {
    if (data.size > 1) {
      val (dataA, dataB)  = data.splitAt(data.size / 2)
      val Seq(refA, refB) = streamRef.split(2)
      List(this.copy(data = dataA, streamRef = refA), this.copy(data = dataB, streamRef = refB))
    } else {
      List(this)
    }
  }

  override def toString = "IngestMessage(%s, %s, %s, (%d records), %s, %s, %s)".format(apiKey, path, writeAs, data.size, jobId, timestamp, streamRef)
}

object IngestMessage {
  import EventMessage._

  val schemaV1 = "apiKey" :: "path" :: "writeAs" :: "data" :: "jobId" :: "timestamp" :: ("streamRef" ||| StreamRef.Append.asInstanceOf[StreamRef]) :: HNil
  implicit def seqExtractor[A: Extractor]: Extractor[Seq[A]] = implicitly[Extractor[List[A]]].map(_.toSeq)

  val decomposerV1: Decomposer[IngestMessage] = decomposerV[IngestMessage](schemaV1, Some("1.1".v))
  val extractorV1: Extractor[EventMessageExtraction] = new Extractor[EventMessageExtraction] {
    private val extractor = extractorV[IngestMessage](schemaV1, Some("1.1".v))
    override def validated(jv: JValue) = extractor.validated(jv).map(\/.right(_))
  }

  val extractorV0: Extractor[EventMessageExtraction] = new Extractor[EventMessageExtraction] {
    override def validated(obj: JValue): Validation[Error, EventMessageExtraction] =
      obj.validated[Ingest]("event").flatMap { ingest =>
        (obj.validated[Int]("producerId") |@|
              obj.validated[Int]("eventId")) { (producerId, sequenceId) =>
          val eventRecords = ingest.data map { jv =>
            IngestRecord(EventId(producerId, sequenceId), jv)
          }
          ingest.writeAs map { authorities =>
            assert(ingest.data.size == 1)
            \/.right(IngestMessage(ingest.apiKey, ingest.path, authorities, eventRecords, ingest.jobId, defaultTimestamp, StreamRef.Append))
          } getOrElse {
            \/.left(
              (ingest.apiKey,
               ingest.path,
               (authorities: Authorities) =>
                 IngestMessage(ingest.apiKey, ingest.path, authorities, eventRecords, ingest.jobId, defaultTimestamp, StreamRef.Append))
            )
          }
        }
      }
  }

  implicit val Decomposer: Decomposer[IngestMessage]        = decomposerV1
  implicit val Extractor: Extractor[EventMessageExtraction] = extractorV1 <+> extractorV0
}

case class ArchiveMessage(apiKey: APIKey, path: Path, jobId: Option[JobId], eventId: EventId, timestamp: Instant) extends EventMessage {
  def fold[A](im: IngestMessage => A, am: ArchiveMessage => A, sf: StoreFileMessage => A): A = am(this)
}

object ArchiveMessage {
  import EventMessage._
  val schemaV1 = "apiKey" :: "path" :: "jobId" :: "eventId" :: "timestamp" :: HNil

  val decomposerV1: Decomposer[ArchiveMessage] = decomposerV[ArchiveMessage](schemaV1, Some("1.0".v))
  val extractorV1: Extractor[ArchiveMessage]   = extractorV[ArchiveMessage](schemaV1, Some("1.0".v))
  val extractorV0: Extractor[ArchiveMessage] = new Extractor[ArchiveMessage] {
    override def validated(obj: JValue): Validation[Error, ArchiveMessage] = {
      (obj.validated[Int]("producerId") |@|
            obj.validated[Int]("deletionId") |@|
            obj.validated[Archive]("deletion")) { (producerId, sequenceId, archive) =>
        ArchiveMessage(archive.apiKey, archive.path, archive.jobId, EventId(producerId, sequenceId), defaultTimestamp)
      }
    }
  }

  implicit val Decomposer: Decomposer[ArchiveMessage] = decomposerV1
  implicit val Extractor: Extractor[ArchiveMessage]   = extractorV1 <+> extractorV0
}

case class StoreFileMessage(apiKey: APIKey,
                            path: Path,
                            writeAs: Authorities,
                            jobId: Option[JobId],
                            eventId: EventId,
                            content: FileContent,
                            timestamp: Instant,
                            streamRef: StreamRef)
    extends EventMessage {
  def fold[A](im: IngestMessage => A, am: ArchiveMessage => A, sf: StoreFileMessage => A): A = sf(this)
}

object StoreFileMessage {
  val schemaV1 = "apiKey" :: "path" :: "writeAs" :: "jobId" :: "eventId" :: "content" :: "timestamp" :: "streamRef" :: HNil

  implicit val Decomposer: Decomposer[StoreFileMessage] = decomposerV[StoreFileMessage](schemaV1, Some("1.0".v))

  implicit val Extractor: Extractor[StoreFileMessage] = extractorV[StoreFileMessage](schemaV1, Some("1.0".v))
}
