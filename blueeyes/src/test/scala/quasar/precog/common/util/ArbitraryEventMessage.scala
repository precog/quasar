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

import security._
import quasar.blueeyes._, json._
import quasar.precog.JsonTestSupport._, Gen._

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

trait ArbitraryEventMessage {
  def genStreamId: Gen[Option[UUID]] = Gen.oneOf(Gen lzy Some(randomUuid), Gen const None)

  def genPath: Gen[Path] = Gen.resize(10, Gen.containerOf[List, String](alphaStr)) map { elements =>
    Path(elements.filter(_.length > 0))
  }

  def genWriteMode: Gen[WriteMode] =
      Gen.oneOf(AccessMode.Create, AccessMode.Replace, AccessMode.Append)

  def genStreamRef: Gen[StreamRef] =
    for {
      terminal <- arbitrary[Boolean]
      storeMode <- genWriteMode
    } yield StreamRef.forWriteMode(storeMode, terminal)

  def genEventId: Gen[EventId] =
    for {
      producerId <- choose(0,1000000)
      sequenceId <- choose(0, 1000000)
    } yield EventId(producerId, sequenceId)

  def genRandomIngest: Gen[Ingest] =
    for {
      apiKey <- alphaStr
      path <- genPath
      ownerAccountId <- alphaStr
      content <- vectorOf(genJValue)
      if !content.isEmpty
      jobId <- genIdentifier.optional
      streamRef <- genStreamRef
    } yield Ingest(apiKey, path, Some(Authorities(ownerAccountId)), content, jobId, instant.now(), streamRef)

  def genRandomArchive: Gen[Archive] =
    for {
      apiKey <- alphaStr
      path <- genPath
      jobId <- genIdentifier.optional
    } yield Archive(apiKey, path, jobId, instant.now())

  def genRandomIngestMessage: Gen[IngestMessage] =
    for {
      ingest <- genRandomIngest if ingest.writeAs.isDefined
      eventIds <- containerOfN[List, EventId](ingest.data.size, genEventId).map(l => Vector(l: _*))
      streamRef <- genStreamRef
    } yield {
      //TODO: Replace with IngestMessage.fromIngest when it's usable
      val data = (eventIds zip ingest.data) map { Function.tupled(IngestRecord.apply) }
      IngestMessage(ingest.apiKey, ingest.path, ingest.writeAs.get, data, ingest.jobId, instant.now(), streamRef)
    }

  def genRandomArchiveMessage: Gen[ArchiveMessage] =
    for {
      eventId <- genEventId
      archive <- genRandomArchive
    } yield ArchiveMessage(archive.apiKey, archive.path, archive.jobId, eventId, archive.timestamp)

  def genRandomEventMessage: Gen[EventMessage] =
    frequency(
      (1, genRandomArchiveMessage),
      (10, genRandomIngestMessage)
    )
}

trait RealisticEventMessage extends ArbitraryEventMessage {
  val ingestAPIKey: APIKey
  val ingestOwnerAccountId: Authorities

  lazy val producers = 4

  lazy val eventIds: Map[Int, AtomicInteger] = 0.until(producers).map(_ -> new AtomicInteger).toMap

  lazy val paths = buildBoundedPaths(3)
  lazy val jpaths = buildBoundedJPaths(3)

  def buildBoundedPaths(depth: Int): List[String] = {
    buildChildPaths(List.empty, depth).map("/" + _.reverse.mkString("/"))
  }

  def buildBoundedJPaths(depth: Int): List[JPath] = {
    buildChildPaths(List.empty, depth).map(_.reverse.mkString(".")).filter(_.length > 0).map(JPath(_))
  }

  def buildChildPaths(parent: List[String], depth: Int): List[List[String]] = {
    if (depth == 0) {
      List(parent)
    } else {
      parent ::
      containerOfN[List, String](choose(2,4).sample.get, resize(10, alphaStr)).map(_.filter(_.length > 1).flatMap(child => buildChildPaths(child :: parent, depth - 1))).sample.get
    }
  }

  def genStablePaths: Gen[Seq[String]] = lzy(paths)
  def genStableJPaths: Gen[Seq[JPath]] = lzy(jpaths)

  def genStablePath: Gen[String] = oneOf(paths)
  def genStableJPath: Gen[JPath] = oneOf(jpaths)

  def genIngestData: Gen[JValue] = for {
    paths  <- containerOfN[Set, JPath](10, genStableJPath)
    values <- containerOfN[Set, JValue](10, genSimple)
  } yield {
    (paths zip values).foldLeft[JValue](JObject(Nil)) {
      case (obj, (path, value)) => obj.set(path, value)
    }
  }

  def genIngest: Gen[Ingest] = for {
    path <- genStablePath
    ingestData <- containerOf[List, JValue](genIngestData).map(l => Vector(l: _*))
    streamRef <- genStreamRef
  } yield Ingest(ingestAPIKey, Path(path), Some(ingestOwnerAccountId), ingestData, None, instant.now(), streamRef)

  def genIngestMessage: Gen[IngestMessage] = for {
    producerId <- choose(0, producers-1)
    ingest <- genIngest
  } yield {
    val records = ingest.data map { jv => IngestRecord(EventId(producerId, eventIds(producerId).getAndIncrement), jv) }
    IngestMessage(ingest.apiKey, ingest.path, ingest.writeAs.get, records, ingest.jobId, ingest.timestamp, ingest.streamRef)
  }
}
