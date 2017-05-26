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

package quasar.yggdrasil.vfs

import quasar.blueeyes.json.{ JParser, JString }
import quasar.blueeyes.json.serialization._
import quasar.blueeyes.json.serialization.DefaultSerialization._
import quasar.blueeyes.json.serialization.Extractor._
import quasar.blueeyes.json.serialization.Versioned._

import quasar.precog.util.{FileLock, IOUtils}

import org.slf4s.Logging

import java.io._
import java.util.UUID
import java.time.Instant

import scalaz._
import scalaz.effect.IO
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.traverse._
import scalaz.syntax.applicative._
import scalaz.syntax.std.boolean._

import shapeless._

object VersionLog {
  final val lockName = "versionLog"
  final val logName = "versionLog"
  final val completedLogName = "completedLog"
  final val currentVersionFilename = "HEAD"

  final val unsetSentinel = "unset"
  final val unsetSentinelJV = unsetSentinel.serialize.renderCompact

  final def currentVersionEntry(dir: File): EitherT[IO, ResourceError, VersionEntry] = {
    import ResourceError._
    val currentFile = new File(dir, currentVersionFilename)
    EitherT {
      IO {
        if (currentFile.exists) {
          for {
            jv <- JParser.parseFromFile(currentFile).leftMap(ioError).disjunction
            version <- jv match {
              case JString(`unsetSentinel`) =>
                \/.left(NotFound("No current data for the path %s exists; it has been archived.".format(dir)))
              case other =>
                other.validated[VersionEntry].disjunction leftMap { err => Corrupt(err.message) }
            }
          } yield version
        } else {
          \/.left(NotFound("No data found for path %s.".format(dir)))
        }
      }
    }
  }

  class LogFiles(val baseDir: File) {
    val headFile = new File(baseDir, currentVersionFilename)
    val logFile = new File(baseDir, logName)
    val completedFile = new File(baseDir, completedLogName)
  }

  def open(baseDir: File): IO[Validation[Error, VersionLog]] = IO {
    if (!baseDir.isDirectory) {
      if (!baseDir.mkdirs) throw new IllegalStateException(baseDir + " cannot be created as a directory.")
    }

    val logFiles = new LogFiles(baseDir)
    import logFiles._

    // Read in the list of versions as well as the current version
    val currentVersion: Validation[Error, Option[VersionEntry]] = if (headFile.exists) {
      for {
        jv <- JParser.parseFromFile(headFile).leftMap(Error.thrown)
        version <- jv match {
          case JString(`unsetSentinel`) => Success(None)
          case other => other.validated[VersionEntry].map(Some(_))
        }
      } yield version
    } else {
      Success(None)
    }

    val allVersions: Validation[Error, List[VersionEntry]] = if (logFile.exists) {
      for {
        jvs <- JParser.parseManyFromFile(logFile).leftMap(Error.thrown)
        versions <- jvs.toList.traverse[({ type λ[α] = Validation[Error, α] })#λ, VersionEntry](_.validated[VersionEntry])
      } yield versions
    } else {
      Success(Nil)
    }

    val completedVersions: Validation[Error, Set[UUID]] = if (completedFile.exists) {
      for {
        jvs <- JParser.parseManyFromFile(completedFile).leftMap(Error.thrown)
        versions <- jvs.toList.traverse[({ type λ[α] = Validation[Error, α] })#λ, UUID](_.validated[UUID])
      } yield versions.toSet
    } else {
      Success(Set.empty)
    }

    (currentVersion |@| allVersions |@| completedVersions) { new VersionLog(logFiles, _, _, _) }
  }
}

/**
  * Track path versions. This class is not thread safe
  */
class VersionLog(logFiles: VersionLog.LogFiles, initVersion: Option[VersionEntry], initAllVersions: List[VersionEntry], initCompletedVersions: Set[UUID]) extends Logging {
  import VersionLog._
  import logFiles._

  private[this] val workLock = FileLock(logFiles.baseDir, lockName)

  private[this] var currentVersion: Option[VersionEntry] = initVersion
  private[this] var allVersions: List[VersionEntry] = initAllVersions
  private[this] var completedVersions: Set[UUID] = initCompletedVersions

  def current: Option[VersionEntry] = currentVersion
  def find(version: UUID): Option[VersionEntry] = allVersions.find(_.id == version)
  def isCompleted(version: UUID) = completedVersions.contains(version)

  def close = {
    workLock.release
  }

  def addVersion(entry: VersionEntry): IO[Unit] = allVersions.find(_ == entry) map { _ =>
    IO(())
  } getOrElse {
    log.debug(s"Adding version entry $entry for base directory $baseDir.")
    IOUtils.writeToFile(entry.serialize.renderCompact + "\n", logFile, true) flatMap { _ =>
      IO(allVersions = allVersions :+ entry)
    }
  }

  def completeVersion(version: UUID): IO[Unit] = {
    if (allVersions.exists(_.id == version)) {
      !isCompleted(version) whenM {
        log.debug("Completing version " + version)
        IOUtils.writeToFile(version.serialize.renderCompact + "\n", completedFile, false)
      } map { _ => () }
    } else {
      IO.throwIO(new IllegalStateException("Cannot make nonexistent version %s current" format version))
    }
  }

  def setHead(newHead: UUID): IO[Unit] = {
    currentVersion.exists(_.id == newHead) unlessM {
      allVersions.find(_.id == newHead) traverse { entry =>
        log.debug("Setting HEAD to " + newHead)
        IOUtils.writeToFile(entry.serialize.renderCompact + "\n", headFile, false) map { _ =>
          currentVersion = Some(entry);
        }
      } flatMap {
        _.isEmpty.whenM(IO.throwIO(new IllegalStateException("Attempt to set head to nonexistent version %s" format newHead)))
      }
    } map { _ => () }
  }

  def clearHead = IOUtils.writeToFile(unsetSentinelJV, headFile, false).map { _ =>
    currentVersion = None
  }
}

case class VersionEntry(id: UUID, typeName: PathData.DataType, timestamp: Instant)

object VersionEntry {
  val schemaV1 = "id" :: "typeName" :: "timestamp" :: HNil

  implicit val Decomposer: Decomposer[VersionEntry] = decomposerV(schemaV1, Some("1.0".v))
  implicit val Extractor: Extractor[VersionEntry] = extractorV(schemaV1, Some("1.0".v))
}
