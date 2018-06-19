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

package quasar.yggdrasil.vfs

import quasar.contrib.pathy.{ADir, RDir, RFile}
import quasar.contrib.scalaz.stateT, stateT._
import quasar.contrib.iota.{:<<:, ACopK}

import argonaut.{Argonaut, Parse}

import cats.effect.IO
import fs2.Stream

import pathy.Path

import scalaz.{Free, Monad, StateT}
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import shims.{monadToCats => _, _}

import scodec.bits.ByteVector

import java.util.UUID

final case class VersionLog(baseDir: ADir, committed: List[Version], versions: Set[Version]) {
  def head: Option[Version] = committed.headOption
}

// TODO implement VERSION files
object VersionLog {
  import Argonaut._

  private type ST[F[_], A] = StateT[F, VersionLog, A]

  private val Head: RDir = Path.dir("HEAD")
  private val VersionsJson: RFile = Path.file("versions.json")
  private val VersionsJsonNew: RFile = Path.file("versions.json.new")

  // keep the 5 most recent versions, by default
  private val KeepLimit = 5

  // TODO failure recovery
  def init[S[a] <: ACopK[a]](baseDir: ADir)(implicit IP: POSIXOp :<<: S, IT: IO :<<: S): Free[S, VersionLog] = {
    for {
      exists <- POSIX.exists[S](baseDir </> VersionsJson)

      committed <- if (exists) {
        for {
          fileStream <- POSIX.openR[S](baseDir </> VersionsJson)

          // TODO character encoding!
          fileString = fileStream.map(_.toArray).map(new String(_)).foldMonoid
          json <- POSIXWithIO.generalize[S](fileString.compile.last)
        } yield json.flatMap(Parse.decodeOption[List[Version]](_)).getOrElse(Nil)
      } else {
        for {

          vnew <- POSIX.openW[S](baseDir </> VersionsJsonNew)

          json = List[Version]().asJson.nospaces
          // TODO character encoding!
          writer = Stream.emit(ByteVector(json.getBytes)).covary[POSIXWithIO].to(vnew).run
          _ <- POSIXWithIO.generalize(writer)

          _ <- POSIX.move[S](baseDir </> VersionsJsonNew, baseDir </> VersionsJson)
        } yield Nil
      }

      paths <- POSIX.ls[S](baseDir)

      versions = for {
        path <- paths
        dir <- Path.maybeDir(path).toList
        dirName <- Path.dirName(dir).toList

        version <- try {
          Version(UUID.fromString(dirName.value)) :: Nil
        } catch {
          case _: IllegalArgumentException => Nil
        }
      } yield version
    } yield VersionLog(baseDir, committed, versions.toSet)
  }

  def fresh[S[a] <: ACopK[a]](implicit I: POSIXOp :<<: S): StateT[Free[S, ?], VersionLog, Version] = {
    for {
      log <- StateTContrib.get[Free[S, ?], VersionLog]
      uuid <- POSIX.genUUID[S].liftM[ST]
      v = Version(uuid)

      back <- if (log.versions.contains(v)) {
        fresh[S]
      } else {
        for {
          _ <- StateTContrib.put[Free[S, ?], VersionLog](log.copy(versions = log.versions + v))
          target <- underlyingDir[Free[S, ?]](v)
          _ <- POSIX.mkDir[S](target).liftM[ST]
        } yield v
      }
    } yield back
  }

  // TODO there isn't any error handling here
  def underlyingDir[F[_]: Monad](v: Version): StateT[F, VersionLog, ADir] =
    StateTContrib.get[F, VersionLog].map(_.baseDir </> Path.dir(v.value.toString))

  // TODO add symlink
  def commit[S[a] <: ACopK[a]](v: Version)(implicit IP: POSIXOp :<<: S, IT: IO :<<: S): StateT[Free[S, ?], VersionLog, Unit] = {
    for {
      log <- StateTContrib.get[Free[S, ?], VersionLog]
      log2 = log.copy(committed = v :: log.committed)

      _ <- if (log.versions.contains(v)) {
        for {
          _ <- StateTContrib.put[Free[S, ?], VersionLog](log2)

          vnew <- POSIX.openW[S](log.baseDir </> VersionsJsonNew).liftM[ST]

          json = log2.committed.asJson.nospaces
          // TODO character encoding!
          writer = Stream.emit(ByteVector(json.getBytes)).covary[POSIXWithIO].to(vnew).run
          _ <- POSIXWithIO.generalize(writer).liftM[ST]

          _ <- POSIX.move[S](log.baseDir </> VersionsJsonNew, log.baseDir </> VersionsJson).liftM[ST]

          _ <- POSIX.delete[S](log.baseDir </> Head).liftM[ST]
          _ <- POSIX.linkDir[S](
            log.baseDir </> Path.dir(v.value.toString),
            log.baseDir </> Head).liftM[ST]
        } yield ()
      } else {
        ().point[StateT[Free[S, ?], VersionLog, ?]]
      }
    } yield ()
  }

  def underlyingHeadDir[F[_]: Monad]: StateT[F, VersionLog, Option[ADir]] = {
    for {
      log <- StateTContrib.get[F, VersionLog]

      back <- log.head.traverse(underlyingDir[F](_))
    } yield back
  }

  def purgeOld[S[a] <: ACopK[a]](implicit I: POSIXOp :<<: S): StateT[Free[S, ?], VersionLog, Unit] = {
    for {
      log <- StateTContrib.get[Free[S, ?], VersionLog]
      toPurge = log.committed.drop(KeepLimit)

      _ <- toPurge traverse { v =>
        for {
          dir <- underlyingDir[Free[S, ?]](v)
          _ <- POSIX.delete[S](dir).liftM[ST]
        } yield ()
      }

      log2 = log.copy(
        committed = log.committed.take(KeepLimit),
        versions = log.versions -- toPurge)

      // TODO write new versions.json
      _ <- StateTContrib.put[Free[S, ?], VersionLog](log2)
    } yield ()
  }
}
