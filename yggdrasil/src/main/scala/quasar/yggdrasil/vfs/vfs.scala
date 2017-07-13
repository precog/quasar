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

import quasar.contrib.pathy.{unsafeSandboxAbs, ADir, AFile, APath, RPath}
import quasar.contrib.scalaz.stateT, stateT._
import quasar.fs.MoveSemantics

import argonaut.{Argonaut, Parse}

import fs2.Stream
import fs2.async, async.mutable
import fs2.interop.scalaz._
import fs2.util.Async

import pathy.Path

import scalaz.{~>, :<:, Coproduct, Free, Monad, NaturalTransformation, StateT}
import scalaz.concurrent.Task
import scalaz.std.list._
import scalaz.std.map._
import scalaz.std.string._
import scalaz.std.vector._
import scalaz.syntax.equal._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._

import scodec.bits.ByteVector

import java.io.File
import java.util.UUID

final case class VFS(
    baseDir: ADir,
    metaLog: VersionLog,
    paths: Map[AFile, Blob],
    index: Map[ADir, Vector[RPath]],
    versions: Map[Blob, VersionLog],    // always getOrElse this with VersionLog.init
    blobs: Set[Blob])

object FreeVFS {
  import Argonaut._

  import RPath._

  private val MetaDir = Path.dir("META")
  private val PathsFile = Path.file("paths.json")
  private val IndexFile = Path.file("index.json")

  private type ST[F[_], A] = StateT[F, VFS, A]

  def init[S[_]](baseDir: ADir)(implicit IP: POSIXOp :<: S, IT: Task :<: S): Free[S, VFS] = {
    for {
      exists <- POSIX.exists[S](baseDir </> MetaDir)

      triple <- if (!exists) {
        for {
          _ <- POSIX.mkDir[S](baseDir </> MetaDir)
          metaLog <- VersionLog.init[S](baseDir </> MetaDir)

          paths = Map[AFile, Blob]()
          index = Map[ADir, Vector[RPath]]()
          metaLog2 <- persistMeta[S](paths, index).exec(metaLog)
        } yield (metaLog2, paths, index)
      } else {
        for {
          metaLog <- VersionLog.init[S](baseDir </> MetaDir)

          pathsST = for {
            dir <- VersionLog.underlyingHeadDir[Free[S, ?]]

            pair <- dir match {
              case Some(dir) =>
                readMeta(dir)

              case None =>
                val paths = Map[AFile, Blob]()
                val index = Map[ADir, Vector[RPath]]()
                persistMeta[S](paths, index).map(_ => (paths, index))
            }
          } yield pair

          pairPair <- pathsST(metaLog)
          (metaLog2, (paths, index)) = pairPair
        } yield (metaLog2, paths, index)
      }

      (metaLog, paths, index) = triple

      blobPaths <- POSIX.ls[S](baseDir)

      dirs = blobPaths flatMap { path =>
        val back = Path.maybeDir(path).flatMap(Path.dirName).map(_.value)

        back.toList
      }

      blobs = dirs.filterNot(n => Path.dir(n) === MetaDir) flatMap { name =>
        try {
          List(Blob(UUID.fromString(name)))
        } catch {
          case _: IllegalArgumentException => Nil
        }
      }
    } yield VFS(baseDir, metaLog, paths, index, Map(), blobs.toSet)
  }

  private def persistMeta[S[_]](paths: Map[AFile, Blob], index: Map[ADir, Vector[RPath]])(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VersionLog, Unit] = {
    for {
      v <- VersionLog.fresh[S]
      target <- VersionLog.underlyingDir[Free[S, ?]](v)

      pathsSink <- POSIX.openW[S](target </> PathsFile).liftM[StateT[?[_], VersionLog, ?]]

      pathsFromStrings = paths map {
        case (k, v) => Path.posixCodec.printPath(k) -> v
      }

      pathsJson = pathsFromStrings.asJson.nospaces
      pathsWriter = Stream.emit(ByteVector(pathsJson.getBytes)).to(pathsSink).run
      _ <- POSIXWithTask.generalize(pathsWriter).liftM[StateT[?[_], VersionLog, ?]]

      indexSink <- POSIX.openW[S](target </> IndexFile).liftM[StateT[?[_], VersionLog, ?]]

      indexFromStrings = index map {
        case (k, v) => Path.posixCodec.printPath(k) -> v
      }

      indexJson = indexFromStrings.asJson.nospaces
      indexWriter = Stream.emit(ByteVector(indexJson.getBytes)).to(indexSink).run
      _ <- POSIXWithTask.generalize[S](indexWriter).liftM[StateT[?[_], VersionLog, ?]]

      _ <- VersionLog.commit[S](v)
      _ <- VersionLog.purgeOld[S]
    } yield ()
  }

  private def readMeta[S[_]](dir: ADir)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VersionLog, (Map[AFile, Blob], Map[ADir, Vector[RPath]])] = {
    for {
      maybeDir <- VersionLog.underlyingHeadDir[Free[S, ?]]

      back <- maybeDir match {
        case Some(dir) =>
          for {
            pathsStream <- POSIX.openR[S](dir </> PathsFile).liftM[StateT[?[_], VersionLog, ?]]
            pathsString = pathsStream.map(_.toArray).map(new String(_)).foldMonoid
            pathsJson <- POSIXWithTask.generalize[S](pathsString.runLast).liftM[StateT[?[_], VersionLog, ?]]

            pathsFromStrings =
              pathsJson.flatMap(Parse.decodeOption[Map[String, Blob]]).getOrElse(Map())

            paths = pathsFromStrings flatMap {
              case (k, v) =>
                val maybe = Path.posixCodec.parseAbsFile(k).map(unsafeSandboxAbs)
                maybe.fold(Map[AFile, Blob]())(k => Map(k -> v))
            }

            indexStream <- POSIX.openR[S](dir </> IndexFile).liftM[StateT[?[_], VersionLog, ?]]
            indexString = indexStream.map(_.toArray).map(new String(_)).foldMonoid
            indexJson <- POSIXWithTask.generalize[S](indexString.runLast).liftM[StateT[?[_], VersionLog, ?]]

            indexFromStrings =
              indexJson.flatMap(Parse.decodeOption[Map[String, Vector[RPath]]](_)).getOrElse(Map())

            index = indexFromStrings flatMap {
              case (k, v) =>
                val maybe = Path.posixCodec.parseAbsDir(k).map(unsafeSandboxAbs)

                maybe.fold(Map[ADir, Vector[RPath]]())(k => Map(k -> v))
            }
          } yield (paths, index)

        case None =>
          (Map[AFile, Blob](), Map[ADir, Vector[RPath]]()).point[StateT[Free[S, ?], VersionLog, ?]]
      }
    } yield back
  }

  def scratch[S[_]](implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Blob] = {
    for {
      vfs <- StateTContrib.get[Free[S, ?], VFS]
      uuid <- POSIX.genUUID[S].liftM[ST]

      target = vfs.baseDir </> Path.dir(uuid.toString)
      blob = Blob(uuid)

      back <- if (!vfs.blobs.contains(blob)) {
        for {
          _ <- POSIX.mkDir[S](target).liftM[ST]
          vlog <- VersionLog.init[S](target).liftM[ST]
          vfs2 = vfs.copy(blobs = vfs.blobs + blob, versions = vfs.versions + (blob -> vlog))
          _ <- StateTContrib.put[Free[S, ?], VFS](vfs2)
        } yield blob
      } else {
        scratch[S]
      }
    } yield back
  }

  def exists[F[_]: Monad](path: APath): StateT[F, VFS, Boolean] = {
    Path.refineType(path).fold(
      dir => StateTContrib.get[F, VFS].map(_.index.contains(dir)),
      file => StateTContrib.get[F, VFS].map(_.paths.contains(file)))
  }

  def ls[F[_]: Monad](parent: ADir): StateT[F, VFS, List[RPath]] = {
    StateTContrib.get[F, VFS] map { vfs =>
      vfs.index.get(parent).map(_.toList).getOrElse(Nil)
    }
  }

  def link[S[_]](from: Blob, to: AFile)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Boolean] = {
    for {
      vfs <- StateTContrib.get[Free[S, ?], VFS]

      success <- if (vfs.paths.contains(to) || !vfs.blobs.contains(from)) {
        false.point[ST[Free[S, ?], ?]]
      } else {
        val paths2 = vfs.paths + (to -> from)
        val index2 = vfs.index |+| computeSubIndex(to)

        for {
          metaLog2 <- persistMeta[S](paths2, index2).exec(vfs.metaLog).liftM[ST]
          vfs2 = vfs.copy(metaLog = metaLog2, paths = paths2, index = index2)
          _ <- StateTContrib.put[Free[S, ?], VFS](vfs2)
        } yield true
      }
    } yield success
  }

  def moveFile[S[_]](from: AFile, to: AFile, semantics: MoveSemantics)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Boolean] = {
    for {
      vfs <- StateTContrib.get[Free[S, ?], VFS]

      success <- if (vfs.paths.contains(from) && semantics(vfs.paths.contains(to))) {
        // we could delegate to delete >> link, but that would persist metadata twice
        val blob = vfs.paths(from)
        val paths2 = vfs.paths - from + (to -> blob)
        val index2 = computeIndex(paths2.keys.toList)

        for {
          metaLog2 <- persistMeta[S](paths2, index2).exec(vfs.metaLog).liftM[ST]
          vfs2 = vfs.copy(metaLog = metaLog2, paths = paths2, index = index2)
          _ <- StateTContrib.put[Free[S, ?], VFS](vfs2)
        } yield true
      } else {
        false.point[ST[Free[S, ?], ?]]
      }
    } yield success
  }

  def moveDir[S[_]](from: ADir, to: ADir, semantics: MoveSemantics)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Boolean] = {
    for {
      vfs <- StateTContrib.get[Free[S, ?], VFS]

      // TODO overwrite file with dir
      success <- if (vfs.index.contains(from) && semantics(vfs.index.contains(to))) {
        for {
          // remove all pre-existing directories
          _ <- if (vfs.index.contains(to))
            deleteDir[S](to, false)
          else
            ().point[ST[Free[S, ?], ?]]

          leaves <- reparentLeaves[Free[S, ?]](from, to)
          vfs2 <- StateTContrib.get[Free[S, ?], VFS]

          index2 = computeIndex(vfs2.paths.keys.toList)

          metaLog2 <- persistMeta[S](vfs2.paths, index2).exec(vfs2.metaLog).liftM[ST]

          _ <- StateTContrib.put[Free[S, ?], VFS](
            vfs2.copy(index = index2, metaLog = metaLog2))
        } yield true
      } else {
        false.point[ST[Free[S, ?], ?]]
      }
    } yield success
  }

  def reparentLeaves[F[_]: Monad](from: ADir, to: ADir): StateT[F, VFS, Unit] = {
    for {
      vfs <- StateTContrib.get[F, VFS]

      _ <- vfs.index.getOrElse(from, Vector.empty) traverse { child =>
        Path.refineType(child).fold[StateT[F, VFS, Unit]](
          d => reparentLeaves(from </> d, to </> d),
          { f =>
            val source = from </> f
            val result = to </> f

            for {
              vfs <- StateTContrib.get[F, VFS]
              blob = vfs.paths(source)
              paths2 = vfs.paths - source + (result -> blob)
              _ <- StateTContrib.put[F, VFS](vfs.copy(paths = paths2))
            } yield ()
          })
      }
    } yield ()
  }

  def delete[S[_]](target: APath)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Boolean] =
    Path.refineType(target).fold(deleteDir[S](_), deleteFile[S](_))

  private def deleteFile[S[_]](target: AFile)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Boolean] = {
    for {
      vfs <- StateTContrib.get[Free[S, ?], VFS]

      success <- if (vfs.paths.contains(target)) {
        val paths2 = vfs.paths - target
        val index2 = computeIndex(paths2.keys.toList)

        for {
          metaLog2 <- persistMeta[S](paths2, index2).exec(vfs.metaLog).liftM[ST]
          vfs2 = vfs.copy(metaLog = metaLog2, paths = paths2, index = index2)
          _ <- StateTContrib.put[Free[S, ?], VFS](vfs2)
        } yield true
      } else {
        false.point[ST[Free[S, ?], ?]]
      }
    } yield success
  }

  private def deleteDir[S[_]](dir: ADir, persist: Boolean = true)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Boolean] = {
    for {
      vfs <- StateTContrib.get[Free[S, ?], VFS]

      _ <- vfs.index.getOrElse(dir, Vector.empty) traverse { child =>
        Path.refineType(child).fold[StateT[Free[S, ?], VFS, Boolean]](
          d => deleteDir(dir </> d),
          { f =>
            for {
              vfs <- StateTContrib.get[Free[S, ?], VFS]
              paths2 = vfs.paths - (dir </> f)
              _ <- StateTContrib.put[Free[S, ?], VFS](vfs.copy(paths = paths2))
            } yield true
          })
      }

      _ <- if (persist) {
        for {
          // get the new one
          vfs2 <- StateTContrib.get[Free[S, ?], VFS]

          index2 = computeIndex(vfs2.paths.keys.toList)

          metaLog2 <- persistMeta[S](vfs2.paths, index2).exec(vfs2.metaLog).liftM[ST]
          _ <- StateTContrib.put[Free[S, ?], VFS](vfs2.copy(metaLog = metaLog2, index = index2))
        } yield ()
      } else {
        ().point[ST[Free[S, ?], ?]]
      }
    } yield true    // TODO failure modes
  }

  def readPath[F[_]: Monad](path: AFile): StateT[F, VFS, Option[Blob]] =
    StateTContrib.get[F, VFS].map(_.paths.get(path))

  def underlyingDir[S[_]](blob: Blob, version: Version)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Option[ADir]] =
    withVLog(blob)(VersionLog.underlyingDir[Free[S, ?]](version))

  private def blobVLog[S[_]](blob: Blob)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, VersionLog] = {
    for {
      vfs <- StateTContrib.get[Free[S, ?], VFS]

      vlog <- vfs.versions.get(blob) match {
        case Some(vlog) => vlog.point[ST[Free[S, ?], ?]]
        case None =>
          for {
            vlog <- VersionLog.init[S](vfs.baseDir </> Path.dir(blob.value.toString)).liftM[ST]
            vfs2 = vfs.copy(versions = vfs.versions + (blob -> vlog))
            _ <- StateTContrib.put[Free[S, ?], VFS](vfs2)
          } yield vlog
      }
    } yield vlog
  }

  private def withVLog[S[_], A](blob: Blob)(st: StateT[Free[S, ?], VersionLog, A])(
    implicit
      IP: POSIXOp :<: S,
      IT: Task :<: S): StateT[Free[S, ?], VFS, Option[A]] = {

    for {
      vfs <- StateTContrib.get[Free[S, ?], VFS]

      back <- if (vfs.blobs.contains(blob)) {
        for {
          vlog <- blobVLog[S](blob)
          pair <- st(vlog).liftM[ST]
          (vlog2, a) = pair

          vfs <- StateTContrib.get[Free[S, ?], VFS]
          vfs2 = vfs.copy(versions = vfs.versions.updated(blob, vlog2))
          _ <- StateTContrib.put[Free[S, ?], VFS](vfs2)
        } yield Some(a)
      } else {
        (None: Option[A]).point[StateT[Free[S, ?], VFS, ?]]
      }
    } yield back
  }

  def headOfBlob[S[_]](blob: Blob)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Option[Version]] =
    blobVLog[S](blob).map(_.head)

  def fresh[S[_]](blob: Blob)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Option[Version]] =
    withVLog(blob)(VersionLog.fresh[S])

  def commit[S[_]](blob: Blob, version: Version)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Unit] =
    withVLog(blob)(VersionLog.commit[S](version)).map(_ => ())

  private def computeIndex(paths: List[APath]): Map[ADir, Vector[RPath]] =
    paths.foldMap(computeSubIndex(_))

  // computes the subset of the index pertaining to the given path, recursively
  private def computeSubIndex(path: APath): Map[ADir, Vector[RPath]] = {
    Path.peel(path) match {
      case Some((dir, dirOrFile)) =>
        val rpath = dirOrFile.fold(Path.dir1, Path.file1)
        computeSubIndex(dir) + (dir -> Vector(rpath))

      case None => Map()
    }
  }
}

/**
 * A Task frontend to FreeVFS which imposes serial access semantics.
 * This may result in lower throughput than a frontend with transactional
 * semantics, but it is considerably easier to implement.  Note that the
 * serialization is imposed without thread locking.
 */
final class SerialVFS private (
    root: File,
    @volatile private var current: VFS,
    worker: mutable.Queue[Task, Task[Unit]],    // this exists solely for #serialize
    interp: POSIXWithTask ~> Task) {

  import SerialVFS.S

  def scratch: Task[Blob] =
    runST(FreeVFS.scratch[S])

  def exists(path: APath): Task[Boolean] =
    runST(FreeVFS.exists[POSIXWithTask](path))

  def ls(parent: ADir): Task[List[RPath]] =
    runST(FreeVFS.ls[POSIXWithTask](parent))

  def link(from: Blob, to: AFile): Task[Boolean] =
    runST(FreeVFS.link[S](from, to))

  def moveFile(from: AFile, to: AFile, semantics: MoveSemantics): Task[Boolean] =
    runST(FreeVFS.moveFile[S](from, to, semantics))

  def moveDir(from: ADir, to: ADir, semantics: MoveSemantics): Task[Boolean] =
    runST(FreeVFS.moveDir[S](from, to, semantics))

  def delete(target: APath): Task[Boolean] =
    runST(FreeVFS.delete[S](target))

  def readPath(path: AFile): Task[Option[Blob]] =
    runST(FreeVFS.readPath[POSIXWithTask](path))

  def underlyingDir(blob: Blob, version: Version): Task[File] = {
    runST(FreeVFS.underlyingDir[S](blob, version)).map(_.get) map { adir =>
      new File(root, Path.posixCodec.printPath(adir))
    }
  }

  def headOfBlob(blob: Blob): Task[Option[Version]] =
    runST(FreeVFS.headOfBlob[S](blob))

  def fresh(blob: Blob): Task[Version] =
    runST(FreeVFS.fresh[S](blob)).map(_.get)

  def commit(blob: Blob, version: Version): Task[Unit] =
    runST(FreeVFS.commit[S](blob, version))

  // runs the given State against the current mutable cell with serialized semantics
  private def runST[A](st: StateT[POSIXWithTask, VFS, A]): Task[A] = {
    val run = for {
      vfs <- Task.delay(current)
      pair <- st.mapT(interp(_)).apply(vfs)
      (vfs2, back) = pair
      _ <- Task.delay(current = vfs2)
    } yield back

    serialize(run)
  }

  private def serialize[A](ta: Task[A]): Task[A] = {
    for {
      ref <- Async[Task].ref[A]
      _ <- worker.enqueue1(ta.flatMap(ref.setPure))
      a <- ref.get
    } yield a
  }
}

object SerialVFS {
  private[SerialVFS] type S[A] = Coproduct[POSIXOp, Task, A]

  /**
   * Returns a stream which will immediately emit SerialVFS and then
   * continue running until the end of the world.  When the stream ends,
   * SerialVFS will no longer function.
   */
  def apply(root: File): Stream[Task, SerialVFS] = {
    for {
      pint <- Stream.eval(RealPOSIX(root))
      interp = λ[S ~> Task](_.fold(pint, NaturalTransformation.refl[Task]))
      vfs <- Stream.eval(FreeVFS.init[S](Path.rootDir).foldMap(interp))
      worker <- Stream.eval(async.boundedQueue[Task, Task[Unit]](10))

      svfs = new SerialVFS(root, vfs, worker, λ[POSIXWithTask ~> Task](_.foldMap(interp)))
      back <- Stream.emit(svfs).merge(worker.dequeue.evalMap(t => t).drain)
    } yield back
  }
}
