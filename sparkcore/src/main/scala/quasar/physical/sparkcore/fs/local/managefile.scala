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

package quasar.physical.sparkcore.fs.local

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.effect.Failure
import quasar.fp.free._
import quasar.fs._,
  FileSystemError._,
  ManageFile._, ManageFile.MoveScenario._,
  PathError._
import quasar.fs.impl.ensureMoveSemantics

import java.io.FileNotFoundException
import java.lang.Exception
import java.nio.{file => nio}
import scala.util.control.NonFatal

import org.apache.commons.io.FileUtils
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

object managefile {

  def chrooted[S[_]](prefix: ADir)(implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): ManageFile ~> Free[S, ?] =
    flatMapSNT(interpret) compose chroot.manageFile[ManageFile](prefix)

  def interpret[S[_]](implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): ManageFile ~> Free[S, ?] =
    new (ManageFile ~> Free[S, ?]) {
      def apply[A](mf: ManageFile[A]): Free[S, A] = mf match {
        case Move(FileToFile(sf, df), semantics) =>
          (ensureMoveSemantics(sf, df, doesPathExist, semantics).toLeft(()) *>
            moveFile(sf, df).liftM[FileSystemErrT]).run
        case Move(DirToDir(sd, dd), semantics) =>
          (ensureMoveSemantics(sd, dd, doesPathExist, semantics).toLeft(()) *>
            moveDir(sd, dd).liftM[FileSystemErrT]).run
        case Delete(path) => delete(path)
        case TempFile(near) => tempFile(near)
      }
    }

  private def toNioPath(path: APath): nio.Path =
    nio.Paths.get(posixCodec.unsafePrintPath(path))

  private def doesPathExist[S[_]](implicit
    s0: Task :<: S
  ): APath => Free[S, Boolean] = path => lift(Task.delay {
    nio.Files.exists(toNioPath(path))
  }).into[S]

  private def moveFile[S[_]](src: AFile, dst: AFile)(implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): Free[S, Unit] = {
    val move: Task[PhysicalError \/ Unit] = Task.delay {
      val deleted = FileUtils.deleteQuietly(toNioPath(dst).toFile())
      FileUtils.moveFile(toNioPath(src).toFile(), toNioPath(dst).toFile)
    }.as(().right[PhysicalError]).handle {
      case NonFatal(ex : Exception) => UnhandledFSError(ex).left[Unit]
    }
    Failure.Ops[PhysicalError, S].unattempt(lift(move).into[S])
  }

  private def moveDir[S[_]](src: ADir, dst: ADir)(implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): Free[S, Unit] = {

    val move: Task[PhysicalError \/ Unit] = Task.delay {
      val deleted = FileUtils.deleteDirectory(toNioPath(dst).toFile())
      FileUtils.moveDirectory(toNioPath(src).toFile(), toNioPath(dst).toFile())
    }.as(().right[PhysicalError]).handle {
      case NonFatal(ex: Exception) => UnhandledFSError(ex).left[Unit]
    }

    Failure.Ops[PhysicalError, S].unattempt(lift(move).into[S])
  }

  private def delete[S[_]](path: APath)(implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): Free[S, FileSystemError \/ Unit] = {
    val del: Task[PhysicalError \/ (FileSystemError \/ Unit)] = Task.delay {
      FileUtils.forceDelete(toNioPath(path).toFile())
    }.as(().right[FileSystemError].right[PhysicalError]).handle {
      case e: FileNotFoundException => pathErr(pathNotFound(path)).left[Unit].right[PhysicalError]
      case NonFatal(e : Exception) => UnhandledFSError(e).left[FileSystemError \/ Unit]
    }
    Failure.Ops[PhysicalError, S].unattempt(lift(del).into[S])
  }

  private def tempFile[S[_]](near: APath)(implicit
    s0: Task :<: S
  ): Free[S, FileSystemError \/ AFile] = injectFT[Task, S].apply{
    Task.delay {
      val parent: ADir = refineType(near).fold(d => d, fileParent(_))
      val random = scala.util.Random.nextInt().toString
        (parent </> file(s"quasar-$random.tmp")).right[FileSystemError]
    }
  }
}
