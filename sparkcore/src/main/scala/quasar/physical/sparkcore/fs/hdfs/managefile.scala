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

package quasar.physical.sparkcore.fs.hdfs

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.effect.Failure
import quasar.fp.free._
import quasar.fs._,
  FileSystemError._,
  ManageFile._, ManageFile.MoveScenario._,
  PathError._
import quasar.fs.impl.ensureMoveSemantics

import java.lang.Exception
import scala.util.control.NonFatal

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

object managefile {

  def chrooted[S[_]](prefix: ADir, fileSystem: Task[FileSystem])(implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): ManageFile ~> Free[S, ?] =
    flatMapSNT(interpret(fileSystem)) compose chroot.manageFile[ManageFile](prefix)

  def interpret[S[_]](fileSystem: Task[FileSystem])(implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): ManageFile ~> Free[S, ?] = {

    new (ManageFile ~> Free[S, ?]) {
      def apply[A](mf: ManageFile[A]): Free[S, A] = mf match {
        case Move(FileToFile(sf, df), semantics) =>
          (ensureMoveSemantics[Free[S, ?]](sf, df, pathExists(fileSystem) _, semantics).toLeft(()) *>
            moveFile(sf, df, fileSystem).liftM[FileSystemErrT]).run
        case Move(DirToDir(sd, dd), semantics) =>
          (ensureMoveSemantics[Free[S, ?]](sd, dd, pathExists(fileSystem) _, semantics).toLeft(()) *>
            moveDir(sd, dd, fileSystem).liftM[FileSystemErrT]).run
        case Delete(path) => delete(path, fileSystem)
        case TempFile(near) => tempFile(near)
      }
    }}

  private def toPath(apath: APath): Task[Path] = Task.delay {
    new Path(posixCodec.unsafePrintPath(apath))
  }

  def pathExists[S[_]](fileSystem: Task[FileSystem])(f: APath)(
    implicit s0: Task :<: S): Free[S, Boolean] = lift(for {
      path <- toPath(f)
      hdfs <- fileSystem
    } yield {
      val exists = hdfs.exists(path)
      hdfs.close
      exists
    }).into[S]

  private def moveFile[S[_]](src: AFile, dst: AFile, fileSystem: Task[FileSystem])(implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): Free[S, Unit] = {
    val move: Task[PhysicalError \/ Unit] = (for {
      hdfs <- fileSystem
      srcPath <- toPath(src)
      dstPath <- toPath(dst)
      dstParent <- toPath(fileParent(dst))
    } yield {
      val deleted = hdfs.delete(dstPath, true)
      val _ = hdfs.mkdirs(dstParent)
      val result = hdfs.rename(srcPath, dstPath)
      hdfs.close()
      result
    }).as(().right[PhysicalError]).handle {
      case NonFatal(ex : Exception) => UnhandledFSError(ex).left[Unit]
    }

    Failure.Ops[PhysicalError, S].unattempt(lift(move).into[S])
  }

  private def moveDir[S[_]](src: ADir, dst: ADir, fileSystem: Task[FileSystem])(implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): Free[S, Unit] = {
    val move: Task[PhysicalError \/ Unit] = (for {
      hdfs <- fileSystem
      srcPath <- toPath(src)
      dstPath <- toPath(dst)
    } yield {
      val deleted = hdfs.delete(dstPath, true)
      val result = hdfs.rename(srcPath, dstPath)
      hdfs.close()
      result
    }).as(().right[PhysicalError]).handle {
      case NonFatal(ex : Exception) => UnhandledFSError(ex).left[Unit]
    }

    Failure.Ops[PhysicalError, S].unattempt(lift(move).into[S])
  }


  private def delete[S[_]](apath: APath, fileSystem: Task[FileSystem])(implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): Free[S, FileSystemError \/ Unit] = {

    val delete: Task[FileSystemError \/ Unit] = for {
      path <- toPath(apath)
      hdfs <- fileSystem
    } yield {
      val result = if(hdfs.exists(path)) {
        hdfs.delete(path, true).right[FileSystemError]
      }
      else {
        pathErr(pathNotFound(apath)).left[Unit]
      }
      hdfs.close()
      result.as(())
    }

    val deleteHandled: Task[PhysicalError \/ (FileSystemError \/ Unit)] =
      delete.map(_.right[PhysicalError]).handle {
        case NonFatal(e : Exception) => UnhandledFSError(e).left[FileSystemError \/ Unit]
      }

    Failure.Ops[PhysicalError, S].unattempt(lift(deleteHandled).into[S])
  }

  private def tempFile[S[_]](near: APath)(implicit
    s0: Task :<: S
  ): Free[S, FileSystemError \/ AFile] = lift(Task.delay {
      val parent: ADir = refineType(near).fold(d => d, fileParent(_))
      val random = scala.util.Random.nextInt().toString
        (parent </> file(s"quasar-$random.tmp")).right[FileSystemError]
    }
  ).into[S]
}
