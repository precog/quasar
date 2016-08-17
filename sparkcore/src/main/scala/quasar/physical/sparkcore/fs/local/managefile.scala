/*
 * Copyright 2014–2016 SlamData Inc.
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

/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.fs._
import quasar.fs.PathError._
import quasar.fs.FileSystemError._
import quasar.fp.free._
import quasar.fs.ManageFile._
import quasar.fs.ManageFile.MoveScenario._
import quasar.fs.impl.ensureMoveSemantics

import java.nio.file._

import org.apache.commons.io.FileUtils
import pathy.Path.posixCodec
import scalaz._
import Scalaz._
import scalaz.concurrent.Task

object managefile {

  def interpret[S[_]](implicit
    s0: Task :<: S
  ): ManageFile ~> Free[S, ?] =
    new (ManageFile ~> Free[S, ?]) {
      def apply[A](mf: ManageFile[A]): Free[S, A] = mf match {
        case Move(FileToFile(sf, df), semantics) =>
          injectFT[Task, S].apply{
            ensureMoveSemantics(df, doesPathExist, semantics)
              .fold(fse => Task.now(fse.left), moveFile(sf, df)).join
          }
        case Move(DirToDir(sd, dd), semantics) =>
          injectFT[Task, S].apply{
            ensureMoveSemantics(dd, doesPathExist, semantics)
              .fold(fse => Task.now(fse.left), moveDir(sd, dd)).join
          }
        case Delete(path) => delete(path)
        case TempFile(near) => tempFile(near)
      }
    }

  private def toNioPath(path: APath): Path =
    Paths.get(posixCodec.unsafePrintPath(path))

  private def toAFile(path: Path): Option[AFile] = {
    val maybeUnboxed = posixCodec.parseAbsFile(path.toString)
    maybeUnboxed.map(sandboxAbs(_))
  }

  private def doesPathExist: APath => Task[Boolean] = path => Task.delay {
    Files.exists(toNioPath(path))
  }

  private def moveFile(src: AFile, dst: AFile): Task[FileSystemError \/ Unit] = Task.delay {
    \/.fromTryCatchNonFatal(
      Files.move(toNioPath(src), toNioPath(dst), StandardCopyOption.REPLACE_EXISTING)
    ) .leftMap {
      case e => pathErr(invalidPath(dst, e.getMessage()))
    }.void
  }

  private def moveDir(src: ADir, dst: ADir): Task[FileSystemError \/ Unit] = Task.delay {
    \/.fromTryCatchNonFatal{
      val deleted = FileUtils.deleteDirectory(toNioPath(dst).toFile())
      FileUtils.moveDirectory(toNioPath(src).toFile(), toNioPath(dst).toFile())
    }
      .leftMap {
      case e => pathErr(invalidPath(dst, e.getMessage()))
    }.void
  }

  private def delete[S[_]](path: APath)(implicit
    s0: Task :<: S
  ): Free[S, FileSystemError \/ Unit] = {
    val task: Task[FileSystemError \/ Unit] = Task.delay {
      \/.fromTryCatchNonFatal(Files.delete(toNioPath(path)))
        .leftMap {
        case e: NoSuchFileException => pathErr(pathNotFound(path))
        case e: DirectoryNotEmptyException => pathErr(pathNotFound(path))
        case e => pathErr(invalidPath(path, e.getMessage()))
      }
    }
    injectFT[Task, S].apply(task)
  }

  private def tempFile[S[_]](near: APath)(implicit
    s0: Task :<: S
  ): Free[S, FileSystemError \/ AFile] = {

    def handleCreationError: Throwable => FileSystemError = {
      case e: NoSuchFileException => pathErr(pathNotFound(near))
      case e: FileAlreadyExistsException => pathErr(pathExists(near))
      case e: FileSystemException if e.getMessage.contains("Not a directory") => pathErr(invalidPath(
        near,
        s"Provided $near is not a directory"))

      case e => pathErr(invalidPath(near, e.getMessage()))
    }

    def fileToMaybeAFile: Path => FileSystemError \/ AFile = f =>
        toAFile(f) \/> (pathErr(invalidPath(near, s"Could not create temp file in dir $near")))
    
    val task: Task[FileSystemError \/ AFile] = Task.delay {
      val posix = posixCodec.unsafePrintPath(near) + "/"
      val prefix = "quasar"
      val suffix = ".tmp"
      \/.fromTryCatchNonFatal(Files.createTempFile(Paths.get(posix), prefix, suffix))
        .leftMap(handleCreationError)
        .flatMap(fileToMaybeAFile)        
    }
    injectFT[Task, S].apply(task)
  }

}
