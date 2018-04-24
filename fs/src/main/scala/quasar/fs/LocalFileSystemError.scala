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

package quasar.fs

import slamdata.Predef._

import java.nio.file.{Path => JPath}

import monocle.Prism
import scalaz._, Scalaz._

sealed abstract class LocalFileSystemError(val throwable: Throwable)

object LocalFileSystemError {
  final case class CopyFailed(pair: ManageFile.PathPair, ex: Throwable) extends LocalFileSystemError(ex)
  final case class DeleteDirFailed(dir: JPath, ex: Throwable) extends LocalFileSystemError(ex)
  final case class DeleteFileFailed(file: JPath, ex: Throwable) extends LocalFileSystemError(ex)
  final case class ListContentsFailed(file: JPath, ex: Throwable) extends LocalFileSystemError(ex)
  final case class MoveFailed(pair: ManageFile.PathPair, sem: MoveSemantics, ex: Throwable) extends LocalFileSystemError(ex)
  final case class ReadFailed(file: JPath, ex: Throwable) extends LocalFileSystemError(ex)
  final case class TempFileCreationFailed(file: JPath, ex: Throwable) extends LocalFileSystemError(ex)
  final case class WriteFailed(file: JPath, ex: Throwable) extends LocalFileSystemError(ex)

  val copyFailed = Prism.partial[LocalFileSystemError, (ManageFile.PathPair, Throwable)] {
    case CopyFailed(pair, throwable) => (pair, throwable)
  } {
    case (p, t) => CopyFailed(p, t)
  }

  val deleteDirFailed = Prism.partial[LocalFileSystemError, (JPath, Throwable)] {
    case DeleteDirFailed(file, throwable) => (file, throwable)
  } {
    case (f, t) => DeleteDirFailed(f, t)
  }

  val deleteFileFailed = Prism.partial[LocalFileSystemError, (JPath, Throwable)] {
    case DeleteFileFailed(file, throwable) => (file, throwable)
  } {
    case (f, t) => DeleteFileFailed(f, t)
  }

  val listContentsFailed = Prism.partial[LocalFileSystemError, (JPath, Throwable)] {
    case ListContentsFailed(file, throwable) => (file, throwable)
  } {
    case (f, t) => ListContentsFailed(f, t)
  }

  val moveFailed = Prism.partial[LocalFileSystemError, (ManageFile.PathPair, MoveSemantics, Throwable)] {
    case MoveFailed(pair, sem, throwable) => (pair, sem, throwable)
  } {
    case (p, s, t) => MoveFailed(p, s, t)
  }

  val readFailed = Prism.partial[LocalFileSystemError, (JPath, Throwable)] {
    case ReadFailed(file, throwable) => (file, throwable)
  } {
    case (f, t) => ReadFailed(f, t)
  }

  val tempFileCreationFailed = Prism.partial[LocalFileSystemError, (JPath, Throwable)] {
    case TempFileCreationFailed(file, throwable) => (file, throwable)
  } {
    case (f, t) => TempFileCreationFailed(f, t)
  }

  val writeFailed = Prism.partial[LocalFileSystemError, (JPath, Throwable)] {
    case WriteFailed(file, throwable) => (file, throwable)
  } {
    case (f, t) => WriteFailed(f, t)
  }

  implicit val show: Show[LocalFileSystemError] =
    Show.shows {
      case CopyFailed(pair, throwable) =>
        s"Failed to copy pair $pair.\nReason:\n${throwable.getMessage}"
      case DeleteDirFailed(dir, throwable) =>
        s"Failed to delete directory $dir.\nReason:\n${throwable.getMessage}"
      case DeleteFileFailed(dir, throwable) =>
        s"Failed to delete file $dir.\nReason:\n${throwable.getMessage}"
      case ListContentsFailed(file, throwable) =>
        s"Failed to list contents for $file.\nReason:\n${throwable.getMessage}"
      case MoveFailed(pair, sem, throwable) =>
        s"Failed to move pair $pair with semantic ${sem.shows}.\nReason:\n${throwable.getMessage}"
      case ReadFailed(file, throwable) =>
        s"Failed to read file $file.\nReason:\n${throwable.getMessage}"
      case TempFileCreationFailed(file, throwable) =>
        s"Failed to create temp file near $file.\nReason:\n${throwable.getMessage}"
      case WriteFailed(file, throwable) =>
        s"Failed to write to file $file.\nReason:\n${throwable.getMessage}"
    }
}
