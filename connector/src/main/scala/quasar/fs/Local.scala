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

import quasar.{Data, DataCodec}
import quasar.common.PhaseResult
import quasar.contrib.pathy.{prettyPrint, AFile, APath, PathSegment}
import quasar.contrib.scalaz.stateT.StateTask
import quasar.fp.TaskRef
import quasar.fp.numeric.{Natural, Positive}
import quasar.fp.ski.κ

import java.io.{File => JFile}
import java.nio.charset.StandardCharsets
import java.nio.file.{
  Files,
  Path => JPath,
  Paths,
  StandardCopyOption,
  StandardOpenOption
}
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Collectors

import scala.collection.JavaConverters._

import pathy.Path.{file1, refineType, DirName, FileName}
import scalaz.{~>, \/, -\/, \/-, EitherT, StateT}
import scalaz.Scalaz._
import scalaz.concurrent.Task

/** A local file system implementation.
  *
  * The implementations are not guaranteed to be atomic.
  */
class Local private (baseDir: JFile) {

  case class ReadState(
    current: Long,
    handles: Map[Long, (AtomicBoolean, Natural, Option[Positive])])

  case class WriteState(
    current: Long,
    handles: Set[Long])

  type Result[A] = EitherT[Task, LocalFileSystemError, A]
  type WriteResult[A] = StateT[Result, WriteState, A]
  type ReadResult[A] = StateT[Result, ReadState, A]

  type FSError[A] = FileSystemError \/ A
  type LocalFSError[A] = LocalFileSystemError \/ A

  def readFile = λ[ReadFile ~> ReadResult] {
    case ReadFile.Open(file, offset, limit) =>
      StateTask.modifyAndGets[ReadState, FSError[ReadFile.ReadHandle]] {
        case ReadState(current, handles) =>
          val state = ReadState(
            current + 1,
            handles + ((current, (new AtomicBoolean(false), offset, limit))))

          val handle = ReadFile.ReadHandle(file, current)

          (state, handle.right[FileSystemError])
      }.mapT(EitherT.rightT(_))

    case ReadFile.Read(h @ ReadFile.ReadHandle(file, id)) => StateT {
      case state @ ReadState(_, handles) => delayEither[FSError[Vector[Data]]] {

        val check: FSError[(AtomicBoolean, Natural, Option[Positive])] =
          toRight(handles.get(id))(
            FileSystemError.unknownReadHandle(h))

        check match {
          case -\/(err) =>
            err.left.right

          case \/-((bool, offset, limit)) =>
            val jpath: JPath = toJPath(file)

            if (bool.getAndSet(true)) { // we've already read from this handle
              Vector[Data]().right.right
            } else {
              if (jpath.toFile.exists) {
                \/.fromTryCatchNonFatal[Vector[String]](
                  Files.readAllLines(jpath, StandardCharsets.UTF_8).asScala.toVector).fold(
                  err => LocalFileSystemError.readFailed(jpath, err).left,
                  _.drop(offset.value.toInt)
                    .take(limit.map(_.value.toInt).getOrElse(Int.MaxValue)) // FIXME horrible performance
                    .traverse(str =>
                      \/.fromEither(Data.jsonParser.parseFromString(str).toEither))
                    .leftMap(err =>
                      FileSystemError.readFailed(err.toString, s"Read for $file failed"))
                    .right)
              } else {
                // FIXME apparently read on a non-existent file is equivalent to reading the empty file??!!
                Vector[Data]().right.right
              }
            }
        }
      }.map(state -> _)
    }

    case ReadFile.Close(ReadFile.ReadHandle(_, id)) =>
      StateTask.modify[ReadState] {
        case ReadState(current, handles) => ReadState(current, handles - id)
      }.mapT(EitherT.rightT(_))
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def writeFile = λ[WriteFile ~> WriteResult] {
    case WriteFile.Open(file) =>
      StateTask.modifyAndGets[WriteState, FSError[WriteFile.WriteHandle]] {
        case WriteState(current, handles) =>
          val state = WriteState(
            current + 1,
            handles + current)

          val handle = WriteFile.WriteHandle(file, current)

          (state, handle.right[FileSystemError])
      }.mapT(EitherT.rightT(_))

    case WriteFile.Write(h @ WriteFile.WriteHandle(path, id), data) => StateT {
      case state @ WriteState(_, handles) => delayEither {
        if (!handles.contains(id)) {
          Vector(FileSystemError.unknownWriteHandle(h)).right
        } else {
          implicit val codec: DataCodec = DataCodec.Precise

          val (errs, strs): (Vector[Data], Vector[String]) = data.foldMap { d =>
            DataCodec.render(d).fold(
              (Vector(d), Vector[String]()))(
              (s: String) => (Vector[Data](), Vector(s)))
          }

          val jpath: JPath = toJPath(path)

          \/.fromTryCatchNonFatal[JPath] {
            Files.createDirectories(jpath.getParent)
            Files.write(jpath,
              strs.asJava,
              StandardCharsets.UTF_8,
              StandardOpenOption.APPEND,
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE)
          }.fold(
            err => LocalFileSystemError.writeFailed(toJPath(path), err).left,
            _ => {
              val partial = if (errs.isEmpty)
                Vector()
              else
                Vector(FileSystemError.partialWrite(errs.size))

              (errs.map(FileSystemError.writeFailed(_, s"Write failed for path $path")) ++ partial).right
            })
        }
      }.map(state -> _)
    }

    case WriteFile.Close(WriteFile.WriteHandle(_, id)) =>
      StateTask.modify[WriteState] {
        case WriteState(current, handles) => WriteState(current, handles - id)
      }.mapT(EitherT.rightT(_))
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def manageFile = λ[ManageFile ~> Result] {
    case ManageFile.Move(pair, sem @ MoveSemantics.FailIfExists) => pair match {
      case ManageFile.PathPair.DirToDir(src, dst) => delayEither[FSError[Unit]] {
        val jsrc: JPath = toJPath(src)
        val jdst: JPath = toJPath(dst)

        failIfTargetExists(src, dst) match {
          case Some(err) => err.left.right
          case None =>
            \/.fromTryCatchNonFatal[Unit]({
              Files.createDirectories(jdst)

              listFiles(jsrc).foreach(f =>
                Files.move(f.toPath, jdst.resolve(f.toPath.getFileName)))
            }).fold(
              err => LocalFileSystemError.MoveFailed(pair, sem, err).left,
              κ(().right.right))
        }
      } flatMapF {
        case err @ -\/(_) => Task.point(err.right)
        case \/-(_) => delete(src)
      }

      case ManageFile.PathPair.FileToFile(src, dst) => delayEither[FSError[Unit]] {
        val jsrc: JPath = toJPath(src)
        val jdst: JPath = toJPath(dst)

        failIfTargetExists(src, dst) match {
          case Some(err) => err.left.right
          case None =>
            \/.fromTryCatchNonFatal[JPath]({
              Files.createDirectories(jdst.getParent)
              Files.move(jsrc, jdst)
            }).fold(
              err => LocalFileSystemError.MoveFailed(pair, sem, err).left,
              κ(().right.right))
        }
      }
    }

    case ManageFile.Move(pair, sem @ MoveSemantics.FailIfMissing) => pair match {
      case ManageFile.PathPair.DirToDir(src, dst) => delayEither[FSError[Unit]] {
        val jsrc: JPath = toJPath(src)
        val jdst: JPath = toJPath(dst)

        failIfTargetMissing(src, dst) match {
          case Some(err) => err.left.right
          case None =>
            \/.fromTryCatchNonFatal[Unit]({
              Files.createDirectories(jdst.getParent)

              listFiles(jsrc).foreach(f =>
                Files.move(f.toPath, jdst, StandardCopyOption.REPLACE_EXISTING))
            }).fold(
              err => LocalFileSystemError.MoveFailed(pair, sem, err).left,
              κ(().right.right))
        }
      } flatMapF {
        case err @ -\/(_) => Task.point(err.right)
        case \/-(_) => delete(src)
      }

      case ManageFile.PathPair.FileToFile(src, dst) => delayEither[FSError[Unit]] {
        val jsrc: JPath = toJPath(src)
        val jdst: JPath = toJPath(dst)

        failIfTargetMissing(src, dst) match {
          case Some(err) => err.left.right
          case None =>
            \/.fromTryCatchNonFatal[JPath]({
              Files.createDirectories(jdst.getParent)
              Files.move(jsrc, jdst, StandardCopyOption.REPLACE_EXISTING)
            }).fold(
              err => LocalFileSystemError.MoveFailed(pair, sem, err).left,
              κ(().right.right))
        }
      }
    }

    case ManageFile.Move(pair, sem @ MoveSemantics.Overwrite) => pair match {
      case ManageFile.PathPair.DirToDir(src, dst) => delayEither[FSError[Unit]] {
        val jsrc: JPath = toJPath(src)
        val jdst: JPath = toJPath(dst)

        failIfSourceMissing(src) match {
          case Some(err) => err.left.right
          case None =>
            \/.fromTryCatchNonFatal[Unit]({
              Files.createDirectories(jdst.getParent)

              listFiles(jsrc).foreach(f =>
                Files.move(f.toPath, jdst, StandardCopyOption.REPLACE_EXISTING))
            }).fold(
              err => LocalFileSystemError.MoveFailed(pair, sem, err).left,
              κ(().right.right))
        }
      } flatMapF {
        case err @ -\/(_) => Task.point(err.right)
        case \/-(_) => delete(src)
      }

      case ManageFile.PathPair.FileToFile(src, dst) => delayEither[FSError[Unit]] {
        val jsrc: JPath = toJPath(src)
        val jdst: JPath = toJPath(dst)

        failIfSourceMissing(src) match {
          case Some(err) => err.left.right
          case None =>
            \/.fromTryCatchNonFatal[JPath]({
              Files.createDirectories(jdst.getParent)
              Files.move(jsrc, jdst, StandardCopyOption.REPLACE_EXISTING)
            }).fold(
              err => LocalFileSystemError.MoveFailed(pair, sem, err).left,
              κ(().right.right))
        }
      }
    }

    case ManageFile.Copy(pair) => pair match {
      case ManageFile.PathPair.DirToDir(src, dst) => delayEither[FSError[Unit]] {
        val jsrc: JPath = toJPath(src)
        val jdst: JPath = toJPath(dst)

        failIfSourceMissing(src) match {
          case Some(err) => err.left.right
          case None =>
            \/.fromTryCatchNonFatal[Unit]({
              Files.createDirectories(jdst.getParent)

              listFiles(jsrc).foreach(f =>
                Files.copy(f.toPath, jdst, StandardCopyOption.REPLACE_EXISTING))
            }).fold(
              err => LocalFileSystemError.CopyFailed(pair, err).left,
              κ(().right.right))
        }
      }

      case ManageFile.PathPair.FileToFile(src, dst) => delayEither[FSError[Unit]] {
        failIfSourceMissing(src) match {
          case Some(err) => err.left.right
          case None =>
            \/.fromTryCatchNonFatal[JPath](
              Files.copy(toJPath(src), toJPath(dst), StandardCopyOption.REPLACE_EXISTING)).fold(
              err => LocalFileSystemError.CopyFailed(pair, err).left,
              κ(().right.right))
        }
      }
    }

    case ManageFile.Delete(path) => EitherT.eitherT(delete(path))

    case ManageFile.TempFile(path, prefix) => delayEither[FSError[AFile]] {
      val uuid: String = UUID.randomUUID().toString

      val fileName: FileName = FileName(prefix.fold("")(_.s) + uuid)

      val jpath: JPath = toJPath(path)

      \/.fromTryCatchNonFatal[JPath] {
        refineType(path).fold(
          _ => { // directory
            Files.createDirectories(jpath)
            Files.createFile(Paths.get(jpath.toFile.getPath, fileName.value))
          },
          _ => { // file
            Files.createDirectories(jpath.getParent)
            Files.createFile(Paths.get(jpath.toFile.getParent, fileName.value))
          })
        }.fold(
          err => LocalFileSystemError.tempFileCreationFailed(toJPath(path), err).left,
          κ((nearDir(path) </> file1(fileName)).right.right))
      }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def queryFile = λ[QueryFile ~> Result] {
    case QueryFile.ExecutePlan(_, _) => delayRight {
      (Vector[PhaseResult](), FileSystemError.unsupportedOperation(
        "QueryFile.ExecutePlan not supported by the local file system.").left)
    }

    case QueryFile.EvaluatePlan(_) => delayRight {
      (Vector[PhaseResult](), FileSystemError.unsupportedOperation(
        "QueryFile.EvaluatePlan not supported by the local file system.").left)
    }

    case QueryFile.More(h) => delayRight {
      FileSystemError.unsupportedOperation(
        "QueryFile.More not supported by the local file system.").left
    }

    case QueryFile.Close(_) => delayRight(())

    case QueryFile.Explain(lp) => delayRight {
      (Vector[PhaseResult](), FileSystemError.unsupportedOperation(
        "QueryFile.Explain not supported by the local file system.").left)
    }

    case QueryFile.ListContents(dir) => delayEither[FSError[Set[Node]]] {
      failIfSourceMissing(dir) match {
        case Some(err) => err.left.right
        case None =>
          \/.fromTryCatchNonFatal[FSError[Set[Node]]] {
            val files: List[PathSegment] =
              listFilesAndDirs(toJPath(dir)).collect {
                case f if f.isDirectory => DirName(f.getName).left
                case f if f.isFile => FileName(f.getName).right
              }

            files.map(Node.fromSegment).toSet.right
          }.leftMap(e => LocalFileSystemError.listContentsFailed(toJPath(dir), e))
      }
    }

    case QueryFile.FileExists(file) =>
      delayRight[Boolean](Files.exists(toJPath(file)))
  }

  ////////

  private def toJPath(path: APath): JPath = Paths.get(prettyPrint(path))

  private def delayEither[A](a: LocalFSError[A]): Result[A] =
    EitherT.eitherT(Task.delay(a))

  private def delayRight[A](a: A): Result[A] =
    EitherT.rightT(Task.delay(a))

  private def listFilesAndDirs(src: JPath): List[JFile] =
    Files.list(src)
      .collect(Collectors.toList()).asScala.toList
      .map(_.toFile)

  private def listFiles(src: JPath): List[JFile] =
    listFilesAndDirs(src).filter(_.isFile)

  private def delete(path: APath): Task[LocalFSError[FSError[Unit]]] =
    Task delay {
      val jpath: JPath = toJPath(path)

      failIfSourceMissing(path) match {
        case Some(err) => err.left.right
        case None => refineType(path) match {
          case -\/(_) => // directory

            // ported from quasar.precog.util.IOUtils
            // this works but using a `java.nio.file.SimpleFileVisitor` does not
            // (recursive delete fails when file gets into an inexplicable permission state)
            @SuppressWarnings(Array("org.wartremover.warts.Null"))
            def listFiles(f: JFile): Task[Vector[JFile]] = Task delay {
              f.listFiles match {
                case null => Vector()
                case xs => xs.toVector
              }
            }

            @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
            def recursiveDeleteSeq(files: Seq[JFile]): Task[Unit] = {
              files.toList match {
                case Nil => Task.now(())
                case hd :: tl => recursiveDelete(hd).flatMap(_ => recursiveDeleteSeq(tl))
              }
            }

            @SuppressWarnings(Array("org.wartremover.warts.Recursion", "org.wartremover.warts.NonUnitStatements"))
            def recursiveDelete(file: JFile): Task[Unit] = {
              def del(): Unit = { file.delete(); () }

              if (!file.isDirectory) Task.delay(del())
              else listFiles(file) flatMap {
                case Vector() => Task.delay(del())
                case xs => recursiveDeleteSeq(xs).flatMap(_ => Task.delay(del()))
              }
            }

            \/.fromTryCatchNonFatal[Unit](recursiveDelete(jpath.toFile).unsafePerformSync).fold(
              err => LocalFileSystemError.deleteDirFailed(jpath, err).left,
              κ(().right.right))

          case \/-(_) => // file
            \/.fromTryCatchNonFatal[Boolean](Files.deleteIfExists(jpath)).fold(
              err => LocalFileSystemError.deleteFileFailed(jpath, err).left,
              bool =>
                if (bool) ().right.right
                else FileSystemError.pathErr(PathError.pathNotFound(path)).left.right)
        }
      }
    }

  private def failIfSourceMissing(src: APath): Option[FileSystemError] =
    if (!Files.exists(toJPath(src)))
      FileSystemError.pathErr(PathError.pathNotFound(src)).some
    else
      None

  private def failIfTargetExists(src: APath, dst: APath): Option[FileSystemError] =
    failIfSourceMissing(src) match {
      case err @ Some(_) => err
      case None =>
        if (Files.exists(toJPath(dst)))
          FileSystemError.pathErr(PathError.pathExists(dst)).some
        else
          None
    }

  private def failIfTargetMissing(src: APath, dst: APath): Option[FileSystemError] =
    failIfSourceMissing(src) match {
      case err @ Some(_) => err
      case None =>
        if (!Files.exists(toJPath(dst)))
          FileSystemError.pathErr(PathError.pathNotFound(dst)).some
        else
          None
    }

  private def errorTask[A](either: LocalFSError[A]): Task[A] =
    either.fold(err => Task.fail(err.throwable), Task.point(_))

  private def resultToTask = λ[Result ~> Task] {
    _.run.flatMap(errorTask)
  }

  private def readToTask: Task[ReadResult ~> Task] =
    TaskRef(ReadState(0, Map())) map { ref =>
      new (ReadResult ~> Task) {
        def apply[A](state: ReadResult[A]) =
          ref.modifyT {
            state.run(_).run.flatMap(errorTask)
          }
      }
   }

  private def writeToTask: Task[WriteResult ~> Task] =
    TaskRef(WriteState(0, Set())) map { ref =>
      new (WriteResult ~> Task) {
        def apply[A](state: WriteResult[A]) =
          ref.modifyT {
            state.run(_).run.flatMap(errorTask)
          }
      }
    }

  ////////

  def runFs: Task[FileSystem ~> Task] = for {
    read <- readToTask
    write <- writeToTask
  } yield { interpretFileSystem(
    resultToTask compose queryFile,
    read compose readFile,
    write compose writeFile,
    resultToTask compose manageFile)
  }
}

object Local {
  def apply(baseDir: JFile): Local = new Local(baseDir)
}
