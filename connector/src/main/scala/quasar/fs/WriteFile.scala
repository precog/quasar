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

package quasar.fs

import slamdata.Predef._
import quasar._, RenderTree.ops._
import quasar.contrib.pathy._
import quasar.effect.LiftedOps
import quasar.fp._

import monocle.Iso
import scalaz._, Scalaz._
import scalaz.stream._

sealed abstract class WriteFile[A]

object WriteFile {
  final case class WriteHandle(file: AFile, id: Long)

  object WriteHandle {
    val tupleIso: Iso[WriteHandle, (AFile, Long)] =
      Iso((h: WriteHandle) => (h.file, h.id))((WriteHandle(_, _)).tupled)

    implicit val show: Show[WriteHandle] = Show.showFromToString

    // TODO: Switch to order once Order[Path[B,T,S]] exists
    implicit val equal: Equal[WriteHandle] = Equal.equalBy(tupleIso.get)
  }

  final case class Open(file: AFile)
    extends WriteFile[FileSystemError \/ WriteHandle]

  final case class Write(h: WriteHandle, chunk: Vector[Data])
    extends WriteFile[Vector[FileSystemError]]

  final case class Close(h: WriteHandle)
    extends WriteFile[Unit]

  final class Ops[S[_]](implicit val unsafe: Unsafe[S]) {
    import FileSystemError._, PathError._

    type F[A]    = unsafe.FreeS[A]
    type M[A]    = unsafe.M[A]
    type G[E, A] = EitherT[F, E, A]

    /** Returns a channel that appends chunks of data to the given file, creating
      * it if it doesn't exist. Any errors encountered while writing are emitted,
      * all attempts are made to continue consuming input until the source is
      * exhausted.
      */
    def appendChannel(dst: AFile): Channel[M, Vector[Data], Vector[FileSystemError]] = {
      def closeHandle(h: WriteHandle): Process[M, Nothing] =
        Process.eval_[M, Unit](unsafe.close(h).liftM[FileSystemErrT])

      def writeChunk(h: WriteHandle): Vector[Data] => M[Vector[FileSystemError]] =
        xs => unsafe.write(h, xs).liftM[FileSystemErrT]

      Process.bracket(unsafe.open(dst))(closeHandle)(h => channel.lift(writeChunk(h)))
    }

    /** Same as `append` but accepts chunked `Data`. */
    def appendChunked(dst: AFile, src: Process[F, Vector[Data]]): Process[M, FileSystemError] = {
      val accumPartialWrites =
        process1.id[FileSystemError]
          .map(partialWrite.getOption)
          .reduceSemigroup
          .pipe(process1.stripNone)
          .map(PartialWrite)

      val dropPartialWrites =
        process1.filter[FileSystemError](partialWrite.isEmpty)

      // NB: We don't use `through` as we want to ensure the `Open` from
      //     `appendChannel` happens even if the src process never emits.
      appendChannel(dst)
        .zipWith(src.translate[M](liftMT[F, FileSystemErrT]))((f, o) => f(o))
        .eval
        .flatMap(Process.emitAll)
        .flatMap(e => Process.emitW(e) ++ Process.emitO(e))
        .pipe(process1.multiplex(dropPartialWrites, accumPartialWrites))
    }

    /** Appends data to the given file, creating it if it doesn't exist. May not
      * write all values from `src` in the presence of errors, will emit a
      * [[FileSystemError]] for each input value that failed to write.
      */
    def append(dst: AFile, src: Process[F, Data]): Process[M, FileSystemError] =
      appendChunked(dst, src map (Vector(_)))

    /** Appends the given data to the specified file, creating it if it doesn't
      * exist. May not write every given value in the presence of errors, returns
      * a [[FileSystemError]] for each input value that failed to write.
      */
    def appendThese(dst: AFile, data: Vector[Data]): M[Vector[FileSystemError]] =
      // NB: the handle will be closed even if `write` produces errors in its value
      unsafe.open(dst) flatMapF (h => unsafe.write(h, data) <* unsafe.close(h) map (_.right))

    /** Same as `save` but accepts chunked `Data`. */
    def saveChunked(dst: AFile, src: Process[F, Vector[Data]])
                   (implicit MF: ManageFile.Ops[S])
                   : Process[M, FileSystemError] = {

      saveChunked0(dst, src, MoveSemantics.Overwrite)
    }

    /** Write the data stream to the given path, replacing any existing contents,
      * atomically. Any errors during writing will abort the entire operation
      * leaving any existing values unaffected.
      */
    def save(dst: AFile, src: Process[F, Data])
            (implicit MF: ManageFile.Ops[S])
            : Process[M, FileSystemError] = {

      saveChunked(dst, src map (Vector(_)))
    }

    /** Write the given data to the specified file, replacing any existing
      * contents, atomically. Any errors during writing will abort the
      * entire operation leaving any existing values unaffected.
      */
    def saveThese(dst: AFile, data: Vector[Data])
                 (implicit MF: ManageFile.Ops[S])
                 : M[Vector[FileSystemError]] = {

      saveThese0(dst, data, MoveSemantics.Overwrite)
    }

    /** Same as `create` but accepts chunked `Data`. */
    def createChunked(dst: AFile, src: Process[F, Vector[Data]])
                     (implicit QF: QueryFile.Ops[S], MF: ManageFile.Ops[S])
                     : Process[M, FileSystemError] = {

      QF.fileExistsM(dst).liftM[Process].ifM(
        shouldNotExist(dst).liftM[Process],
        saveChunked0(dst, src, MoveSemantics.FailIfExists))
    }

    /** Create the given file with the contents of `src`. Fails if already exists. */
    def create(dst: AFile, src: Process[F, Data])
              (implicit QF: QueryFile.Ops[S], MF: ManageFile.Ops[S])
              : Process[M, FileSystemError] = {

      createChunked(dst, src map (Vector(_)))
    }

    /** Create the given file with the contents of `data`. Fails if already exists. */
    def createThese(dst: AFile, data: Vector[Data])
                   (implicit QF: QueryFile.Ops[S], MF: ManageFile.Ops[S])
                   : M[Vector[FileSystemError]] = {

      QF.fileExistsM(dst).ifM(
        shouldNotExist(dst) map (Vector(_)),
        saveThese0(dst, data, MoveSemantics.FailIfExists))
    }

    /** Same as `replace` but accepts chunked `Data`. */
    def replaceChunked(dst: AFile, src: Process[F, Vector[Data]])
                      (implicit QF: QueryFile.Ops[S], MF: ManageFile.Ops[S])
                      : Process[M, FileSystemError] = {

      QF.fileExistsM(dst).liftM[Process].ifM(
        saveChunked0(dst, src, MoveSemantics.FailIfMissing),
        shouldExist(dst).liftM[Process])
    }

    /** Replace the contents of the given file with `src`. Fails if the file
      * doesn't exist.
      */
    def replace(dst: AFile, src: Process[F, Data])
               (implicit QF: QueryFile.Ops[S], MF: ManageFile.Ops[S])
               : Process[M, FileSystemError] = {

      replaceChunked(dst, src map (Vector(_)))
    }

    /** Replace the contents of the given file with `data`. Fails if the file
      * doesn't exist.
      */
    def replaceWithThese(dst: AFile, data: Vector[Data])
                        (implicit QF: QueryFile.Ops[S], MF: ManageFile.Ops[S])
                        : M[Vector[FileSystemError]] = {

      QF.fileExistsM(dst).ifM(
        saveThese0(dst, data, MoveSemantics.FailIfMissing),
        shouldExist(dst) map (Vector(_)))
    }

    ////

    type GE[A] = G[FileSystemError,A]

    private def shouldNotExist(dst: AFile): M[FileSystemError] =
      MonadError[GE, FileSystemError].raiseError(pathErr(pathExists(dst)))

    private def shouldExist(dst: AFile): M[FileSystemError] =
      MonadError[GE, FileSystemError].raiseError(pathErr(pathNotFound(dst)))

    private def saveChunked0(dst: AFile, src: Process[F, Vector[Data]], sem: MoveSemantics)
                            (implicit MF: ManageFile.Ops[S])
                            : Process[M, FileSystemError] = {

      def cleanupTmp(tmp: AFile)(t: Throwable): Process[M, Nothing] =
        Process.eval_(ensureAbsent(tmp)).causedBy(Cause.Error(t))

      MF.tempFile(dst).liftM[Process] flatMap { tmp =>
        appendChunked(tmp, src)
          .map(some).append(Process.emit(none))
          .take(1)
          .flatMap(_.cata(
            werr => ensureAbsent(tmp).as(werr).liftM[Process],
            Process.eval_(MF.moveFile(tmp, dst, sem))))
          .onFailure(cleanupTmp(tmp))
      }
    }

    private def saveThese0(dst: AFile, data: Vector[Data], sem: MoveSemantics)
                          (implicit MF: ManageFile.Ops[S])
                          : M[Vector[FileSystemError]] = {
      for {
        tmp  <- MF.tempFile(dst)
        errs <- appendThese(tmp, data)
        _    <- if (errs.isEmpty) MF.moveFile(tmp, dst, sem) else ensureAbsent(tmp)
      } yield errs
    }

    private def ensureAbsent(f: AFile)(implicit MF: ManageFile.Ops[S]): M[Unit] =
      MF.delete(f).run.void.liftM[FileSystemErrT]
  }

  object Ops {
    implicit def apply[S[_]](implicit U: Unsafe[S]): Ops[S] =
      new Ops[S]
  }

  /** Low-level, unsafe operations. Clients are responsible for resource-safety
    * when using these.
    */
  final class Unsafe[S[_]](implicit S: WriteFile :<: S)
    extends LiftedOps[WriteFile, S] {

    type M[A] = FileSystemErrT[FreeS, A]

    /** Returns a write handle for the specified file which may be used to
      * append data to the file it represents, creating it if necessary.
      *
      * Care must be taken to `close` the handle when it is no longer needed
      * to avoid potential resource leaks.
      */
    def open(file: AFile): M[WriteHandle] =
      EitherT(lift(Open(file)))

    /** Write a chunk of data to the file represented by the write handle.
      *
      * Attempts to write as much of the chunk as possible, even if parts of
      * it fail, any such failures will be returned in the output `Vector`
      * An empty `Vector` means the entire chunk was written successfully.
      */
    def write(h: WriteHandle, chunk: Vector[Data]): FreeS[Vector[FileSystemError]] =
      lift(Write(h, chunk))

    /** Close the write handle, freeing any resources it was using. */
    def close(h: WriteHandle): FreeS[Unit] =
      lift(Close(h))
  }

  object Unsafe {
    implicit def apply[S[_]](implicit S: WriteFile :<: S): Unsafe[S] =
      new Unsafe[S]
  }

  implicit def renderTree[A]: RenderTree[WriteFile[A]] =
    new RenderTree[WriteFile[A]] {
      def render(wf: WriteFile[A]) = wf match {
        case Open(file)           => NonTerminal(List("Open"), None, List(file.render))
        case Write(handle, chunk) =>
          NonTerminal(List("Read"), handle.shows.some,
            chunk.map(d => Terminal(List("Data"), d.shows.some)).toList)
        case Close(handle)        => Terminal(List("Close"), handle.shows.some)
      }
    }
}
