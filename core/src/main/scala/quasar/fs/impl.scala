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

package quasar.fs

import quasar.Predef._
import quasar.fp.numeric._
import quasar.fp.free._
import quasar.Data
import quasar.effect.{KeyValueStore, MonotonicSeq}

import scalaz._, Scalaz._
import scalaz.stream.Process

object impl {

  final case class ReadOpts(offset: Natural, limit: Option[Positive])

  def read[S[_], C](
    open: (AFile, ReadOpts) => Free[S,FileSystemError \/ C],
    read: C => Free[S,FileSystemError \/ (C, Vector[Data])],
    close: C => Free[S,Unit])(implicit
    cursors: KeyValueStore.Ops[ReadFile.ReadHandle, C, S],
    idGen: MonotonicSeq.Ops[S]
  ): ReadFile ~> Free[S,?] = new (ReadFile ~> Free[S, ?]) {
    def apply[A](fa: ReadFile[A]): Free[S, A] = fa match {
      case ReadFile.Open(file, offset, limit) =>
        (for {
          cursor <- EitherT(open(file,ReadOpts(offset, limit)))
          id <- idGen.next.liftM[FileSystemErrT]
          handle = ReadFile.ReadHandle(file, id)
          _ <- cursors.put(handle, cursor).liftM[FileSystemErrT]
        } yield handle).run

      case ReadFile.Read(handle) =>
        (for {
          cursor <- cursors.get(handle).toRight(FileSystemError.unknownReadHandle(handle))
          result <- EitherT(read(cursor))
          (newCursor, data) = result
          _ <- cursors.put(handle, newCursor).liftM[FileSystemErrT]
        } yield data).run

      case ReadFile.Close(handle) =>
        (for {
          cursor <- cursors.get(handle)
          _ <- close(cursor).liftM[OptionT]
          _ <- cursors.delete(handle).liftM[OptionT]
        } yield ()).run.void
    }
  }

  def readFromProcess[S[_], F[_]:Monad:Catchable](f: (AFile, ReadOpts) => Free[S, FileSystemError \/ Process[F, Vector[Data]]])(
    implicit
      state: KeyValueStore.Ops[ReadFile.ReadHandle, Process[F, Vector[Data]], S],
      idGen: MonotonicSeq.Ops[S],
      S0: F :<: S
  ): ReadFile ~> Free[S, ?] =
    new (ReadFile ~> Free[S, ?]) {
      def apply[A](fa: ReadFile[A]): Free[S, A] = fa match {
        case ReadFile.Open(file, offset, limit) =>
          (for {
            readStream <- EitherT(f(file, ReadOpts(offset, limit)))
            id <- idGen.next.liftM[FileSystemErrT]
            handle = ReadFile.ReadHandle(file, id)
            _ <- state.put(handle, readStream).liftM[FileSystemErrT]
          } yield handle).run

        case ReadFile.Read(handle) =>
          (for {
            stream <- state.get(handle).toRight(FileSystemError.unknownReadHandle(handle))
            data   <- EitherT(lift(stream.unconsOption).into[S].flatMap {
                        case Some((value, streamTail)) =>
                          state.put(handle, streamTail).as(value.right[FileSystemError])
                        case None                      =>
                          state.delete(handle).as(Vector.empty[Data].right[FileSystemError])
                      })
          } yield data).run

        case ReadFile.Close(handle) =>
          (for {
            stream <- state.get(handle)
            _      <- lift(stream.kill.run).into[S].liftM[OptionT]
            _      <- state.delete(handle).liftM[OptionT]
          } yield ()).run.void
      }
    }
}
