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
import quasar.Data
import quasar.effect.{KeyValueStore, MonotonicSeq}

import scalaz._, Scalaz._

object impl {

  final case class ReadOpts(offset: Natural, limit: Option[Positive])

  def read[S[_], C](
    open: (AFile, ReadOpts) => Free[S,FileSystemError \/ C],
    read: C => Free[S,FileSystemError \/ Vector[Data]],
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
          data <- EitherT(read(cursor))
        } yield data).run

      case ReadFile.Close(handle) =>
        (for {
          cursor <- cursors.get(handle)
          _ <- close(cursor).liftM[OptionT]
          _ <- cursors.delete(handle).liftM[OptionT]
        } yield ()).run.void
    }
  }
}
