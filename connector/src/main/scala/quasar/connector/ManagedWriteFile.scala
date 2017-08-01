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

package quasar.connector

import slamdata.Predef._
import quasar.Data
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fs._

import scalaz._, Scalaz._

trait ManagedWriteFile[C] { self: BackendModule =>
  import WriteFile._, FileSystemError._

  def MonoSeqM: MonoSeq[M]
  def WriteKvsM: Kvs[M, WriteHandle, C]

  trait ManagedWriteFileModule {
    def writeCursor(file: AFile): Backend[C]
    def writeChunk(c: C, chunk: Vector[Data]): Configured[Vector[FileSystemError]]
    def closeCursor(c: C): Configured[Unit]
  }

  def ManagedWriteFileModule: ManagedWriteFileModule

  object WriteFileModule extends WriteFileModule {
    private final implicit def _MonadM = MonadM

    def open(file: AFile): Backend[WriteHandle] =
      for {
        id <- MonoSeqM.next.liftB
        h  =  WriteHandle(file, id)
        c  <- ManagedWriteFileModule.writeCursor(file)
        _  <- WriteKvsM.put(h, c).liftB
      } yield h

    def write(h: WriteHandle, chunk: Vector[Data]): Configured[Vector[FileSystemError]] =
      OptionT(WriteKvsM.get(h).liftM[ConfiguredT])
        .flatMapF(ManagedWriteFileModule.writeChunk(_, chunk))
        .getOrElse(Vector(unknownWriteHandle(h)))

    def close(h: WriteHandle): Configured[Unit] =
      OptionT(WriteKvsM.get(h).liftM[ConfiguredT])
        .flatMapF(c =>
          ManagedWriteFileModule.closeCursor(c) *>
          WriteKvsM.delete(h).liftM[ConfiguredT])
        .orZero
  }
}
