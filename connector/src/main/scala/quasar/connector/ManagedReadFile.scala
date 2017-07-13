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
import quasar.fp.numeric._

import scalaz._, Scalaz._

trait ManagedReadFile[C] { self: BackendModule =>
  import ReadFile._, FileSystemError._

  def MonoSeqM: MonoSeq[M]
  def ReadKvsM: Kvs[M, ReadHandle, C]

  trait ManagedReadFileModule {
    def readCursor(file: AFile, offset: Natural, limit: Option[Positive]): Backend[C]
    def nextChunk(c: C): Backend[(C, Vector[Data])]
    def closeCursor(c: C): Configured[Unit]
  }

  def ManagedReadFileModule: ManagedReadFileModule

  object ReadFileModule extends ReadFileModule {
    private final implicit def _MonadM = MonadM

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] =
      for {
        id <- MonoSeqM.next.liftB
        h  =  ReadHandle(file, id)
        c  <- ManagedReadFileModule.readCursor(file, offset, limit)
        _  <- ReadKvsM.put(h, c).liftB
      } yield h

    def read(h: ReadHandle): Backend[Vector[Data]] =
      for {
        c0 <- ReadKvsM.get(h).liftB
        c  <- c0 getOrElseF unknownReadHandle(h).raiseError[Backend, C]
        r  <- ManagedReadFileModule.nextChunk(c)
        (c1, data) = r
        _  <- ReadKvsM.put(h, c1).liftB
      } yield data

    def close(h: ReadHandle): Configured[Unit] =
      OptionT(ReadKvsM.get(h).liftM[ConfiguredT])
        .flatMapF(c =>
          ManagedReadFileModule.closeCursor(c) *>
          ReadKvsM.delete(h).liftM[ConfiguredT])
        .orZero
  }
}
