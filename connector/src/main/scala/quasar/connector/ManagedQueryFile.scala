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

trait ManagedQueryFile[C] { self: BackendModule =>
  import QueryFile._, FileSystemError._

  def MonoSeqM: MonoSeq[M]
  def ResultKvsM: Kvs[M, ResultHandle, C]

  trait ManagedQueryFileModule {
    def executePlan(repr: Repr, out: AFile): Backend[AFile]
    def explain(repr: Repr): Backend[String]
    def listContents(dir: ADir): Backend[Set[PathSegment]]
    def fileExists(file: AFile): Configured[Boolean]

    def resultsCursor(repr: Repr): Backend[C]
    def nextChunk(c: C): Backend[(C, Vector[Data])]
    def closeCursor(c: C): Configured[Unit]
  }

  def ManagedQueryFileModule: ManagedQueryFileModule

  object QueryFileModule extends QueryFileModule {
    private final implicit def _MonadM = MonadM

    def executePlan(repr: Repr, out: AFile): Backend[AFile] =
      ManagedQueryFileModule.executePlan(repr, out)

    def explain(repr: Repr): Backend[String] =
      ManagedQueryFileModule.explain(repr)

    def listContents(dir: ADir): Backend[Set[PathSegment]] =
      ManagedQueryFileModule.listContents(dir)

    def fileExists(file: AFile): Configured[Boolean] =
      ManagedQueryFileModule.fileExists(file)

    def evaluatePlan(repr: Repr): Backend[ResultHandle] =
      for {
        id <- MonoSeqM.next.liftB
        h  =  ResultHandle(id)
        c  <- ManagedQueryFileModule.resultsCursor(repr)
        _  <- ResultKvsM.put(h, c).liftB
      } yield h

    def more(h: ResultHandle): Backend[Vector[Data]] =
      for {
        c0 <- ResultKvsM.get(h).liftB
        c  <- c0 getOrElseF unknownResultHandle(h).raiseError[Backend, C]
        r  <- ManagedQueryFileModule.nextChunk(c)
        (c1, data) = r
        _  <- ResultKvsM.put(h, c1).liftB
      } yield data

    def close(h: ResultHandle): Configured[Unit] =
      OptionT(ResultKvsM.get(h).liftM[ConfiguredT])
        .flatMapF(c =>
          ManagedQueryFileModule.closeCursor(c) *>
          ResultKvsM.delete(h).liftM[ConfiguredT])
        .orZero
  }
}
