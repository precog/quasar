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

package quasar.physical.rdbms.fs

import slamdata.Predef._
import quasar.contrib.pathy.AFile
import quasar.contrib.scalaz._
import quasar.contrib.scalaz.eitherT._
import quasar.Data
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fs._
import quasar.physical.rdbms.Rdbms
import quasar.physical.rdbms.common.TablePath

import doobie.imports.Meta
import scalaz.Monad
import scalaz.syntax.monad._
import scalaz.std.vector._

trait RdbmsWriteFile extends RdbmsInsert with RdbmsDescribeTable with RdbmsCreate {
  this: Rdbms =>

  import WriteFile._

  val writeKvs = KeyValueStore.Ops[WriteHandle, TablePath, Eff]

  implicit def MonadM: Monad[M]
  implicit def dataMeta: Meta[Data]

  override def WriteFileModule: WriteFileModule = new WriteFileModule {

    private def getDbPath(h: WriteHandle) =
      writeKvs
        .get(h)
        .toRight(FileSystemError.unknownWriteHandle(h))
        .run
        .liftB
      .unattempt

    override def write(
        h: WriteHandle,
        chunk: Vector[Data]): Configured[Vector[FileSystemError]] = {
      (for {
        _ <- writeKvs.get(h).toRight(FileSystemError.unknownWriteHandle(h)).run.liftB.unattempt
        dbPath <- getDbPath(h)
        _ <- batchInsert(dbPath, chunk).liftB
      } yield Vector()).run.value.map(_.valueOr(Vector(_)))
    }

    override def open(file: AFile): Backend[WriteHandle] =
      for {
        i <- MonotonicSeq.Ops[Eff].next.liftB
        dbPath = TablePath.create(file)
        _ <- createTable(dbPath).liftB
        handle = WriteHandle(file, i)
        _ <- writeKvs.put(handle, dbPath).liftB
      } yield handle

    override def close(h: WriteHandle): Configured[Unit] =
      writeKvs.delete(h).liftM[ConfiguredT]

  }
}
