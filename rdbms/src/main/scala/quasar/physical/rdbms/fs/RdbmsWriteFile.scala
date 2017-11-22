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
import quasar.contrib.scalaz.eitherT._
import quasar.Data
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fs._
import quasar.physical.rdbms.Rdbms
import quasar.physical.rdbms.common.TablePath
import quasar.physical.rdbms.model.TableModel

import doobie.imports.Meta
import scalaz._
import Scalaz._

final case class WriteCursor(tablePath: TablePath, model: Option[TableModel])

trait RdbmsWriteFile
    extends RdbmsInsert
    with RdbmsDescribeTable
    with RdbmsCreate {
  this: Rdbms =>

  import WriteFile._

  implicit def MonadM: Monad[M]
  implicit def dataMeta: Meta[Data]

  val writeKvs = KeyValueStore.Ops[WriteHandle, WriteCursor, Eff]

  override def WriteFileModule: WriteFileModule = new WriteFileModule {

    def prepareTable(c: WriteCursor,
                     chunk: Vector[Data]): Backend[TableModel] = {
      ME.unattempt(
        TableModel
          .fromData(chunk)
          .flatMap { newModel =>
            (c.model match {
              case Some(prevModel) =>
                TableModel
                  .alter(prevModel, newModel)
                  .map(alterTable(c.tablePath, _))
              case None =>
                createTable(c.tablePath, newModel).right
            }).map(_.map(_ => newModel))
          }
          .sequence
          .liftB)
    }

    override def write(
        h: WriteHandle,
        chunk: Vector[Data]): Configured[Vector[FileSystemError]] = {

      (for {
        c <- ME.unattempt(
          writeKvs
            .get(h)
            .toRight(FileSystemError.unknownWriteHandle(h))
            .run
            .liftB)
        dbPath = c.tablePath
        newModel <- prepareTable(c, chunk)
        _ <- batchInsert(dbPath, chunk, newModel).liftB
        _ <- writeKvs.put(h, WriteCursor(dbPath, model = Some(newModel))).liftB
      } yield Vector()).run.value.map(_.valueOr(Vector(_)))
    }

    override def open(file: AFile): Backend[WriteHandle] = {
      val dbPath = TablePath.create(file)
      for {
        i <- MonotonicSeq.Ops[Eff].next.liftB
        handle = WriteHandle(file, i)
        initialModel <- tableModel(dbPath).liftB
        _ <- writeKvs.put(handle, WriteCursor(dbPath, initialModel)).liftB
      } yield handle
    }

    override def close(h: WriteHandle): Configured[Unit] =
      writeKvs.delete(h).liftM[ConfiguredT]

  }
}
