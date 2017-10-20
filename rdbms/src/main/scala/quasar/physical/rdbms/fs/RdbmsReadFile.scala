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
import quasar.fp.numeric.{Natural, Positive}
import quasar.fs._
import quasar.physical.rdbms.Rdbms
import quasar.physical.rdbms.common.TablePath

import doobie.free.connection.ConnectionIO
import doobie.syntax.string._
import doobie.syntax.process._
import doobie.util.fragment.Fragment
import doobie.util.meta.Meta
import eu.timepit.refined.api.RefType.ops._
import scalaz._
import Scalaz._

final case class SqlReadCursor(data: Vector[Data])

trait RdbmsReadFile extends RdbmsDescribeTable {
  this: Rdbms =>

  import ReadFile._

  private val kvs = KeyValueStore.Ops[ReadHandle, SqlReadCursor, Eff]

  implicit def MonadM: Monad[M]
  implicit def dataMeta: Meta[Data]

  val ReadFileModule: ReadFileModule = new ReadFileModule {

    private def readAll(
        dbPath: TablePath,
        offset: Int,
        limit: Option[Int]
    ): ConnectionIO[Vector[Data]] = {
      val streamWithOffset = (fr"select data from" ++ Fragment.const(dbPath.shows))
        .query[Data]
        .process
        .drop(offset)

      val streamWithLimit = limit match {
        case Some(l) => streamWithOffset.take(l)
        case None    => streamWithOffset
      }
      streamWithLimit
        .vector
    }

    private def toInt(long: Long, varName: String): Backend[\/[FileSystemError, Int]] = {
      (
        \/.fromTryCatchNonFatal(long.toInt)
          .leftMap(
            _ => FileSystemError.readFailed(long.toString, s"$varName not convertible to Int.")
          ).point[ConnectionIO]
        ).liftB
    }

    def openExisting(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] = {
      for {
        i <- MonotonicSeq.Ops[Eff].next.liftB
        dbPath = TablePath.create(file)
        handle = ReadHandle(file, i)
        limitInt <- limit.traverse(l => toInt(l.unwrap, "limit").unattempt)
        offsetInt <- toInt(offset.unwrap, "offset").unattempt
        sqlResult <- readAll(dbPath, offsetInt, limitInt).liftB
        _ <- kvs.put(handle, SqlReadCursor(sqlResult)).liftB
      } yield handle
    }

    def openMissing(file: AFile): Backend[ReadHandle] = {
      for {
        i <- MonotonicSeq.Ops[Eff].next.liftB
        handle = ReadHandle(file, i)
        _ <- kvs.put(handle, SqlReadCursor(Vector.empty)).liftB
      } yield handle
    }

    override def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] = {
      val dbPath = TablePath.create(file)
      for {
        exists <- tableExists(dbPath).liftB
        handle <- if (exists) openExisting(file, offset, limit) else openMissing(file)
      } yield handle
    }

    override def read(h: ReadHandle): Backend[Vector[Data]] = {
      for {
        c <- kvs.get(h).toRight(FileSystemError.unknownReadHandle(h)).run.liftB.unattempt
        data = c.data
        _ <- kvs.put(h, SqlReadCursor(Vector.empty)).liftB
      } yield data
    }

    override def close(h: ReadHandle): Configured[Unit] =
      kvs.delete(h).liftM[ConfiguredT]
  }
}
