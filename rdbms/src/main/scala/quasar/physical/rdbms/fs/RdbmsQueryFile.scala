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
import quasar.contrib.pathy.{ADir, AFile, PathSegment}
import quasar.Data
import quasar.fp._
import quasar.fp.free.lift
import quasar.fs.FileSystemError._
import quasar.fs.PathError._
import quasar.fs.QueryFile
import quasar.physical.rdbms.Rdbms
import quasar.physical.rdbms.common.{Schema, TablePath}
import quasar.physical.rdbms.common.TablePath.showTableName
import pathy.Path
import quasar.physical.rdbms.planner.sql.RenderQuery
import quasar.effect.{KeyValueStore, MonotonicSeq}

import doobie.syntax.string._
import doobie.util.fragment.Fragment
import doobie.util.composite.Composite
import scalaz._
import Scalaz._

trait RdbmsQueryFile {
  this: Rdbms =>

  import QueryFile._
  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import EitherT.eitherTMonad

  implicit def MonadM: Monad[M]

  def renderQuery: RenderQuery
  def createComposite: Repr => Composite[Data]
  private val kvs = KeyValueStore.Ops[ResultHandle, SqlReadCursor, Eff]

  def QueryFileModule: QueryFileModule = new QueryFileModule {

    override def explain(repr: Repr): Backend[String] = {
      ME.unattempt(renderQuery.asString(repr).leftMap(qscriptPlanningFailed(_)).traverse { q =>
        (fr"explain" ++ Fragment.const(q))
        .query[String]
        .unique
        .liftB
      })
    }

    override def executePlan(repr: Repr, out: AFile): Backend[Unit] = ???

    override def evaluatePlan(repr: Repr): Backend[ResultHandle] = {
      ME.unattempt(renderQuery.asString(repr).leftMap(qscriptPlanningFailed(_)).traverse { q =>

        val loadedData = Fragment
          .const(q)
          .query[Data](createComposite(repr))
          .vector
          .liftB

        for {
          i <- MonotonicSeq.Ops[Eff].next.liftB
          handle = ResultHandle(i)
          data <- loadedData
          _ <- kvs.put(handle, SqlReadCursor(data)).liftB
        } yield handle
      })
    }

    override def more(h: ResultHandle): Backend[Vector[Data]] = {
      for {
        c <- ME.unattempt(kvs.get(h).toRight(unknownResultHandle(h)).run.liftB)
        data = c.data
        _ <- kvs.put(h, SqlReadCursor(Vector.empty)).liftB
      } yield data
    }

    override def fileExists(file: AFile): Configured[Boolean] =
      lift(tableExists(TablePath.create(file))).into[Eff].liftM[ConfiguredT]

    override def listContents(dir: ADir): Backend[Set[PathSegment]] = {
      val schema = TablePath.dirToSchema(dir)
      schemaExists(schema).liftB.flatMap(_.unlessM(ME.raiseError(pathErr(pathNotFound(dir))))) *>
        (for {
        childSchemas <- findChildSchemas(schema)
        childTables <- findChildTables(schema)
        childDirs = childSchemas.map(d => -\/(Schema.lastDirName(d))).toSet
        childFiles = childTables.map(t => \/-(Path.FileName(t.shows))).toSet
      }
        yield childDirs ++ childFiles)
          .liftB
    }

    override def close(h: ResultHandle): Configured[Unit] =  kvs.delete(h).liftM[ConfiguredT]
  }
}
