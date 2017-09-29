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
import quasar.fp.free.lift
import quasar.fs.FileSystemError._
import quasar.fs.PathError._
import quasar.fs.QueryFile
import quasar.physical.rdbms.Rdbms
import quasar.physical.rdbms.common.{Schema, TablePath}
import quasar.physical.rdbms.common.TablePath.showTableName
import pathy.Path

import scalaz.{-\/, \/-, Monad}
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.std.boolean._
import scalaz.std.vector._

trait RdbmsQueryFile {
  this: Rdbms =>

  import QueryFile._
  implicit def MonadM: Monad[M]

  def QueryFileModule: QueryFileModule = new QueryFileModule {

    override def explain(repr: Repr): Backend[String] = ???

    override def executePlan(repr: Repr, out: AFile): Backend[Unit] = ???

    override def evaluatePlan(repr: Repr): Backend[ResultHandle] = ???

    override def more(h: ResultHandle): Backend[Vector[Data]] = ???

    override def fileExists(file: AFile): Configured[Boolean] =
      lift(tableExists(TablePath.create(file))).into[Eff].liftM[ConfiguredT]

    override def listContents(dir: ADir): Backend[Set[PathSegment]] = {
      val schema = TablePath.dirToSchema(dir)
      schemaExists(schema).liftB
        .flatMap(_.unlessM(ME.raiseError(pathErr(pathNotFound(dir))))) *>
        (for {
          childSchemas <- findChildSchemas(schema)
          childTables <- findChildTables(schema)
          childDirs = childSchemas.map(d => -\/(Schema.lastDirName(d))).toSet
          childFiles = childTables.map(t => \/-(Path.FileName(t.shows))).toSet
        } yield childDirs ++ childFiles).liftB
    }

    override def close(h: ResultHandle): Configured[Unit] = ???
  }
}
