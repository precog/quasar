/*
 * Copyright 2014–2018 SlamData Inc.
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
import quasar.contrib.pathy.{ADir, AFile, APath}
import quasar.contrib.scalaz._
import quasar.contrib.scalaz.eitherT._
import quasar.effect.MonotonicSeq
import quasar.fp.free.lift
import quasar.fs.ManageFile.PathPair.{DirToDir, FileToFile}
import quasar.fs._
import quasar.fs.FileSystemError._
import quasar.fs.PathError._
import quasar.fs.impl.ensureMoveSemantics
import quasar.physical.rdbms.Rdbms
import quasar.physical.rdbms.common.TablePath.dirToSchema
import quasar.physical.rdbms.common._

import doobie.imports._
import pathy.Path
import pathy.Path._
import scalaz._
import Scalaz._

trait RdbmsManageFile
    extends RdbmsDescribeTable
    with RdbmsCreate
    with RdbmsMove {
  this: Rdbms =>
  implicit def MonadM: Monad[M]

  val defaultPrefix = TempFilePrefix("__quasar_tmp_table_")

  def dropTableIfExists(table: TablePath): ConnectionIO[Unit]

  def dropTable(table: TablePath): ConnectionIO[Unit] = {
    (fr"DROP TABLE" ++ Fragment.const(table.shows)).update.run.void
  }

  def dropSchema(schema: Schema): ConnectionIO[Unit] = {
    (fr"DROP SCHEMA" ++ Fragment.const(s""""${schema.shows}"""") ++ fr"CASCADE").update.run.void
  }

  override def ManageFileModule = new ManageFileModule {

    def dropSchemaWithChildren(parent: Schema): ConnectionIO[Unit] = {
      findChildSchemas(parent)
        .flatMap(cs => (cs :+ parent).foldMap(dropSchema))
    }

    def tempSchema: M[Schema] = {
      MonotonicSeq
        .Ops[Eff]
        .next
        .map { i =>
          Schema(s"__quasar_tmp_schema_$i")
        }
        .flatMap(s => lift(createSchema(s).map(_ => s)).into[Eff])
    }

    override def move(scenario: ManageFile.PathPair,
                      semantics: MoveSemantics): Backend[Unit] = {

      def moveFile(src: AFile, dst: AFile): M[Unit] = {
        val dstPath = TablePath.create(dst)
        tempSchema.flatMap { tmp =>
          lift(for {
            _ <- dropTableIfExists(dstPath)
            tmpTable <- moveTableToSchema(TablePath.create(src), tmp)
            renamed <- renameTable(tmpTable, dstPath.table)
            _ <- createSchema(dstPath.schema)
            _ <- moveTableToSchema(renamed, dstPath.schema)
          } yield ()).into[Eff]
        }
      }

      def moveDir(src: ADir, dst: ADir): FileSystemErrT[M, Unit] = {
        val srcSchema = dirToSchema(src)
        val dstSchema = dirToSchema(dst)

        val dbCalls = for {
          dstSchemaExists <- schemaExists(dstSchema)
          _ <- dstSchemaExists.whenM(dropSchema(dstSchema))
          _ <- ensureSchemaParents(dstSchema)
          _ <- renameSchema(srcSchema, dstSchema)
        } yield ()

        lift(dbCalls).into[Eff].liftM[FileSystemErrT]
      }

      def fExists: APath => M[Boolean] = { path =>
        lift(
          refineType(path).fold(
            d => schemaExists(dirToSchema(d)),
            f => tableExists(TablePath.create(f))
          )).into[Eff]
      }

      scenario match {
        case FileToFile(sf, df) =>
          for {
            _ <- (((ensureMoveSemantics(sf, df, fExists, semantics)
              .toLeft(()) *>
              moveFile(sf, df).liftM[FileSystemErrT]).run).liftB).unattempt
          } yield ()

        case DirToDir(sd, dd) =>
          for {
            _ <- (((ensureMoveSemantics(sd, dd, fExists, semantics)
              .toLeft(()) *>
              moveDir(sd, dd)).run).liftB).unattempt
          } yield ()
      }
    }

    def copy(pair: ManageFile.PathPair): Backend[Unit] =
      ME.raiseError(
        unsupportedOperation(
          "Rdbms connector does not currently support copying"))

    def deleteFile(aFile: AFile): Backend[Unit] = {
      val dbTablePath = TablePath.create(aFile)
      for {
        exists <- tableExists(dbTablePath).liftB
        _ <- exists.unlessM(ME.raiseError(pathErr(pathNotFound(aFile))))
        _ <- (fr"DROP TABLE" ++ Fragment
          .const(dbTablePath.shows)).update.run.liftB
      } yield ()
    }

    def deleteDir(aDir: ADir): Backend[Unit] = {
      val schema = dirToSchema(aDir)
      for {
        exists <- schemaExists(schema).liftB
        _ <- exists.unlessM(ME.raiseError(pathErr(pathNotFound(aDir))))
        _ <- dropSchemaWithChildren(schema).liftB
      } yield ()
    }

    override def delete(path: APath): Backend[Unit] = {
      Path
        .maybeFile(path)
        .map(deleteFile)
        .orElse(Path.maybeDir(path).map(deleteDir))
        .getOrElse(().point[Backend])
    }

    override def tempFile(near: APath, prefix: Option[TempFilePrefix]): Backend[AFile] =
      MonotonicSeq.Ops[Eff].next.map(
        TmpFile.tmpFile0(near, prefix.getOrElse(defaultPrefix), _)).liftB

  }
}
