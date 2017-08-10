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

package quasar.physical.sparkcore.fs.cassandra

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.fs._,
  ManageFile._,
  MoveScenario._,
  FileSystemError._,
  PathError._
import quasar.effect._
import quasar.fp.free._
import quasar.fs.impl.ensureMoveSemantics

import org.apache.spark.SparkContext
import pathy._, Path._
import scalaz._, Scalaz._

object managefile {

  def chrooted[S[_]](prefix: ADir)(implicit
    s0: Read.Ops[SparkContext, S],
    s1: CassandraDDL.Ops[S]
  ): ManageFile ~> Free[S, ?] =
    flatMapSNT(interpret) compose chroot.manageFile[ManageFile](prefix)

  def interpret[S[_]](implicit
    s0: Read.Ops[SparkContext, S],
    s1: CassandraDDL.Ops[S]
    ): ManageFile ~> Free[S, ?] = 
    new (ManageFile ~> Free[S, ?]) {
      def apply[A](mf: ManageFile[A]): Free[S, A] = mf match {
        case Move(FileToFile(sf, df), semantics) =>
          (ensureMoveSemantics[Free[S, ?]](sf, df, pathExists[S] _, semantics).toLeft(()) *>
            moveFile(sf, df).liftM[FileSystemErrT]).run
        case Move(DirToDir(sd, dd), semantics) =>
          (ensureMoveSemantics[Free[S, ?]](sd, dd, pathExists _, semantics).toLeft(()) *>
            moveDir(sd, dd).liftM[FileSystemErrT]).run
        case Delete(path) => delete(path)
        case TempFile(near) => tempFile(near)
      }
    }

  def pathExists[S[_]](path: APath)(implicit 
    cass: CassandraDDL.Ops[S]
    ): Free[S, Boolean] = 
    refineType(path).fold(
      d => cass.keyspaceExists(common.keyspace(d)),
      f => cass.tableExists(common.keyspace(fileParent(f)), common.tableName(f))
    )

  def moveFile[S[_]](source: AFile, destination: AFile)(implicit
    cass: CassandraDDL.Ops[S]
    ): Free[S, Unit] = {
    val destinationKeyspace = common.keyspace(fileParent(destination))
    val destinationTable = common.tableName(destination)
    val sourceKeyspace = common.keyspace(fileParent(source))
    val sourceTable = common.tableName(source)
    for {
      keyspaceExists <- cass.keyspaceExists(destinationKeyspace)
      _              <- if (!keyspaceExists) cass.createKeyspace(destinationKeyspace) else Free.pure[S, Unit](())
      tableExists    <- cass.tableExists(destinationKeyspace, destinationTable)
      _              <- if(tableExists) cass.dropTable(destinationKeyspace, destinationTable) else Free.pure[S, Unit](())
      _              <- cass.moveTable(sourceKeyspace, sourceTable, destinationKeyspace, destinationTable)
      _              <- cass.dropTable(sourceKeyspace, sourceTable)
    } yield ()
  }

  def moveDir[S[_]](source: ADir, destination: ADir)(implicit
    cass: CassandraDDL.Ops[S]
  ): Free[S, Unit] = {
    for {
      tables <- cass.listTables(common.keyspace(source))
      _      <- tables.map { tn =>
                  moveFile(source </> file(tn), destination </> file(tn))
                }.toList.sequence
      _      <- cass.dropKeyspace(common.keyspace(source))
    } yield ()
  }

  def delete[S[_]](path: APath)(implicit
    cass: CassandraDDL.Ops[S]
    ): Free[S, FileSystemError \/ Unit] =
      maybeFile(path).fold(deleteDir(path))(file => deleteFile(file))

  private def deleteFile[S[_]](file: AFile)(implicit
    cass: CassandraDDL.Ops[S]
  ): Free[S, FileSystemError \/ Unit] = {
    val keyspace = common.keyspace(fileParent(file))
    val table = common.tableName(file)

    (for {
      tableExists    <- cass.tableExists(keyspace, table).liftM[FileSystemErrT]
      _              <- EitherT((
                                  if(tableExists)
                                    ().right
                                  else
                                    pathErr(pathNotFound(file)).left[Unit]
                                ).point[Free[S, ?]])
      _              <- cass.dropTable(keyspace, table).liftM[FileSystemErrT]
    } yield ()).run
  }

  private def deleteDir[S[_]](dir: APath)(implicit
    cass: CassandraDDL.Ops[S]
  ): Free[S, FileSystemError \/ Unit] = {
    val aDir: ADir = refineType(dir).fold(d => d, fileParent(_))
    val keyspace = common.keyspace(aDir)

    (for {
      keyspaces <- cass.listKeyspaces(keyspace).liftM[FileSystemErrT]
      _         <- EitherT((
                             if(keyspaces.isEmpty)
                               pathErr(pathNotFound(dir)).left[Unit]
                             else
                               ().right
                           ).point[Free[S, ?]])
      _         <- keyspaces.map { keyspace =>
                     cass.dropKeyspace(keyspace)
                   }.toList.sequence.liftM[FileSystemErrT]
    } yield ()).run
  }

  def tempFile[S[_]](near: APath)(implicit
    read: Read.Ops[SparkContext, S]
    ): Free[S, FileSystemError \/ AFile] = 
    read.asks { sc =>
      val randomFileName = s"q${scala.math.abs(scala.util.Random.nextInt(9999))}"
      val aDir: ADir = refineType(near).fold(d => d, fileParent(_))
      (aDir </> file(randomFileName)).right[FileSystemError]
    }

}
