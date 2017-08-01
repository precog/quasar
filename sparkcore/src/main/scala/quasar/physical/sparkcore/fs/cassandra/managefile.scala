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

  import common._

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
      d => cass.keyspaceExists(keyspace(d)),
      f => cass.tableExists(keyspace(fileParent(f)), tableName(f))
    )

  def moveFile[S[_]](sf: AFile, df: AFile)(implicit
    cass: CassandraDDL.Ops[S]
    ): Free[S, Unit] = {
    val dks = keyspace(fileParent(df))
    val dft = tableName(df)
    for {
      keyspaceExists <- cass.keyspaceExists(dks)
      _              <- if (!keyspaceExists) cass.createKeyspace(dks) else Free.pure[S, Unit](())
      tableExists    <- cass.tableExists(dks, dft)
      _              <- if(tableExists) cass.dropTable(dks, dft) else Free.pure[S, Unit](())
      _              <- cass.moveTable(keyspace(fileParent(sf)), tableName(sf), dks, dft)
      _              <- cass.dropTable(keyspace(fileParent(sf)), tableName(sf))
    } yield ()
  }

  def moveDir[S[_]](sd: ADir, dd: ADir)(implicit
    cass: CassandraDDL.Ops[S]
  ): Free[S, Unit] = {
    for {
      tables <- cass.listTables(keyspace(sd))
      _      <- tables.map { tn =>
                  moveFile(sd </> file(tn), dd </> file(tn))
                }.toList.sequence
      _      <- cass.dropKeyspace(keyspace(sd))
    } yield ()
  }

  def delete[S[_]](path: APath)(implicit
    cass: CassandraDDL.Ops[S]
    ): Free[S, FileSystemError \/ Unit] =
      maybeFile(path).fold(deleteDir(path))(file => deleteFile(file))

  private def deleteFile[S[_]](file: AFile)(implicit
    cass: CassandraDDL.Ops[S]
  ): Free[S, FileSystemError \/ Unit] = {
    val ks = keyspace(fileParent(file))
    val tb = tableName(file)

    (for {
      keyspaceExists <- cass.keyspaceExists(ks).liftM[FileSystemErrT]
      tableExists    <- cass.tableExists(ks, tb).liftM[FileSystemErrT]
      _              <- EitherT((
                                  if(keyspaceExists && tableExists)
                                    ().right
                                  else
                                    pathErr(pathNotFound(file)).left[Unit]
                                ).point[Free[S, ?]])
      _              <- cass.dropTable(ks, tb).liftM[FileSystemErrT]
    } yield ()).run
  }

  private def deleteDir[S[_]](dir: APath)(implicit
    cass: CassandraDDL.Ops[S]
  ): Free[S, FileSystemError \/ Unit] = {
    val aDir: ADir = refineType(dir).fold(d => d, fileParent(_))
    val ks = keyspace(aDir)

    (for {
      keyspaces <- cass.listKeyspaces(ks).liftM[FileSystemErrT]
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
