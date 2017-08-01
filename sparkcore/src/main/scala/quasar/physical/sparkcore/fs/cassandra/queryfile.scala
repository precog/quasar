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
import quasar.effect._
import quasar.fp.free._
import quasar.fp.ski._
import quasar.contrib.pathy._
import quasar.fs.FileSystemError
import quasar.fs.FileSystemErrT
import quasar.{Data, DataCodec}
import quasar.physical.sparkcore.fs.queryfile.Input
import quasar.fs._,
  FileSystemError._, 
  PathError._

import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import com.datastax.spark.connector._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import pathy.Path._


object queryfile {

  import common._

  def fromFile(sc: SparkContext, file: AFile): Task[RDD[Data]] = Task.delay {
    sc.cassandraTable[String](keyspace(fileParent(file)), tableName(file))
      .select("data")
      .map{ raw =>
        DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι)
      }
  }

  def store[S[_]](rdd: RDD[Data], out: AFile)(implicit
    cass: CassandraDDL.Ops[S],
    S0: Task :<: S
  ): Free[S, Unit] = {
    val ks = keyspace(fileParent(out))
    val tb = tableName(out)

    for {
      keyspaceExists <- cass.keyspaceExists(ks)
      _ <- if(keyspaceExists) ().point[Free[S,?]] else cass.createKeyspace(ks)
      tableExists <- cass.tableExists(ks, tb)
      _ <- if(tableExists) {
        cass.dropTable(ks, tb) *> cass.createTable(ks, tb)
      } else cass.createTable(ks, tb)
      jsons <- {
        lift(Task.delay(rdd.flatMap(DataCodec.render(_)(DataCodec.Precise).toList)
                          .collect().toList)
        ).into[S]
      }
      _ <- jsons.map(cass.insertData(ks, tb, _)).sequence
    } yield ()
  }

  def fileExists[S[_]](f: AFile)(implicit
    cass: CassandraDDL.Ops[S],
    read: Read.Ops[SparkContext, S]
  ): Free[S, Boolean] =
    for {
      tableExists <- cass.tableExists(keyspace(fileParent(f)), tableName(f))
    } yield tableExists

  def listContents[S[_]](d: ADir)(implicit
    read: Read.Ops[SparkContext, S],
    cass: CassandraDDL.Ops[S]
  ): FileSystemErrT[Free[S, ?], Set[PathSegment]] = EitherT{
    val ks = keyspace(d)
    for {
      dirs <- cass.listKeyspaces(ks)
      isKeyspaceExists <- cass.keyspaceExists(ks)
      files <- if(ks.length > 0 && isKeyspaceExists) cass.listTables(ks) else Set.empty[String].point[Free[S,?]]
    } yield {
      if(dirs.length > 0)
        (
          files.map{f => FileName(f).right[DirName]} ++
            dirs.map{d => d.replace(ks, "").split("_")(0)}
            .filter(_.length > 0)
            .map(d => DirName(d).left[FileName])
        ).right[FileSystemError]
      else
        pathErr(pathNotFound(d)).left[Set[PathSegment]]
    }
  }

  def readChunkSize: Int = 5000

  def input[S[_]](implicit
    cass: CassandraDDL.Ops[S],
    read: Read.Ops[SparkContext, S],
    S0: Task :<: S
  ): Input[S] =
    Input(fromFile _, store[S] _, fileExists[S] _, listContents[S] _, readChunkSize _)

}
