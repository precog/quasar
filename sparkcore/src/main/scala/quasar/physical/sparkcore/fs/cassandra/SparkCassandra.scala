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
import quasar.{Data, DataCodec}
import quasar.connector._
import quasar.contrib.pathy._
import quasar.contrib.scalaz._
import quasar.effect._
import quasar.fp.free._
import quasar.fp.TaskRef
import quasar.fs._,
  QueryFile.ResultHandle,
  ReadFile.ReadHandle,
  WriteFile.WriteHandle,
  FileSystemError._,
  PathError._
import quasar.fs.mount._, BackendDef._
import quasar.physical.sparkcore.fs._
import quasar.physical.sparkcore.fs.SparkCore
import quasar.physical.sparkcore.fs.{SparkCore, SparkConnectorDetails}, SparkConnectorDetails._
import quasar.qscript.{QScriptTotal, Injectable, QScriptCore, EquiJoin, ShiftedRead, ::/::, ::\::}

import org.apache.spark._
import org.apache.spark.rdd._
import org.http4s.{ParseFailure, Uri}
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

final case class CassandraConfig(sparkConf: SparkConf, prefix: ADir)

object SparkCassandra extends SparkCore with ManagedWriteFile[AFile] with ChrootedInterpreter {

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import EitherT.eitherTMonad
  import common._

  def rootPrefix(cfg: Config): ADir = cfg.prefix

  val Type = FileSystemType("spark-cassandra")

  type Eff7[A] = Coproduct[KeyValueStore[ResultHandle, SparkCursor, ?], Read[SparkContext, ?], A]
  type Eff6[A] = Coproduct[KeyValueStore[ReadHandle, SparkCursor, ?], Eff7, A]
  type Eff5[A] = Coproduct[KeyValueStore[WriteHandle, AFile, ?], Eff6, A]
  type Eff4[A] = Coproduct[Task, Eff5, A]
  type Eff3[A] = Coproduct[PhysErr, Eff4, A]
  type Eff2[A] = Coproduct[CassandraDDL, Eff3, A]
  type Eff1[A] = Coproduct[MonotonicSeq, Eff2, A]
  type Eff[A]  = Coproduct[SparkConnectorDetails, Eff1, A]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])

  def ReadSparkContextInj = Inject[Read[SparkContext, ?], Eff]
  def RFKeyValueStoreInj = Inject[KeyValueStore[ReadFile.ReadHandle, SparkCursor, ?], Eff]
  def MonotonicSeqInj = Inject[MonotonicSeq, Eff]
  def TaskInj = Inject[Task, Eff]
  def SparkConnectorDetailsInj = Inject[SparkConnectorDetails, Eff]
  def QFKeyValueStoreInj = Inject[KeyValueStore[QueryFile.ResultHandle, SparkCursor, ?], Eff]

  val cass = CassandraDDL.Ops[Eff]
  def MonoSeqM = MonoSeq[M]
  def WriteKvsM = Kvs[M, WriteFile.WriteHandle, AFile]

  def toLowerLevel[S[_]](sc: SparkContext, config: CassandraConfig)(implicit
    S0: Task :<: S, S1: PhysErr :<: S
  ): Task[Free[Eff, ?] ~> Free[S, ?]] = (TaskRef(0L) |@|
    TaskRef(Map.empty[ResultHandle, SparkCursor]) |@|
    TaskRef(Map.empty[ReadHandle, SparkCursor]) |@|
    TaskRef(Map.empty[WriteHandle, AFile])) {
    // TODO better names!
    (genState, rddStates, sparkCursors, writehandlers) =>

    type Temp[A] = Coproduct[CassandraDDL, Task, A]
    def temp: Free[Temp, ?] ~> Task =
      foldMapNT(CassandraDDL.interpreter[S](sc) :+: injectNT[Task, Task])

    val interpreter: Eff ~> S =
      (details.interpreter[Temp] andThen temp  andThen injectNT[Task, S]) :+:
    (MonotonicSeq.fromTaskRef(genState) andThen injectNT[Task, S]) :+:
    (CassandraDDL.interpreter[S](sc) andThen injectNT[Task, S]) :+:
    injectNT[PhysErr, S] :+:
    injectNT[Task, S]  :+:
    (KeyValueStore.impl.fromTaskRef[WriteHandle, AFile](writehandlers) andThen injectNT[Task, S]) :+:
    (KeyValueStore.impl.fromTaskRef[ReadHandle, SparkCursor](sparkCursors) andThen injectNT[Task, S]) :+:
    (KeyValueStore.impl.fromTaskRef[ResultHandle, SparkCursor](rddStates) andThen injectNT[Task, S]) :+:
    (Read.constant[Task, SparkContext](sc) andThen injectNT[Task, S])

    mapSNT[Eff, S](interpreter)
  }

  type Config = CassandraConfig
  def parseConfig(connUri: ConnectionUri): DefErrT[Task, Config] = {

    def liftErr(msg: String): DefinitionError = NonEmptyList(msg).left[EnvironmentError]

    def master(host: String, port: Int): State[SparkConf, Unit] =
      State.modify(_.setMaster(s"spark://$host:$port"))
    def appName: State[SparkConf, Unit] = State.modify(_.setAppName("quasar"))

    def config(name: String, uri: Uri): State[SparkConf, Unit] =
      State.modify(c => uri.params.get(name).fold(c)(c.set(name, _)))

    val uriOrErr: DefErrT[Task, Uri] =
      EitherT(Uri.fromString(connUri.value).leftMap((pf: ParseFailure) => liftErr(pf.toString)).point[Task])

    val sparkConfOrErr: DefErrT[Task, SparkConf] = for {
      uri <- uriOrErr
      host <- EitherT(uri.host.fold(liftErr("host not provided").left[Uri.Host])(_.right[DefinitionError]).point[Task])
      port <- EitherT(uri.port.fold(liftErr("port not provided").left[Int])(_.right[DefinitionError]).point[Task])
    } yield {
      (master(host.value, port)                        *>
        appName                                        *>
        config("spark.executor.memory", uri)           *>
        config("spark.executor.cores", uri)            *>
        config("spark.executor.extraJavaOptions", uri) *>
        config("spark.default.parallelism", uri)       *>
        config("spark.files.maxPartitionBytes", uri)   *>
        config("spark.driver.cores", uri)              *>
        config("spark.driver.maxResultSize", uri)      *>
        config("spark.driver.memory", uri)             *>
        config("spark.local.dir", uri)                 *>
        config("spark.reducer.maxSizeInFlight", uri)   *>
        config("spark.reducer.maxReqsInFlight", uri)   *>
        config("spark.shuffle.file.buffer", uri)       *>
        config("spark.shuffle.io.retryWait", uri)      *>
        config("spark.memory.fraction", uri)           *>
        config("spark.memory.storageFraction", uri)    *>
        config("spark.cores.max", uri)                 *>
        config("spark.speculation", uri)               *>
        config("spark.task.cpus", uri)
      ).exec(new SparkConf())
    }

    def fetchParameter(name: String): DefErrT[Task, String] = uriOrErr.flatMap(uri =>
      uri.params.get(name).fold(
        EitherT(liftErr(s"'$name' parameter not provided").left[String].point[Task]))(_.point[DefErrT[Task, ?]])
    )

    val rootPathOrErr: DefErrT[Task, ADir] =
      uriOrErr
        .flatMap(uri =>
          EitherT(uri.params.get("rootPath").fold(liftErr("'rootPath' parameter not provided").left[String])(_.right[DefinitionError]).point[Task])
        )
        .flatMap(pathStr =>
          EitherT(posixCodec.parseAbsDir(pathStr)
            .map(unsafeSandboxAbs)
            .fold(liftErr("'rootPath' is not a valid path").left[ADir])(_.right[DefinitionError]).point[Task])
        )


    for {
      initSparkConf              <- sparkConfOrErr
      rootPath                   <- rootPathOrErr
      cassandraHostAndPort       <- fetchParameter("cassandraHost").tuple(fetchParameter("cassandraPort"))
      (host, port)               = cassandraHostAndPort
    } yield {
      val sparkConf = initSparkConf
        .set("spark.cassandra.connection.host", host)
        .set("spark.cassandra.connection.port", port)
      CassandraConfig(sparkConf, rootPath)
    }
  }

  def getSparkConf: Config => SparkConf = _.sparkConf

  def generateSC: (APath, Config) => DefErrT[Task, SparkContext] =
    (jar, conf) => initSC(conf).map { sc =>
      sc.addJar(posixCodec.printPath(jar))
      sc
    }

  object details {

    import common._

    def rddFrom[S[_]](f: AFile)(implicit
      cass: CassandraDDL.Ops[S]
    ): Free[S, RDD[Data]] =
      cass.readTable(keyspace(fileParent(f)), tableName(f))

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
      cass: CassandraDDL.Ops[S]
    ): Free[S, Boolean] =
      for {
        tableExists <- cass.tableExists(keyspace(fileParent(f)), tableName(f))
      } yield tableExists

    def listContents[S[_]](d: ADir)(implicit
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

    def interpreter[S[_]](implicit
      cass: CassandraDDL.Ops[S],
      S: Task :<: S
    ): SparkConnectorDetails ~> Free[S, ?] = new (SparkConnectorDetails ~> Free[S, ?]) {
      def apply[A](from: SparkConnectorDetails[A]) = from match {
        case FileExists(f)       => fileExists(f)
        case ReadChunkSize       => 5000.point[Free[S, ?]]
        case StoreData(rdd, out) => store(rdd, out)
        case ListContents(d)     => listContents(d).run
        case RDDFrom(f)          => rddFrom(f)
      }
    }
  }


  object ManagedWriteFileModule extends ManagedWriteFileModule {

    def writeCursor(file: AFile): Backend[AFile] = {
      val ks = keyspace(fileParent(file))
      val tb = tableName(file)

      (for {
        keyspaceExists <- cass.keyspaceExists(ks)
        _              <- if(keyspaceExists) ().point[Free[Eff, ?]] else cass.createKeyspace(ks)
        tableExists    <- cass.tableExists(ks, tb)
        _              <- if(tableExists) ().point[Free[Eff, ?]] else cass.createTable(ks, tb)
      } yield file).liftB
    }

    def writeChunk(f: AFile, chunk: Vector[Data]): Configured[Vector[FileSystemError]] = {
      implicit val codec: DataCodec = DataCodec.Precise

      val ks = keyspace(fileParent(f))
      val tn = tableName(f)

      val lines = chunk.map(data => DataCodec.render(data)).toList.map(_.toList).join
      lines.map {
        case text => cass.insertData(ks, tn, text)
      }.sequence.as(Vector.empty[FileSystemError]).liftM[ConfiguredT]
    }

    def closeCursor(f: AFile): Configured[Unit] =
      ().point[Configured]
  }

  object ElasticManageFileModule extends SparkCoreManageFileModule {

    def moveFile(source: AFile, destination: AFile): Free[Eff, Unit] = {
      val destinationKeyspace = common.keyspace(fileParent(destination))
      val destinationTable = common.tableName(destination)
      val sourceKeyspace = common.keyspace(fileParent(source))
      val sourceTable = common.tableName(source)
      for {
        keyspaceExists <- cass.keyspaceExists(destinationKeyspace)
        _              <- if (!keyspaceExists) cass.createKeyspace(destinationKeyspace) else ().point[Free[Eff, ?]]
        tableExists    <- cass.tableExists(destinationKeyspace, destinationTable)
        _              <- if(tableExists) cass.dropTable(destinationKeyspace, destinationTable) else ().point[Free[Eff, ?]]
        _              <- cass.moveTable(sourceKeyspace, sourceTable, destinationKeyspace, destinationTable)
        _              <- cass.dropTable(sourceKeyspace, sourceTable)
      } yield ()
    }

    def moveDir(source: ADir, destination: ADir): Free[Eff, Unit] =  for {
      tables <- cass.listTables(common.keyspace(source))
      _      <- tables.map { tn =>
        moveFile(source </> file(tn), destination </> file(tn))
      }.toList.sequence
      _      <- cass.dropKeyspace(common.keyspace(source))
    } yield ()

    def doesPathExist: APath => Free[Eff, Boolean] = (path: APath) =>
    refineType(path).fold(
      d => cass.keyspaceExists(common.keyspace(d)),
      f => cass.tableExists(common.keyspace(fileParent(f)), common.tableName(f))
    )

    def delete(path: APath): Backend[Unit] =
      refineType(path).fold(deleteDir _, deleteFile _).liftB.unattempt

    private def deleteFile(file: AFile): Free[Eff, FileSystemError \/ Unit] = {
      val keyspace = common.keyspace(fileParent(file))
      val table = common.tableName(file)

      (for {
        tableExists    <- cass.tableExists(keyspace, table).liftM[FileSystemErrT]
        _              <- EitherT((
          if(tableExists)
            ().right
          else
            pathErr(pathNotFound(file)).left[Unit]
        ).point[Free[Eff, ?]])
        _              <- cass.dropTable(keyspace, table).liftM[FileSystemErrT]
      } yield ()).run
    }

    private def deleteDir(dir: ADir): Free[Eff, FileSystemError \/ Unit] = {
      val keyspace = common.keyspace(dir)
        (for {
          keyspaces <- cass.listKeyspaces(keyspace).liftM[FileSystemErrT]
          _         <- EitherT((
            if(keyspaces.isEmpty)
              pathErr(pathNotFound(dir)).left[Unit]
            else
              ().right
          ).point[Free[Eff, ?]])
          _         <- keyspaces.map { keyspace =>
            cass.dropKeyspace(keyspace)
          }.toList.sequence.liftM[FileSystemErrT]
        } yield ()).run
    }


  }

  def ManageFileModule: ManageFileModule = ElasticManageFileModule


}
