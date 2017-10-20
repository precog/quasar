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

package quasar.physical.sparkcore.fs.elastic

import slamdata.Predef._
import quasar.{Data, DataCodec}
import quasar.connector._
import quasar.contrib.pathy._
import quasar.contrib.scalaz._
import quasar.effect._
import quasar.fp.ski._
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

final case class ElasticConfig(sparkConf: SparkConf, host: String, port: Int)

object SparkElastic extends SparkCore with ManagedWriteFile[AFile] with ChrootedInterpreter {

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import EitherT.eitherTMonad

  def rootPrefix(cfg: ElasticConfig): ADir = rootDir

  val Type = FileSystemType("spark-elastic")

  type Eff0[A]  = Coproduct[Task, PhysErr, A]
  type Eff1[A]  = Coproduct[Read[SparkContext, ?], Eff0, A]
  type Eff2[A]  = Coproduct[ElasticCall, Eff1, A]
  type Eff3[A]  = Coproduct[MonotonicSeq, Eff2, A]
  type Eff4[A]  = Coproduct[KeyValueStore[ResultHandle, SparkCursor, ?], Eff3, A]
  type Eff5[A]  = Coproduct[KeyValueStore[ReadHandle, SparkCursor, ?], Eff4, A]
  type Eff6[A]  = Coproduct[KeyValueStore[WriteHandle, AFile, ?], Eff5, A]
  type Eff[A]   = Coproduct[SparkConnectorDetails, Eff6, A]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]] =
        ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])

  def ReadSparkContextInj = Inject[Read[SparkContext, ?], Eff]
  def RFKeyValueStoreInj = Inject[KeyValueStore[ReadFile.ReadHandle, SparkCursor, ?], Eff]
  def MonotonicSeqInj = Inject[MonotonicSeq, Eff]
  def TaskInj = Inject[Task, Eff]
  def SparkConnectorDetailsInj = Inject[SparkConnectorDetails, Eff]
  def QFKeyValueStoreInj = Inject[KeyValueStore[QueryFile.ResultHandle, SparkCursor, ?], Eff]

  def MonoSeqM = MonoSeq[M]
  def WriteKvsM = Kvs[M, WriteFile.WriteHandle, AFile]

  val elasticCallOps: ElasticCall.Ops[Eff] = ElasticCall.Ops[Eff]

  val separator = "__"

  def file2ES(afile: AFile): IndexType = {
    val folder = posixCodec.unsafePrintPath(fileParent(afile))
    val typ = fileName(afile).value
    val index = folder.substring(1, folder.length - 1).replace("/", separator)
    IndexType(index, typ)
  }

  def dirPath2Index(dirPath: String): String =
    dirPath.substring(1).replace("/", separator)

  def dir2Index(adir: ADir): String = dirPath2Index(posixCodec.unsafePrintPath(adir))

  def toFile(indexType: IndexType): AFile = {
    val adir: ADir = indexType.index.split(separator).foldLeft(rootDir){
      case (acc, dirName) => acc </> dir(dirName)
    }
    adir </> file(indexType.typ)
  }

  def toLowerLevel[S[_]](sc: SparkContext, config: ElasticConfig)(implicit
    S0: Task :<: S, S1: PhysErr :<: S
  ): Task[Free[Eff, ?] ~> Free[S, ?]] =
    ( TaskRef(0L)                                 |@|
      TaskRef(Map.empty[ResultHandle, SparkCursor])  |@|
      TaskRef(Map.empty[ReadHandle, SparkCursor]) |@|
      TaskRef(Map.empty[WriteHandle, AFile])) {
      (genState, rddStates, readCursors, writeCursors) => {

        val read = Read.constant[Task, SparkContext](sc)

        type Temp1[A] = Coproduct[Task, Read[SparkContext, ?], A]
        type Temp[A] = Coproduct[ElasticCall, Temp1, A]

        val elasticInterpreter = ElasticCall.interpreter(config.host, config.port)

        def temp: Free[Temp, ?] ~> Task =
          foldMapNT(elasticInterpreter :+: injectNT[Task, Task] :+: read)

        val interpreter: Eff ~> S =
          (details.interpreter[Temp] andThen temp andThen injectNT[Task, S]) :+:
        (KeyValueStore.impl.fromTaskRef[WriteHandle, AFile](writeCursors) andThen injectNT[Task, S])  :+:
        (KeyValueStore.impl.fromTaskRef[ReadHandle, SparkCursor](readCursors) andThen injectNT[Task, S]) :+:
        (KeyValueStore.impl.fromTaskRef[ResultHandle, SparkCursor](rddStates) andThen injectNT[Task, S]) :+:
        (MonotonicSeq.fromTaskRef(genState) andThen injectNT[Task, S]) :+:
        (elasticInterpreter andThen injectNT[Task, S]) :+:
        (read andThen injectNT[Task, S]) :+:
        injectNT[Task, S] :+:
        injectNT[PhysErr, S]

        mapSNT[Eff, S](interpreter)
      }
    }

  type Config = ElasticConfig
  def parseConfig(connUri: ConnectionUri): DefErrT[Task, Config] = {

    def liftErr(msg: String): DefinitionError = NonEmptyList(msg).left[EnvironmentError]

    def master(host: String, port: Int): State[SparkConf, Unit] =
      State.modify(_.setMaster(s"spark://$host:$port"))
    def indexAuto: State[SparkConf, Unit] = State.modify(_.set("es.index.auto.create", "true"))
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
        indexAuto                                      *>
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

    for {
      sparkConf                  <- sparkConfOrErr
      elasticHostAndPort         <- fetchParameter("elasticHost").tuple(fetchParameter("elasticPort"))
      (elasticHost, elasticPort) = elasticHostAndPort
    } yield ElasticConfig(sparkConf, elasticHost, elasticPort.toInt)
  }

  def getSparkConf: Config => SparkConf = _.sparkConf

  def generateSC: (APath, Config) => DefErrT[Task, SparkContext] =
    (jar, conf) => initSC(conf).map { sc =>
      sc.addJar(posixCodec.printPath(jar))
      sc
    }

  object details {

    import org.elasticsearch.spark._

    private def parseIndex(adir: ADir) = posixCodec.unsafePrintPath(adir).replace("/", "") // TODO_ES handle invalid paths

    private def fromFile(sc: SparkContext, file: AFile): Task[RDD[Data]] = Task.delay {
      sc
        .esJsonRDD(file2ES(file).shows)
        .map(_._2)
        .map(raw => DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι))
    }

    def rddFrom[S[_]](f: AFile)(implicit
      read: Read.Ops[SparkContext, S],
      E: ElasticCall :<: S,
      S: Task :<: S
    ): Free[S, RDD[Data]] = for {
      sc <- read.ask
      rdd <- lift(fromFile(sc, f)).into[S]
    } yield rdd

    def store[S[_]](rdd: RDD[Data], out: AFile)(implicit
      S: Task :<: S
    ): Free[S, Unit] = lift(Task.delay {
      rdd.flatMap(DataCodec.render(_)(DataCodec.Precise).toList)
        .saveJsonToEs(file2ES(out).shows)
    }).into[S]

    def fileExists[S[_]](f: AFile)(implicit
      E: ElasticCall.Ops[S]
    ): Free[S, Boolean] = E.typeExists(file2ES(f))

    def listContents[S[_]](adir: ADir)(implicit
      E: ElasticCall.Ops[S]
    ): FileSystemErrT[Free[S, ?], Set[PathSegment]] = {
      val toDirName: String => PathSegment = t => DirName(t).left[FileName]
      val toFileName: String => PathSegment = t => FileName(t).right[DirName]
      val rootFolder: String => String = _.split(separator).head

      val segments = if(adir === rootDir)
        E.listIndices.map(_.map(rootFolder).toSet.map(toDirName))
      else {
        val prefix = dir2Index(adir)
        val folders = E.listIndices.map(indices =>
          indices
            .filter(_.startsWith(prefix))
            .map(s => s.substring(s.indexOf(prefix) + prefix.length))
            .map {
              case s if s.contains(separator) => s.substring(0, s.indexOf(separator))
              case s => s
            }
            .toSet
            .map(toDirName))
        val index = if(prefix.endsWith(separator)) prefix.substring(0, prefix.length - separator.length) else prefix
        val files = E.listTypes(index).map(_.map(toFileName).toSet)
        (folders |@| files)(_ ++ _)
      }
      EitherT(segments.map(_.right[FileSystemError]))
    }

    def readChunkSize: Int = 5000

    def interpreter[S[_]](implicit
      read: Read.Ops[SparkContext, S],
      E: ElasticCall :<: S,
      S: Task :<: S
    ): SparkConnectorDetails ~> Free[S, ?] = new (SparkConnectorDetails ~> Free[S, ?]) {
      def apply[A](from: SparkConnectorDetails[A]) = from match {
        case FileExists(f)       => ElasticCall.Ops[S].typeExists(file2ES(f))
        case ReadChunkSize       => 5000.point[Free[S, ?]]
        case StoreData(rdd, out) => lift(Task.delay {
          rdd.flatMap(DataCodec.render(_)(DataCodec.Precise).toList)
            .saveJsonToEs(file2ES(out).shows)
        }).into[S]
        case ListContents(d)     => listContents[S](d).run
        case RDDFrom(f)          => rddFrom(f)
      }
    }
  }

  object ManagedWriteFileModule extends ManagedWriteFileModule {

    import org.elasticsearch.spark._

    def writeCursor(file: AFile): Backend[AFile] =
      file.point[Backend]

    def writeChunk(f: AFile, chunk: Vector[Data]): Configured[Vector[FileSystemError]] = {
      implicit val codec: DataCodec = DataCodec.Precise

      val lines = chunk.map(data => DataCodec.render(data)).toList.map(_.toList).join
      (for {
        sc     <- readScOps.ask
        result <- lift(Task.delay {
          sc.makeRDD(lines).saveJsonToEs(file2ES(f).shows)
        }).into[Eff].as(Vector.empty[FileSystemError])
      } yield result).liftM[ConfiguredT]
    }

    def closeCursor(f: AFile): Configured[Unit] =
      ().point[Configured]
  }

  object ElasticManageFileModule extends SparkCoreManageFileModule {

    def moveFile(sf: AFile, df: AFile): Free[Eff, Unit] = for {
      src                          <- file2ES(sf).point[Free[Eff, ?]]
      dst                          <- file2ES(df).point[Free[Eff, ?]]
      dstIndexExists               <- elasticCallOps.indexExists(dst.index)
      dstTypeExists                <- elasticCallOps.typeExists(dst)
      _                            <- if(dstTypeExists) elasticCallOps.deleteType(dst)
      else if(!dstIndexExists) elasticCallOps.createIndex(dst.index)
      else ().point[Free[Eff, ?]]
      _                            <- elasticCallOps.copyType(src, dst)
      _                            <- elasticCallOps.deleteType(src)
    } yield ()

    def moveDir(sd: ADir, dd: ADir): Free[Eff, Unit] = {

      def calculateDestinationIndex(index: String): String = {
        val destinationPath = posixCodec.unsafePrintPath(dd) ++ index.diff(posixCodec.unsafePrintPath(sd))
        dirPath2Index(destinationPath)
      }

      for {
        src      <- dir2Index(sd).point[Free[Eff, ?]]
        toMove   <- elasticCallOps.listIndices.map(_.filter(i => i.startsWith(src)))
        _        <- toMove.map(i => elasticCallOps.copyIndex(i, calculateDestinationIndex(i))).sequence
        _        <- toMove.map(elasticCallOps.deleteIndex(_)).sequence
      } yield ()
    }

    def doesPathExist: APath => Free[Eff, Boolean] = (path: APath) => refineType(path).fold(
      d => elasticCallOps.listIndices.map(_.contains(dir2Index(d))),
      f => elasticCallOps.typeExists(file2ES(f))
    )

    def delete(path: APath): Backend[Unit] = refineType(path).fold(d => deleteDir(d),f => deleteFile(f)).liftB.unattempt

    private def deleteFile(file: AFile): Free[Eff, FileSystemError \/ Unit] = for {
      indexType <- file2ES(file).point[Free[Eff, ?]]
      exists <- elasticCallOps.typeExists(indexType)
      res <- if(exists) elasticCallOps.deleteType(indexType).map(_.right) else pathErr(pathNotFound(file)).left[Unit].point[Free[Eff, ?]]
    } yield res

    private def deleteDir(dir: ADir): Free[Eff, FileSystemError \/ Unit] = for {
      indices <- elasticCallOps.listIndices
      index = dir2Index(dir)
      result <- if(indices.isEmpty) pathErr(pathNotFound(dir)).left[Unit].point[Free[Eff, ?]] else {
        indices.filter(_.startsWith(index)).map(elasticCallOps.deleteIndex(_)).sequence.as(().right[FileSystemError])
      }
    } yield result

  }

  def ManageFileModule: ManageFileModule = ElasticManageFileModule
}
