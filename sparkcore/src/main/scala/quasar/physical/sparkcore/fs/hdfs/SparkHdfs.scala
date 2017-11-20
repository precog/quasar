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

package quasar.physical.sparkcore.fs.hdfs

import slamdata.Predef._
import quasar.{Data, DataCodec}
import quasar.contrib.pathy._
import quasar.contrib.scalaz._, readerT._
import quasar.connector.{ChrootedInterpreter, EnvironmentError}
import quasar.effect._
import quasar.fp, fp.ski._, fp.TaskRef,  fp.free._
import quasar.fs._,
  mount._,
  FileSystemError._, PathError._, WriteFile._,
  BackendDef.{DefinitionError, DefErrT},
  QueryFile.ResultHandle, ReadFile.ReadHandle, WriteFile.WriteHandle
import quasar.physical.sparkcore.fs._, SparkConnectorDetails._
import quasar.physical.sparkcore.fs.hdfs.parquet.ParquetRDD

import java.io.BufferedWriter
import java.net.{URLDecoder, URI}

import org.http4s.{ParseFailure, Uri}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem => HdfsFileSystem, Path}
import org.apache.hadoop.util.Progressable;
import org.apache.spark._
import org.apache.spark.rdd._
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

final case class HdfsWriteCursor(hdfs: HdfsFileSystem, bw: BufferedWriter)

final case class S3Info(accessKey: String, securityKey: String, region: String)

final case class HdfsConfig(sparkConf: SparkConf, hdfsUriStr: String, prefix: ADir, s3: Option[S3Info])

object SparkHdfs extends SparkCore with ChrootedInterpreter {

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import EitherT.eitherTMonad

  def rootPrefix(cfg: HdfsConfig): ADir = cfg.prefix

  val Type = FileSystemType("spark-hdfs")

  type Eff1[A]  = Coproduct[KeyValueStore[ResultHandle, SparkCursor, ?], Read[SparkContext, ?], A]
  type Eff2[A]  = Coproduct[KeyValueStore[ReadHandle, SparkCursor, ?], Eff1, A]
  type Eff3[A]  = Coproduct[KeyValueStore[WriteHandle, HdfsWriteCursor, ?], Eff2, A]
  type Eff4[A]  = Coproduct[Task, Eff3, A]
  type Eff5[A]  = Coproduct[PhysErr, Eff4, A]
  type Eff6[A]  = Coproduct[MonotonicSeq, Eff5, A]
  type Eff7[A]  = Coproduct[SparkConnectorDetails, Eff6, A]
  type Eff[A]   = Coproduct[Read[HdfsFileSystem, ?], Eff7, A]

  def ReadSparkContextInj = Inject[Read[SparkContext, ?], Eff]
  def RFKeyValueStoreInj = Inject[KeyValueStore[ReadFile.ReadHandle, SparkCursor, ?], Eff]
  def MonotonicSeqInj = Inject[MonotonicSeq, Eff]
  def TaskInj = Inject[Task, Eff]
  def SparkConnectorDetailsInj = Inject[SparkConnectorDetails, Eff]
  def QFKeyValueStoreInj = Inject[KeyValueStore[QueryFile.ResultHandle, SparkCursor, ?], Eff]

  def writersOps = KeyValueStore.Ops[WriteHandle, HdfsWriteCursor, Eff]
  def sequenceOps = MonotonicSeq.Ops[Eff]
  def hdfsFSOps = Read.Ops[HdfsFileSystem, Eff]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def generateHdfsFS(sfsConf: HdfsConfig): Task[HdfsFileSystem] =
    for {
      _ <- Task.delay(java.lang.Thread.currentThread.setContextClassLoader(getClass.getClassLoader))
      fs <- Task.delay {
        val configuration = sfsConf.s3.map { info =>
          val conf = new Configuration()
          conf.set("fs.s3a.access.key", info.accessKey)
          conf.set("fs.s3a.secret.key", info.securityKey)
          conf.set("fs.s3a.endpoint",   info.region)
          conf
        }.getOrElse(new Configuration())
        configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
        HdfsFileSystem.get(new URI(sfsConf.hdfsUriStr), configuration)
      }
      uriStr = fs.getUri().toASCIIString()
      _ <- if(uriStr.startsWith("file:///")) Task.fail(new RuntimeException("Provided URL is not valid HDFS URL")) else ().point[Task]
    } yield fs

  def hdfsPathStr(sfsConf: HdfsConfig): AFile => ReaderT[Task, SparkContext, String] = (afile: AFile) => ReaderT(κ(Task.delay {
    sfsConf.hdfsUriStr + posixCodec.unsafePrintPath(afile)
  }))

  def toLowerLevel[S[_]](sc: SparkContext, config: HdfsConfig)(implicit
    S0: Task :<: S, S1: PhysErr :<: S
  ): Task[Free[Eff, ?] ~> Free[S, ?]] =
    (TaskRef(0L) |@|
      TaskRef(Map.empty[ResultHandle, SparkCursor]) |@|
      TaskRef(Map.empty[ReadHandle, SparkCursor]) |@|
      TaskRef(Map.empty[WriteHandle, HdfsWriteCursor]) |@|
      generateHdfsFS(config)
    ) {
      (genState, rddStates, sparkCursors, writeCursors, hdfsFS) =>

      val detailsInterpreter =
        details.interpreter[ReaderT[Task, SparkContext, ?]](readChunkSize = 5000, ReaderT(κ(hdfsFS.point[Task])), hdfsPathStr(config))

      val interpreter: Eff ~> S =
        (Read.constant[Task, HdfsFileSystem](hdfsFS) andThen injectNT[Task, S]) :+:
        (detailsInterpreter andThen  runReaderNT(sc) andThen injectNT[Task, S]) :+:
      (MonotonicSeq.fromTaskRef(genState) andThen injectNT[Task, S]) :+:
      injectNT[PhysErr, S] :+:
      injectNT[Task, S]  :+:
      (KeyValueStore.impl.fromTaskRef[WriteHandle, HdfsWriteCursor](writeCursors) andThen injectNT[Task, S])  :+:
      (KeyValueStore.impl.fromTaskRef[ReadHandle, SparkCursor](sparkCursors) andThen injectNT[Task, S]) :+:
      (KeyValueStore.impl.fromTaskRef[ResultHandle, SparkCursor](rddStates) andThen injectNT[Task, S]) :+:
      (Read.constant[Task, SparkContext](sc) andThen injectNT[Task, S])

      mapSNT[Eff, S](interpreter)
    }

  type Config = HdfsConfig
  def parseConfig(connUri: ConnectionUri): DefErrT[Task, HdfsConfig] = {

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
      (master(host.value, port) *> appName *>
        config("spark.executor.memory", uri) *>
        config("spark.executor.cores", uri) *>
        config("spark.executor.extraJavaOptions", uri) *>
        config("spark.default.parallelism", uri) *>
        config("spark.files.maxPartitionBytes", uri) *>
        config("spark.driver.cores", uri) *>
        config("spark.driver.maxResultSize", uri) *>
        config("spark.driver.memory", uri) *>
        config("spark.local.dir", uri) *>
        config("spark.reducer.maxSizeInFlight", uri) *>
        config("spark.reducer.maxReqsInFlight", uri) *>
        config("spark.shuffle.file.buffer", uri) *>
        config("spark.shuffle.io.retryWait", uri) *>
        config("spark.memory.fraction", uri) *>
        config("spark.memory.storageFraction", uri) *>
        config("spark.cores.max", uri) *>
        config("spark.speculation", uri) *>
        config("spark.task.cpus", uri)
      ).exec(new SparkConf())
    }

    val hdfsUrlOrErr: DefErrT[Task, String] = uriOrErr.flatMap(uri =>
      EitherT(uri.params.get("hdfsUrl").map(url => URLDecoder.decode(url, "UTF-8")).fold(liftErr("'hdfsUrl' parameter not provided").left[String])(_.right[DefinitionError]).point[Task])
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

    val s3InfoOrErr: DefErrT[Task, Option[S3Info]] =
      uriOrErr.map(uri => (uri.params.get("s3AccessKey") |@| uri.params.get("s3SecretKey") |@| uri.params.get("s3Endpoint")) (S3Info(_, _, _)))

    (sparkConfOrErr |@| hdfsUrlOrErr |@| rootPathOrErr |@| s3InfoOrErr) (HdfsConfig(_, _, _, _))
  }

  def getSparkConf: Config => SparkConf = _.sparkConf

  def generateSC: (APath, HdfsConfig) => DefErrT[Task, SparkContext] =
    (jar, conf) => initSC(conf).map { sc =>
      sc.addJar(posixCodec.printPath(jar))
      conf.s3.map { info =>
        sc.hadoopConfiguration.set("fs.s3a.access.key", info.accessKey)
        sc.hadoopConfiguration.set("fs.s3a.secret.key", info.securityKey)
        sc.hadoopConfiguration.set("fs.s3a.endpoint",   info.region)
        sc
      }.getOrElse(sc)
    }

  private def toPath(apath: APath): Free[Eff, Path] = lift(Task.delay {
    new Path(posixCodec.unsafePrintPath(apath))
  }).into[Eff]

  class details[F[_]:Capture:Bind](fileSystem: F[HdfsFileSystem]) {

    private def toPath(apath: APath): F[Path] = Capture[F].capture {
      new Path(posixCodec.unsafePrintPath(apath))
    }

    private def fetchRdd(sc: SparkContext, pathStr: String): F[RDD[Data]] = Capture[F].capture {
      import ParquetRDD._
      // TODO add magic number support to distinguish
      if(pathStr.endsWith(".parquet"))
        sc.parquet(pathStr)
      else
        sc.textFile(pathStr)
          .map(raw => DataCodec.parse(raw)(DataCodec.Precise) | Data.NA)
    }

    def rddFrom(f: AFile)(hdfsPathStr: AFile => F[String])(implicit
      reader: MonadReader[F, SparkContext]
    ): F[RDD[Data]] = for {
      pathStr <- hdfsPathStr(f)
      sc <- reader.ask
      rdd <- fetchRdd(sc, pathStr)
    } yield rdd

    def store(rdd: RDD[Data], out: AFile): F[Unit] = for {
      path <- toPath(out)
      hdfs <- fileSystem
    } yield {
      val os: OutputStream = hdfs.create(path, new Progressable() {
        override def progress(): Unit = {}
      })
      val bw = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"))

      rdd.flatMap(DataCodec.render(_)(DataCodec.Precise).toList).collect().foreach(v => {
        bw.write(v)
        bw.newLine()
      })
      bw.close()
    }

    def fileExists(f: AFile): F[Boolean] = for {
      path <- toPath(f)
      hdfs <- fileSystem
    } yield hdfs.exists(path)

    def listContents(d: ADir): FileSystemErrT[F, Set[PathSegment]] = EitherT(for {
      path <- toPath(d)
      hdfs <- fileSystem
    } yield if(hdfs.exists(path)) {
        hdfs.listStatus(path).toSet.map {
          case file if file.isFile() => FileName(file.getPath().getName()).right[DirName]
          case directory => DirName(directory.getPath().getName()).left[FileName]
        }.right[FileSystemError]
      } else pathErr(pathNotFound(d)).left[Set[PathSegment]]
    )}

  object details {

    def interpreter[F[_]:Capture](
      readChunkSize: Int,
      fileSystem: F[HdfsFileSystem],
      hdfsPathStr: AFile => F[String]
    )(implicit
      reader: MonadReader[F, SparkContext]
    ): SparkConnectorDetails ~> F =
      new (SparkConnectorDetails ~> F) {
        val qf = new details[F](fileSystem)

        def apply[A](from: SparkConnectorDetails[A]) = from match {
          case FileExists(f)       => qf.fileExists(f)
          case ReadChunkSize       => readChunkSize.point[F]
          case StoreData(rdd, out) => qf.store(rdd, out)
          case ListContents(d)     => qf.listContents(d).run
          case RDDFrom(f)          => qf.rddFrom(f)(hdfsPathStr)
        }
      }
  }

  object HdfsWriteFileModule extends WriteFileModule {
    import WriteFile._

    def open(file: AFile): Backend[WriteHandle] = {
      def createCursor: Free[Eff, HdfsWriteCursor] = for {
        path <- toPath(file)
        hdfs <- hdfsFSOps.ask
      } yield {
        val os: OutputStream = hdfs.create(path, new Progressable() {
          override def progress(): Unit = {}
        })
        val bw = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) )
        HdfsWriteCursor(hdfs, bw)
      }

      (for {
        hwc <- createCursor
        id <- sequenceOps.next
        h = WriteHandle(file, id)
        _ <- writersOps.put(h, hwc)
      } yield h).liftB
    }

    def write(h: WriteHandle, chunk: Vector[Data]): Configured[Vector[FileSystemError]] = {

      implicit val codec: DataCodec = DataCodec.Precise

      def _write(bw: BufferedWriter): Free[Eff, Vector[FileSystemError]] = {

        val lines: Vector[(String, Data)] =
          chunk.map(data => DataCodec.render(data) strengthR data).unite

        lift(Task.delay(lines.flatMap {
          case (line, data) =>
            \/.fromTryCatchNonFatal{
              bw.write(line)
              bw.newLine()
            }.fold(
              ex => Vector(writeFailed(data, ex.getMessage)),
              u => Vector.empty[FileSystemError]
            )
        })).into[Eff]
      }

      val findAndWrite: OptionT[Free[Eff, ?], Vector[FileSystemError]] = for {
        HdfsWriteCursor(_, bw) <- writersOps.get(h)
        errors                 <- _write(bw).liftM[OptionT]
      } yield errors

      findAndWrite.fold (
        errs => errs,
        Vector[FileSystemError](unknownWriteHandle(h))
      ).liftM[ConfiguredT]
    }

    def close(h: WriteHandle): Configured[Unit] = (for {
      HdfsWriteCursor(hdfs, br) <- writersOps.get(h)
      _                         <- writersOps.delete(h).liftM[OptionT]
    } yield {
      br.close()
    }).run.void.liftM[ConfiguredT]
  }

  def WriteFileModule = HdfsWriteFileModule

  object HdfsManageFileModule extends SparkCoreManageFileModule {

    def moveFile(src: AFile, dst: AFile): Free[Eff, Unit] = {
      val move: Free[Eff, PhysicalError \/ Unit] = (for {
        hdfs <- hdfsFSOps.ask
        srcPath <- toPath(src)
        dstPath <- toPath(dst)
        dstParent <- toPath(fileParent(dst))
      } yield {
        val deleted = hdfs.delete(dstPath, true)
        val _ = hdfs.mkdirs(dstParent)
        hdfs.rename(srcPath, dstPath)
      }).as(().right[PhysicalError])
      Failure.Ops[PhysicalError, Eff].unattempt(move)
    }

    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    def moveDir(src: ADir, dst: ADir): Free[Eff, Unit] = {
      val move: Free[Eff, PhysicalError \/ Unit] = (for {
        hdfs <- hdfsFSOps.ask
        srcPath <- toPath(src)
        dstPath <- toPath(dst)
      } yield {
        hdfs.delete(dstPath, true)
        hdfs.mkdirs(dstPath.getParent)
        hdfs.rename(srcPath, dstPath)
      }).as(().right[PhysicalError])

      Failure.Ops[PhysicalError, Eff].unattempt(move)
    }

    def doesPathExist: APath => Free[Eff, Boolean] = (ap) => for {
      path <- toPath(ap)
      hdfs <- hdfsFSOps.ask
    } yield hdfs.exists(path)

    def delete(p: APath): Backend[Unit] = {

      val delete: Free[Eff, FileSystemError \/ Unit] = for {
        path <- toPath(p)
        hdfs <- hdfsFSOps.ask
      } yield (if (hdfs.exists(path))
        hdfs.delete(path, true).right[FileSystemError].void
      else
        pathErr(pathNotFound(p)).left[Unit])

      delete.liftB.unattempt
    }
  }

  def ManageFileModule: ManageFileModule = HdfsManageFileModule
}
