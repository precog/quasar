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

package quasar.physical.sparkcore.fs

import slamdata.Predef._
import quasar.connector.EnvironmentError
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fp.TaskRef
import quasar.fp.free._
import quasar.fs._, QueryFile.ResultHandle, ReadFile.ReadHandle, WriteFile.WriteHandle
import quasar.fs.mount._, BackendDef._
import quasar.physical.sparkcore.fs.{queryfile => corequeryfile, readfile => corereadfile, genSc => coreGenSc}
import quasar.physical.sparkcore.fs.hdfs.writefile.HdfsWriteCursor


import java.net.{URLDecoder, URI}

import org.http4s.{ParseFailure, Uri}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem => HdfsFileSystem}
import org.apache.spark._
import pathy.Path._
import pathy.Path.posixCodec
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

package object hdfs {

  import corequeryfile.RddState

  val FsType = FileSystemType("spark-hdfs")

  type EffM1[A] = Coproduct[KeyValueStore[ResultHandle, RddState, ?], Read[SparkContext, ?], A]
  type Eff0[A] = Coproduct[KeyValueStore[ReadHandle, SparkCursor, ?], EffM1, A]
  type Eff1[A] = Coproduct[KeyValueStore[WriteHandle, HdfsWriteCursor, ?], Eff0, A]
  type Eff2[A] = Coproduct[Task, Eff1, A]
  type Eff3[A] = Coproduct[PhysErr, Eff2, A]
  type Eff[A]  = Coproduct[MonotonicSeq, Eff3, A]

  final case class SparkFSConf(sparkConf: SparkConf, hdfsUriStr: String, prefix: ADir)

  val parseUri: ConnectionUri => DefinitionError \/ (SparkConf, SparkFSConf) = (connUri: ConnectionUri) => {

    def liftErr(msg: String): DefinitionError = NonEmptyList(msg).left[EnvironmentError]

    def master(host: String, port: Int): State[SparkConf, Unit] =
      State.modify(_.setMaster(s"spark://$host:$port"))

    def appName: State[SparkConf, Unit] = State.modify(_.setAppName("quasar"))

    def config(name: String, uri: Uri): State[SparkConf, Unit] =
      State.modify(c => uri.params.get(name).fold(c)(c.set(name, _)))

    val uriOrErr: DefinitionError \/ Uri = Uri.fromString(connUri.value).leftMap((pf: ParseFailure) => liftErr(pf.toString))

    val sparkConfOrErr: DefinitionError \/ SparkConf = for {
      uri <- uriOrErr
      host <- uri.host.fold(NonEmptyList("host not provided").left[EnvironmentError].left[Uri.Host])(_.right[DefinitionError])
      port <- uri.port.fold(NonEmptyList("port not provided").left[EnvironmentError].left[Int])(_.right[DefinitionError])
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

    val hdfsUrlOrErr: DefinitionError \/ String = uriOrErr.flatMap(uri =>
      uri.params.get("hdfsUrl").map(url => URLDecoder.decode(url, "UTF-8")).fold(liftErr("'hdfsUrl' parameter not provided").left[String])(_.right[DefinitionError])
    )

    val rootPathOrErr: DefinitionError \/ ADir =
      uriOrErr
        .flatMap(uri =>
          uri.params.get("rootPath").fold(liftErr("'rootPath' parameter not provided").left[String])(_.right[DefinitionError])
        )
        .flatMap(pathStr =>
          posixCodec.parseAbsDir(pathStr)
            .map(unsafeSandboxAbs)
            .fold(liftErr("'rootPath' is not a valid path").left[ADir])(_.right[DefinitionError])
        )

    for {
      sparkConf <- sparkConfOrErr
      hdfsUrl <- hdfsUrlOrErr
      rootPath <- rootPathOrErr
    } yield (sparkConf, SparkFSConf(sparkConf, hdfsUrl, rootPath))
  }


  private def sparkCoreJar: EitherT[Task, String, APath] = {
    /* Points to quasar-web.jar or target/classes if run from sbt repl/run */
    val fetchProjectRootPath = Task.delay {
      val pathStr = URLDecoder.decode(this.getClass().getProtectionDomain.getCodeSource.getLocation.toURI.getPath, "UTF-8")
      posixCodec.parsePath[Option[APath]](_ => None, Some(_).map(unsafeSandboxAbs), _ => None, Some(_).map(unsafeSandboxAbs))(pathStr)
    }
    val jar: Task[Option[APath]] =
      fetchProjectRootPath.map(_.flatMap(s => parentDir(s).map(_ </> file("sparkcore.jar"))))
    OptionT(jar).toRight("Could not fetch sparkcore.jar")
  }

  def sparkFsDef[S[_]](implicit
    S0: Task :<: S,
    S1: PhysErr :<: S,
    FailOps: Failure.Ops[PhysicalError, S]
  ): SparkConf => Free[S, SparkFSDef[Eff, S]] = (sparkConf: SparkConf) => {

    val genScWithJar: Free[S, String \/ SparkContext] = lift((for {
      sc <- coreGenSc(sparkConf)
      jar <- sparkCoreJar
    } yield {
      sc.addJar(posixCodec.printPath(jar))
      sc
    }).run).into[S]

    val definition: SparkContext => Free[S, SparkFSDef[Eff, S]] = (sc: SparkContext) => lift( (TaskRef(0L) |@|
      TaskRef(Map.empty[ResultHandle, RddState]) |@|
      TaskRef(Map.empty[ReadHandle, SparkCursor]) |@|
      TaskRef(Map.empty[WriteHandle, HdfsWriteCursor])
      ) {
      // TODO better names!
      (genState, rddStates, sparkCursors, writeCursors) =>
      val interpreter: Eff ~> S = (MonotonicSeq.fromTaskRef(genState) andThen injectNT[Task, S]) :+:
      injectNT[PhysErr, S] :+:
      injectNT[Task, S]  :+:
      (KeyValueStore.impl.fromTaskRef[WriteHandle, HdfsWriteCursor](writeCursors) andThen injectNT[Task, S])  :+:
      (KeyValueStore.impl.fromTaskRef[ReadHandle, SparkCursor](sparkCursors) andThen injectNT[Task, S]) :+:
      (KeyValueStore.impl.fromTaskRef[ResultHandle, RddState](rddStates) andThen injectNT[Task, S]) :+:
      (Read.constant[Task, SparkContext](sc) andThen injectNT[Task, S])

      SparkFSDef(mapSNT[Eff, S](interpreter), lift(Task.delay(sc.stop())).into[S])
    }).into[S]

    genScWithJar >>= (_.fold(
      msg => FailOps.fail[SparkFSDef[Eff, S]](UnhandledFSError(new RuntimeException(msg))),
      definition(_)
    ))
  }

  def generateHdfsFS(sfsConf: SparkFSConf): Task[HdfsFileSystem] = for {
    fs <- Task.delay {
      val conf = new Configuration()
      conf.setBoolean("fs.hdfs.impl.disable.cache", true)
      HdfsFileSystem.get(new URI(sfsConf.hdfsUriStr), conf)
    }
    uriStr = fs.getUri().toASCIIString()
    _ <- if(uriStr.startsWith("file:///")) Task.fail(new RuntimeException("Provided URL is not valid HDFS URL")) else ().point[Task]
  } yield fs

  val fsInterpret: SparkFSConf => (FileSystem ~> Free[Eff, ?]) = (sparkFsConf: SparkFSConf) => {

    def hdfsPathStr: AFile => Task[String] = (afile: AFile) => Task.delay {
      sparkFsConf.hdfsUriStr + posixCodec.unsafePrintPath(afile)
    }

    val fileSystem = generateHdfsFS(sparkFsConf)

    interpretFileSystem(
      corequeryfile.chrooted[Eff](queryfile.input(fileSystem), FsType, sparkFsConf.prefix),
      corereadfile.chrooted(readfile.input[Eff](hdfsPathStr, fileSystem), sparkFsConf.prefix),
      writefile.chrooted[Eff](sparkFsConf.prefix, fileSystem),
      managefile.chrooted[Eff](sparkFsConf.prefix, fileSystem))
  }

  def definition[S[_]](implicit
    S0: Task :<: S, S1: PhysErr :<: S
  ) =
    quasar.physical.sparkcore.fs.definition[Eff, S, SparkFSConf](FsType, parseUri, sparkFsDef, fsInterpret)
}
