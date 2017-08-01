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
import quasar.fp.free._
import quasar.fp.TaskRef
import quasar.fs._, QueryFile.ResultHandle, ReadFile.ReadHandle, WriteFile.WriteHandle
import quasar.fs.mount._, BackendDef._
import quasar.physical.sparkcore.fs.{readfile => corereadfile, queryfile => corequeryfile, genSc => coreGenSc}

import java.net.URLDecoder

import org.apache.spark._
import org.http4s.{ParseFailure, Uri}
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task


package object elastic {

  import corequeryfile.RddState

  val FsType = FileSystemType("spark-elastic")

  type Eff0[A] = Coproduct[Task, PhysErr, A]
  type Eff1[A] = Coproduct[Read[SparkContext, ?], Eff0, A]
  type Eff2[A] = Coproduct[ElasticCall, Eff1, A]
  type Eff3[A] = Coproduct[MonotonicSeq, Eff2, A]
  type Eff4[A]  = Coproduct[KeyValueStore[ResultHandle, RddState, ?], Eff3, A]
  type Eff5[A]  = Coproduct[KeyValueStore[ReadHandle, SparkCursor, ?], Eff4, A]
  type Eff[A]  = Coproduct[KeyValueStore[WriteHandle, writefile.WriteCursor, ?], Eff5, A]

  final case class SparkFSConf(sparkConf: SparkConf)

  val parseUri: ConnectionUri => DefinitionError \/ (SparkConf, SparkFSConf) = (connUri: ConnectionUri) => {

    def liftErr(msg: String): DefinitionError = NonEmptyList(msg).left[EnvironmentError]

    def master(host: String, port: Int): State[SparkConf, Unit] =
      State.modify(_.setMaster(s"spark://$host:$port"))

    def appName: State[SparkConf, Unit] = State.modify(_.setAppName("quasar"))

    def indexAuto: State[SparkConf, Unit] = State.modify(_.set("es.index.auto.create", "true"))

    val uriOrErr: DefinitionError \/ Uri = Uri.fromString(connUri.value).leftMap((pf: ParseFailure) => liftErr(pf.toString))

    val sparkConfOrErr: DefinitionError \/ SparkConf = for {
      uri <- uriOrErr
      host <- uri.host.fold(NonEmptyList("host not provided").left[EnvironmentError].left[Uri.Host])(_.right[DefinitionError])
      port <- uri.port.fold(NonEmptyList("port not provided").left[EnvironmentError].left[Int])(_.right[DefinitionError])
    } yield {
      // TODO_ES missng ES nodes and port
      (master(host.value, port) *> appName *> indexAuto).exec(new SparkConf())
    }

    sparkConfOrErr.map(sparkConf => (sparkConf, SparkFSConf(sparkConf)))
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

  private def sparkFsDef[S[_]](implicit
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

    val definition: SparkContext => Free[S, SparkFSDef[Eff, S]] =
      (sc: SparkContext) => lift((TaskRef(0L) |@| TaskRef(Map.empty[ResultHandle, RddState]) |@| TaskRef(Map.empty[ReadHandle, SparkCursor]) |@| TaskRef(Map.empty[WriteHandle, writefile.WriteCursor])) {
        (genState, rddStates, readCursors, writeCursors) => {
          val interpreter: Eff ~> S =
            (KeyValueStore.impl.fromTaskRef[WriteHandle, writefile.WriteCursor](writeCursors) andThen injectNT[Task, S])  :+:
            (KeyValueStore.impl.fromTaskRef[ReadHandle, SparkCursor](readCursors) andThen injectNT[Task, S]) :+:
            (KeyValueStore.impl.fromTaskRef[ResultHandle, RddState](rddStates) andThen injectNT[Task, S]) :+:
          (MonotonicSeq.fromTaskRef(genState) andThen injectNT[Task, S]) :+:
          (ElasticCall.interpreter andThen injectNT[Task, S]) :+:
          (Read.constant[Task, SparkContext](sc) andThen injectNT[Task, S]) :+:
          injectNT[Task, S] :+:
          injectNT[PhysErr, S]

          SparkFSDef(mapSNT[Eff, S](interpreter), lift(Task.delay(sc.stop())).into[S])
        }
      }).into[S]

    genScWithJar >>= (_.fold(
      msg => FailOps.fail[SparkFSDef[Eff, S]](UnhandledFSError(new RuntimeException(msg))),
      definition(_)
    ))
  }

  private val fsInterpret: SparkFSConf => (FileSystem ~> Free[Eff, ?]) = (sparkFsConf: SparkFSConf) => {

    type FreeEff[A]  = Free[Eff, A]
    interpretFileSystem(
      corequeryfile.interpreter[Eff](queryfile.input[Eff], FsType),
      corereadfile.interpret[Eff](readfile.input[Eff]),
      writefile.interpreter[Eff],
      managefile.interpreter[Eff])
  }

  def definition[S[_]](implicit
    S0: Task :<: S, S1: PhysErr :<: S
  ) =
    quasar.physical.sparkcore.fs.definition[Eff, S, SparkFSConf](FsType, parseUri, sparkFsDef, fsInterpret)

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
}
