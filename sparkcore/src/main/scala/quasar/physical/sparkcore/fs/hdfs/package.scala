/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.connector.EnvironmentError
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fp.TaskRef
import quasar.fp.free._
import quasar.fs._, QueryFile.ResultHandle, ReadFile.ReadHandle, WriteFile.WriteHandle
import quasar.fs.mount._, FileSystemDef._
import quasar.physical.sparkcore.fs.{queryfile => corequeryfile, readfile => corereadfile}
import quasar.physical.sparkcore.fs.hdfs.writefile.HdfsWriteCursor

import java.net.URI
import scala.sys

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem => HdfsFileSystem}
import org.apache.spark._
import pathy.Path._
import scalaz._, Scalaz._
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

  val parseUri: ConnectionUri => DefinitionError \/ (SparkConf, SparkFSConf) = (uri: ConnectionUri) => {

    def error(msg: String): DefinitionError \/ (SparkConf, SparkFSConf) =
      NonEmptyList(msg).left[EnvironmentError].left[(SparkConf, SparkFSConf)]

    def forge(master: String, hdfsUriStr: String, rootPath: String): DefinitionError \/ (SparkConf, SparkFSConf) =
      posixCodec.parseAbsDir(rootPath)
        .map { prefix => {
          val conf = SparkFSConf(new SparkConf().setMaster(master).setAppName("quasar"), hdfsUriStr, sandboxAbs(prefix))
          (conf.sparkConf, conf)
        }
      }.fold(error(s"Could not extract a path from $rootPath"))(_.right[DefinitionError])

    uri.value.split('|').toList match {
      case master :: hdfsUriStr :: prefixPath :: Nil => forge(master, hdfsUriStr, prefixPath)
      case _ =>
        error("Missing master and prefixPath (seperated by |)" +
          " e.g spark://spark_host:port|hdfs://hdfs_host:port|/user/")
    }
  }

  private def fetchSparkCoreJar: Task[String] = Task.delay {
    sys.env("QUASAR_HOME") + "/sparkcore.jar"
  }

  def sparkFsDef[S[_]](implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): SparkConf => Free[S, SparkFSDef[Eff, S]] = (sparkConf: SparkConf) => {

    val genSc = fetchSparkCoreJar.map { jar => 
      val sc = new SparkContext(sparkConf)
      sc.addJar(jar)
      sc
    }

    lift((TaskRef(0L) |@|
      TaskRef(Map.empty[ResultHandle, RddState]) |@|
      TaskRef(Map.empty[ReadHandle, SparkCursor]) |@|
      TaskRef(Map.empty[WriteHandle, HdfsWriteCursor]) |@|
      genSc) {
      // TODO better names!
      (genState, rddStates, sparkCursors, writeCursors, sc) =>
      val interpreter: Eff ~> S = (MonotonicSeq.fromTaskRef(genState) andThen injectNT[Task, S]) :+:
      injectNT[PhysErr, S] :+:
      injectNT[Task, S]  :+:
      (KeyValueStore.impl.fromTaskRef[WriteHandle, HdfsWriteCursor](writeCursors) andThen injectNT[Task, S])  :+:
      (KeyValueStore.impl.fromTaskRef[ReadHandle, SparkCursor](sparkCursors) andThen injectNT[Task, S]) :+:
      (KeyValueStore.impl.fromTaskRef[ResultHandle, RddState](rddStates) andThen injectNT[Task, S]) :+:
      (Read.constant[Task, SparkContext](sc) andThen injectNT[Task, S])

      SparkFSDef(mapSNT[Eff, S](interpreter), lift(Task.delay(sc.stop())).into[S])
    }).into[S]
  }

  val fsInterpret: SparkFSConf => (FileSystem ~> Free[Eff, ?]) = (sparkFsConf: SparkFSConf) => {

    def hdfsPathStr: AFile => Task[String] = (afile: AFile) => Task.delay {
      sparkFsConf.hdfsUriStr + posixCodec.unsafePrintPath(afile)
    }

    def fileSystem: Task[HdfsFileSystem] = Task.delay {
      val conf = new Configuration()
      conf.setBoolean("fs.hdfs.impl.disable.cache", true)
      HdfsFileSystem.get(new URI(sparkFsConf.hdfsUriStr), conf)
    }

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
