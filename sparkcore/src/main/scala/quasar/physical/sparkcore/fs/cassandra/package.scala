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
import quasar.contrib.pathy._
import quasar.connector.EnvironmentError
import quasar.effect._
import quasar.fp.free._
import quasar.fp.TaskRef
import quasar.fs._, QueryFile.ResultHandle, ReadFile.ReadHandle, WriteFile.WriteHandle
import quasar.fs.mount._, BackendDef._
import quasar.physical.sparkcore.fs.{queryfile => corequeryfile, readfile => corereadfile}

import scala.sys

import org.apache.spark._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

package object cassandra {

  import corequeryfile.RddState

  val FsType = FileSystemType("spark-cassandra")

  type EffM1[A] = Coproduct[KeyValueStore[ResultHandle, RddState, ?], Read[SparkContext, ?], A]
  type Eff0[A] = Coproduct[KeyValueStore[ReadHandle, SparkCursor, ?], EffM1, A]
  type Eff1[A] = Coproduct[KeyValueStore[WriteHandle, AFile, ?], Eff0, A]
  type Eff2[A] = Coproduct[Task, Eff1, A]
  type Eff3[A] = Coproduct[PhysErr, Eff2, A]
  type Eff4[A] = Coproduct[CassandraDDL, Eff3, A]
  type Eff[A]  = Coproduct[MonotonicSeq, Eff4, A]

  final case class SparkFSConf(sparkConf: SparkConf, prefix: ADir)

  def parseUri: ConnectionUri => DefinitionError \/ (SparkConf, SparkFSConf) = (uri: ConnectionUri) => {

    def error(msg: String): DefinitionError \/ (SparkConf, SparkFSConf) =
      NonEmptyList(msg).left[EnvironmentError].left[(SparkConf, SparkFSConf)]

    def forge(
      master: String,
      cassandraHost: String,
      rootPath: String
    ): DefinitionError \/ (SparkConf, SparkFSConf) =
      posixCodec.parseAbsDir(rootPath)
        .map { prefix =>
          val sparkConf = new SparkConf().setMaster(master).setAppName("quasar").set("spark.cassandra.connection.host", cassandraHost)
          (sparkConf, SparkFSConf(sparkConf, unsafeSandboxAbs(prefix)))
      }.fold(error(s"Could not extrat a path from $rootPath"))(_.right[DefinitionError])

    uri.value.split('|').toList match {
      case master :: cassandraHost :: prefixPath :: Nil => forge(master, cassandraHost, prefixPath)
      case _ =>
        error("Missing master and prefixPath (seperated by |)" +
          " e.g spark://spark_host:port|cassandra://cassandra_host:port|/user/")
    }
  }

  private def fetchSparkCoreJar: Task[String] = Task.delay {
    sys.env("QUASAR_HOME") + "/sparkcore.jar"
  }

  private def sparkFsDef[S[_]](implicit
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
      TaskRef(Map.empty[WriteHandle, AFile]) |@|
      genSc) {
      // TODO better names!
      (genState, rddStates, sparkCursors, writehandlers, sc) =>
      val interpreter: Eff ~> S = (MonotonicSeq.fromTaskRef(genState) andThen injectNT[Task, S]) :+:
      (CassandraDDL.interpreter[S](sc) andThen injectNT[Task, S]) :+:
      injectNT[PhysErr, S] :+:
      injectNT[Task, S]  :+:
      (KeyValueStore.impl.fromTaskRef[WriteHandle, AFile](writehandlers) andThen injectNT[Task, S]) :+:
      (KeyValueStore.impl.fromTaskRef[ReadHandle, SparkCursor](sparkCursors) andThen injectNT[Task, S]) :+:
      (KeyValueStore.impl.fromTaskRef[ResultHandle, RddState](rddStates) andThen injectNT[Task, S]) :+:
      (Read.constant[Task, SparkContext](sc) andThen injectNT[Task, S])

      SparkFSDef(mapSNT[Eff, S](interpreter), lift(Task.delay(sc.stop())).into[S])
    }).into[S]
  }

  private def fsInterpret: SparkFSConf => (FileSystem ~> Free[Eff, ?]) =
    (sparkFsConf: SparkFSConf) => interpretFileSystem(
      corequeryfile.chrooted[Eff](queryfile.input, FsType, sparkFsConf.prefix),
      corereadfile.chrooted(readfile.input[Eff], sparkFsConf.prefix),
      writefile.chrooted[Eff](sparkFsConf.prefix),
      managefile.chrooted[Eff](sparkFsConf.prefix))

  def definition[S[_]](implicit S0: Task :<: S, S1: PhysErr :<: S): BackendDef[Free[S, ?]] =
    quasar.physical.sparkcore.fs.definition[Eff, S, SparkFSConf](FsType, parseUri, sparkFsDef, fsInterpret)

}
