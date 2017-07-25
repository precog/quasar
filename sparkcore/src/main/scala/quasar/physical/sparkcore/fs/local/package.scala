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
import quasar.fs.mount.{ConnectionUri, BackendDef}, BackendDef._
import quasar.physical.sparkcore.fs.{queryfile => corequeryfile, readfile => corereadfile, genSc => coreGenSc}

import java.io.PrintWriter

import org.apache.spark._
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

package object local {

  import corequeryfile.RddState

  val FsType = FileSystemType("spark-local")

  type EffM1[A] = Coproduct[KeyValueStore[ResultHandle, RddState, ?], Read[SparkContext, ?], A]
  type Eff0[A] = Coproduct[KeyValueStore[ReadHandle, SparkCursor, ?], EffM1, A]
  type Eff1[A] = Coproduct[KeyValueStore[WriteHandle, PrintWriter, ?], Eff0, A]
  type Eff2[A] = Coproduct[Task, Eff1, A]
  type Eff3[A] = Coproduct[PhysErr, Eff2, A]
  type Eff[A]  = Coproduct[MonotonicSeq, Eff3, A]

  final case class SparkFSConf(sparkConf: SparkConf, prefix: ADir)

  def parseUri: ConnectionUri => DefinitionError \/ (SparkConf, SparkFSConf) =
    (uri: ConnectionUri) => {

     def error(msg: String): DefinitionError \/ (SparkConf, SparkFSConf) =
        NonEmptyList(msg).left[EnvironmentError].left[(SparkConf, SparkFSConf)]

      def forge(master: String, rootPath: String): DefinitionError \/ (SparkConf, SparkFSConf) =
        posixCodec.parseAbsDir(rootPath)
          .map { prefix =>
          val sc = new SparkConf().setMaster(master).setAppName("quasar")
          (sc, SparkFSConf(sc, unsafeSandboxAbs(prefix)))
        }.fold(error(s"Could not extract a path from $rootPath"))(_.right[DefinitionError])

      forge("local[*]", uri.value)
    }

  def sparkFsDef[S[_]](implicit
    S0: Task :<: S,
    S1: PhysErr :<: S,
    FailOps: Failure.Ops[PhysicalError, S]
  ): SparkConf => Free[S, SparkFSDef[Eff, S]] = (sparkConf: SparkConf) => {

    val definition: SparkContext => Free[S, SparkFSDef[Eff, S]] = (sc: SparkContext) => lift((TaskRef(0L) |@|
      TaskRef(Map.empty[ResultHandle, RddState]) |@|
      TaskRef(Map.empty[ReadHandle, SparkCursor]) |@|
      TaskRef(Map.empty[WriteHandle, PrintWriter])
      ) {
      (genState, rddStates, sparkCursors, printWriters) =>
      val interpreter: Eff ~> S = (MonotonicSeq.fromTaskRef(genState) andThen injectNT[Task, S]) :+:
      injectNT[PhysErr, S] :+:
      injectNT[Task, S]  :+:
      (KeyValueStore.impl.fromTaskRef[WriteHandle, PrintWriter](printWriters) andThen injectNT[Task, S])  :+:
      (KeyValueStore.impl.fromTaskRef[ReadHandle, SparkCursor](sparkCursors) andThen injectNT[Task, S]) :+:
      (KeyValueStore.impl.fromTaskRef[ResultHandle, RddState](rddStates) andThen injectNT[Task, S]) :+:
      (Read.constant[Task, SparkContext](sc) andThen injectNT[Task, S])

      SparkFSDef(mapSNT[Eff, S](interpreter), lift(Task.delay(sc.stop())).into[S])
    }).into[S]

    lift(coreGenSc(sparkConf).run).into[S] >>= (_.fold(
      msg => FailOps.fail[SparkFSDef[Eff, S]](UnhandledFSError(new RuntimeException(msg))),
      definition(_)
    ))
  }

  def fsInterpret: SparkFSConf => (FileSystem ~> Free[Eff, ?]) =
    (fsConf: SparkFSConf) => interpretFileSystem(
      corequeryfile.chrooted[Eff](queryfile.input, FsType, fsConf.prefix),
      corereadfile.chrooted(readfile.input[Eff], fsConf.prefix),
      writefile.chrooted[Eff](fsConf.prefix),
      managefile.chrooted[Eff](fsConf.prefix))

  def definition[S[_]](implicit
    S0: Task :<: S, S1: PhysErr :<: S
  ) =
    quasar.physical.sparkcore.fs.definition[Eff, S, SparkFSConf](FsType, parseUri, sparkFsDef, fsInterpret)
}
