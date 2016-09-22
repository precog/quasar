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
import quasar.fs._
import quasar.fs.mount.FileSystemDef, FileSystemDef.DefErrT
import quasar.physical.sparkcore.fs.{readfile => corereadfile}
import quasar.physical.sparkcore.fs.{queryfile => corequeryfile}
import quasar.effect._
import quasar.fs.ReadFile.ReadHandle
import quasar.fs.WriteFile.WriteHandle
import quasar.fp.TaskRef
import quasar.fp.free._
import quasar.fs.mount.FileSystemDef._

import java.io.PrintWriter

import org.apache.spark._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

package object local {
  val FsType = FileSystemType("sparklocal")

  type Eff0[A] = Coproduct[KeyValueStore[ReadHandle, SparkCursor, ?], Read[SparkContext, ?], A]
  type Eff1[A] = Coproduct[KeyValueStore[WriteHandle, PrintWriter, ?], Eff0, A]
  type Eff2[A] = Coproduct[Task, Eff1, A]
  type Eff3[A] = Coproduct[PhysErr, Eff2, A]
  type Eff[A]  = Coproduct[MonotonicSeq, Eff3, A]

  final case class SparkFSDef[S[_]](run: Free[Eff, ?] ~> Free[S, ?], close: Free[S, Unit])

  private def sparkFsDef[S[_]](sparkConf: SparkConf)(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): Free[S, SparkFSDef[S]] = {

    val genSc = Task.delay {
      new SparkContext(sparkConf.setAppName("quasar"))
    }

    lift((TaskRef(0L) |@|
      TaskRef(Map.empty[ReadHandle, SparkCursor]) |@|
      TaskRef(Map.empty[WriteHandle, PrintWriter]) |@|
      genSc) {
      (genState, sparkCursors, printWriters, sc) =>
      val interpreter: Eff ~> S = (MonotonicSeq.fromTaskRef(genState) andThen injectNT[Task, S]) :+:
        injectNT[PhysErr, S] :+:
        injectNT[Task, S]  :+:
        (KeyValueStore.impl.fromTaskRef[WriteHandle, PrintWriter](printWriters) andThen injectNT[Task, S])  :+:
        (KeyValueStore.impl.fromTaskRef[ReadHandle, SparkCursor](sparkCursors) andThen injectNT[Task, S]) :+:
        (Read.constant[Task, SparkContext](sc) andThen injectNT[Task, S])

      SparkFSDef(mapSNT[Eff, S](interpreter), lift(Task.delay(sc.stop())).into[S])
    }).into[S]
  }

  private def fsInterpret(fsConf: SparkFSConf): FileSystem ~> Free[Eff, ?] = interpretFileSystem(
    corequeryfile.chrooted[Eff](queryfile.input, fsConf.prefix),
    corereadfile.chrooted(readfile.input[Eff], fsConf.prefix),
    writefile.chrooted[Eff](fsConf.prefix),
    managefile.chrooted[Eff](fsConf.prefix))

  def definition[S[_]](implicit S0: Task :<: S, S1: PhysErr :<: S):
      FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case (FsType, uri) =>
        for {
          fsConf <- EitherT(parseUri(uri).point[Free[S, ?]])
          res <- {
            sparkFsDef(fsConf.sparkConf).map {
              case SparkFSDef(run, close) =>
                FileSystemDef.DefinitionResult[Free[S, ?]](
                  fsInterpret(fsConf) andThen run,
                  close)
            }.liftM[DefErrT]
          }
        }  yield res
    }
}
