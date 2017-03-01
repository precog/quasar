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

package quasar.physical.postgresql

import quasar.Predef._
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fp._, free._
import quasar.fs._, ReadFile.ReadHandle, WriteFile.WriteHandle
import quasar.fs.mount._, FileSystemDef.DefErrT

import doobie.imports._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

// NB [quasar.physical.postgresql]:
// - The PostgreSQL Connector is incomplete and under development.
// - Queries are constructed manually instead of using doobie's sql interpolator
//   due to PreparedStatements not handling dynamic table names. The current
//   approach does not prevent SQL injection attacks and should be revisited.
package object fs {
  val FsType = FileSystemType("postgresql")

  type Eff[A] = (
        ConnectionIO
    :\: MonotonicSeq
    :\: KeyValueStore[ReadHandle, impl.DataStream[ConnectionIO], ?]
    :/: KeyValueStore[WriteHandle, writefile.TableName, ?]
  )#M[A]

  def interp[S[_]](uri: ConnectionUri)(
    implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): Free[S, Free[Eff, ?] ~> Free[S, ?]] = {
    val transactor = DriverManagerTransactor[Task]("org.postgresql.Driver", uri.value)

    def taskInterp: Task[Free[Eff, ?] ~> Free[S, ?]]  =
      (TaskRef(Map.empty[ReadHandle,  impl.DataStream[ConnectionIO]]) |@|
       TaskRef(Map.empty[WriteHandle, writefile.TableName])           |@|
       TaskRef(0L)
      )((kvR, kvW, i) =>
        mapSNT(injectNT[Task, S] compose (
          transactor.trans                    :+:
          MonotonicSeq.fromTaskRef(i)         :+:
          KeyValueStore.impl.fromTaskRef(kvR) :+:
          KeyValueStore.impl.fromTaskRef(kvW))))

    lift(taskInterp).into[S]
  }

  def definition[S[_]](
    implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case (FsType, uri) =>
        interp(uri).map { run =>
          FileSystemDef.DefinitionResult[Free[S, ?]](
            run compose interpretFileSystem(
              queryfile.interpret,
              readfile.interpret,
              writefile.interpret,
              managefile.interpret),
            Free.point(()))
        }.liftM[DefErrT]
    }
}
