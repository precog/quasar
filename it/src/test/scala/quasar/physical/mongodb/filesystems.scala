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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.{EnvironmentError, EnvErrF, EnvErr}
import quasar.config.{CfgErr, CfgErrF, ConfigError}
import quasar.effect.Failure
import quasar.fp.free._
import quasar.fs._
import quasar.fs.mount.ConnectionUri
import quasar.physical.mongodb.fs._
import quasar.physical.mongodb.util.createAsyncMongoClient
import quasar.regression._

import com.mongodb.MongoException
import scalaz.{Failure => _, _}
import scalaz.concurrent.Task

object filesystems {
  def testFileSystem(
    uri: ConnectionUri,
    prefix: ADir
  ): Task[FileSystem ~> Task] = {
    val prg = for {
      client   <- createAsyncMongoClient[MongoEff](uri)
      mongofs0 =  mongoDbFileSystem[MongoEff](client, DefaultDb fromPath prefix)
      mongofs  <- envErr.unattempt(lift(mongofs0.run).into[MongoEff])
    } yield mongofs

    mongoEffMToTask(prg) map (mongoEffMToTask compose _)
  }

  def testFileSystemIO(
    uri: ConnectionUri,
    prefix: ADir
  ): Task[FileSystemIO ~> Task] =
    testFileSystem(uri, prefix)
      .map(interpret2(NaturalTransformation.refl[Task], _))

  ////

  private type MongoEff0[A] = Coproduct[MongoErrF, Task, A]
  private type MongoEff1[A] = Coproduct[WorkflowExecErrF, MongoEff0, A]
  private type MongoEff2[A] = Coproduct[EnvErrF, MongoEff1, A]
  private type MongoEff[A]  = Coproduct[CfgErrF, MongoEff2, A]
  private type MongoEffM[A] = Free[MongoEff, A]

  private val envErr =
    Failure.Ops[EnvironmentError, MongoEff](implicitly, Inject[EnvErrF, MongoEff])

  private val mongoEffToTask: MongoEff ~> Task =
    interpret5[CfgErrF, EnvErrF, WorkflowExecErrF, MongoErrF, Task, Task](
      Coyoneda.liftTF[CfgErr, Task](Failure.toRuntimeError[ConfigError]),
      Coyoneda.liftTF[EnvErr, Task](Failure.toRuntimeError[EnvironmentError]),
      Coyoneda.liftTF[WorkflowExecErr, Task](Failure.toRuntimeError[WorkflowExecutionError]),
      Coyoneda.liftTF[MongoErr, Task](Failure.toTaskFailure[MongoException]),
      NaturalTransformation.refl)

  private val mongoEffMToTask: MongoEffM ~> Task =
    foldMapNT(mongoEffToTask)
}
