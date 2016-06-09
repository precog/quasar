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
import quasar.fp._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.mount.{ConnectionUri, FileSystemDef}
import quasar.physical.mongodb.fs._
import quasar.regression._

import com.mongodb.MongoException
import scalaz.{Failure => _, :+: => _, _}, Scalaz._
import scalaz.concurrent.Task

object filesystems {
  def testFileSystem(
    uri: ConnectionUri,
    prefix: ADir
  ): Task[(FileSystem ~> Task, Task[Unit])] = {
    val fsDef = quasar.physical.mongodb.fs.mongoDbFileSystemDef[MongoEff].apply(MongoDBFsType, uri).run
      .flatMap[FileSystemDef.DefinitionResult[MongoEffM]] {
        case -\/(-\/(strs)) => injectFT[Task, MongoEff].apply(Task.fail(new RuntimeException(strs.list.toList.mkString)))
        case -\/(\/-(err))  => injectFT[Task, MongoEff].apply(Task.fail(new RuntimeException(err.shows)))
        case \/-(d)         => d.point[MongoEffM]
      }

    mongoEffMToTask(fsDef).map(d =>
      (mongoEffMToTask compose d.run,
        mongoEffMToTask(d.close)))
  }

  def testFileSystemIO(
    uri: ConnectionUri,
    prefix: ADir
  ): Task[(FileSystemIO ~> Task, Task[Unit])] =
    testFileSystem(uri, prefix)
      .map { case (run, close) => (NaturalTransformation.refl[Task] :+: run, close) }

  ////

  private type MongoEff[A]  =
    (CfgErrF :+: (EnvErrF :+: (MongoErrF :+: Task)#λ)#λ)#λ[A]
  private type MongoEffM[A] = Free[MongoEff, A]

  private val envErr = Failure.Ops[EnvironmentError, MongoEff]

  private val mongoEffToTask: MongoEff ~> Task =
    Coyoneda.liftTF[CfgErr, Task](Failure.toRuntimeError[Task,ConfigError])      :+:
    Coyoneda.liftTF[EnvErr, Task](Failure.toRuntimeError[Task,EnvironmentError]) :+:
    Coyoneda.liftTF[MongoErr, Task](Failure.toCatchable[Task,MongoException])    :+:
    NaturalTransformation.refl

  private val mongoEffMToTask: MongoEffM ~> Task =
    foldMapNT(mongoEffToTask)
}
