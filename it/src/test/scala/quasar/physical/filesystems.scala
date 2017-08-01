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

package quasar.physical

import slamdata.Predef._
import quasar.connector.{EnvironmentError, EnvErr}
import quasar.config.{CfgErr, ConfigError}
import quasar.contrib.pathy._
import quasar.effect.Failure
import quasar.fp._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.mount.{ConnectionUri, BackendDef}

import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

object filesystems {
  type Eff[A]  = (CfgErr :\: EnvErr :\: PhysErr :/: Task)#M[A]
  type EffM[A] = Free[Eff, A]

  def testFileSystem(
    uri: ConnectionUri,
    prefix: ADir,
    f: Free[Eff, BackendDef.DefinitionError \/ BackendDef.DefinitionResult[Free[Eff, ?]]]
  ): Task[(BackendEffect ~> Task, Task[Unit])] = {
    val fsDef = f.flatMap[BackendDef.DefinitionResult[EffM]] {
        case -\/(-\/(strs)) => injectFT[Task, Eff].apply(Task.fail(new RuntimeException(strs.list.toList.mkString)))
        case -\/(\/-(err))  => injectFT[Task, Eff].apply(Task.fail(new RuntimeException(err.shows)))
        case \/-(d)         => d.point[EffM]
      }

    effMToTask(fsDef).map(d =>
      (effMToTask compose d.run,
        effMToTask(d.close)))
  }

  ////

  private val envErr = Failure.Ops[EnvironmentError, Eff]

  private val effToTask: Eff ~> Task =
    Failure.toRuntimeError[Task, ConfigError]      :+:
    Failure.toRuntimeError[Task, EnvironmentError] :+:
    Failure.toRuntimeError[Task, PhysicalError]    :+:
    NaturalTransformation.refl

  private val effMToTask: EffM ~> Task =
    foldMapNT(effToTask)

}
