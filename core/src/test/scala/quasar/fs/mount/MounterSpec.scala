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

package quasar.fs.mount

import slamdata.Predef._
import quasar.effect._
import quasar.fp._, free._
import quasar.fp.ski._

import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

class MounterSpec extends MountingSpec[MounterSpec.Eff] {
  import MountConfig._, MountingError._, MountRequest._
  import Mounting.PathTypeMismatch

  type MEff0[A] = Coproduct[MountConfigs, MounterSpec.Eff0, A]
  type MEff[A]  = Coproduct[Task, MEff0, A]

  val invalidUri = ConnectionUri(uriA.value + "INVALID")
  val invalidCfg = fileSystemConfig(dbType, invalidUri)
  val invalidErr = invalidConfig(invalidCfg, "invalid URI".wrapNel)

  val doMount: MountRequest => MountingError \/ Unit = {
    case MountFileSystem(_, `dbType`, `invalidUri`) => invalidErr.left
    case _                                          => ().right
  }

  def interpName = "Mounter"

  def interpret: MounterSpec.Eff ~> Task = {
    val mm: Mounting ~> Free[MEff, ?] = Mounter.kvs[Task, MEff](doMount.andThen(_.point[Task]), κ(Task.now(())))

    val interpEff: MounterSpec.Eff ~> Free[MEff, ?] =
      mm :+: injectFT[MountingFailure, MEff] :+: injectFT[PathMismatchFailure, MEff]

    val interpMnts: MountConfigs ~> Task =
      KeyValueStore.impl.default.unsafePerformSync

    val interpMEff: MEff ~> Task =
      reflNT[Task]                                   :+:
      interpMnts                                     :+:
      Failure.toRuntimeError[Task, MountingError]    :+:
      Failure.toRuntimeError[Task, PathTypeMismatch]

    free.foldMapNT(interpMEff) compose interpEff
  }

  "Handling mounts" should {
    "fail when mount handler fails" >>* {
      val loc = rootDir </> dir("fs")
      val cfg = MountConfig.fileSystemConfig(dbType, invalidUri)

      mntErr.attempt(mnt.mountFileSystem(loc, dbType, invalidUri))
        .tuple(mnt.lookupConfig(loc).run.run)
        .map(_ must_=== ((MountingError.invalidConfig(cfg, "invalid URI".wrapNel).left, None)))
    }
  }
}

object MounterSpec {
  type Eff0[A] = Coproduct[MountingFailure, PathMismatchFailure, A]
  type Eff[A]  = Coproduct[Mounting, Eff0, A]
}
