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

package quasar.fs.mount

import quasar.Predef._
import quasar.effect._
import quasar.fp._, free._
import quasar.fs.APath

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class MounterSpec extends MountingSpec[Mounting] {
  import MountConfig._, MountingError._, MountRequest._

  type MEff[A] = Coproduct[Task, MountConfigs, A]

  val invalidUri = ConnectionUri(uriA.value + "INVALID")
  val invalidCfg = fileSystemConfig(dbType, invalidUri)
  val invalidErr = invalidConfig(invalidCfg, "invalid URI".wrapNel)

  val doMount: MountRequest => MountingError \/ Unit = {
    case MountFileSystem(_, `dbType`, `invalidUri`) => invalidErr.left
    case _                                          => ().right
  }

  def interpName = "Mounter"

  def interpret = {
    val mm = Mounter[Task, MEff](doMount.andThen(_.point[Task]), κ(Task.now(())))
    val cfgRef = TaskRef(Map.empty[APath, MountConfig]).unsafePerformSync

    val interpMnts: MountConfigs ~> Task =
      KeyValueStore.fromTaskRef(cfgRef)

    val interpEff: MEff ~> Task =
      NaturalTransformation.refl[Task] :+: interpMnts

    free.foldMapNT(interpEff) compose mm
  }

  "Handling mounts" should {
    "fail when mount handler fails" >>* {
      val loc = rootDir </> dir("fs")
      val cfg = MountConfig.fileSystemConfig(dbType, invalidUri)

      mnt.mountFileSystem(loc, dbType, invalidUri)
        .run.tuple(mnt.lookup(loc).run)
        .map(_ must_== ((MountingError.invalidConfig(cfg, "invalid URI".wrapNel).left, None)))
    }
  }
}
