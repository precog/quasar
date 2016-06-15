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

package quasar.config

import quasar.Predef._
import quasar.effect._
import quasar.fp.TaskRef
import quasar.fp.free, free._
import quasar.fs.APath
import quasar.fs.mount._

import argonaut.EncodeJson
import monocle.Lens
import scalaz.{Lens => _, _}
import scalaz.concurrent.Task

/** Interpreter providing access to configuration, based on some concrete Config type. */
object writeConfig {
  def apply[Cfg: ConfigOps: EncodeJson](
    mountingsLens: Lens[Cfg, MountingsConfig], ref: TaskRef[Cfg], loc: Option[FsFile])
    : MountConfigs ~> Task = {

    type MRef[A] = AtomicRef[Map[APath, MountConfig], A]

    type ConfigRef[A]  = AtomicRef[Cfg, A]
    type ConfigRefM[A] = Free[ConfigRef, A]

    type ConfigRefPlusTask[A]  = Coproduct[ConfigRef, Task, A]
    type ConfigRefPlusTaskM[A] = Free[ConfigRefPlusTask, A]

    // AtomicRef[Cfg, ?] ~> Task (with writing the config file):
    val configRef: ConfigRef ~> Task = {
      val refToTask: ConfigRef ~> Task = AtomicRef.fromTaskRef(ref)

      val write: Cfg => Task[Unit] = ConfigOps[Cfg].toFile(_, loc)
      def writing: ConfigRef ~> ConfigRefPlusTaskM = AtomicRef.onSet[Cfg](write)

      val refToTaskF: ConfigRefPlusTask ~> Task =
        refToTask :+: NaturalTransformation.refl

      free.foldMapNT(refToTaskF) compose writing
    }

    // AtomicRef[Map[APath, MountConfig], ?] ~> Free[AtomicRef[Cfg, ?], ?]:
    val mapToConfig: MRef ~> ConfigRefM =
      AtomicRef.zoom(mountingsLens composeIso MountingsConfig.mapIso)
        .into[AtomicRef[Cfg, ?]]

    // KeyValueStore[APath, MountConfig, ?] ~> Free[AtomicRef[Map[APath, MountConfig], ?]]:
    val storeToMap: MountConfigs ~> Free[MRef, ?] =
      KeyValueStore.toAtomicRef[APath, MountConfig]()

    free.foldMapNT(configRef) compose (free.foldMapNT(mapToConfig) compose storeToMap)
  }
}
