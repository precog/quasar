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
import quasar.fp.{free, TaskRef}
import quasar.fs.{APath}
import quasar.fs.mount._

import argonaut.{EncodeJson}
import pathy.Path
import monocle.{Lens}
import scalaz.{Lens => _, _}
import scalaz.concurrent.{Task}

/** Interpreter providing access to configuration, based on some concrete Config type. */
object writeConfig {
  def apply[Cfg](configOps: ConfigOps[Cfg])(ref: TaskRef[Cfg], loc: Option[FsFile])
      (implicit E: EncodeJson[Cfg]): MountConfigs ~> Task = {

    type MRef[A] = AtomicRef[Map[APath, MountConfig2], A]
    type MRefF[A] = Coyoneda[MRef, A]

    type ConfigRef[A] = AtomicRef[Cfg, A]
    type ConfigRefF[A] = Coyoneda[ConfigRef, A]
    type ConfigRefM[A] = Free[ConfigRefF, A]

    type ConfigRefPlusTask[A] = Coproduct[ConfigRefF, Task, A]
    type ConfigRefPlusTaskM[A] = Free[ConfigRefPlusTask, A]

    // AtomicRef[Cfg, ?] ~> Task (with writing the config file):
    val configRef: ConfigRefF ~> Task = {
      val refToTask: ConfigRef ~> Task = AtomicRef.fromTaskRef(ref)

      val write: Cfg => Task[Unit] = configOps.toFile(_, loc)
      def writing: ConfigRef ~> ConfigRefPlusTaskM =
        AtomicRef.onSet[Cfg](write)(
          implicitly, implicitly, implicitly,
          Inject[ConfigRefF, ConfigRefPlusTask]) // NB: this one is not resolved

      val refToTaskF: ConfigRefPlusTask ~> Task =
        free.interpret2[ConfigRefF, Task, Task](
          Coyoneda.liftTF(refToTask), NaturalTransformation.refl)

      free.foldMapNT(refToTaskF).compose[ConfigRefF](Coyoneda.liftTF(writing))
    }

    // AtomicRef[Map[APath, MountConfig2], ?] ~> Free[AtomicRef[Cfg, ?], ?]:
    val mapToConfig: MRefF ~> ConfigRefM =  {
      val mountingsLens: Lens[Cfg, Map[APath, MountConfig2]] =
        configOps.mountingsLens composeIso MountingsConfig2.mapIso

      Coyoneda.liftTF[MRef, ConfigRefM]{
        val aux = AtomicRef.zoom(mountingsLens)
        aux.into[aux.RefAF]
      }
    }

    // KeyValueStore[APath, MountConfig2, ?] ~> Free[AtomicRef[Map[APath, MountConfig2], ?]]:
    val storeToMap: MountConfigs ~> Free[MRefF, ?] = KeyValueStore.toAtomicRef[APath, MountConfig2]()

    free.foldMapNT(configRef) compose (free.foldMapNT(mapToConfig) compose storeToMap)
  }
}
