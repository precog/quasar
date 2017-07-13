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

import quasar.contrib.pathy._
import quasar.effect.KeyValueStore
import quasar.fp._
import quasar.fp.free._

import monocle.Lens
import scalaz._, Id._

object Fixture {

  def constant: Mounting ~> State[Map[APath, MountConfig], ?] = {
    type F[A] = State[Map[APath, MountConfig], A]
    val mntr = Mounter.trivial[MountConfigs]
    val kvf = KeyValueStore.impl.toState[F](Lens.id[Map[APath, MountConfig]])
    mntr andThen foldMapNT(kvf)
  }

  def runConstantMount[F[_]: Applicative](mnts: Map[APath, MountConfig]): Mounting ~> F =
    constant andThen evalNT[Id, Map[APath, MountConfig]](mnts) andThen pointNT[F]
}