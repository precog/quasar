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

package quasar.connector

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.fp.free._
import quasar.fs._, mount.BackendDef.DefErrT

import pathy.Path._
import scalaz._, Scalaz._, concurrent.Task

trait ChrootedInterpreter extends BackendModule {

  def rootPrefix(cfg: Config): ADir

  override def interpreter(cfg: Config): DefErrT[Task, (BackendEffect ~> Task, Task[Unit])] = {
    val xformPaths =
      if (rootPrefix(cfg) === rootDir) liftFT[BackendEffect]
      else chroot.backendEffect[BackendEffect](rootPrefix(cfg))(???, ???, ???, ???, ???)

    super.interpreter(cfg) map {
      case (f, c) => (foldMapNT(f) compose xformPaths, c)
    }
  }

}
