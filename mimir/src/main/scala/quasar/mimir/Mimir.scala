/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.mimir

import quasar.Data
import quasar.contrib.pathy.{ADir, AFile, RPath}
import quasar.fs.FileSystemType
import quasar.fs.mount.ConnectionUri

import fs2.Stream
import scalaz.concurrent.Task

object MimirFileSystem extends LightweightFileSystem {
  def children(dir: ADir): Task[List[RPath]] =
    Task.now(List[RPath]())

  def exists(file: AFile): Task[Boolean] =
    Task.now(false)

  def read(file: AFile): Task[Option[Stream[Task, Data]]] =
    Task.now(None)
}

object MimirLightweight extends LightweightConnector {
  val Type: FileSystemType = FileSystemType("mimir")

  def init(uri: ConnectionUri): Task[(LightweightFileSystem, Task[Unit])] =
    Task.now((MimirFileSystem, Task.now(())))
}

object Mimir extends SlamDB {
  val lwc: LightweightConnector = MimirLightweight
}
