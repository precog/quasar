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

package quasar.physical.rdbms

import slamdata.Predef.Map
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.effect.uuid.GenUUID
import quasar.fp.TaskRef
import quasar.fp.free._
import quasar.fs.ReadFile.ReadHandle
import quasar.fs.WriteFile.WriteHandle
import quasar.physical.rdbms.common.TablePath
import quasar.physical.rdbms.fs.SqlReadCursor

import doobie.imports.Transactor
import scalaz.concurrent.Task
import scalaz.syntax.applicative._
import scalaz.~>

trait Interpreter {
  this: Rdbms =>

  def interp(xa: Task[Transactor[Task]]): Task[Eff ~> Task] =
    (
      TaskRef(Map.empty[ReadHandle, SqlReadCursor]) |@|
        TaskRef(Map.empty[WriteHandle, TablePath]) |@|
        xa.map(_.trans) |@|
        TaskRef(0L) |@|
        GenUUID.type1[Task]
    )(
      (kvR, kvW, x, i, genUUID) =>
        x :+:
          MonotonicSeq.fromTaskRef(i) :+:
          genUUID :+:
          KeyValueStore.impl.fromTaskRef(kvR) :+:
          KeyValueStore.impl.fromTaskRef(kvW))
}
