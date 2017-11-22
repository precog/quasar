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
import quasar.effect.{KeyValueStore, MonotonicSeq, Read}
import quasar.effect.uuid.GenUUID
import quasar.fp.{TaskRef, reflNT}
import quasar.fp.free._
import quasar.fs.ReadFile.ReadHandle
import quasar.fs.WriteFile.WriteHandle
import quasar.physical.rdbms.fs.WriteCursor
import quasar.physical.rdbms.model.DbDataStream

import doobie.imports.Transactor
import scalaz.concurrent.Task
import scalaz.syntax.applicative._
import scalaz.{~>}

trait Interpreter {
  this: Rdbms =>

  def interp(xa: Task[Transactor[Task]]): Task[Eff ~> Task] =
    (
      TaskRef(Map.empty[ReadHandle, DbDataStream])   |@|
        TaskRef(Map.empty[WriteHandle, WriteCursor]) |@|
        xa                                           |@|
        TaskRef(0L)                                  |@|
        GenUUID.type1[Task]
      )(
      (kvR, kvW, x, i, genUUID) =>
        reflNT[Task]                               :+:
          Read.constant[Task, Transactor[Task]](x) :+:
          x.trans                                  :+:
          MonotonicSeq.fromTaskRef(i)              :+:
          genUUID                                  :+:
          KeyValueStore.impl.fromTaskRef(kvR)      :+:
          KeyValueStore.impl.fromTaskRef(kvW))
}
