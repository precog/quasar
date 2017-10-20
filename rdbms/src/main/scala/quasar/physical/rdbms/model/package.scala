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

import quasar.contrib.pathy.AFile
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.effect.uuid.GenUUID
import quasar.fp.{:/:, :\:}
import quasar.fs.ReadFile.ReadHandle
import quasar.fs.WriteFile.WriteHandle
import quasar.physical.rdbms.common.TablePath
import quasar.physical.rdbms.fs.SqlReadCursor
import quasar.physical.rdbms.planner.sql.SqlExpr
import quasar.qscript.{EquiJoin, QScriptCore, ShiftedRead}
import doobie.imports.ConnectionIO
import matryoshka.data.Fix
import quasar.fs.QueryFile.ResultHandle

import scalaz.{Const, Free}

package object model {

  type Eff[A] = (
    ConnectionIO :\:
      MonotonicSeq :\:
      GenUUID :\:
      KeyValueStore[ResultHandle, SqlReadCursor, ?] :\:
      KeyValueStore[ReadHandle, SqlReadCursor, ?] :/:
      KeyValueStore[WriteHandle, TablePath, ?]
    )#M[A]

  type QS[T[_[_]]] = QScriptCore[T, ?] :\: EquiJoin[T, ?] :/: Const[ShiftedRead[AFile], ?]
  type Repr        = Fix[SqlExpr]
  type M[A]        = Free[Eff, A]

  type Config = common.Config

}
