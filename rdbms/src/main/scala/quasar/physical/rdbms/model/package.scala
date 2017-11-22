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

import slamdata.Predef._
import quasar.effect.uuid.GenUUID
import quasar.effect.{KeyValueStore, MonotonicSeq, Read}
import quasar.fp.{:/:, :\:}
import quasar.fs.ReadFile.ReadHandle
import quasar.fs.WriteFile.WriteHandle
import quasar.fs.impl.DataStream
import quasar.physical.rdbms.fs.WriteCursor

import doobie.imports.{ConnectionIO, Transactor}
import scalaz.Free
import scalaz.concurrent.Task
import scalaz.stream.Process

package object model {

  final case class DbDataStream(stream: DataStream[Task])

  type ReadTransactor[A] = Read[Transactor[Task], A]

  object DbDataStream {
    def empty = DbDataStream(Process.empty)
  }

  type Eff[A] = (
          Task
      :\: ReadTransactor
      :\: ConnectionIO
      :\: MonotonicSeq
      :\: GenUUID
      :\: KeyValueStore[ReadHandle, DbDataStream, ?]
      :/: KeyValueStore[WriteHandle, WriteCursor, ?]
  )#M[A]

  type M[A] = Free[Eff, A]

  implicit class StringTypeMapperOps(s: String) {
    def mapToColumnType(implicit tm: TypeMapper): ColumnType = tm.comap(s)
  }

  implicit class ColumnTypeMapperOps(cte: ColumnType) {
    def mapToStringName(implicit tm: TypeMapper): String = tm.map(cte)
  }
}

