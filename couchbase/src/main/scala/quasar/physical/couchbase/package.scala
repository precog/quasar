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

package quasar.physical

import quasar.contrib.iota.SubInject
import quasar.fp.Injectable
import quasar.qscript._
import quasar.contrib.pathy.AFile
import scalaz.Const
import iotaz.TListK.:::
import iotaz.{TNilK, CopK}

package object couchbase {
  type CouchbaseQScriptCP[T[_[_]]] =
    QScriptCore[T, ?]            :::
    EquiJoin[T, ?]               :::
    Const[ShiftedRead[AFile], ?] :::
    TNilK

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable[CopK[CouchbaseQScriptCP[T], ?], QScriptTotal[T, ?]] =
    SubInject[CopK[CouchbaseQScriptCP[T], ?], QScriptTotal[T, ?]]

  implicit def qScriptCoreToQScript[T[_[_]]]: Injectable[QScriptCore[T, ?], CopK[CouchbaseQScriptCP[T], ?]] =
    Injectable.inject[QScriptCore[T, ?], CopK[CouchbaseQScriptCP[T], ?]]

  implicit def equiJoinToQScript[T[_[_]]]: Injectable[EquiJoin[T, ?], CopK[CouchbaseQScriptCP[T], ?]] =
    Injectable.inject[EquiJoin[T, ?], CopK[CouchbaseQScriptCP[T], ?]]

  implicit def shiftedReadToQScript[T[_[_]]]: Injectable[Const[ShiftedRead[AFile], ?], CopK[CouchbaseQScriptCP[T], ?]] =
    Injectable.inject[Const[ShiftedRead[AFile], ?], CopK[CouchbaseQScriptCP[T], ?]]
}
