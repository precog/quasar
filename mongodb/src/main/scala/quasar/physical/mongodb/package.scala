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

import slamdata.Predef._
import quasar.common.SortDir
import quasar.effect.NameGenerator
import quasar.javascript.Js
import quasar.fs.PhysicalError
import quasar.qscript._
import quasar.contrib.pathy.AFile
import quasar.fp.Injectable

import com.mongodb.async.AsyncBatchCursor
import org.bson.BsonValue
import scalaz._
import iotaz.CopK
import quasar.contrib.iota.SubInject

package object mongodb {
  type BsonCursor         = AsyncBatchCursor[BsonValue]

  type MongoErrT[F[_], A] = EitherT[F, PhysicalError, A]

  type WorkflowExecErrT[F[_], A] = EitherT[F, WorkflowExecutionError, A]

  type JavaScriptPrg    = Vector[Js.Stmt]
  type JavaScriptLog[A] = Writer[JavaScriptPrg, A]

  // TODO: parameterize over label (SD-512)
  def freshName: State[Long, BsonField.Name] =
    NameGenerator[State[Long, ?]]
      .prefixedName("__tmp")
      .map(BsonField.Name(_))

  // TODO use implicit class
  def sortDirToBson(sort: SortDir): Bson = sort match {
    case SortDir.Ascending => Bson.Int32(1)
    case SortDir.Descending => Bson.Int32(-1)
  }

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable[CopK[fs.MongoQScriptCP[T], ?], QScriptTotal[T, ?]] =
    SubInject[CopK[fs.MongoQScriptCP[T], ?], QScriptTotal[T, ?]]

  implicit def qScriptCoreToQScript[T[_[_]]]: Injectable[QScriptCore[T, ?], CopK[fs.MongoQScriptCP[T], ?]] =
    Injectable.inject[QScriptCore[T, ?], CopK[fs.MongoQScriptCP[T], ?]]

  implicit def equiJoinToQScript[T[_[_]]]: Injectable[EquiJoin[T, ?], CopK[fs.MongoQScriptCP[T], ?]] =
    Injectable.inject[EquiJoin[T, ?], CopK[fs.MongoQScriptCP[T], ?]]

  implicit def shiftedReadToQScript[T[_[_]]]: Injectable[Const[ShiftedRead[AFile], ?], CopK[fs.MongoQScriptCP[T], ?]] =
    Injectable.inject[Const[ShiftedRead[AFile], ?], CopK[fs.MongoQScriptCP[T], ?]]

}
