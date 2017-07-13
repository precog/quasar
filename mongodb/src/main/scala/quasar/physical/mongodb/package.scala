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

package quasar.physical

import slamdata.Predef._
import quasar.TernaryFunc
import quasar.common.SortDir
import quasar.javascript.Js
import quasar.fs.PhysicalError
import quasar.namegen._

import com.mongodb.async.AsyncBatchCursor
import org.bson.BsonDocument
import scalaz._

package object mongodb {
  type BsonCursor         = AsyncBatchCursor[BsonDocument]

  type MongoErrT[F[_], A] = EitherT[F, PhysicalError, A]

  type WorkflowExecErrT[F[_], A] = EitherT[F, WorkflowExecutionError, A]

  type JavaScriptPrg    = Vector[Js.Stmt]
  type JavaScriptLog[A] = Writer[JavaScriptPrg, A]

  // TODO: actually give these funcs their own type
  type MongoJoinType = TernaryFunc

  // TODO: parameterize over label (SD-512)
  def freshName: State[NameGen, BsonField.Name] =
    quasar.namegen.freshName("tmp").map(BsonField.Name(_))

  // TODO use implicit class
  def sortDirToBson(sort: SortDir): Bson = sort match {
    case SortDir.Ascending => Bson.Int32(1)
    case SortDir.Descending => Bson.Int32(-1)
  }
}
