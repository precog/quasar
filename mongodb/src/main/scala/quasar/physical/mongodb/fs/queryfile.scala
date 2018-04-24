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

package quasar.physical.mongodb.fs

import slamdata.Predef._
import quasar.common.PhaseResults
import quasar.fp._
import quasar.fs._
import quasar.physical.mongodb._, WorkflowExecutor.WorkflowCursor

import com.mongodb.async.client.MongoClient
import scalaz._
import scalaz.concurrent.Task

object queryfileTypes {
  import QueryFile.ResultHandle

  type ResultCursor[C]     = List[Bson] \/ WorkflowCursor[C]
  type ResultMap[C]        = Map[ResultHandle, ResultCursor[C]]
  type EvalState[C]        = (Long, ResultMap[C])
  type QueryRT[F[_], C, A] = ReaderT[F, TaskRef[EvalState[C]], A]
  type MongoQuery[C, A]    = QueryRT[MongoDbIO, C, A]

  type QRT[F[_], A]        = QueryRT[F, BsonCursor, A]
  type MQ[A]               = QRT[MongoDbIO, A]
  type MQErr[A]            = MQ[FileSystemError \/ A]
  type MQPhErr[A]          = MQ[(PhaseResults, FileSystemError \/ A)]
}

object queryfile {
  import queryfileTypes._

  def run[C, S[_]](
    client: MongoClient,
  )(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): Task[MongoQuery[C, ?] ~> Free[S, ?]] = {
    type MQ[A] = MongoQuery[C, A]
    type F[A]  = Free[S, A]

    def runMQ(ref: TaskRef[EvalState[C]]): MQ ~> F =
      new (MQ ~> F) {
        def apply[A](mq: MQ[A]) =
          mq.run(ref).runF(client)
      }

    TaskRef((0L, Map.empty: ResultMap[C])) map runMQ
  }
}
