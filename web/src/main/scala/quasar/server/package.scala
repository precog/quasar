/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar

import quasar.api._
import quasar.fp._
import quasar.fs.{FileSystemError, FileSystemFailure, FileSystemFailureF}
import quasar.main._
import quasar.physical.mongodb.{MongoErr, MongoErrF}

import com.mongodb.MongoException
import scalaz.{Coyoneda, ~>}
import scalaz.concurrent.Task

package object server {
  /** Interpretes errors into `Response`s, for use in web services. */
  def toResponseOr(evalCfgsIO: MntCfgsIO ~> Task): CfgsErrsIOM ~> ResponseOr = {
    val f = free.interpret3[FileSystemFailureF, MongoErrF, MntCfgsIO, ResponseOr](
      Coyoneda.liftTF[FileSystemFailure, ResponseOr](failureResponseOr[FileSystemError]),
      Coyoneda.liftTF[MongoErr, ResponseOr](failureResponseOr[MongoException]),
      liftMT[Task, ResponseT] compose evalCfgsIO)

    hoistFree(f: CfgsErrsIO ~> ResponseOr)
  }
}
