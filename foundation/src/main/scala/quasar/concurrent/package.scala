/*
 * Copyright 2014–2020 SlamData Inc.
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

import slamdata.Predef.String
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

import cats.{effect => ce}

package object concurrent {

  object Blocker {
    def apply(ec: ExecutionContext): ce.Blocker =
      ce.Blocker.liftExecutionContext(ec)

    def cached(name: String): ce.Blocker =
      apply(ExecutionContext.fromExecutor(Executors.newCachedThreadPool(NamedDaemonThreadFactory(name))))
  }
}
