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

package quasar.fs.mount

import slamdata.Predef._
import quasar.QuasarSpecification
import quasar.connector.EnvironmentError
import quasar.fp.ski.κ
import quasar.fs._

import monocle.std.{disjunction => D}
import monocle.function.Cons1
import scalaz._, Scalaz._

class BackendDefSpec extends QuasarSpecification {
  import BackendDef._, EnvironmentError._

  type DefId[A] = DefErrT[Id, A]

  val defnResult =
    DefinitionResult[Id](Empty.backendEffect[Id], ())

  val successfulDef =
    BackendDef(κ(defnResult.point[DefId].some))

  val failedDef =
    BackendDef(κ(Some(
      connectionFailed(new RuntimeException("NOPE"))
        .right[NonEmptyList[String]]
        .raiseError[DefId, DefinitionResult[Id]])))

  val unhandledDef =
    BackendDef[Id](κ(None))

  val someType = FileSystemType("somefs")
  val someUri = ConnectionUri("some://filesystem")

  val firstErrMsg =
    D.left[DefinitionError, DefinitionResult[Id]]  composePrism
    D.left[NonEmptyList[String], EnvironmentError] composeLens
    Cons1.head

  "apply" should {
    "return an unsupported filesytem error when unhandled" >> {
      firstErrMsg.getOption(unhandledDef(someType, someUri).run) must
        beSome(startWith("Unsupported filesystem"))
    }
  }

  "orElse" should {
    "return the first result when handled success" >> {
      successfulDef.orElse(failedDef)(someType, someUri).run must be_\/-
    }

    "return the first result when handled failure" >> {
      failedDef.orElse(successfulDef)(someType, someUri).run must be_-\/
    }

    "return the second result when unhandled" >> {
      unhandledDef.orElse(successfulDef)(someType, someUri).run must be_\/-
    }
  }
}
