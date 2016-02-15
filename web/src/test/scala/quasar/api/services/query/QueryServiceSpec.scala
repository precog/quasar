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

package quasar.api.services.query

import quasar.Predef._
import quasar._, fs._
import quasar.fs.InMemory._

import argonaut._, Argonaut._
import org.http4s._
import org.http4s.server.HttpService
import org.http4s.argonaut._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class QueryServiceSpec extends org.specs2.mutable.Specification with FileSystemFixture with ScalaCheck {
  import queryFixture._

  "Execute and Compile Services" should {
    def testBoth[A](test: (InMemState => HttpService) => Unit) = {
      "Compile" should {
        test(compileService)
      }
      "Execute" should {
        test(executeService)
      }
    }

    testBoth { service =>
      "GET" >> {
        "be 404 for missing directory" ! prop { (dir: ADir, file: AFile) =>
          get(service)(
            path = dir,
            query = Some(Query(selectAll(file))),
            state = InMemState.empty,
            status = Status.NotFound,
            response = (a: String) => a must_== "???"
          )
        }.pendingUntilFixed("SD-773")
        "be 400 for missing query" ! prop { filesystem: SingleFileMemState =>
          get(service)(
            path = filesystem.parent,
            query = None,
            state = filesystem.state,
            status = Status.BadRequest,
            response = (_: Json) must_== Json("error" := "The request must contain a query")
          )
        }
        "be 400 for query error" ! prop { filesystem: SingleFileMemState =>
          get(compileService)(
            path = filesystem.parent,
            query = Some(Query("select date where")),
            state = filesystem.state,
            status = Status.BadRequest,
            response = (_: Json) must_== Json("error" := "end of input; ErrorToken(illegal character)")
          )
        }
      }

      () // TODO: Remove after upgrading to specs2 3.x
    }
  }

}
