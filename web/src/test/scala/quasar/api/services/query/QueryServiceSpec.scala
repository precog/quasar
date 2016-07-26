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
import quasar.api.PathUtils
import quasar.api.matchers._
import quasar.api.ApiError
import quasar.api.ApiErrorEntityDecoder._
import quasar.fs._, InMemory._

import argonaut._, Argonaut._
import org.http4s._
import org.specs2.specification.core.Fragments
import org.specs2.ScalaCheck
import pathy.Path, Path._
import pathy.argonaut.PosixCodecJson._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._

class QueryServiceSpec extends quasar.QuasarSpecification with FileSystemFixture with PathUtils with ScalaCheck {
  import queryFixture._

  "Execute and Compile Services" should {
    def testBoth[A](test: (InMemState => HttpService) => Fragments) = {
      "Compile" should {
        test(compileService)
      }
      "Execute" should {
        test(executeService)
      }
    }

    testBoth { service =>
      "GET" >> {
        "be 404 for missing directory" >> prop { (dir: ADir, file: AFile) =>
          get(service)(
            path = dir,
            query = Some(Query(selectAll(file))),
            state = InMemState.empty,
            status = Status.NotFound,
            response = (a: String) => a must_== "???"
          )
        }.pendingUntilFixed("SD-773")

        "be 400 for missing query" >> prop { filesystem: SingleFileMemState =>
          get(service)(
            path = filesystem.parent,
            query = None,
            state = filesystem.state,
            status = Status.BadRequest,
            response = (_: ApiError) must equal(ApiError.fromStatus(
              Status.BadRequest withReason "No SQL^2 query found in URL."))
          )
        }

        "be 400 for query error" >> prop { filesystem: SingleFileMemState =>
          get(service)(
            path = filesystem.parent,
            query = Some(Query("select date where")),
            state = filesystem.state,
            status = Status.BadRequest,
            response = (_: ApiError) must beApiErrorWithMessage(
              Status.BadRequest withReason "Malformed SQL^2 query.")
          )
        }

        def asFile[B, S](dir: Path[B, Dir, S]): Option[Path[B, Path.File, S]] =
          peel(dir).flatMap {
            case (p, -\/(d)) => (p </> file(d.value)).some
            case _ => None
          }

        "be 400 for bad path (file instead of dir)" >> prop { filesystem: SingleFileMemState =>
          filesystem.parent =/= rootDir ==> {

            val parentAsFile = asFile(filesystem.parent).get

            val req = Request(uri = pathUri(parentAsFile).+??("q", selectAll(filesystem.file).some))
            val resp = service(filesystem.state)(req).unsafePerformSync
            resp.status must_== Status.BadRequest
            resp.as[ApiError].unsafePerformSync must beApiErrorWithMessage(
              Status.BadRequest withReason "Directory path expected.",
              "path" := parentAsFile)
          }
        }
      }
    }
  }

}
