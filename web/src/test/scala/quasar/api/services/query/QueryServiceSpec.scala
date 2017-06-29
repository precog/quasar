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

package quasar.api.services.query

import slamdata.Predef._
import quasar.api.ApiError
import quasar.api.ApiError._
import quasar.api.ApiErrorEntityDecoder._
import quasar.api.PathUtils._
import quasar.api.matchers._
import quasar.contrib.pathy._
import quasar.fs._, InMemory._, mount._

import argonaut._, Argonaut._
import org.http4s._
import org.http4s.argonaut._
import org.specs2.specification.core.Fragment
import pathy.Path, Path._
import pathy.argonaut.PosixCodecJson._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._

class QueryServiceSpec extends quasar.Qspec with FileSystemFixture {
  import queryFixture._

  "Execute and Compile Services" should {
    def testBoth[A](test: ((InMemState, Map[APath, MountConfig]) => HttpService) => Fragment) = {
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

        "be 400 if variables are missing and return which variables are missing" >> {
          "one variable" >> prop { filesystem: SingleFileMemState =>
            get(service)(
              path = filesystem.parent,
              query = Some(Query("select * from :foo")),
              state = filesystem.state,
              status = Status.BadRequest,
              response = (_: Json) must_=== Json(
                "error" -> Json(
                  "status" := "Unbound variable.",
                  "detail" -> Json(
                    "message" := "There is no binding for the variable :foo",
                    "varName" := "foo")))
            )
          }
          "multiple variables" >> prop { filesystem: SingleFileMemState =>
            get(service)(
              path = filesystem.parent,
              query = Some(Query("select * from :foo where :baz")),
              state = filesystem.state,
              status = Status.BadRequest,
              response = (_: Json) must_=== Json(
                "error" -> Json(
                  "status" := "Multiple errors",
                  "detail" -> Json(
                    "errors" := List(
                      Json(
                        "status" := "Unbound variable.",
                        "detail" -> Json(
                          "message" := "There is no binding for the variable :foo",
                          "varName" := "foo")),
                      Json(
                        "status" := "Unbound variable.",
                        "detail" -> Json(
                          "message" := "There is no binding for the variable :baz",
                          "varName" := "baz"))))))
            )
          }
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
            val resp = service(filesystem.state, Map.empty)(req).unsafePerformSync
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
