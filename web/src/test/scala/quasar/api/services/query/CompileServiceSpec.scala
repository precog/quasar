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
import quasar.fs._, InMemory.InMemState

import argonaut.{Json => AJson}
import org.http4s._
import org.http4s.argonaut._
import pathy.Path._, posixCodec._
import pathy.scalacheck.AlphaCharacters
import rapture.json._, jsonBackends.json4s._, patternMatching.exactObjects._
import scalaz._, Scalaz._

class CompileServiceSpec extends quasar.Qspec with FileSystemFixture {
  import queryFixture._

  "Compile" should {

    "plan simple query" >> prop { filesystem: SingleFileMemState =>
      // Representation of the directory as a string without the leading slash
      val pathString = printPath(filesystem.file).drop(1)
      get[AJson](compileService)(
        path = filesystem.parent,
        query = Some(Query(selectAll(file1(filesystem.filename)))),
        state = filesystem.state,
        status = Status.Ok,
        response = json => Json.parse(json.nospaces) must beLike {
          case json"""{ "type": "in-memory", "inputs": $inputs, "physicalPlan": $physicalPlan }""" =>
            inputs.as[List[String]] must_=== List(filesystem.file).map(printPath)
        }
      )
    }

    "plan query with var" >> prop { (filesystem: SingleFileMemState, varName: AlphaCharacters, var_ : Int) =>
      val pathString = printPath(filesystem.file).drop(1)
      val query = selectAllWithVar(file1(filesystem.filename),varName.value)
      get[AJson](compileService)(
        path = filesystem.parent,
        query = Some(Query(query,varNameAndValue = Some((varName.value, var_.toString)))),
        state = filesystem.state,
        status = Status.Ok,
        response = json => Json.parse(json.nospaces) must beLike {
          case json"""{ "type": "in-memory", "inputs": $inputs, "physicalPlan": $physicalPlan }""" =>
            inputs.as[List[String]] must_=== List(filesystem.file).map(printPath)
        }
      )
    }

    "return all inputs of a query" >> {
      get[AJson](compileService)(
        path = rootDir </> dir("foo"),
        query = Some(Query("""SELECT c1.user, c2.type FROM `/users` as c1 JOIN `events` as c2 ON c1.`_id` = c2.userId""")),
        state = InMemState.empty,
        status = Status.Ok,
        response = json => Json.parse(json.nospaces) must beLike {
          case json"""{ "type": "in-memory", "inputs": $inputs, "physicalPlan": $physicalPlan }""" =>
            ISet.fromFoldable(inputs.as[List[String]]) must_=== ISet.fromFoldable(List(
              rootDir </> file("users"),
              rootDir </> dir("foo") </> file("events")
            ).map(printPath))
        }
      )
    }

    "plan a constant query" >> {
      get[AJson](compileService)(
        path = rootDir </> dir("foo"),
        query = Some(Query("4 + 3")),
        state = InMemState.empty,
        status = Status.Ok,
        response = json => Json.parse(json.nospaces) must_=== json""" { "type": "constant", "value": [7] }""")
    }

  }

}
