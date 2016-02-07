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

package quasar.api

import quasar.Predef._

import org.specs2.mutable._
import org.specs2.scalaz._

import scalaz._

class ServerSpec extends Specification with DisjunctionMatchers {
  import ServerOps.StaticContent, Server._

  val defaultOpts = ServerOps.Options(None, None, None, false, false, None)

  "interpretPaths" should {
    "be empty with defaults" in {
      interpretPaths(defaultOpts).run.run must beRightDisjunction(None)
    }

    "fail with loc and no path" in {
      val opts = defaultOpts.copy(contentLoc = Some("foo"))
      interpretPaths(opts).run.run must beLeftDisjunction
    }

    "default to /files" in {
      val opts = defaultOpts.copy(contentPath = Some("foo"))
      interpretPaths(opts).run.run must beRightDisjunction(Some(StaticContent("/files", "foo")))
    }

    "handle loc and path" in {
      val opts = defaultOpts.copy(contentLoc = Some("/foo"), contentPath = Some("bar"))
      interpretPaths(opts).run.run must beRightDisjunction(Some(StaticContent("/foo", "bar")))
    }

    "relative" in {
      val opts = defaultOpts.copy(contentPath = Some("foo"), contentPathRelative = true)
      (interpretPaths(opts).run.run match {
        case \/-(Some(StaticContent(_, path))) => path must endWith("/foo")
        case _ => failure
      }): org.specs2.execute.Result
    }
  }
}
