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

package quasar.server

import slamdata.Predef._

import scalaz._

class StaticContentSpec extends quasar.Qspec {
  import StaticContent.fromCliOptions

  val defLoc = "/static"

  "fromCliOptions" should {
    "be empty with defaults" in {
      fromCliOptions(defLoc, CliOptions.default).run.unsafePerformSync must beRightDisjunction(None)
    }

    "fail with loc and no path" in {
      val opts = CliOptions.default.copy(contentLoc = Some("foo"))
      fromCliOptions(defLoc, opts).run.unsafePerformSync must beLeftDisjunction
    }

    "use supplied default location when none specified" in {
      val opts = CliOptions.default.copy(contentPath = Some("foo"))
      fromCliOptions(defLoc, opts).run.unsafePerformSync must beRightDisjunction(Some(StaticContent("/static", "foo")))
    }

    "handle loc and path" in {
      val opts = CliOptions.default.copy(contentLoc = Some("/foo"), contentPath = Some("bar"))
      fromCliOptions(defLoc, opts).run.unsafePerformSync must beRightDisjunction(Some(StaticContent("/foo", "bar")))
    }

    "relative" in {
      val opts = CliOptions.default.copy(contentPath = Some("foo"), contentPathRelative = true)
      fromCliOptions(defLoc, opts).run.unsafePerformSync must beLike {
        case \/-(Some(StaticContent(_, path))) => path must endWith("/foo")
      }
    }
  }
}
