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

package quasar.api.services

import quasar.Predef._
import quasar.build

import org.http4s.Request

class WelcomeServiceSpec extends quasar.QuasarSpecification {
  "Welcome service" should {
    "show a welcome message" in {
      val req = Request()
      val resp = welcome.service(req).unsafePerformSync
      resp.as[String].unsafePerformSync must contain("quasar-logo-vector.png")
    }
    "show the current version" in {
      val req = Request()
      val resp = welcome.service(req).unsafePerformSync
      resp.as[String].unsafePerformSync must contain("Quasar " + build.BuildInfo.version)
    }
  }
}
