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

import argonaut.Json
import org.http4s.{Method, Request, Status}
import org.http4s.argonaut._
import org.http4s.syntax.service._

class InfoServiceSpec extends quasar.Qspec {

  "Info Service" should {
    "be capable of providing it's name and version" in {
      val response = info.service.orNotFound(Request(method = Method.GET)).unsafePerformSync
      response.status must_== Status.Ok
      response.as[Json].unsafePerformSync must_== info.nameAndVersionInfo
    }
  }
}
