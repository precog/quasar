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

package quasar.server

import quasar.Predef._

import org.http4s.{Status, Method, Request}
import org.http4s.argonaut._
import org.specs2.mutable

import argonaut.Json

class InfoServiceSpec extends mutable.Specification {

  "Info Service" should {
    "be capable of providing it's name and version" in {
      val response = info.service(Request(method = Method.GET)).run
      response.status must_== Status.Ok
      response.as[Json].run must_== info.nameAndVersionInfo
    }
  }
}
