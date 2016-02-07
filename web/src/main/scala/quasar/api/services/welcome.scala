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

package quasar
package api.services

import Predef._

import org.http4s.{StaticFile, MediaType}
import org.http4s.dsl._
import org.http4s.headers.`Content-Type`
import org.http4s.server.HttpService

import scalaz.concurrent.Task

object welcome {

  def resource(path: String): Task[String] = Task.delay {
    scala.io.Source.fromInputStream(getClass.getResourceAsStream(path), "UTF-8").getLines.toList.mkString("\n")
  }

  def service: HttpService = HttpService {
    case GET -> Root =>
      resource("/quasar/api/index.html").flatMap { html =>
        Ok(html
          .replaceAll("__version__", build.BuildInfo.version)
        ).withContentType(Some(`Content-Type`(MediaType.`text/html`)))
      }
    case GET -> Root / path =>
      StaticFile.fromResource("/quasar/api/" + path).fold(NotFound())(Task.now)
  }
}
