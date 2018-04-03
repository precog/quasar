/*
 * Copyright 2014–2018 SlamData Inc.
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

import slamdata.Predef._

import org.http4s.Headers
import org.http4s.server.{HttpMiddleware, Middleware}
import org.http4s.util.CaseInsensitiveString
import org.slf4j.Logger
import scalaz.concurrent.Task
import scalaz.syntax.apply._

/** Logs the details of requests using the provided logger, redacting the value of
  * the specified headers in addition to `Authorization` and cookie-related headers.
  *
  * The request line is logged at DEBUG level, headers are included at TRACE level.
  */
object RequestLogging {
  def apply(log: Logger, redactHeaders: Set[CaseInsensitiveString]): HttpMiddleware =
    Middleware { (req, svc) =>
      val logged = Task.delay {
        if (log.isDebugEnabled) {
          val requestLine =
            s"${req.method} ${req.uri} ${req.httpVersion}"

          if (log.isTraceEnabled) {
            val withHeaders =
              req.headers
                .redactSensitive(redactHeaders ++ Headers.SensitiveHeaders)
                .foldLeft(requestLine)((l, h) => l + s" | $h")

            log.trace(withHeaders)
          } else {
            log.debug(requestLine)
          }
        }
      }

      logged *> svc(req)
    }
}
