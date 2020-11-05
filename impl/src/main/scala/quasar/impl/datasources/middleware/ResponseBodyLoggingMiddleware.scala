/*
 * Copyright 2020 Precog Data
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

package quasar.impl
package datasources.middleware

import scala._, Predef._

import cats.effect.{Resource, Sync}

import org.http4s.{Request, Response}
import org.http4s.client.Client

object ResponseBodyLoggingMiddleware {

  def apply[F[_]: Sync](
      max: Int,
      log: String => F[Unit])(
      client: Client[F])
      : Client[F] = {

    def logBody(req: Request[F]): Resource[F, Response[F]] =
      client.run(req) map { res =>
        val body = LoggingUtils.logFirstN[F](res.body, max, log)
        res.copy(body = body)
      }

    Client[F](logBody)
  }
}
