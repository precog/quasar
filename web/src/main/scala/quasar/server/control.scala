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

import quasar.Predef.{ -> => _, _ }

import org.http4s.dsl._
import org.http4s.HttpService

import scalaz._, Scalaz._
import scalaz.concurrent.Task

object control {

  /** A service that exposes the ability to change the port the server is listens on.
    * @param defaultPort The default port for this server. Will restart the server on this port
    *                    if the `DELETE` http method is used.
    * @param restart A function that will restart the server on the specified port
    */
  def service(defaultPort: Int, restart: Int => Task[Unit]): HttpService = HttpService {
    case req @ PUT -> Root =>
      req.as[String].flatMap(body =>
        body.parseInt.fold(
          e => BadRequest(e.getMessage),
          portNum => Http4sUtils.unavailableReason(portNum).run.flatMap { possibleReason =>
            possibleReason map { reason =>
              PreconditionFailed(s"Could not restart on new port because $reason")
            } getOrElse {
              (restart(portNum) *> Accepted(s"Restarting on port $portNum")) handleWith {
                case e => InternalServerError(s"Failed to restart on port $portNum")
              }
            }
          }
        )
      )

    case DELETE -> Root =>
      restart(defaultPort) *> Accepted(s"Restarting on default port $defaultPort")
  }
}
