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

import slamdata.Predef.{ -> => _, _ }

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
  def service(defaultPort: Int, restart: Int => Task[Unit]): HttpService = {
    def safeRestart(port: Int, default: Boolean, currentPort: Int) =
      if (currentPort ≟ port)
        Ok(s"Server is already running on port $port")
      else
        Http4sUtils.unavailableReason(port).run.flatMap { possibleReason =>
          possibleReason map { reason =>
            PreconditionFailed(s"Could not restart on new port because $reason")
          } getOrElse {
            val defaultString = if(default) " default " else ""
            (restart(port) *> Accepted(s"Restarting on $defaultString port $port")) handleWith {
              case e: Exception => InternalServerError(s"Failed to restart on port $port because ${e.getMessage}")
              case _            => InternalServerError(s"Failed to restart on port $port for unknown reason")
            }
          }
        }

    HttpService {
      case req @ PUT -> Root =>
        req.as[String].flatMap(body =>
          body.parseInt.fold(
            e => BadRequest(e.getMessage),
            portNum => safeRestart(portNum, default = false, req.serverPort)))

      case req @ DELETE -> Root =>
        safeRestart(defaultPort, default = true, req.serverPort)
    }
  }
}
