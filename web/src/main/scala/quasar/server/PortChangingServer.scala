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
import quasar.console.stdout
import quasar.fp._
import quasar.server.Http4sUtils._

import scala.concurrent.duration._

import org.http4s
import org.http4s.HttpService
import scalaz._, Scalaz._
import scalaz.concurrent.Task

/**
 * A server that can change ports on which it is serving clients when requested to do so.
 * A known limitation is that this server will always fail to send a response to a request
 * makes use of restart function.
 */
object PortChangingServer {
  type ServiceStarter = (Int => Task[String \/ Unit]) => HttpService

  /** Start this server
   * @param initialPort The port on which to start the initial server
   * @param produceService A function that given a function to restart a server
   *                       on a new port, returns an `HttpService`
   * @return A function to shutdown the active server
   */
  def start(
      initialPort: Int,
      produceService: ServiceStarter
  ): Task[Task[Unit]] =
    TaskRef[Option[(http4s.server.Server, Int)]](None).flatMap { serverRef =>
      def shutdown(s: Option[(http4s.server.Server, Int)]): Task[Unit] =
        s.fold(Task.now(())) {
          case (server, port) =>
            server.shutdown >> stdout(s"Stopped server listening on port $port")
        }
      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def startNew(port: Int): Task[String \/ Unit] = {
        for {
          oldServer <- serverRef.read
          newServer <- startServer(
            ServerBlueprint(port, idleTimeout = Duration.Inf, produceService(startNew)))
          succeeded <- serverRef.compareAndSet(oldServer, (newServer, port).some)
          result <- if (succeeded)
            for {
              _ <- shutdown(oldServer)
              _ <- stdout(s"Server started listening on port $port")
            } yield ().right
          else
            newServer.shutdown.as("Concurrent attempt to restart server. Try again later".left)
        } yield result
      }
      startNew(initialPort).as(serverRef.read.flatMap(shutdown))
    }
}
