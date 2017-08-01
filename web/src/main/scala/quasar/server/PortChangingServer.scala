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
import scalaz.stream.{async, Process}

/**
  * A server that can change ports on which it is serving clients when requested to do so.
  * @param servers An infinite stream of http4s servers. Every time a port changed is requested, a new server will
  *                appear at the end of this stream
  * @param shutdownImpl Should be called to cleanly shutdown this server
  */
final case class PortChangingServer(servers: Process[Task, (http4s.server.Server,Int)], shutdownImpl: Task[Unit]) {
  /** Wait for the command line user to press enter before shutting down the server */
  def shutdownOnUserInput: Task[Unit] =
    stdout("Press Enter to stop.") <*
    Task.delay(Task.fork(waitForInput).unsafePerformAsync(_ => shutdownImpl.unsafePerformSync)) <*
     servers.run
  def shutdown: Task[Unit] =
    shutdownImpl *> servers.run // We need to run the servers in order to make sure everything is cleaned up properly
}

object PortChangingServer {
  /** Produce a stream of servers that can be restarted on a supplied port
    * @param initialPort The port on which to start the initial server
    * @param produceService A function that given a function to restart a server
    *                       on a new port, returns an `HttpService`
    * @return The `Task` will start the first server and provide a function to
    *         shutdown the active server. It will also return a process of
    *         servers and ports. This `Process` must be run in order for
    *         servers to actually be started and stopped. The `Process` must be
    *         run to completion in order for appropriate clean up to occur.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  def start(
    initialPort: Int,
    produceService: (Int => Task[Unit]) => HttpService
  ): Task[PortChangingServer] = {
    val configQ = async.boundedQueue[ServerBlueprint](1)
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def startNew(port: Int): Task[Unit] = {
      val conf = ServerBlueprint(port, idleTimeout = Duration.Inf, produceService(startNew))
      configQ.enqueueOne(conf)
    }
    startNew(initialPort) >> (servers(configQ.dequeue, false).unconsOption.map {
      case None => throw new java.lang.AssertionError("should never happen")
      case Some((head, rest)) => PortChangingServer(Process.emit(head) ++ rest, configQ.close)
    })
  }

  /** Given a `Process` of [[ServerBlueprint]], returns a `Process` of `Server`.
    *
    * The returned process will emit each time a new server configuration is provided and ensures only
    * one server is running at a time, i.e. providing a new Configuration ensures
    * the previous server has been stopped.
    *
    * When the process of configurations terminates for any reason, the last server is shutdown and the
    * process of servers will terminate.
    * @param flexibleOnPort Whether or not to choose an alternative port if requested port is not available
    */
  def servers(configurations: Process[Task, ServerBlueprint], flexibleOnPort: Boolean): Process[Task, (http4s.server.Server,Int)] = {

    val serversAndPort = configurations.evalMap(conf =>
      startServer(conf, flexibleOnPort).onSuccess { case (_, port) =>
        stdout(s"Server started listening on port $port") })

    serversAndPort.evalScan1 { case ((oldServer, oldPort), newServerAndPort) =>
      oldServer.shutdown.flatMap(_ => stdout(s"Stopped server listening on port $oldPort")) *>
        Task.now(newServerAndPort)
    }.cleanUpWithA{ server =>
      server.map { case (lastServer, lastPort) =>
        lastServer.shutdown.flatMap(_ => stdout(s"Stopped last server listening on port $lastPort"))
      }.getOrElse(Task.now(()))
    }
  }
}


