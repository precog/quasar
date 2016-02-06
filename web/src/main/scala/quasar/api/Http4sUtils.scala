/*
 * Copyright 2014 - 2015 SlamData Inc.
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

import quasar.Predef._
import quasar.console._
import quasar.fp._

import org.http4s.server.HttpService
import org.http4s.server.ServerBuilder
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.server.{Server => Http4sServer}
import scalaz.concurrent.Task
import scalaz._, Scalaz._
import scala.concurrent.duration.Duration
import scalaz.stream.Process
import scalaz.stream.async
import shapeless._
import shapeless.Nat
import shapeless.ops.nat._
import shapeless.nat._

object Http4sUtils {

//  sealed trait BindingType
//  case object HTTP extends BindingType
//  case object Local extends BindingType

  final case class ServerBlueprint(port: Int,
                                   idleTimeout: Duration,
                                   //bindingType: BindingType,
                                   svcs: Map[String, HttpService])

  // Lifted from unfiltered.
  // NB: available() returns 0 when the stream is closed, meaning the server
  //     will run indefinitely when started from a script.
  private def waitForInput: Task[Unit] = {
    import java.lang.System
    for {
      _    <- Task.delay(java.lang.Thread.sleep(250))
        .handle { case _: java.lang.InterruptedException => () }
      test <- Task.delay(Option(System.console).isEmpty || System.in.available() <= 0)
        .handle { case _ => true }
      done <- if (test) waitForInput else Task.now(())
    } yield done
  }

  private def openBrowser(port: Int): Task[Unit] = {
    val url = "http://localhost:" + port + "/"
    Task.delay(java.awt.Desktop.getDesktop().browse(java.net.URI.create(url)))
      .or(stderr("Failed to open browser, please navigate to " + url))
  }

  /** Returns why the given port is unavailable or None if it is available. */
  def unavailableReason(port: Int): OptionT[Task, String] =
    OptionT(Task.delay(new java.net.ServerSocket(port)).attempt.flatMap {
      case -\/(err: java.net.BindException) => Task.now(Some(err.getMessage))
      case -\/(err)                         => Task.fail(err)
      case \/-(s)                           => Task.delay(s.close()).as(None)
    })

  /** An available port number. */
  def anyAvailablePort: Task[Int] = anyAvailablePorts[_1].map(_.head)

  /** Available port numbers. */
  def anyAvailablePorts[A <: Nat: ToInt]: Task[Sized[IndexedSeq[Int], A]] = Task.delay {
    Sized.wrap(
      (1 to toInt[A])
        .map(_ => { val s = new java.net.ServerSocket(0); (s, s.getLocalPort) })
        .map { case (s, p) => { s.close; p } })
  }

  /** Returns the requested port if available, or the next available port. */
  def choosePort(requested: Int): Task[Int] =
    unavailableReason(requested)
      .flatMapF(rsn => stderr("Requested port not available: " + requested + "; " + rsn) *>
        anyAvailablePort)
      .getOrElse(requested)

  /** Start `Server` with supplied [[ServerBlueprint]]
    * @param flexibleOnPort Whether or not to choose an alternative port if requested port is not available
    * @return Server that has been started along with the port on which it was started
    */
  def startServerFromBlueprint(blueprint: ServerBlueprint, flexibleOnPort: Boolean): Task[(Http4sServer, Int)] = {
    for {
      actualPort <- if (flexibleOnPort) choosePort(blueprint.port) else Task.now(blueprint.port)
      builder = BlazeBuilder.withIdleTimeout(blueprint.idleTimeout).bindHttp(actualPort, "0.0.0.0")
      server <- startServer(blueprint.svcs, builder)
    } yield (server, actualPort)
  }

  /** Start `Server` of services with supplied `ServerBuilder`
    * @param services Map of `HttpService` indexed by where to mount them
    * @return A running `org.http4s.server.Server`
    */
  def startServer(services: Map[String, HttpService], builder: ServerBuilder): Task[Http4sServer] = {
    val builderAll = services.toList.reverse.foldLeft(builder) {
      // TODO: Consider getting rid of Prefix in favor of http4s prefix
      case (b, (path, svc)) => b.mountService(Prefix(path)(svc))
    }
    Task.delay(builderAll.run)
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
  def servers(configurations: Process[Task, ServerBlueprint], flexibleOnPort: Boolean): Process[Task, (Http4sServer,Int)] = {

    val serversAndPort = configurations.evalMap(conf =>
      startServerFromBlueprint(conf, flexibleOnPort).onSuccess { case (_, port) =>
        stdout("Server started listening on port " + port) })

    serversAndPort.evalScan1 { case ((oldServer, oldPort), newServerAndPort) =>
      oldServer.shutdown.flatMap(_ => stdout("Stopped server listening on port " + oldPort)) *>
        Task.now(newServerAndPort)
    }.cleanUpWithA{ server =>
      server.map { case (lastServer, lastPort) =>
        lastServer.shutdown.flatMap(_ => stdout("Stopped last server listening on port " + lastPort))
      }.getOrElse(Task.now(()))
    }
  }

  /** Produce a stream of servers that can be restarted on a supplied port
    * @param initialPort The port on which to start the initial server
    * @param produceRoutes A function that given a function to restart a server on a new port, supplies a server mapping
    *                      from path to `Server`
    * @return The `Task` will start the first server and provide a function to shutdown the active server.
    *         It will also return a process of servers and ports. This `Process` must be run in order for servers
    *         to actually be started and stopped. The `Process` must be run to completion in order for appropriate
    *         clean up to occur.
    */
  def startServers(initialPort: Int,
                   produceRoutes: (Int => Task[Unit]) => Map[String, HttpService]): Task[(Process[Task, (Http4sServer,Int)], Task[Unit])] = {
    val configQ = async.boundedQueue[ServerBlueprint](1)
    def startNew(port: Int): Task[Unit] = {
      val conf = ServerBlueprint(port, idleTimeout = Duration.Inf, produceRoutes(startNew))
      configQ.enqueueOne(conf)
    }
    startNew(initialPort).flatMap(_ => servers(configQ.dequeue, false).unconsOption.map {
      case None => throw new java.lang.AssertionError("should never happen")
      case Some((head, rest)) => (Process.emit(head) ++ rest, configQ.close)
    })
  }

  def startAndWait(port: Int, routes: (Int => Task[Unit]) => Map[String, HttpService], openClient: Boolean): Task[Unit] = for {
    result <- startServers(port, routes)
    (servers, shutdown) = result
    _ <- if(openClient) openBrowser(port) else Task.now(())
    _ <- stdout("Press Enter to stop.")
    _ <- Task.delay(waitForInput.runAsync(_ => shutdown.run))
    _ <- servers.run // We need to run the servers in order to make sure everything is cleaned up properly
  } yield ()
}
