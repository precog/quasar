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
import quasar.console._

import org.http4s.HttpService
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.server.{Server => Http4sServer}
import scalaz.concurrent.Task
import scalaz._, Scalaz._
import scala.concurrent.duration._
import scalaz.stream.Process
import shapeless._
import shapeless.Nat
import shapeless.ops.nat._
import shapeless.nat._

object Http4sUtils {

  final case class ServerBlueprint(port: Int, idleTimeout: Duration, svc: HttpService)

  // Lifted from unfiltered.
  // NB: available() returns 0 when the stream is closed, meaning the server
  //     will run indefinitely when started from a script.
  // jedesah (03/04/16):
  // Consider breaking out of the loop when a script is detected (`Option(System.console).isEmpty == true`?)
  // as it seems unecessary to constantly poll for user input in such a scenario.
  def waitForInput: Task[Unit] = {
    import java.lang.System

    val inputPresent = Task.delay(Option(System.console).nonEmpty && System.in.available() > 0)
                           .handle { case _ => false }

    Process.eval(inputPresent.after(250.milliseconds)).repeat.takeWhile(!_).run
  }

  def openBrowser(port: Int): Task[Unit] = {
    val url = s"http://localhost:$port/"
    Task.delay(java.awt.Desktop.getDesktop().browse(java.net.URI.create(url)))
      .or(stderr(s"Failed to open browser, please navigate to $url"))
  }

  /** Returns why the given port is unavailable or None if it is available. */
  def unavailableReason(port: Int): OptionT[Task, String] =
    OptionT(Task.delay(new java.net.ServerSocket(port)).attempt.flatMap {
      case -\/(err: java.net.BindException) => Task.now(Some(err.getMessage))
      case -\/(err)                         => Task.fail(err)
      case \/-(s)                           => Task.delay(s.close()).as(None)
    })

  /** An available port number. */
  def anyAvailablePort: Task[Int] = anyAvailablePorts[_1].map(_(0))

  /** Available port numbers. */
  def anyAvailablePorts[N <: Nat: ToInt]: Task[Sized[Seq[Int], N]] = Task.delay {
    Sized.wrap(
      List.tabulate(toInt[N]){_ => val s = new java.net.ServerSocket(0); (s, s.getLocalPort)}
          .map { case (s, p) => s.close; p})
  }

  /** Returns the requested port if available, or the next available port. */
  def choosePort(requested: Int): Task[Int] =
    unavailableReason(requested)
      .flatMapF(rsn => stderr(s"Requested port not available: $requested; $rsn") *>
        anyAvailablePort)
      .getOrElse(requested)

  /** Start `Server` with supplied [[ServerBlueprint]]
    * @param flexibleOnPort Whether or not to choose an alternative port if requested port is not available
    * @return Server that has been started along with the port on which it was started
    */
  def startServer(blueprint: ServerBlueprint, flexibleOnPort: Boolean): Task[(Http4sServer, Int)] = {
    for {
      actualPort <- if (flexibleOnPort) choosePort(blueprint.port) else Task.now(blueprint.port)
      server     <- BlazeBuilder.withIdleTimeout(blueprint.idleTimeout).bindHttp(actualPort, "0.0.0.0").mountService(blueprint.svc).start
    } yield (server, actualPort)
  }
}
