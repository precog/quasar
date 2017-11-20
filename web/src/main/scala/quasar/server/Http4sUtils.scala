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
import shapeless._
import shapeless.Nat
import shapeless.ops.nat._
import shapeless.nat._

object Http4sUtils {

  final case class ServerBlueprint(port: Int, idleTimeout: Duration, svc: HttpService)

  /**
    * Wait for the user to press enter before returning. This is a blocking operation,
    * so the thread will be suspended until it is notified of the user pressing Enter following which
    * the `Task` will complete. If the standard input stream is ended, which would usually
    * indicate that this application was launched from a script, then this Task will
    * immediately complete with a return value of `false`
    * @return `true` if we waited on user input before returning, false if returning
    *        immediately for lack of any possibility of receiving user input
    */
  def waitForUserEnter: Task[Boolean] = Task.delay {
    Option(scala.io.StdIn.readLine).isDefined
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
    * @return Server that has been started along with the port on which it was started
    */
  def startServerOnAnyPort(blueprint: ServerBlueprint): Task[(Http4sServer, Int)] = {
    for {
      actualPort <- choosePort(blueprint.port)
      server     <- startServer(blueprint.copy(port = actualPort))
    } yield (server, actualPort)
  }

  def startServer(blueprint: ServerBlueprint): Task[Http4sServer] = {
    // Extracted from http4s in order to make Server run on non-daemon threads
    def newPool(name: String, min: Int, cpuFactor: Double, timeout: Boolean) = {
      val cpus = java.lang.Runtime.getRuntime.availableProcessors
      val exec = new java.util.concurrent.ThreadPoolExecutor(
        scala.math.max(min, cpus), scala.math.max(min, (cpus * cpuFactor).ceil.toInt),
        10L, java.util.concurrent.TimeUnit.SECONDS,
        new java.util.concurrent.LinkedBlockingQueue[java.lang.Runnable],
        org.http4s.util.threads.threadFactory(i => s"${name}-$i", daemon = false))
      exec.allowCoreThreadTimeOut(timeout)
      exec
    }
    BlazeBuilder
      .enableHttp2(true)
      .withIdleTimeout(blueprint.idleTimeout)
      .bindHttp(blueprint.port, "0.0.0.0")
      .mountService(blueprint.svc)
      .withServiceExecutor(newPool("http4s-pool", 4, 3.0, false))
      .start
  }
}
