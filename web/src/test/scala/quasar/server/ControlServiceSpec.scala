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

import scala.collection.Seq

import org.http4s.Uri.Authority
import org.http4s.{Method, Request, Status, Uri}
import org.http4s.server.syntax._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import shapeless.nat._

class ControlServiceSpec extends quasar.Qspec {
  sequential // These tests spin up a server and there is potential for port conflicts if
  // they don't run sequentially

  val client = org.http4s.client.blaze.defaultClient

  def withServerExpectingRestart[B](
      timeoutMillis: Long = 30000,
      initialPort: Int = 8888,
      defaultPort: Int = 8888)(causeRestart: Uri => Task[Unit])(afterRestart: Task[B]): B = {
    val uri = Uri(authority = Some(Authority(port = Some(initialPort))))

    (for {
      shutdown <- PortChangingServer.start(
        initialPort,
        reload => control.service(defaultPort, reload) orElse info.service)
      b <- (causeRestart(uri) >> afterRestart).onFinish(_ => shutdown)
    } yield b).unsafePerformSyncFor(timeoutMillis)
  }

  "Control Service" should {
    def checkRunningOn(port: Int) = {
      val req =
        Request(uri = Uri(authority = Some(Authority(port = Some(port)))), method = Method.GET)
      client.fetch(req)(response => Task.now(response.status must_== Status.Ok))
    }
    "restart on new port when it receives a PUT with a new port number (even if it might not respond properly to the request)" in {
      val Seq(startPort, newPort) = Http4sUtils.anyAvailablePorts[_2].unsafePerformSync.unsized

      withServerExpectingRestart(initialPort = startPort) { baseUri: Uri =>
        for {
          req <- Request(uri = baseUri, method = Method.PUT).withBody(newPort.toString)
          _ <- client
            .fetch(req)(Task.now)
            .attempt // Currently, sometimes the server doesn't respond back to this request
          // because it's already been killed which is why we call `attempt` and
          // ignore the result in favor of making sure the server is now running
          // on a new port
        } yield ()
      } { checkRunningOn(newPort) }
    }

    "restart on default port when it receives a DELETE request (even if it might not respond properly to the request)" in {
      val Seq(startPort, defaultPort) =
        Http4sUtils.anyAvailablePorts[_2].unsafePerformSync.unsized

      withServerExpectingRestart(initialPort = startPort, defaultPort = defaultPort) {
        baseUri: Uri =>
          val req = Request(uri = baseUri, method = Method.DELETE)
          client
            .fetch(req)(Task.now)
            .void
            .attempt
            .void // Currently, sometimes the server doesn't respond back to this request
        // because it's already been killed which is why we call `attempt` and
        // ignore the result in favor of making sure the server is now running
        // on a new port
      } { checkRunningOn(defaultPort) }
    }
  }
}
