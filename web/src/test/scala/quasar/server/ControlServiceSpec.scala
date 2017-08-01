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

import scala.concurrent.duration._
import scala.collection.Seq

import org.http4s.Uri.Authority
import org.http4s.client.middleware.Retry
import org.http4s.{Method, Request, Status, Uri}
import org.http4s.server.syntax._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.concurrent.Strategy.DefaultTimeoutScheduler
import shapeless.nat._

class ControlServiceSpec extends quasar.Qspec {

  val client = Retry(_ => Some(250.milliseconds))(org.http4s.client.blaze.defaultClient)

  def withServerExpectingRestart[B](timeoutMillis: Long = 30000, initialPort: Int = 8888, defaultPort: Int = 8888)
                                      (causeRestart: Uri => Task[Unit])(afterRestart: Task[B]): B = {
    val uri = Uri(authority = Some(Authority(port = Some(initialPort))))

    (for {
      server <- PortChangingServer.start(initialPort, reload => control.service(defaultPort, reload) orElse info.service)
      b <- (for {
        unconsResult <- server.servers.unconsOption
        (_, others) = unconsResult.get
        _ <- causeRestart(uri)
        unconsResult2 <- others.unconsOption
        (_, others2) = unconsResult2.get
        b <- afterRestart
        _ <- server.shutdown
        _ <- others2.run
      } yield b).timed(timeoutMillis)(DefaultTimeoutScheduler).onFinish(_ => server.shutdown)
    } yield b).unsafePerformSyncFor(timeoutMillis)
  }

  "Control Service" should {
    def checkRunningOn(port: Int) = {
      val req = Request(uri = Uri(authority = Some(Authority(port = Some(port)))), method = Method.GET)
      client.fetch(req)(response => Task.now(response.status must_== Status.Ok))
    }
    "restart on new port when PUT succeeds" in {
      val Seq(startPort, newPort) = Http4sUtils.anyAvailablePorts[_2].unsafePerformSync.unsized

      withServerExpectingRestart(initialPort = startPort){ baseUri: Uri =>
        for {
          req <- Request(uri = baseUri, method = Method.PUT).withBody(newPort.toString)
          _   <- client.fetch(req)(Task.now)
        } yield ()
      }{ checkRunningOn(newPort) }
    }.flakyTest("java.util.concurrent.TimeoutException: Timed out after 30000 milliseconds. (see SD-1532)")

    "restart on default port when DELETE succeeds" in {
      val Seq(startPort, defaultPort) = Http4sUtils.anyAvailablePorts[_2].unsafePerformSync.unsized

      withServerExpectingRestart(initialPort = startPort, defaultPort = defaultPort){ baseUri: Uri =>
        val req = Request(uri = baseUri, method = Method.DELETE)
        client.fetch(req)(Task.now).void
      }{ checkRunningOn(defaultPort) }
    }.flakyTest("java.util.concurrent.TimeoutException: Timed out after 30000 milliseconds. (see SD-1532)")
  }
}
