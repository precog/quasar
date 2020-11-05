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

package quasar.impl.datasources.middleware

import scala.{Stream => _, _}, Predef._
import java.nio.charset.StandardCharsets

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.effect.testing.specs2.CatsIO

import fs2.Stream

import org.http4s._
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl._
import org.http4s.implicits._

import org.specs2.mutable.Specification

object ResponseBodyLoggingMiddlewareSpec extends Specification
    with CatsIO
    with Http4sDsl[IO]
    with Http4sClientDsl[IO] {

  val routes = HttpRoutes.of[IO] {
    case _ => IO(Response[IO](
      Status.Ok,
      body = Stream.emits("abcdefg".getBytes(StandardCharsets.UTF_8)).chunkN(1).flatMap(Stream.chunk)))
  }

  def client(max: Int, ref: Ref[IO, String]): Client[IO] = {
    def update(s: String): IO[Unit] = ref.getAndUpdate(_ ++ s).map(_ => ())
    ResponseBodyLoggingMiddleware.apply[IO](max, update)(Client.fromHttpApp(routes.orNotFound))
  }

  "log zero chunks" >> {
    Ref.of[IO, String]("") flatMap { ref =>
      for {
        _ <- client(0, ref).fetch(GET(Uri.uri("")))(_.as[String])
        res <- ref.get
      } yield {
        res mustEqual("")
      }
    }
  }

  "log one chunk" >> {
    Ref.of[IO, String]("") flatMap { ref =>
      for {
        _ <- client(1, ref).fetch(GET(Uri.uri("")))(_.as[String])
        res <- ref.get
      } yield {
        res mustEqual("a")
      }
    }
  }

  "log two chunks" >> {
    Ref.of[IO, String]("") flatMap { ref =>
      for {
        _ <- client(2, ref).fetch(GET(Uri.uri("")))(_.as[String])
        res <- ref.get
      } yield {
        res mustEqual("ab")
      }
    }
  }

  "log three chunks" >> {
    Ref.of[IO, String]("") flatMap { ref =>
      for {
        _ <- client(3, ref).fetch(GET(Uri.uri("")))(_.as[String])
        res <- ref.get
      } yield {
        res mustEqual("abc")
      }
    }
  }
}
