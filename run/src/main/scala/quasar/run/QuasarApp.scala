/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.run

import slamdata.Predef._

import scala.concurrent.ExecutionContext

import cats.effect.{ConcurrentEffect, IO}
import fs2.Stream
import fs2.async.Promise
import scalaz._, Scalaz._
import shims._

trait QuasarApp {
  def quasarApp[F[_], A](
    quasarStream: Stream[F, Quasar[F, IO]],
    onQuasar: Quasar[F, IO] => F[A])(
    implicit F: ConcurrentEffect[F], ec: ExecutionContext)
      : F[A] =
    for {
      qDeferred <- Promise.empty[F, Quasar[F, IO]]
      sdownDeferred <- Promise.empty[F, Unit]
      _ <- F.start(quasarStream.evalMap(q => qDeferred.complete(q) *> sdownDeferred.get).compile.drain)
      q <- qDeferred.get
      res <- onQuasar(q)
      _ <- sdownDeferred.complete(())
    } yield res

  def runQuasar[A](
    quasarStream: Stream[IO, Quasar[IO, IO]],
    onQuasar: Quasar[IO, IO] => IO[Unit])(
    implicit ec: ExecutionContext)
      : IO[Unit] =
    quasarApp(quasarStream, onQuasar)

}
