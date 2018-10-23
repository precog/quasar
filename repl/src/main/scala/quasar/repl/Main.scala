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

package quasar
package repl

import slamdata.Predef._
import quasar.impl.external.ExternalConfig
import quasar.common.{PhaseResultCatsT, PhaseResultListen, PhaseResultTell}
import quasar.contrib.cats.writerT.{catsWriterTMonadListen_, catsWriterTMonadTell_}
import quasar.contrib.scalaz.MonadError_
import quasar.impl.schema.SstEvalConfig
import quasar.mimir.Precog
import quasar.run.{MonadQuasarErr, Quasar, QuasarError}

import java.nio.file.Path
import scala.concurrent.ExecutionContext.Implicits.global

import cats.arrow.FunctionK
import cats.effect.{ConcurrentEffect, IO, Timer}
import eu.timepit.refined.auto._
import fs2.{Stream, StreamApp}, StreamApp.ExitCode
import fs2.async.Ref
import scalaz._, Scalaz._
import shims._

object Main extends StreamApp[PhaseResultCatsT[IO, ?]] {

  type IOT[A] = PhaseResultCatsT[IO, A]

  implicit val iotQuasarError: MonadError_[IOT, QuasarError] =
    MonadError_.facet[IOT](QuasarError.throwableP)

  implicit val iotTimer: Timer[IOT] = Timer.derive

  def paths[F[_]](implicit F: cats.Applicative[F]): Stream[F, (Path, Path)] =
    for {
      basePath <- Paths.getBasePath[Stream[F, ?]]
      dataDir = basePath.resolve(Paths.QuasarDataDirName)
      _ <- Paths.mkdirs[Stream[F, ?]](dataDir)
      pluginDir = basePath.resolve(Paths.QuasarPluginsDirName)
      _ <- Paths.mkdirs[Stream[F, ?]](pluginDir)
    } yield (dataDir, pluginDir)

  def quasarStream[F[_]: ConcurrentEffect: MonadQuasarErr: PhaseResultTell: Timer]
      : Stream[F, Quasar[F]] =
    for {
      (dataPath, pluginPath) <- paths[F]
      precog <- Precog.stream(dataPath.toFile).translate(λ[FunctionK[IO, F]](_.to[F]))
      evalCfg = SstEvalConfig(1000L, 2L, 250L)
      q <- Quasar[F](precog, true, ExternalConfig.PluginDirectory(pluginPath), evalCfg)
    } yield q

  def repl[F[_]: ConcurrentEffect: MonadQuasarErr: PhaseResultListen: PhaseResultTell: Timer](
      q: Quasar[F])
      : F[ExitCode] =
    for {
      ref <- Ref[F, ReplState](ReplState.mk)
      repl <- Repl.mk[F](ref, q)
      l <- repl.loop
    } yield l

  override def stream(args: List[String], requestShutdown: IOT[Unit])
      : Stream[IOT, ExitCode] = {
    quasarStream[IOT] >>= { q: Quasar[IOT] =>
      Stream.eval(repl(q))
    }
  }
}
