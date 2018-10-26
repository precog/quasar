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
import quasar.yggdrasil.vfs.contextShiftForS

import java.nio.file.Path
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext, ExecutionContext.Implicits.global

import cats.arrow.FunctionK
import cats.effect._
import cats.effect.concurrent.Ref
import eu.timepit.refined.auto._
import fs2.Stream
import scalaz._, Scalaz._
import shims._

object Main extends IOApp {

  type IOT[A] = PhaseResultCatsT[IO, A]

  implicit val iotQuasarError: MonadError_[IOT, QuasarError] =
    MonadError_.facet[IOT](QuasarError.throwableP)

  implicit val iotTimer: Timer[IOT] = Timer.deriveWriterT

  def paths[F[_]](implicit F: Sync[F]): Stream[F, (Path, Path)] =
    for {
      basePath <- Stream.eval(Paths.getBasePath[F])
      dataDir = basePath.resolve(Paths.QuasarDataDirName)
      _ <- Stream.eval(Paths.mkdirs[F](dataDir))
      pluginDir = basePath.resolve(Paths.QuasarPluginsDirName)
      _ <- Stream.eval(Paths.mkdirs[F](pluginDir))
    } yield (dataDir, pluginDir)

  def quasarStream[F[_]: ConcurrentEffect: ContextShift: MonadQuasarErr: PhaseResultTell: Timer](
    blockingPool: ExecutionContext)
      : Stream[F, Quasar[F]] =
    for {
      (dataPath, pluginPath) <- paths[F]
      precog <- Precog.stream(dataPath.toFile, blockingPool).translate(λ[FunctionK[IO, F]](_.to[F]))
      evalCfg = SstEvalConfig(1000L, 2L, 250L)
      q <- Quasar[F](precog, ExternalConfig.PluginDirectory(pluginPath), evalCfg, blockingPool)
    } yield q

  def repl[F[_]: ConcurrentEffect: ContextShift: MonadQuasarErr: PhaseResultListen: PhaseResultTell: Timer](
      q: Quasar[F])
      : F[ExitCode] =
    for {
      ref <- Ref.of[F, ReplState](ReplState.mk)
      repl <- Repl.mk[F](ref, q)
      l <- repl.loop
    } yield l

  override def run(args: List[String]): IO[ExitCode] = {
    val blockingPool = ExecutionContext.fromExecutor(Executors.newCachedThreadPool)
    val s: Stream[IOT, ExitCode] =
      quasarStream[IOT](blockingPool) >>= { q: Quasar[IOT] =>
        Stream.eval(repl(q))
      }
    s.compile.last.run.map(_._2.getOrElse(ExitCode.Success))
  }

}
