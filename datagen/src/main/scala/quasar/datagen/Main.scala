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

package quasar.datagen

import slamdata.Predef.{Stream => _, _}
import quasar.RenderedTree
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.sst._
import quasar.contrib.iota.copkTraverse

import java.io.File
import scala.Console, Console.{RED, RESET}

import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.syntax.functor._
import fs2.Stream
import fs2.text
import fs2.io.file
import matryoshka.data.Mu
import scalaz.\/
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.syntax.either._
import scalaz.syntax.show._
import spire.std.double._

object Main extends IOApp {

  def run(args: List[String]) =
    Stream.eval(CliOptions.parse[IO](args))
      .unNone
      .flatMap(opts =>
        sstsFromFile[IO](opts.sstFile, opts.sstSource)
          .flatMap(generatedJson[IO])
          .take(opts.outSize.value)
          .intersperse("\n")
          .through(text.utf8Encode)
          .through(file.writeAllAsync(opts.outFile.toPath, opts.writeOptions)))
      .compile
      .drain
      .redeemWith(printErrors, _ => IO.pure(ExitCode.Success))

  ////

  type EJ = Mu[EJson]
  type SSTS = PopulationSST[EJ, Double] \/ SST[EJ, Double]

  // TODO: Should this be a CLI option?
  val MaxCollLength: Double = 10.0

  /** A stream of JSON-encoded records generated from the input `SSTS`. */
  def generatedJson[F[_]: Sync](ssts: SSTS): Stream[F, String] =
    generate.ejson[F](MaxCollLength, ssts)
      .getOrElse(failedStream("Unable to generate data from the provided SST."))
      .through(codec.ejsonEncodePreciseData[F, EJ])

  /** A stream of `SSTS` decoded from the given file. */
  def sstsFromFile[F[_]: Sync](src: File, kind: SstSource): Stream[F, SSTS] = {
    def decodingErr[F[_], A](t: RenderedTree, msg: String): Stream[F, A] =
      failedStream(s"Failed to decode SST: ${msg}\n\n${t.shows}")

    file.readAll[F](src.toPath, 4096)
      .through(text.utf8Decode)
      .through(text.lines)
      .take(1)
      .through(codec.ejsonDecodePreciseData[F, EJ])
      .flatMap(ej =>
        kind.fold(
          ej.decodeAs[PopulationSST[EJ, Double]] map (_.left),
          ej.decodeAs[SST[EJ, Double]] map (_.right)
        ).fold(decodingErr, Stream.emit(_).covary[F]))
  }

  val printErrors: PartialFunction[Throwable, IO[ExitCode]] = {
    case alreadyExists: java.nio.file.FileAlreadyExistsException =>
      printError(s"Output file already exists: ${alreadyExists.getFile}.")

    case notFound: java.nio.file.NoSuchFileException =>
      printError(s"SST file not found: ${notFound.getFile}.")

    case other =>
      printError(other.getMessage)
  }

  def printError(msg: String): IO[ExitCode] =
    IO(Console.err.println(s"${RESET}${RED}[ERROR] ${msg}${RESET}"))
      .as(ExitCode.Error)
}
