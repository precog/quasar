/*
 * Copyright 2014–2019 SlamData Inc.
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
import quasar.concurrent.BlockingContext
import quasar.contrib.iota.copkTraverse
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.sst._

import java.io.File
import scala.Console, Console.{RED, RESET}

import cats.effect.{Concurrent, ContextShift, ExitCode, IO, IOApp, Sync}
import cats.syntax.functor._
import fs2.{RaiseThrowable, Stream}
import fs2.text
import fs2.io.file
import matryoshka.data.Mu
import scalaz.\/
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.syntax.either._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import spire.std.double._

object Main extends IOApp {

  def run(args: List[String]) = {

    val blockingPool = BlockingContext.cached("quasar-datagen-blocking")

    Stream.eval(CliOptions.parse[IO](args))
      .unNone
      .flatMap(opts =>
        sstsFromFile[IO](opts.sstFile, opts.sstSource, blockingPool)
          .flatMap(generatedJson[IO])
          .take(opts.outSize.value)
          .intersperse("\n")
          .through(text.utf8Encode)
          .through(file.writeAll[IO](opts.outFile.toPath, blockingPool.unwrap, opts.writeOptions)))
      .compile
      .drain
      .redeemWith(printErrors, _ => IO.pure(ExitCode.Success))
  }

  ////

  type EJ = Mu[EJson]
  type SSTS = PopulationSST[EJ, Double] \/ SST[EJ, Double]

  // TODO: Should this be a CLI option?
  val MaxCollLength: Double = 10.0
  val Parallelism = scala.math.min(java.lang.Runtime.getRuntime().availableProcessors() / 2, 4)

  /** A stream of JSON-encoded records generated from the input `SSTS`. */
  def generatedJson[F[_]: Concurrent](ssts: SSTS): Stream[F, String] = {
    val msg = "Unable to generate data from the provided SST."

    generate.ejson[F](MaxCollLength, ssts).fold(failedStream[F, String](msg)) { gen =>
      val ejs = gen.through(codec.ejsonEncodePreciseData[F, EJ])

      if (Parallelism > 1)
        Stream.constant(ejs, 1).parJoin(Parallelism)
      else
        ejs
    }
  }

  /** A stream of `SSTS` decoded from the given file. */
  def sstsFromFile[F[_]: RaiseThrowable: Sync: ContextShift](
      src: File,
      kind: SstSource,
      blockingPool: BlockingContext)
      : Stream[F, SSTS] = {

    def decodingErr[A](t: RenderedTree, msg: String): Stream[F, A] =
      failedStream[F, A](s"Failed to decode SST: ${msg}\n\n${t.shows}")

    file.readAll[F](src.toPath, blockingPool.unwrap, 32768)
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
      IO(other.printStackTrace(Console.err)).as(ExitCode.Error)
  }

  def printError(msg: String): IO[ExitCode] =
    IO(Console.err.println(s"${RESET}${RED}[ERROR] ${msg}${RESET}"))
      .as(ExitCode.Error)
}
