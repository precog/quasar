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

import fs2.Stream
import fs2.{io, text}
import fs2.interop.scalaz._
import fs2.util.Suspendable
import matryoshka.data.Mu
import scalaz.{\/, ImmutableArray}
import scalaz.concurrent._
import scalaz.std.anyVal._
import scalaz.syntax.either._
import scalaz.syntax.show._
import spire.std.double._

object Main extends TaskApp {

  override def run(args: ImmutableArray[String]) =
    Stream.eval(CliOptions.parse[Task](args))
      .unNone
      .flatMap(opts =>
        sstsFromFile[Task](opts.sstFile, opts.sstSource)
          .flatMap(generatedJson[Task])
          .take(opts.outSize.value)
          .intersperse("\n")
          .through(text.utf8Encode)
          .through(io.file.writeAllAsync(opts.outFile.toPath, opts.writeOptions)))
      .run
      .handleWith(printErrors)

  ////

  type EJ = Mu[EJson]
  type SSTS = PopulationSST[EJ, Double] \/ SST[EJ, Double]

  // TODO: Should this be a CLI option?
  val MaxCollLength: Double = 10.0

  /** A stream of JSON-encoded records generated from the input `SSTS`. */
  def generatedJson[F[_]: Suspendable](ssts: SSTS): Stream[F, String] =
    generate.ejson[F](MaxCollLength, ssts)
      .getOrElse(failedStream("Unable to generate data from the provided SST."))
      .through(codec.ejsonEncodePreciseData[F, EJ])

  /** A stream of `SSTS` decoded from the given file. */
  def sstsFromFile[F[_]: Suspendable](src: File, kind: SstSource): Stream[F, SSTS] = {
    def decodingErr[F[_], A](t: RenderedTree, msg: String): Stream[F, A] =
      failedStream(s"Failed to decode SST: ${msg}\n\n${t.shows}")

    io.file.readAll[F](src.toPath, 4096)
      .through(text.utf8Decode)
      .through(text.lines)
      .take(1)
      .through(codec.ejsonDecodePreciseData[F, EJ])
      .flatMap(ej =>
        kind.fold(
          ej.decodeAs[PopulationSST[EJ, Double]] map (_.left),
          ej.decodeAs[SST[EJ, Double]] map (_.right)
        ).fold(decodingErr, Stream.emit))
  }

  val printErrors: PartialFunction[Throwable, Task[Unit]] = {
    case alreadyExists: java.nio.file.FileAlreadyExistsException =>
      printError(s"Output file already exists: ${alreadyExists.getFile}.")

    case notFound: java.nio.file.NoSuchFileException =>
      printError(s"SST file not found: ${notFound.getFile}.")

    case other =>
      printError(other.getMessage)
  }

  def printError(msg: String): Task[Unit] =
    Task.delay(Console.err.println(s"${RESET}${RED}[ERROR] ${msg}${RESET}"))
}
