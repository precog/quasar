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

package quasar.impl.local

import slamdata.Predef._

import quasar.connector.{CompressionScheme, QueryResult, MonadResourceErr, DataFormat}, DataFormat.JsonVariant
import quasar.connector.datasource.LightweightDatasourceModule

import java.nio.file.{Path => JPath}
import scala.collection.mutable.ArrayBuffer

import cats.effect.{Blocker, ContextShift, Effect, Sync, Timer}

import fs2.{compress, io, Chunk, Pipe}

import qdata.{QDataDecode, QDataEncode}
import qdata.tectonic.QDataPlate

import tectonic.{json, csv}
import tectonic.fs2.StreamParser

object LocalParsedDatasource {

  val DecompressionBufferSize: Int = 32768

  /* @param readChunkSizeBytes the number of bytes per chunk to use when reading files.
  */
  def apply[F[_]: ContextShift: Effect: MonadResourceErr: Timer, A: QDataDecode: QDataEncode](
      root: JPath,
      readChunkSizeBytes: Int,
      format: DataFormat,
      blocker: Blocker)
      : LightweightDatasourceModule.DS[F] = {

    EvaluableLocalDatasource[F](LocalParsedType, root) { iRead =>
      val rawBytes =
        io.file.readAll[F](iRead.path, blocker, readChunkSizeBytes)

      val parsedValues = rawBytes.through(encode[F, A](format))

      QueryResult.parsed[F, A](QDataDecode[A], parsedValues, iRead.stages)
    }
  }

  ////

  private val DefaultDecompressionBufferSize: Int = 32768

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def encode[F[_]: Sync, A: QDataEncode](format: DataFormat): Pipe[F, Byte, A] = {
    format match {
      case DataFormat.Json(vnt, isPrecise) =>
        val mode: json.Parser.Mode = vnt match {
          case JsonVariant.ArrayWrapped => json.Parser.UnwrapArray
          case JsonVariant.LineDelimited => json.Parser.ValueStream
        }

        StreamParser(json.Parser(QDataPlate[F, A, ArrayBuffer[A]](isPrecise), mode))(
          Chunk.buffer,
          bufs => Chunk.buffer(concatArrayBufs[A](bufs)))

      case sv: DataFormat.SeparatedValues =>
        val config = csv.Parser.Config(
          header = sv.header,
          row1 = sv.row1.toByte,
          row2 = sv.row2.toByte,
          record = sv.record.toByte,
          openQuote = sv.openQuote.toByte,
          closeQuote = sv.closeQuote.toByte,
          escape = sv.escape.toByte)

        StreamParser(csv.Parser(QDataPlate[F, A, ArrayBuffer[A]](false), config))(
          Chunk.buffer,
          bufs => Chunk.buffer(concatArrayBufs[A](bufs)))

      case DataFormat.Compressed(CompressionScheme.Gzip, pt) =>
        compress.gunzip[F](DefaultDecompressionBufferSize) andThen encode[F, A](pt)
    }
  }

  private def concatArrayBufs[A](bufs: List[ArrayBuffer[A]]): ArrayBuffer[A] = {
    val totalSize = bufs.foldLeft(0)(_ + _.length)
    bufs.foldLeft(new ArrayBuffer[A](totalSize))(_ ++= _)
  }
}
