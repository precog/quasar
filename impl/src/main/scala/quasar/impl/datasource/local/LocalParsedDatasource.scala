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

package quasar.impl.datasource.local

import slamdata.Predef._

import quasar.concurrent.BlockingContext
import quasar.connector._, LightweightDatasourceModule.DS

import java.nio.file.{Path => JPath}

import cats.effect.{ContextShift, Effect, Timer}
import fs2.{gzip, io, Pipe}
import jawnfs2._
import org.typelevel.jawn.Facade
import qdata.{QDataDecode, QDataEncode}
import qdata.json.QDataFacade
import scalaz.syntax.tag._

object LocalParsedDatasource {

  val DecompressionBufferSize: Int = 32768

  /* @param readChunkSizeBytes the number of bytes per chunk to use when reading files.
  */
  def apply[F[_]: ContextShift: Effect: MonadResourceErr: Timer, A: QDataDecode: QDataEncode](
      root: JPath,
      readChunkSizeBytes: Int,
      format: ParsableType,
      compressionScheme: Option[CompressionScheme],
      blockingPool: BlockingContext)
      : DS[F] = {

    import ParsableType.JsonVariant

    def parsedJson(
        variant: JsonVariant,
        precise: Boolean)
        : Pipe[F, Byte, A] = {

      implicit val facade: Facade[A] = QDataFacade(isPrecise = precise)

      val parser = variant match {
        case JsonVariant.ArrayWrapped => unwrapJsonArray[F, ByteBuffer, A]
        case JsonVariant.LineDelimited => parseJsonStream[F, ByteBuffer, A]
      }

      _.chunks.map(_.toByteBuffer).through(parser)
    }

    EvaluableLocalDatasource[F](LocalParsedType, root) { iRead =>
      val rawBytes =
        io.file.readAll[F](iRead.path, blockingPool.unwrap, readChunkSizeBytes)

      val decompressedBytes = compressionScheme match {
        case Some(CompressionScheme.Gzip) =>
          rawBytes.through(gzip.decompress[F](DecompressionBufferSize))

        case None =>
          rawBytes
      }

      val parsedValues = format match {
        case ParsableType.Json(variant, isPrecise) =>
          decompressedBytes.through(parsedJson(variant, isPrecise))
      }

      QueryResult.parsed[F, A](QDataDecode[A], parsedValues, iRead.stages)
    }
  }
}
