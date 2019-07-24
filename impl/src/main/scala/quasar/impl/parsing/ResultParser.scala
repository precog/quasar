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

package quasar.impl.parsing

import slamdata.Predef._

import quasar.ScalarStages
import quasar.connector.{CompressionScheme, ParsableType, QueryResult}
import quasar.connector.ParsableType.JsonVariant

import java.lang.IllegalArgumentException

import scala.collection.mutable.ArrayBuffer

import cats.effect.Sync

import fs2.{gzip, Chunk, Stream}

import qdata.{QData, QDataEncode}
import qdata.tectonic.QDataPlate

import scalaz.syntax.equal._

import tectonic.fs2.StreamParser
import tectonic.json.{Parser => TParser}

object ResultParser {
  val DefaultDecompressionBufferSize: Int = 32768

  def apply[F[_]: Sync, A: QDataEncode](queryResult: QueryResult[F]): Stream[F, A] = {
    def concatArrayBufs(bufs: List[ArrayBuffer[A]]): ArrayBuffer[A] = {
      val totalSize = bufs.foldLeft(0)(_ + _.length)
      bufs.foldLeft(new ArrayBuffer[A](totalSize))(_ ++= _)
    }

    @tailrec
    def parsedStream(qr: QueryResult[F]): Stream[F, A] =
      qr match {
        case QueryResult.Parsed(qdd, data, _) =>
          data.map(QData.convert(_)(qdd, QDataEncode[A]))

        case QueryResult.Compressed(CompressionScheme.Gzip, content) =>
          parsedStream(content.modifyBytes(gzip.decompress[F](DefaultDecompressionBufferSize)))

        case QueryResult.Typed(js @ ParsableType.Json(vnt, isPrecise), data, _) =>
          val mode: TParser.Mode = vnt match {
            case JsonVariant.ArrayWrapped => TParser.UnwrapArray
            case JsonVariant.LineDelimited => TParser.ValueStream
          }

          val parser =
            TParser(QDataPlate[F, A, ArrayBuffer[A]](isPrecise), mode)

          val parserPipe =
            StreamParser(parser)(Chunk.buffer, bufs => Chunk.buffer(concatArrayBufs(bufs)))

          data.through(parserPipe)
      }

    if (queryResult.stages === ScalarStages.Id)
      parsedStream(queryResult)
    else
      // TODO: Be nice to have a static representation of the absence of parse instructions
      Stream.raiseError(new IllegalArgumentException("ParseInstructions not supported."))
  }
}
