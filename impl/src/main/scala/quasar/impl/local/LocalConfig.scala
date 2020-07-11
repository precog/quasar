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

import slamdata.Predef.{Int, Option, String, StringContext}

import quasar.connector.{CompressionScheme, DataFormat}

import argonaut.{CodecJson, DecodeResult}
import argonaut.Argonaut._

import cats.{Eq, Show}
import cats.instances.int._
import cats.instances.string._
import cats.instances.tuple._
import cats.syntax.show._

final case class LocalConfig(
    rootDir: String,
    readChunkSizeBytes: Int,
    format: DataFormat)

object LocalConfig {
  /** Default to 1MB chunks when unspecified. */
  val DefaultReadChunkSizeBytes: Int = 1048576

  implicit val codecCompressionScheme: CodecJson[CompressionScheme] = CodecJson({
      case CompressionScheme.Gzip => "gzip".asJson
    }, (c => c.as[String] flatMap {
      case "gzip" => DecodeResult.ok(CompressionScheme.Gzip)
      case _ => DecodeResult.fail("CompressionScheme", c.history)
    }))

  implicit val codecLocalConfig: CodecJson[LocalConfig] = CodecJson({
    case LocalConfig(rootDir, chunkSize, dataFormat) =>
      ("rootDir" := rootDir) ->:
      ("readChunkSizeBytes" := chunkSize) ->:
      dataFormat.asJson
  }, (c => for {
    rootDir <- (c --\ "rootDir").as[String]
    maybeChunkSize <- (c --\ "readChunkSizeBytes").as[Option[Int]]
    chunkSize = maybeChunkSize getOrElse DefaultReadChunkSizeBytes
    format = c.as[DataFormat].toOption getOrElse DataFormat.precise(DataFormat.ldjson)
  } yield LocalConfig(rootDir, chunkSize, format)))

  implicit val eqv: Eq[LocalConfig] =
    Eq.by(lc => (lc.rootDir, lc.readChunkSizeBytes, lc.format))

  implicit val show: Show[LocalConfig] =
    Show.show(lc =>
      s"LocalConfig(${lc.rootDir}, ${lc.readChunkSizeBytes}, ${lc.format.show}")
}
