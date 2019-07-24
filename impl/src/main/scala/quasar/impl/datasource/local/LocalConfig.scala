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

import slamdata.Predef.{Boolean, Int, Option, String, StringContext}

import quasar.connector.{CompressionScheme, ParsableType}

import argonaut.{DecodeJson, DecodeResult}

import cats.{Eq, Show}
import cats.instances.int._
import cats.instances.option._
import cats.instances.string._
import cats.instances.tuple._
import cats.syntax.show._

import shims._

final case class LocalConfig(
    rootDir: String,
    readChunkSizeBytes: Int,
    format: ParsableType,
    compressionScheme: Option[CompressionScheme])

object LocalConfig {
  /** Default to 1MB chunks when unspecified. */
  val DefaultReadChunkSizeBytes: Int = 1048576

  implicit val decodeJson: DecodeJson[LocalConfig] = {
    import ParsableType.JsonVariant

    val preciseLdjson = ParsableType.json(JsonVariant.LineDelimited, true)

    implicit val decodeCompressionScheme: DecodeJson[CompressionScheme] =
      DecodeJson(c => c.as[String] flatMap {
        case "gzip" => DecodeResult.ok(CompressionScheme.Gzip)
        case _ => DecodeResult.fail("CompressionScheme", c.history)
      })

    implicit val decodeJsonVariant: DecodeJson[JsonVariant] =
      DecodeJson(c => c.as[String] flatMap {
        case "array-wrapped" => DecodeResult.ok(JsonVariant.ArrayWrapped)
        case "line-delimited" => DecodeResult.ok(JsonVariant.LineDelimited)
        case _ => DecodeResult.fail("JsonVariant", c.history)
      })

    implicit val decodeParsableType: DecodeJson[ParsableType] =
      DecodeJson(c => (c --\ "type").as[String] flatMap {
        case "json" =>
          for {
            variant <- (c --\ "variant").as[JsonVariant]
            precise <- (c --\ "precise").as[Option[Boolean]]
          } yield ParsableType.json(variant, precise getOrElse false)

        case _ => DecodeResult.fail("ParsableType", c.history)
      })

    DecodeJson(c => for {
      rootDir <- (c --\ "rootDir").as[String]

      maybeChunkSize <- (c --\ "readChunkSizeBytes").as[Option[Int]]
      chunkSize = maybeChunkSize getOrElse DefaultReadChunkSizeBytes

      maybeFormat <- (c --\ "format").as[Option[ParsableType]]
      format = maybeFormat getOrElse preciseLdjson

      compScheme <- (c --\ "compressionScheme").as[Option[CompressionScheme]]
    } yield LocalConfig(rootDir, chunkSize, format, compScheme))
  }

  implicit val eqv: Eq[LocalConfig] =
    Eq.by(lc => (lc.rootDir, lc.readChunkSizeBytes, lc.format, lc.compressionScheme))

  implicit val show: Show[LocalConfig] =
    Show.show(lc =>
      s"LocalConfig(${lc.rootDir}, ${lc.readChunkSizeBytes}, ${lc.format.show}, ${lc.compressionScheme.fold("uncompressed")(_.show)}")
}
