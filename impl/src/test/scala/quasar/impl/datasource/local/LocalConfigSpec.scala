/*
 * Copyright 2014–2020 SlamData Inc.
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

import quasar.connector._

import argonaut.Parse

import cats.instances.either._
import cats.instances.string._
import cats.syntax.either._

import shims.{eqToScalaz, showToScalaz}

object LocalConfigSpec extends quasar.Qspec {
  "json decoding" >> {

    "v2 options (backwards)" >> {
      val js = """
        {
          "rootDir": "/data",
          "readChunkSizeBytes": 2048,
          "format": { "type": "json", "variant": "array-wrapped", "precise": false },
          "compressionScheme": "gzip"
        }
      """.stripMargin

      val exp =
        LocalConfig(
          "/data",
          2048,
          DataFormat.gzipped(DataFormat.json))

      Parse.decodeEither[LocalConfig](js) must equal(exp.asRight[String])
    }

    "backwards compatible with v1" >> {
      val js = """
        {
          "rootDir": "/data",
          "readChunkSizeBytes": 2048
        }
      """.stripMargin

      val exp =
        LocalConfig(
          "/data",
          2048,
          DataFormat.precise(DataFormat.ldjson))

      Parse.decodeEither[LocalConfig](js) must equal(exp.asRight[String])
    }

    "precise is optional" >> {
      val js = """
        {
          "rootDir": "/data",
          "readChunkSizeBytes": 2048,
          "format": { "type": "json", "variant": "array-wrapped" }
        }
      """.stripMargin

      val exp =
        LocalConfig(
          "/data",
          2048,
          DataFormat.json)

      Parse.decodeEither[LocalConfig](js) must equal(exp.asRight[String])
    }

    "chunk size is optional" >> {
      val js = """
        {
          "rootDir": "/data",
          "format": { "type": "json", "variant": "array-wrapped" },
          "compressionScheme": null
        }
      """.stripMargin

      val exp =
        LocalConfig(
          "/data",
          LocalConfig.DefaultReadChunkSizeBytes,
          DataFormat.json)

      Parse.decodeEither[LocalConfig](js) must equal(exp.asRight[String])
    }

    "compression scheme is optional" >> {
      val js = """
        {
          "rootDir": "/data",
          "readChunkSizeBytes": 2048,
          "format": { "type": "json", "variant": "array-wrapped" }
        }
      """.stripMargin

      val exp =
        LocalConfig(
          "/data",
          2048,
          DataFormat.json)

      Parse.decodeEither[LocalConfig](js) must equal(exp.asRight[String])
    }

    "can handle csv format" >> {
      val js = """
        {
          "rootDir": "/data",
          "format": { "type": "separated-values", "header": true, "row1": "\r", "row2": "\n", "record": ",", "openQuote": "\"", "closeQuote": "\"", "escape": "\"" }
        }
      """.stripMargin
      val exp =
        LocalConfig(
          "/data",
          LocalConfig.DefaultReadChunkSizeBytes,
          DataFormat.SeparatedValues.Default)

      Parse.decodeEither[LocalConfig](js) must equal(exp.asRight[String])
    }
    "can handle compressed" >> {
      val js = """
        {
          "rootDir": "/data",
          "format": { "type": "json", "variant": "array-wrapped", "precise": false },
          "compressionScheme": "gzip"
        }
      """.stripMargin
      val exp =
        LocalConfig(
          "/data",
          LocalConfig.DefaultReadChunkSizeBytes,
          DataFormat.gzipped(DataFormat.json))

      val exp1 =
        LocalConfig(
          "/data",
          LocalConfig.DefaultReadChunkSizeBytes,
          DataFormat.gzipped(DataFormat.json))

      Parse.decodeEither[LocalConfig](js) must equal(exp.asRight[String])
    }
  }
}
