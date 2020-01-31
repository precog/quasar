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

package quasar.connector

import slamdata.Predef._

import quasar.{NonTerminal, RenderTree}, RenderTree.ops._

import cats.{Show, Eq}
import cats.syntax.eq._
import cats.syntax.show._
import cats.kernel.instances.char._

import argonaut.{HCursor, DecodeResult, CodecJson, Argonaut, Json => AJson}, Argonaut._

sealed trait DataFormat extends Product with Serializable
sealed trait ParsingFormat extends DataFormat

object DataFormat {
  final case class Json(variant: JsonVariant, isPrecise: Boolean) extends ParsingFormat
  final case class SeparatedValues(
      header: Boolean,
      row1: Char,
      row2: Char,
      record: Char,
      openQuote: Char,
      closeQuote: Char,
      escape: Char)
      extends ParsingFormat
  final case class Compressed(scheme: CompressionScheme, parsing: ParsingFormat) extends DataFormat

  object SeparatedValues {
    val Default: DataFormat = SeparatedValues(
      header = true,
      row1 = '\r',
      row2 = '\n',
      record = ',',
      openQuote = '"',
      closeQuote = '"',
      escape = '"')
  }

  sealed trait JsonVariant extends Product with Serializable
  object JsonVariant {
    final case object ArrayWrapped extends JsonVariant
    final case object LineDelimited extends JsonVariant

    implicit val codecJsonVariant: CodecJson[JsonVariant] = CodecJson({
      case ArrayWrapped => "array-wrapped".asJson
      case LineDelimited => "line-delimited".asJson
    }, (c => c.as[String].flatMap {
      case "array-wrapped" => DecodeResult.ok(ArrayWrapped)
      case "line-delimited" => DecodeResult.ok(LineDelimited)
      case other => DecodeResult.fail(s"Unrecognized json variant: $other", c.history)
    }))

    implicit val showJsonVariant: Show[JsonVariant] = Show.fromToString
    implicit val eqJsonVariant: Eq[JsonVariant] = Eq.fromUniversalEquals
  }

  import JsonVariant._

  implicit val codecParsingFormat: CodecJson[ParsingFormat] = CodecJson({
    case Json(v, precise) => AJson(
      "type" := "json",
      "variant" := v,
      "precise" := precise)
    case config: SeparatedValues => AJson(
      "type" := "separated-values",
      "header" := config.header,
      "row1" := config.row1.toChar,
      "row2" := (if (config.row2 === 0.toByte.toChar) "" else config.row2.toChar.toString),
      "record" := config.record.toChar,
      "openQuote" := config.openQuote.toChar,
      "closeQuote" := config.closeQuote.toChar,
      "escape" := config.escape.toChar)
  }, (c => (c --\ "type").as[String].flatMap {
    case "json" =>
      for {
        variant <- (c --\ "variant").as[JsonVariant]
        isPrecise <- (c --\ "precise").as[Option[Boolean]]
      } yield Json(variant, isPrecise.getOrElse(false))
    case "separated-values" =>
      for {
        header <- (c --\ "header").as[Boolean]
        row1 <- (c --\ "row1").as[Char]
        row2 <- (c --\ "row2").as[String]
        record <- (c --\ "record").as[Char]
        openQuote <- (c --\ "openQuote").as[Char]
        closeQuote <- (c --\ "closeQuote").as[Char]
        escape <- (c --\ "escape").as[Char]
      } yield SeparatedValues(
        header = header,
        row1 = row1,
        row2 = row2.headOption match {
          case None => 0.toByte.toChar
          case Some(h) => h
        },
        record = record,
        openQuote = openQuote,
        closeQuote = closeQuote,
        escape = escape
      )
    case other => DecodeResult.fail(s"Unrecognized type field in ParsingType: $other", c.history)
  }))

  implicit val codecDataFormat: CodecJson[DataFormat] = {
    def decode(c: HCursor): DecodeResult[DataFormat] = for {
      parsing <- (c --\ "format").as[ParsingFormat]
      compression <- (c --\ "compressionScheme").as[Option[CompressionScheme]]
    } yield {
      compression match {
        case None => parsing: DataFormat
        case Some(scheme) => Compressed(scheme, parsing)
      }
    }
    CodecJson({
      case Compressed(scheme, rt) =>
        ("compressionScheme" := scheme) ->:
        ("format" := rt) ->:
        jEmptyObject
      case parsing: ParsingFormat =>
        ("format" := parsing) ->:
        jEmptyObject
    }, (c => decode(c)))
  }

  implicit val showDataFormat: Show[DataFormat] = Show.fromToString

  implicit val renderTreeParsingFormat: RenderTree[ParsingFormat] = RenderTree.make {
    case Json(v, precise) => NonTerminal(List("Json"), None, List(v.show.render, precise.render))
    case SeparatedValues(header, row1, row2, record, openQuote, closeQuote, escape) => NonTerminal(List("SeparatedValues"), None, List(
      header.toString.render,
      row1.toString.render,
      row2.toString.render,
      record.toString.render,
      openQuote.toString.render,
      closeQuote.toString.render,
      escape.toString.render))
  }

  implicit val renderTreeDataFormat: RenderTree[DataFormat] = RenderTree.make {
    case u: ParsingFormat => u.render
    case Compressed(scheme, pt) => NonTerminal(List("Compressed"), None, List(pt.render))
  }

  implicit val eqParsingFormat: Eq[ParsingFormat] = Eq.fromUniversalEquals
  implicit val eqDataFormat: Eq[DataFormat] = Eq.fromUniversalEquals

  def json: DataFormat = Json(ArrayWrapped, false)
  def ldjson: DataFormat = Json(LineDelimited, false)
  def precise0(inp: ParsingFormat): ParsingFormat = inp match {
    case Json(v, _) => Json(v, true)
    case _ => inp
  }
  def precise(inp: DataFormat): DataFormat = inp match {
    case Compressed(scheme, pt) => Compressed(scheme, precise0(pt))
    case u: ParsingFormat => precise0(u): DataFormat
  }
  def gzipped(inp: DataFormat): DataFormat = inp match {
    case u: ParsingFormat => Compressed(CompressionScheme.Gzip, u)
    case _ => inp
  }
}
