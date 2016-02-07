/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.api

import argonaut.EncodeJson
import org.http4s.parser.HttpHeaderParser
import quasar.Predef._
import quasar.fp._

import org.http4s.{ParseFailure, EntityDecoder, MediaType}
import org.http4s.headers.{Accept, `Content-Disposition`}
import quasar.repl.Prettify

import quasar.{DataEncodingError, Data, DataCodec}

import scalaz.concurrent.Task
import scalaz._, Scalaz._
import scalaz.stream.Process

sealed trait JsonPrecision {
  def codec: DataCodec
  def name: String
}
object JsonPrecision {
  case object Readable extends JsonPrecision {
    val codec = DataCodec.Readable
    val name = "readable"
  }
  case object Precise extends JsonPrecision {
    val codec = DataCodec.Precise
    val name = "precise"
  }
}
sealed trait JsonFormat {
  def mediaType: MediaType
}
object JsonFormat {
  case object LineDelimited extends JsonFormat {
    val mediaType = new MediaType("application", "ldjson", true, true) // ldjson => line delimited json
  }
  case object SingleArray extends JsonFormat {
    val mediaType = MediaType.`application/json`
  }
}

final case class DecodeError(msg: String)

object DecodeError {
  implicit val show: Show[DecodeError] = Show.shows(_.msg)
}

trait Decoder {
  def mediaType: MediaType
  def decode(txt: String): DecodeError \/ Process[Task, DecodeError \/ Data]
  def decode(txtStream: Process[Task,String]): Task[DecodeError \/ Process[Task, DecodeError \/ Data]] =
    txtStream.runLog.map(_.mkString).map(decode(_))
  def decoder: EntityDecoder[Process[Task, DecodeError \/ Data]] = EntityDecoder.decodeBy(mediaType) { msg =>
    EitherT(decode(msg.bodyAsText).map(_.leftMap(err => ParseFailure(err.msg, ""))))
  }
}

sealed trait MessageFormat extends Decoder {
  def disposition: Option[`Content-Disposition`]
  def encode[F[_]](data: Process[F, Data]): Process[F, String]
  protected def dispositionExtension: Map[String, String] =
    disposition.map(disp => Map("disposition" -> disp.value)).getOrElse(Map.empty)
}
object MessageFormat {
  final case class JsonContentType(
    mode: JsonPrecision,
    format: JsonFormat,
    disposition: Option[`Content-Disposition`]
  ) extends MessageFormat {
    val LineSep = "\r\n"
    def mediaType = {
      val extensions = Map("mode" -> mode.name) ++ dispositionExtension
      format.mediaType.withExtensions(extensions)
    }
    override def encode[F[_]](data: Process[F, Data]): Process[F, String] = {
      val encodedData =
        data.map(DataCodec.render(_)(mode.codec).fold(EncodeJson.of[DataEncodingError].encode(_).toString, ι))
      format match {
        case JsonFormat.SingleArray =>
          Process.emit("[" + LineSep) ++ encodedData.intersperse("," + LineSep) ++ Process.emit(LineSep + "]" + LineSep)
        case JsonFormat.LineDelimited =>
          encodedData.map(_ + LineSep)
      }
    }
    def decode(txt: String): DecodeError \/ Process[Task, DecodeError \/ Data] =
      if (txt.isEmpty) Process.empty.right
      else {
        implicit val codec = mode.codec
        format match {
          case JsonFormat.SingleArray =>
            DataCodec.parse(txt).fold(
              err => DecodeError("parse error: " + err.message).left,
              {
                case Data.Arr(data) => Process.emitAll(data.map(_.right)).right
                case _              => DecodeError("Provided body is not a json array").left
              }
            )
          case JsonFormat.LineDelimited =>
            val jsonByLine = txt.split("\n").map(line => DataCodec.parse(line).leftMap(
              e => DecodeError(s"parse error: ${e.message} in the following line: $line")
            )).toList
            Process.emitAll(jsonByLine).right
        }
      }
  }

  object JsonContentType {
    def apply(mode: JsonPrecision, format: JsonFormat): JsonContentType = JsonContentType(mode, format, disposition = None)
  }

  val Default = JsonContentType(JsonPrecision.Readable, JsonFormat.LineDelimited, None)

  final case class Csv(columnDelimiter: Char, rowDelimiter: String, quoteChar: Char, escapeChar: Char, disposition: Option[`Content-Disposition`]) extends MessageFormat {
    import Csv._

    val CsvColumnsFromInitialRowsCount = 1000

    def mediaType = {
      val alwaysExtensions = Map(
        "columnDelimiter" -> escapeNewlines(columnDelimiter.toString),
        "rowDelimiter" -> escapeNewlines(rowDelimiter),
        "quoteChar" -> escapeNewlines(quoteChar.toString),
        "escapeChar" -> escapeNewlines(escapeChar.toString))
      val extensions = alwaysExtensions ++ dispositionExtension
      Csv.mediaType.withExtensions(extensions)
    }
    override def encode[F[_]](data: Process[F, Data]): Process[F, String] = {
      import quasar.repl.Prettify
      import com.github.tototoshi.csv._

      val format = Some(CsvParser.Format(columnDelimiter,quoteChar,escapeChar,rowDelimiter))

      Prettify.renderStream(data, CsvColumnsFromInitialRowsCount).map { v =>
        val w = new java.io.StringWriter
        val cw = format.map(f => CSVWriter.open(w)(f)).getOrElse(CSVWriter.open(w))
        cw.writeRow(v)
        cw.close
        w.toString
      }
    }
    override def decode(txt: String): DecodeError \/ Process[Task, DecodeError \/ Data] = Csv.decode(txt)
  }
  object Csv extends Decoder {
    val mediaType = MediaType.`text/csv`

    val Default = Csv(',', "\r\n", '"', '"', None)

    def escapeNewlines(str: String): String =
      str.replace("\r", "\\r").replace("\n", "\\n")

    def unescapeNewlines(str: String): String =
      str.replace("\\r", "\r").replace("\\n", "\n")

    override def decode(txt: String): (DecodeError \/ Process[Task, DecodeError \/ Data]) = {
      CsvDetect.parse(txt).fold(
        err => DecodeError("parse error: " + err).left,
        lines => {
          val data = lines.headOption.map { header =>
            val paths = header.fold(κ(Nil), _.fields.map(Prettify.Path.parse(_).toOption))
            lines.drop(1).map(_.bimap(
              err => DecodeError("parse error: " + err),
              rec => {
                val pairs = paths zip rec.fields.map(Prettify.parse)
                val good = pairs.map { case (p, s) => (p |@| s).tupled }.foldMap(_.toList)
                Prettify.unflatten(good.toListMap)
              }
            ))
          }.getOrElse(Stream.empty)
          Process.emitAll(data).right
        }
      )
    }
  }

  case object UnsupportedContentType extends scala.Exception

  val decoder: EntityDecoder[Process[Task, DecodeError \/ Data]] = {
    val json = {
      import JsonPrecision._
      import JsonFormat._
      JsonContentType(Readable,LineDelimited).decoder orElse
      JsonContentType(Precise,LineDelimited).decoder orElse
      JsonContentType(Readable,SingleArray).decoder orElse
      JsonContentType(Precise,SingleArray).decoder
    }
    Csv.decoder orElse json

  }

  def fromAccept(accept: Option[Accept]): MessageFormat = {
    val mediaTypes = NonEmptyList(
      JsonFormat.LineDelimited.mediaType,
      new MediaType("application", "x-ldjson"),
      JsonFormat.SingleArray.mediaType,
      Csv.mediaType)

    (for {
      acc       <- accept
      // TODO: MediaRange needs an Order instance – combining QValue ordering
      //       with specificity (EG, application/json sorts before
      //       application/* if they have the same q-value).
      chosenMediaType <- acc.values.sortBy(_.qValue).list.find(a => mediaTypes.list.exists(a.satisfies(_)))
    } yield {
      val disposition = chosenMediaType.extensions.get("disposition").flatMap { str =>
        HttpHeaderParser.CONTENT_DISPOSITION(str).toOption
      }
      if (chosenMediaType satisfies Csv.mediaType) {
        def toChar(str: String): Option[Char] = str.toList match {
          case c :: Nil => Some(c)
          case _ => None
        }
        Csv(chosenMediaType.extensions.get("columnDelimiter").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse(','),
          chosenMediaType.extensions.get("rowDelimiter").map(Csv.unescapeNewlines).getOrElse("\r\n"),
          chosenMediaType.extensions.get("quoteChar").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse('"'),
          chosenMediaType.extensions.get("escapeChar").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse('"'),
          disposition)
      }
      else {
        val format = if ((chosenMediaType satisfies JsonFormat.SingleArray.mediaType) &&
                          chosenMediaType.extensions.get("boundary") != Some("NL")) JsonFormat.SingleArray
                     else JsonFormat.LineDelimited
        val precision =
          if (chosenMediaType.extensions.get("mode") == Some(JsonPrecision.Precise.name))
            JsonPrecision.Precise
          else
            JsonPrecision.Readable
         JsonContentType(precision, format, disposition)
      }
    }).getOrElse(MessageFormat.Default)
  }
}
