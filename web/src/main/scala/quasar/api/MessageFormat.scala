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

import quasar.Predef._

import quasar.{Data, DataCodec}
import quasar.csv._
import quasar.fp._
import quasar.fp.ski._
import quasar.main.Prettify

import org.http4s._
import org.http4s.parser.HttpHeaderParser
import org.http4s.headers._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

sealed trait JsonPrecision extends Product with Serializable {
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
sealed trait JsonFormat extends Product with Serializable {
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
  /* Does not decode in a streaming fashion */
  def decode(txtStream: Process[Task,String]): Task[DecodeError \/ Process[Task, DecodeError \/ Data]] =
    txtStream.runLog.map(_.mkString).map(decode(_))
  /* Not a streaming decoder */
  def decoder: EntityDecoder[Process[Task, DecodeError \/ Data]] = EntityDecoder.decodeBy(mediaType) { msg =>
    EitherT(decode(msg.bodyAsText).map(_.leftMap(err => InvalidMessageBodyFailure(err.msg))))
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
      val encodedData = data.map(DataCodec.render(_)(mode.codec)).unite
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
      val writer = CsvWriter(Some(CsvParser.Format(columnDelimiter, quoteChar, escapeChar, rowDelimiter)))
      Prettify.renderStream(data, CsvColumnsFromInitialRowsCount).map(writer(_))
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

  val supportedMediaTypes: Set[MediaRange] = Set(
    JsonFormat.LineDelimited.mediaType,
    new MediaType("application", "x-ldjson"),
    JsonFormat.SingleArray.mediaType,
    Csv.mediaType)

  val decoder: EntityDecoder[Process[Task, DecodeError \/ Data]] = {
    type Result = Process[Task, DecodeError \/ Data]
    new EntityDecoder[Result] {
      override def decode(msg: Message, strict: Boolean): DecodeResult[Result] = {
        msg.headers.get(`Content-Type`).map { contentType =>
          fromMediaType(contentType.mediaType).map { format =>
            EitherT[Task, DecodeFailure, Result](format.decode(msg.bodyAsText).map(_.leftMap(err => InvalidMessageBodyFailure(err.msg))))
          }.getOrElse(EitherT.left[Task, DecodeFailure, Result](MediaTypeMismatch(contentType.mediaType, consumes).point[Task]))
        }.getOrElse(EitherT.left[Task, DecodeFailure, Result](MediaTypeMissing(consumes).point[Task]))
      }

      override val consumes: Set[MediaRange] = supportedMediaTypes
    }
  }

  // FIXME: I don’t know why this is triggering here.
  @SuppressWarnings(Array("org.wartremover.warts.NoNeedForMonad"))
  def fromMediaType(mediaType: MediaRange): Option[MessageFormat] = {
    val disposition = mediaType.extensions.get("disposition").flatMap(str =>
      HttpHeaderParser.CONTENT_DISPOSITION(str).toOption)
    if (mediaType satisfies Csv.mediaType) {
      def toChar(str: String): Option[Char] = str.toList match {
        case c :: Nil => Some(c)
        case _ => None
      }
      Some(Csv(mediaType.extensions.get("columnDelimiter").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse(','),
        mediaType.extensions.get("rowDelimiter").map(Csv.unescapeNewlines).getOrElse("\r\n"),
        mediaType.extensions.get("quoteChar").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse('"'),
        mediaType.extensions.get("escapeChar").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse('"'),
        disposition))
    }
    else {
      val format =
        if (mediaType satisfies JsonFormat.SingleArray.mediaType)
          if (mediaType.extensions.get("boundary") =/= Some("NL")) Some(JsonFormat.SingleArray)
          else Some(JsonFormat.LineDelimited)
        else if ((mediaType satisfies JsonFormat.LineDelimited.mediaType) ||
                 (mediaType satisfies new MediaType("application", "x-ldjson"))) Some(JsonFormat.LineDelimited)
        else None
      format.map { f =>
        val precision =
          if (mediaType.extensions.get("mode") == Some(JsonPrecision.Precise.name))
            JsonPrecision.Precise
          else
            JsonPrecision.Readable
        JsonContentType(precision, f, disposition)
      }
    }
  }

  def fromAccept(accept: Option[Accept]): MessageFormat =
    // TODO: MediaRange needs an Order instance – combining QValue ordering
    //       with specificity (EG, application/json sorts before
    //       application/* if they have the same q-value).
    accept.flatMap(_.values.sortBy(_.qValue).list.map(_.mediaRange).map(fromMediaType).flatten(Option.option2Iterable).lastOption)
      .getOrElse(MessageFormat.Default)
}
