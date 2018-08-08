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

package quasar.frontend.data

import slamdata.Predef._
import quasar.common.data.Data
import quasar.fp._
import quasar.fp.ski._

import argonaut._, Argonaut._
import scalaz._, Scalaz._

trait DataEncodingError {
  def message: String
}
object DataEncodingError {
  final case class UnrepresentableDataError(data: Data) extends DataEncodingError {
    def message = s"not representable: ${data.shows}"
  }

  final case class UnescapedKeyError(json: Json) extends DataEncodingError {
    def message = s"un-escaped key: ${json.pretty(minspace)}"
  }

  final case class UnexpectedValueError(expected: String, json: Json) extends DataEncodingError {
    def message = s"expected $expected, found: ${json.pretty(minspace)}"
  }

  final case class ParseError(cause: String) extends DataEncodingError {
    def message = cause
  }

  implicit val encodeJson: EncodeJson[DataEncodingError] =
    EncodeJson(err => Json.obj("error" := err.message))

  implicit val show: Show[DataEncodingError] =
    Show.showFromToString[DataEncodingError]
}

trait DataCodec {
  def encode(data: Data): Option[Json]
  def decode(json: Json): DataEncodingError \/ Data
}

object DataCodec {
  import DataEncodingError._

  def parse(str: String)(implicit C: DataCodec): DataEncodingError \/ Data =
    \/.fromEither(Parse.parse(str)).leftMap(ParseError.apply).flatMap(C.decode(_))

  def render(data: Data)(implicit C: DataCodec): Option[String] =
    C.encode(data).map(_.pretty(minspace))

  object PreciseKeys {
    val LocalDateTimeKey = "$localdatetime"
    val LocalDateKey = "$localdate"
    val LocalTimeKey = "$localtime"
    val OffsetDateTimeKey = "$offsetdatetime"
    val OffsetDateKey = "$offsetdate"
    val OffsetTimeKey = "$offsettime"
    val IntervalKey = "$interval"
    val ObjKey = "$obj"
  }

  object VerboseDateTimeFormatters {
    import java.time.format._
    import java.time.temporal.ChronoField

    val LocalTimeFormatter = new DateTimeFormatterBuilder()
      .appendValue(ChronoField.HOUR_OF_DAY, 2)
      .appendLiteral(':')
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
      .appendLiteral(':')
      .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
      .appendFraction(ChronoField.NANO_OF_SECOND, 9, 9, true)
      .toFormatter()

    val OffsetTimeFormatter = new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .append(LocalTimeFormatter)
      .appendOffsetId()
      .toFormatter()

    val LocalDateTimeFormatter = new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral('T')
      .append(LocalTimeFormatter)
      .toFormatter()

    val OffsetDateTimeFormatter = new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .append(LocalDateTimeFormatter)
      .appendOffsetId()
      .toFormatter()
  }

  val Precise = new DataCodec {
    import PreciseKeys._

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def encode(data: Data): Option[Json] = {
      import Data._
      import VerboseDateTimeFormatters._
      data match {
        case d@(Null | Bool(_) | Int(_) | Dec(_) | Str(_)) => Readable.encode(d)
        // For Object, if we find one of the above keys, which means we serialized something particular
        // to the precise encoding, wrap this object in another object with a single field with the name ObjKey
        case Obj(value) =>
          val obj = Json.obj(value.toList.map { case (k, v) => encode(v).map(k -> _) }.unite: _*)
          value.keys.find(_.startsWith("$")).fold(obj)(κ(Json.obj(ObjKey -> obj))).some

        case Arr(value) => Json.array(value.map(encode).unite: _*).some

        case OffsetDateTime(value) =>
          Json.obj(OffsetDateTimeKey -> jString(OffsetDateTimeFormatter.format(value))).some
        case Data.OffsetDate(value) =>
          Json.obj(OffsetDateKey -> jString(value.toString)).some
        case OffsetTime(value) =>
          Json.obj(OffsetTimeKey -> jString(OffsetTimeFormatter.format(value))).some
        case LocalDateTime(value) =>
          Json.obj(LocalDateTimeKey -> jString(LocalDateTimeFormatter.format(value))).some
        case LocalDate(value) =>
          Json.obj(LocalDateKey -> jString(value.toString)).some
        case LocalTime(value) =>
          Json.obj(LocalTimeKey -> jString(LocalTimeFormatter.format(value))).some
        case Interval(value) =>
          Json.obj(IntervalKey -> jString(value.toString)).some

        case NA => None
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def decode(json: Json): DataEncodingError \/ Data =
      json.fold(
        \/-(Data.Null),
        bool => \/-(Data.Bool(bool)),
        num => num match {
          case JsonLong(x) => \/-(Data.Int(x))
          case _           => \/-(Data.Dec(num.toBigDecimal))
        },
        str => \/-(Data.Str(str)),
        arr => arr.traverse(decode).map(Data.Arr(_)),
        obj => {
          import quasar.std.DateLib._

          def unpack[A](a: Option[A], expected: String)(f: A => DataEncodingError \/ Data) =
            (a \/> UnexpectedValueError(expected, json)) flatMap f

          def decodeObj(obj: JsonObject): DataEncodingError \/ Data =
            obj.toList.traverse { case (k, v) => decode(v).map(k -> _) }.map(pairs => Data.Obj(ListMap(pairs: _*)))

          obj.toList match {
            case (`OffsetDateTimeKey`, value) :: Nil => unpack(value.string, "string value for $offsetdatetime")(parseOffsetDateTime(_).leftMap(err => ParseError(err.message)))
            case (`OffsetTimeKey`, value) :: Nil     => unpack(value.string, "string value for $offsettime")(parseOffsetTime(_).leftMap(err => ParseError(err.message)))
            case (`OffsetDateKey`, value) :: Nil     => unpack(value.string, "string value for $offsetdate")(parseOffsetDate(_).leftMap(err => ParseError(err.message)))
            case (`LocalDateTimeKey`, value) :: Nil  => unpack(value.string, "string value for $localdatetime")(parseLocalDateTime(_).leftMap(err => ParseError(err.message)))
            case (`LocalTimeKey`, value) :: Nil      => unpack(value.string, "string value for $localtime")(parseLocalTime(_).leftMap(err => ParseError(err.message)))
            case (`LocalDateKey`, value) :: Nil      => unpack(value.string, "string value for $localdate")(parseLocalDate(_).leftMap(err => ParseError(err.message)))
            case (`IntervalKey`, value) :: Nil       => unpack(value.string, "string value for $interval")(parseInterval(_).leftMap(err => ParseError(err.message)))
            case (`ObjKey`, value) :: Nil            => unpack(value.obj,    "object value for $obj")(decodeObj)
            case _ => obj.fields.find(_.startsWith("$")).fold(decodeObj(obj))(κ(-\/(UnescapedKeyError(json))))
          }
        })
  }

  val Readable = new DataCodec {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def encode(data: Data): Option[Json] = {
      import Data._
      data match {
        case Null => jNull.some
        case Bool(true) => jTrue.some
        case Bool(false) => jFalse.some
        case Int(x)   =>
          if (x.isValidLong) jNumber(JsonLong(x.longValue)).some
          else jNumber(JsonBigDecimal(new java.math.BigDecimal(x.underlying))).some
        case Dec(x)   => jNumber(JsonBigDecimal(x)).some
        case Str(s)   => jString(s).some

        case Obj(value) => Json.obj(value.toList.map({ case (k, v) => encode(v) strengthL k }).unite: _*).some
        case Arr(value) => Json.array(value.map(encode).unite: _*).some

        case OffsetDateTime(value)  => jString(value.toString).some
        case OffsetTime(value)      => jString(value.toString).some
        case Data.OffsetDate(value) => jString(value.toString).some
        case LocalDateTime(value)   => jString(value.toString).some
        case LocalTime(value)       => jString(value.toString).some
        case LocalDate(value)       => jString(value.toString).some
        case Interval(value)        => jString(value.toString).some

        case NA => None
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def decode(json: Json): DataEncodingError \/ Data =
      json.fold(
        \/-(Data.Null),
        bool => \/-(Data.Bool(bool)),
        num => num match {
          case JsonLong(x) => \/-(Data.Int(x))
          case _           => \/-(Data.Dec(num.toBigDecimal))
        },
        str => {
          import quasar.std.DateLib._

          // TODO: make this not stupidly slow
          val converted = List(
              parseOffsetDateTime(str),
              parseOffsetTime(str),
              parseOffsetDate(str),
              parseLocalDateTime(str),
              parseLocalTime(str),
              parseLocalDate(str),
              parseInterval(str))
          \/-(converted.flatMap(_.toList).headOption.getOrElse(Data.Str(str)))
        },
        arr => arr.traverse(decode).map(Data.Arr(_)),
        obj => obj.toList.traverse { case (k, v) => decode(v).map(k -> _) }.map(pairs => Data.Obj(ListMap(pairs: _*))))
  }

  // Identify the sub-set of values that can be represented in Precise JSON in
  // such a way that the parser recovers the original type. These are:
  // - integer values that don't fit into Long, which Argonaut does not allow us to distinguish from decimals
  // - Data.NA
  // NB: For Readable, this does not account for Str values that will be confused with
  // other types (e.g. `Data.Str("12:34")`, which becomes `Data.Time`).
  @SuppressWarnings(Array("org.wartremover.warts.Equals","org.wartremover.warts.Recursion"))
  def representable(data: Data, codec: DataCodec): Boolean = data match {
    case Data.NA => false
    case Data.Int(x) => x.isValidLong
    case Data.Arr(list) => list.forall(representable(_, codec))
    case Data.Obj(map) => map.values.forall(representable(_, codec))
    case _ => true
  }
}
