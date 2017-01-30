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

package quasar

import quasar.Predef._
import quasar.fp._
import quasar.fp.ski._

import java.time.format.DateTimeFormatter
import java.time.ZoneOffset.UTC

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

  val dateTimeFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX")

  val timeFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("HH:mm:ss.SSS")

  val Precise = new DataCodec {
    val TimestampKey = "$timestamp"
    val DateKey = "$date"
    val TimeKey = "$time"
    val IntervalKey = "$interval"
    val BinaryKey = "$binary"
    val ObjKey = "$obj"
    val IdKey = "$oid"

    def encode(data: Data): Option[Json] = {
      import Data._
      data match {
        case d@(Null | Bool(_) | Int(_) | Dec(_) | Str(_)) => Readable.encode(d)
        // For Object, if we find one of the above keys, which means we serialized something particular
        // to the precise encoding, wrap this object in another object with a single field with the name ObjKey
        case Obj(value) =>
          val obj = Json.obj(value.toList.map { case (k, v) => encode(v).map(k -> _) }.unite: _*)
          value.keys.find(_.startsWith("$")).fold(obj)(κ(Json.obj(ObjKey -> obj))).some

        case Arr(value) => Json.array(value.map(encode).unite: _*).some
        case Set(_)     => None

        case Timestamp(value) => Json.obj(TimestampKey -> jString(value.atZone(UTC).format(dateTimeFormatter))).some
        case Date(value)      => Json.obj(DateKey      -> jString(value.toString)).some
        case Time(value)      => Json.obj(TimeKey      -> jString(value.format(timeFormatter))).some
        case Interval(value)  => Json.obj(IntervalKey  -> jString(value.toString)).some

        case bin @ Binary(_)  => Json.obj(BinaryKey    -> jString(bin.base64)).some

        case Id(value)        => Json.obj(IdKey        -> jString(value)).some

        case NA               => None
      }
    }

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
          import std.DateLib._

          def unpack[A](a: Option[A], expected: String)(f: A => DataEncodingError \/ Data) =
            (a \/> UnexpectedValueError(expected, json)) flatMap f

          def decodeObj(obj: JsonObject): DataEncodingError \/ Data =
            obj.toList.traverse { case (k, v) => decode(v).map(k -> _) }.map(pairs => Data.Obj(ListMap(pairs: _*)))

          obj.toList match {
            case (`TimestampKey`, value) :: Nil => unpack(value.string, "string value for $timestamp")(parseTimestamp(_).leftMap(err => ParseError(err.message)))
            case (`DateKey`, value) :: Nil      => unpack(value.string, "string value for $date")(parseDate(_).leftMap(err => ParseError(err.message)))
            case (`TimeKey`, value) :: Nil      => unpack(value.string, "string value for $time")(parseTime(_).leftMap(err => ParseError(err.message)))
            case (`IntervalKey`, value) :: Nil  => unpack(value.string, "string value for $interval")(parseInterval(_).leftMap(err => ParseError(err.message)))
            case (`ObjKey`, value) :: Nil       => unpack(value.obj,    "object value for $obj")(decodeObj)
            case (`BinaryKey`, value) :: Nil    => unpack(value.string, "string value for $binary") { str =>
              \/.fromTryCatchNonFatal(Data.Binary.fromArray(new sun.misc.BASE64Decoder().decodeBuffer(str))).leftMap(_ => UnexpectedValueError("BASE64-encoded data", json))
            }
            case (`IdKey`, value) :: Nil        => unpack(value.string, "string value for $oid")(str => \/-(Data.Id(str)))
            case _ => obj.fields.find(_.startsWith("$")).fold(decodeObj(obj))(κ(-\/(UnescapedKeyError(json))))
          }
        })
  }

  val Readable = new DataCodec {
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
        case Set(_)     => None

        case Timestamp(value) => jString(value.toString).some
        case Date(value)      => jString(value.toString).some
        case Time(value)      => jString(value.toString).some
        case Interval(value)  => jString(value.toString).some

        case bin @ Binary(_)  => jString(bin.base64).some

        case Id(value)        => jString(value).some

        case NA               => None
      }
    }

    def decode(json: Json): DataEncodingError \/ Data =
      json.fold(
        \/-(Data.Null),
        bool => \/-(Data.Bool(bool)),
        num => num match {
          case JsonLong(x) => \/-(Data.Int(x))
          case _           => \/-(Data.Dec(num.toBigDecimal))
        },
        str => {
          import std.DateLib._

          val converted = List(
              parseTimestamp(str),
              parseDate(str),
              parseTime(str),
              parseInterval(str))
          \/-(converted.flatMap(_.toList).headOption.getOrElse(Data.Str(str)))
        },
        arr => arr.traverse(decode).map(Data.Arr(_)),
        obj => obj.toList.traverse { case (k, v) => decode(v).map(k -> _) }.map(pairs => Data.Obj(ListMap(pairs: _*))))
  }
}
