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

  // this is just copy-pasted from the old version; we should remove it eventually
  def OldPrecise(recDecode: Json => DataEncodingError \/ Data) = new DataCodec {
    val TimestampKey = "$timestamp"
    val DateKey = "$date"
    val TimeKey = "$time"
    val IntervalKey = "$interval"
    val BinaryKey = "$binary"
    val ObjKey = "$obj"
    val IdKey = "$oid"

    // we really only care about decoding old precise
    def encode(data: Data): Option[Json] = None

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
        arr => arr.traverse(recDecode).map(Data.Arr(_)),
        obj => {
          import quasar.std.DateLib._

          def unpack[A](a: Option[A], expected: String)(f: A => DataEncodingError \/ Data) =
            (a \/> UnexpectedValueError(expected, json)) flatMap f

          def decodeObj(obj: JsonObject): DataEncodingError \/ Data =
            obj.toList.traverse { case (k, v) => recDecode(v).map(k -> _) }.map(pairs => Data.Obj(ListMap(pairs: _*)))

          obj.toList match {
            case (`TimestampKey`, value) :: Nil => unpack(value.string, "string value for $timestamp")(parseTimestamp(_).leftMap(err => ParseError(err.message)))
            case (`DateKey`, value) :: Nil      => unpack(value.string, "string value for $date")(parseLocalDate(_).leftMap(err => ParseError(err.message)))
            case (`TimeKey`, value) :: Nil      => unpack(value.string, "string value for $time")(parseLocalTime(_).leftMap(err => ParseError(err.message)))
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

  object PreciseKeys {
    val LocalDateTimeKey = "$localdatetime"
    val LocalDateKey = "$localdate"
    val LocalTimeKey = "$localtime"
    val OffsetDateTimeKey = "$offsetdatetime"
    val OffsetDateKey = "$offsetdate"
    val OffsetTimeKey = "$offsettime"
    val IntervalKey = "$interval"
    val BinaryKey = "$binary"
    val ObjKey = "$obj"
    val IdKey = "$oid"
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

  /*
   * The purpose behind the explicit fixed-point here is to allow interleaved
   * composition at depth.  Specifically, we want to be able to write the `orElse`
   * combinator and have it apply not just at the top level, but also at *every*
   * level of the recursive hierarchy.  Thus, we need to ensure that recursive
   * calls to decode delegate to the orElse'd codec, and not the current
   * specialized one.  This is the difference between singular backtracking and
   * recursive backtracking.  Take note of the "decode timestamp AND localdatetimp
   * within array" test, which is impossible to satisfy without this trick.
   *
   * Note that only decoding is composed in this way.  Encoding is still singular
   * in its backtracking.  This is mostly because I'm lazy and we relaly don't
   * need recursive backtracking on encoding... YET.  If you need to add this
   * (i.e. because of a situation analogous to the decoding test I referenced),
   * then you can add it using a very similar trick.
   */
  def NewPrecise(recDecode: Json => DataEncodingError \/ Data) = new DataCodec {
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

        case OffsetDateTime(value)  =>
          Json.obj(OffsetDateTimeKey -> jString(OffsetDateTimeFormatter.format(value))).some
        case Data.OffsetDate(value) =>
          Json.obj(OffsetDateKey     -> jString(value.toString)).some
        case OffsetTime(value)      =>
          Json.obj(OffsetTimeKey     -> jString(OffsetTimeFormatter.format(value))).some
        case LocalDateTime(value)   =>
          Json.obj(LocalDateTimeKey  -> jString(LocalDateTimeFormatter.format(value))).some
        case LocalDate(value)       =>
          Json.obj(LocalDateKey      -> jString(value.toString)).some
        case LocalTime(value)       =>
          Json.obj(LocalTimeKey      -> jString(LocalTimeFormatter.format(value))).some
        case Interval(value)        =>
          Json.obj(IntervalKey       -> jString(value.toString)).some

        case bin @ Binary(_)        => Json.obj(BinaryKey         -> jString(bin.base64)).some

        case Id(value)              => Json.obj(IdKey             -> jString(value)).some

        case NA                     => None
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
        arr => arr.traverse(recDecode).map(Data.Arr(_)),
        obj => {
          import quasar.std.DateLib._

          def unpack[A](a: Option[A], expected: String)(f: A => DataEncodingError \/ Data) =
            (a \/> UnexpectedValueError(expected, json)) flatMap f

          def decodeObj(obj: JsonObject): DataEncodingError \/ Data =
            obj.toList.traverse { case (k, v) => recDecode(v).map(k -> _) }.map(pairs => Data.Obj(ListMap(pairs: _*)))

          obj.toList match {
            case (`OffsetDateTimeKey`, value) :: Nil => unpack(value.string, "string value for $offsetdatetime")(parseOffsetDateTime(_).leftMap(err => ParseError(err.message)))
            case (`OffsetTimeKey`, value) :: Nil     => unpack(value.string, "string value for $offsettime")(parseOffsetTime(_).leftMap(err => ParseError(err.message)))
            case (`OffsetDateKey`, value) :: Nil     => unpack(value.string, "string value for $offsetdate")(parseOffsetDate(_).leftMap(err => ParseError(err.message)))
            case (`LocalDateTimeKey`, value) :: Nil  => unpack(value.string, "string value for $localdatetime")(parseLocalDateTime(_).leftMap(err => ParseError(err.message)))
            case (`LocalTimeKey`, value) :: Nil      => unpack(value.string, "string value for $localtime")(parseLocalTime(_).leftMap(err => ParseError(err.message)))
            case (`LocalDateKey`, value) :: Nil      => unpack(value.string, "string value for $localdate")(parseLocalDate(_).leftMap(err => ParseError(err.message)))
            case (`IntervalKey`, value) :: Nil       => unpack(value.string, "string value for $interval")(parseInterval(_).leftMap(err => ParseError(err.message)))
            case (`ObjKey`, value) :: Nil            => unpack(value.obj,    "object value for $obj")(decodeObj)
            case (`BinaryKey`, value) :: Nil         => unpack(value.string, "string value for $binary") { str =>
              \/.fromTryCatchNonFatal(Data.Binary.fromArray(new sun.misc.BASE64Decoder().decodeBuffer(str))).leftMap(_ => UnexpectedValueError("BASE64-encoded data", json))
            }
            case (`IdKey`, value) :: Nil             => unpack(value.string, "string value for $oid")(str => \/-(Data.Id(str)))
            case _ => obj.fields.find(_.startsWith("$")).fold(decodeObj(obj))(κ(-\/(UnescapedKeyError(json))))
          }
        })
  }

  val Precise = orElse(NewPrecise _, OldPrecise _)

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

        case bin @ Binary(_)  => jString(bin.base64).some

        case Id(value)        => jString(value).some

        case NA               => None
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
    case (Data.Binary(_) | Data.Id(_)) if codec == Readable => false
    case Data.NA                                            => false
    case Data.Int(x)                                        => x.isValidLong
    case Data.Arr(list)                                     => list.forall(representable(_, codec))
    case Data.Obj(map)                                      => map.values.forall(representable(_, codec))
    case _                                                  => true
  }

  // TODO enable support for fixpoint encoding as well
  private def orElse(
      selfF: (Json => DataEncodingError \/ Data) => DataCodec,
      otherF: (Json => DataEncodingError \/ Data) => DataCodec): DataCodec = new DataCodec {

    private lazy val self = selfF(decode)
    private lazy val other = otherF(decode)

    def encode(data: Data): Option[Json] =
      self.encode(data).orElse(other.encode(data))

    def decode(json: Json): DataEncodingError \/ Data =
      self.decode(json).orElse(other.decode(json))
  }
}
