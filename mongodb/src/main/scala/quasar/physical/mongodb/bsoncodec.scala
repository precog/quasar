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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar._
import quasar.Data.DateTimeConstants
import quasar.ejson.{EJson, TypeTag, ExtEJson, CommonEJson}
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski._
import quasar.fs.Planner, Planner._

import java.time.{
  Instant,
  LocalDate=> JLocalDate,
  LocalDateTime => JLocalDateTime,
  LocalTime => JLocalTime,
  OffsetDateTime => JOffsetDateTime,
  OffsetTime => JOffsetTime,
  ZoneOffset
}
import java.time.format.DateTimeFormatter

import matryoshka._
import matryoshka.implicits._
import matryoshka.patterns._
import monocle.Prism
import scalaz._, Scalaz._
import scodec.bits.ByteVector

object BsonCodec {
  private def pad2(x: Int) = if (x < 10) "0" + x.toString else x.toString
  private def pad3(x: Int) =
    if (x < 10) "00" + x.toString else if (x < 100) "0" + x.toString else x.toString

  object EJsonType {
    def apply(typ: String): Bson =
      Bson.Doc(ListMap("_ejson.type" -> Bson.Text(typ)))

    def unapply(bson: Bson) = bson match {
      case Bson.Doc(map) => map.get("_ejson.type") >>= {
        case Bson.Text(str) => str.some
        case _              => None
      }
      case _ => None
    }
  }

  object EJsonTypeSize {
    def apply(typ: String, size: Int): Bson =
      Bson.Doc(ListMap(
        "_ejson.type" -> Bson.Text(typ),
        "_ejson.size" -> Bson.Int32(size)))

    def unapply(bson: Bson) = bson match {
      case Bson.Doc(map) =>
        ((map.get("_ejson.type") ⊛ map.get("_ejson.size")) {
          case (Bson.Text(str), Bson.Int32(size)) => (str, size).some
          case (Bson.Text(str), Bson.Int64(size)) => (str, size.toInt).some
          case _                                  => None
        }).join
      case _ => None

    }
  }

  def fromCommon(v: BsonVersion): Algebra[ejson.Common, Bson] = {
    case ejson.Arr(value)  => Bson.Arr(value)
    case ejson.Null()      => Bson.Null
    case ejson.Bool(value) => Bson.Bool(value)
    case ejson.Str(value)  => Bson.Text(value)
    case ejson.Dec(value) if v lt BsonVersion.`1.1`
                           => Bson.Dec(value.toDouble)
    case ejson.Dec(value) if value.isDecimalDouble
                           => Bson.Dec(value.toDouble)
    case ejson.Dec(value)  => Bson.Dec128(value)
  }

  def extract[A](fa: Option[Bson], p: Prism[Bson, A]): Option[A] =
    fa.flatMap(p.getOption)

  val millisPerSec = 1000
  val nanosPerSec = 1000000000L

  val fromExtension: AlgebraM[PlannerError \/ ?, ejson.Extension, Bson] = {
    case ejson.Map(value) => value.traverse(_.bitraverse({
      case Bson.Text(key) => key.right
      case _ =>
        NonRepresentableEJson(value.toString + " is not a valid document key").left
    }, _.right)) ∘ (m => Bson.Doc(ListMap(m: _*)))

    // FIXME: cheating, but it’s what we’re already doing in the SQL parser
    case ejson.Char(value) => Bson.Text(value.toString).right
    case ejson.Byte(value) => Bson.Binary.fromArray(Array[Byte](value)).right

    case ejson.Int(value) =>
      if (value.isValidInt) Bson.Int32(value.toInt).right
      else if (value.isValidLong) Bson.Int64(value.toLong).right
      else NonRepresentableEJson(value.toString + " is too large").left

    case ejson.Meta(value, meta) => (meta, value) match {
      case (EJsonType("_bson.oid"), Bson.Text(oid)) =>
        Bson.ObjectId.fromString(oid) \/> ObjectIdFormatError(oid)

      case (EJsonTypeSize(TypeTag.Binary.value, size), Bson.Text(data)) =>
        if (size.isValidInt)
          ejson.z85.decode(data).fold[PlannerError \/ Bson](
            NonRepresentableEJson("“" + data + "” is not a valid Z85-encoded string").left)(
            bv => Bson.Binary(ImmutableArray.fromArray(bv.take(size.toLong).toArray)).right)
        else NonRepresentableEJson(size.shows + " is too large for binary data").left

      case (EJsonType(TypeTag.OffsetDateTime.value), Bson.Doc(map)) =>
        (extract(map.get(DateTimeConstants.year), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.month), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.day), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.hour), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.minute), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.second), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.nanosecond), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.offset), Bson._int32)) {
            (y, mo, d, h, mi, s, n, o) =>
              Bson.Date.fromInstant(
                JOffsetDateTime.of(y, mo, d, h, mi, s, n, ZoneOffset.ofTotalSeconds(o)).toInstant)
          }.join \/> NonRepresentableEJson(value.shows + " is not a valid OffsetDateTime")

      case (EJsonType(TypeTag.OffsetTime.value), Bson.Doc(map)) =>
        (extract(map.get(DateTimeConstants.hour), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.minute), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.second), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.nanosecond), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.offset), Bson._int32)) {
            (h, m, s, n, o) =>
              Bson.Text(
                JOffsetTime.of(h, m, s, n,ZoneOffset.ofTotalSeconds(o)).toString)
          } \/> NonRepresentableEJson(value.shows + " is not a valid OffsetTime")

      case (EJsonType(TypeTag.OffsetDate.value), Bson.Doc(map)) =>
        (extract(map.get(DateTimeConstants.year), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.month), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.day), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.offset), Bson._int32)) {
            (y, m, d, o) =>
              Bson.Date.fromInstant(
                JLocalDate.of(y, m, d).atStartOfDay.toInstant(ZoneOffset.ofTotalSeconds(o)))
          }.join \/> NonRepresentableEJson(value.shows + " is not a valid OffsetDate")

      case (EJsonType(TypeTag.LocalDateTime.value), Bson.Doc(map)) =>
        (extract(map.get(DateTimeConstants.year), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.month), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.day), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.hour), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.minute), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.second), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.nanosecond), Bson._int32)) {
            (y, mo, d, h, mi, s, n) =>
              Bson.Date.fromInstant(
                JLocalDateTime.of(y, mo, d, h, mi, s, n).toInstant(ZoneOffset.UTC))
          }.join \/> NonRepresentableEJson(value.shows + " is not a valid LocalDateTime")

      case (EJsonType(TypeTag.LocalTime.value), Bson.Doc(map)) =>
        (extract(map.get(DateTimeConstants.hour), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.minute), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.second), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.nanosecond), Bson._int32)) {
            (h, m, s, n) =>
              Bson.Text(
                JLocalTime.of(h, m, s, n).format(DateTimeFormatter.ISO_LOCAL_TIME))
          } \/> NonRepresentableEJson(value.shows + " is not a valid LocalTime")

      case (EJsonType(TypeTag.LocalDate.value), Bson.Doc(map)) =>
        (extract(map.get(DateTimeConstants.year), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.month), Bson._int32) ⊛
          extract(map.get(DateTimeConstants.day), Bson._int32)) {
            (y, m, d) =>
              Bson.Date.fromInstant(
                JLocalDate.of(y, m, d).atStartOfDay.toInstant(ZoneOffset.UTC))
          }.join \/> NonRepresentableEJson(value.shows + " is not a valid LocalDate")

      case (EJsonType(TypeTag.Interval.value), Bson.Doc(map)) =>
        (for {
          y <- extract[Int](map.get(DateTimeConstants.year), Bson._int32)
          m <- extract[Int](map.get(DateTimeConstants.month), Bson._int32)
          d <- extract[Int](map.get(DateTimeConstants.day), Bson._int32)
          s <- extract[Int](map.get(DateTimeConstants.second), Bson._int32)
          n <- extract[Int](map.get(DateTimeConstants.nanosecond), Bson._int32)
          if y === 0 && m === 0 // TODO support intervals with dates in them
        } yield {
          Bson.Dec(d*86400000 + s*1000 + n*1e-6)
        }) \/> NonRepresentableEJson(value.shows + " is either not a valid Interval or contains a nonzero year or month (currently only time-like intervals and days are supported)")

      case (_, _) => value.right
    }
  }

  def fromEJson(v: BsonVersion): AlgebraM[PlannerError \/ ?, EJson, Bson] = {
    case ExtEJson(ext) => fromExtension(ext)
    case CommonEJson(com) => fromCommon(v)(com).right
  }

  /** Converts the parts of `Bson` that it can, then stores the rest in,
    * effectively, `Free.Pure`.
    */
  def toEJson[F[a] <: ACopK[a]](implicit C: ejson.Common :<<: F, E: ejson.Extension :<<: F):
      ElgotCoalgebra[Bson \/ ?, F, Bson] = {
    case Bson.Arr(value)       => C.inj(ejson.Arr(value)).right
    case Bson.Doc(value)       =>
      E.inj(ejson.Map(value.toList.map(_.leftMap(Bson.Text(_))))).right
    case Bson.Null             => C.inj(ejson.Null()).right
    case Bson.Bool(value)      => C.inj(ejson.Bool(value)).right
    case Bson.Text(value)      => C.inj(ejson.Str(value)).right
    case Bson.Dec(value) if (!value.isNaN && !value.isInfinity)
                               => C.inj(ejson.Dec(value)).right
    case Bson.Dec128(value)    => C.inj(ejson.Dec(value)).right
    case Bson.Int32(value)     => E.inj(ejson.Int(value)).right
    case Bson.Int64(value)     => E.inj(ejson.Int(value)).right
    case Bson.Date(value)      =>
      // Bson dates are stored in UTC
      val dt = JLocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC)
      E.inj(ejson.Meta(
        Bson.Doc(ListMap(
          DateTimeConstants.year       -> Bson.Int32(dt.getYear),
          DateTimeConstants.month      -> Bson.Int32(dt.getMonth.getValue),
          DateTimeConstants.day        -> Bson.Int32(dt.getDayOfMonth),
          DateTimeConstants.hour       -> Bson.Int32(dt.getHour),
          DateTimeConstants.minute     -> Bson.Int32(dt.getMinute),
          DateTimeConstants.second     -> Bson.Int32(dt.getSecond),
          DateTimeConstants.nanosecond -> Bson.Int32(dt.getNano),
          DateTimeConstants.offset     -> Bson.Int32(0))),
        EJsonType(TypeTag.OffsetDateTime.value))).right
    case Bson.Binary(value)    =>
      E.inj(ejson.Meta(
        Bson.Text(ejson.z85.encode(ByteVector.view(value.toArray))),
        EJsonTypeSize(TypeTag.Binary.value, value.size))).right
    case id @ Bson.ObjectId(_) =>
      E.inj(ejson.Meta(Bson.Text(id.str), EJsonType("_bson.oid"))).right
    case bson                  => bson.left
  }

  def fromData(v: BsonVersion, data: Data): PlannerError \/ Bson =
    data.hyloM[PlannerError\/ ?, CoEnv[Data, EJson, ?], Bson](
      interpretM({
        case Data.NA => Bson.Undefined.right
        case data    => NonRepresentableData(data).left
      },
        fromEJson(v)),
      Data.toEJson[EJson].apply(_).right)

  def toData(bson: Bson): Data =
    bson.hylo[CoEnv[Bson, EJson, ?], Data](
      interpret(κ(Data.NA), Data.fromEJson),
      toEJson[EJson] ⋙ (CoEnv(_)))
}
