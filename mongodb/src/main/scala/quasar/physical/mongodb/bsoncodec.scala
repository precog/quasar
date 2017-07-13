/*
 * Copyright 2014–2017 SlamData Inc.
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
import quasar.ejson.EJson
import quasar.fp._
import quasar.fp.ski._
import quasar._, Planner._

import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}

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

  val fromCommon: Algebra[ejson.Common, Bson] = {
    case ejson.Arr(value)  => Bson.Arr(value)
    case ejson.Null()      => Bson.Null
    case ejson.Bool(value) => Bson.Bool(value)
    case ejson.Str(value)  => Bson.Text(value)
    case ejson.Dec(value)  => Bson.Dec(value.toDouble)
  }

  def extract[A](fa: Option[Bson], p: Prism[Bson, A]): Option[A] =
    fa.flatMap(p.getOption)

  val millisPerSec = 1000
  val nanosPerSec = 1000000000L

  val fromExtension: AlgebraM[PlannerError \/ ?, ejson.Extension, Bson] = {
    case ejson.Map(value) => value.traverse(_.bitraverse({
      case Bson.Text(key) => key.right
      case _              =>
        NonRepresentableEJson(value.toString + " is not a valid document key").left
    }, _.right)) ∘ (m => Bson.Doc(ListMap(m: _*)))
    // FIXME: cheating, but it’s what we’re already doing in the SQL parser
    case ejson.Char(value)      => Bson.Text(value.toString).right
    case ejson.Byte(value)      => Bson.Binary.fromArray(Array[Byte](value)).right
    case ejson.Int(value)       =>
      if (value.isValidInt) Bson.Int32(value.toInt).right
      else if (value.isValidLong) Bson.Int64(value.toLong).right
      else NonRepresentableEJson(value.toString + " is too large").left
    case ejson.Meta(value, meta) => (meta, value) match {
      case (EJsonType("_bson.oid"), Bson.Text(oid)) =>
        Bson.ObjectId.fromString(oid) \/> ObjectIdFormatError(oid)
      case (EJsonTypeSize("_ejson.binary", size), Bson.Text(data)) =>
        if (size.isValidInt)
          ejson.z85.decode(data).fold[PlannerError \/ Bson](
            NonRepresentableEJson("“" + data + "” is not a valid Z85-encoded string").left)(
            bv => Bson.Binary(ImmutableArray.fromArray(bv.take(size.toLong).toArray)).right)
        else NonRepresentableEJson(size.shows + " is too large for binary data").left
      case (EJsonType("_ejson.date"), Bson.Doc(map)) =>
        (extract(map.get("year"), Bson._int32) ⊛
          extract(map.get("day_of_year"), Bson._int32))((y, d) =>
          LocalDate.ofYearDay(y, d))
          .orElse((extract(map.get("year"), Bson._int32) ⊛
            extract(map.get("month"), Bson._int32) ⊛
            extract(map.get("day_of_month"), Bson._int32))((y, m, d) =>
            LocalDate.of(y, m, d)))
          .flatMap(date => Bson.Date.fromInstant(date.atStartOfDay.toInstant(ZoneOffset.UTC))) \/>
          NonRepresentableEJson(value.shows + " is not a valid date")
      case (EJsonType("_ejson.time"), Bson.Doc(map)) =>
        (extract(map.get("hour"), Bson._int32) ⊛
          extract(map.get("minute"), Bson._int32) ⊛
          extract(map.get("second"), Bson._int32) ⊛
          extract(map.get("nanosecond"), Bson._int32))((h, m, s, n) =>
          Bson.Text(
            pad2(h) + ":" +
              pad2(m) + ":" +
              pad2(s) + "." +
              pad3(n))) \/>
        NonRepresentableEJson(value.shows + " is not a valid time")
      case (EJsonType("_ejson.interval"), Bson.Doc(map)) =>
        extract(map.get("seconds"), Bson._dec).map(s => Bson.Dec(s * millisPerSec)) \/>
          NonRepresentableEJson(value.shows + " is not a valid interval")
      case (EJsonType("_ejson.timestamp"), Bson.Doc(map)) =>
        (extract(map.get("year"), Bson._int32) ⊛
          extract(map.get("month"), Bson._int32) ⊛
          extract(map.get("day_of_month"), Bson._int32) ⊛
          extract(map.get("hour"), Bson._int32) ⊛
          extract(map.get("minute"), Bson._int32) ⊛
          extract(map.get("second"), Bson._int32) ⊛
          extract(map.get("nanosecond"), Bson._int32))((y, mo, d, h, mi, s, n) =>
          Bson.Date.fromInstant(LocalDateTime.of(y, mo, d, h, mi, s, n).toInstant(ZoneOffset.UTC))).join \/>
          NonRepresentableEJson(value.shows + " is not a valid timestamp")
      case (_, _) => value.right
    }
  }

  val fromEJson: AlgebraM[PlannerError \/ ?, EJson, Bson] =
    _.run.fold(fromExtension, fromCommon(_).right)

  /** Converts the parts of `Bson` that it can, then stores the rest in,
    * effectively, `Free.Pure`.
    */
  def toEJson[F[_]](implicit C: ejson.Common :<: F, E: ejson.Extension :<: F):
      ElgotCoalgebra[Bson \/ ?, F, Bson] = {
    case Bson.Arr(value)       => C.inj(ejson.Arr(value)).right
    case Bson.Doc(value)       =>
      E.inj(ejson.Map(value.toList.map(_.leftMap(Bson.Text(_))))).right
    case Bson.Null             => C.inj(ejson.Null()).right
    case Bson.Bool(value)      => C.inj(ejson.Bool(value)).right
    case Bson.Text(value)      => C.inj(ejson.Str(value)).right
    case Bson.Dec(value) if (!value.isNaN && !value.isInfinity)
                               => C.inj(ejson.Dec(value)).right
    case Bson.Int32(value)     => E.inj(ejson.Int(value)).right
    case Bson.Int64(value)     => E.inj(ejson.Int(value)).right
    case Bson.Date(value)      =>
      val ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC)
      E.inj(ejson.Meta(
        Bson.Doc(ListMap(
          "year"         -> Bson.Int32(ldt.getYear),
          "month"        -> Bson.Int32(ldt.getMonth.getValue),
          "day_of_month" -> Bson.Int32(ldt.getDayOfMonth),
          "hour"         -> Bson.Int32(ldt.getHour),
          "minute"       -> Bson.Int32(ldt.getMinute),
          "second"       -> Bson.Int32(ldt.getSecond),
          "nanosecond"   -> Bson.Int32(ldt.getNano))),
        EJsonType("_ejson.timestamp"))).right
    case Bson.Binary(value)    =>
      E.inj(ejson.Meta(
        Bson.Text(ejson.z85.encode(ByteVector.view(value.toArray))),
        EJsonTypeSize("_ejson.binary", value.size))).right
    case id @ Bson.ObjectId(_) =>
      E.inj(ejson.Meta(Bson.Text(id.str), EJsonType("_bson.oid"))).right
    case bson                  => bson.left
  }

  def fromData(data: Data): PlannerError \/ Bson =
    data.hyloM[PlannerError\/ ?, CoEnv[Data, EJson, ?], Bson](
      interpretM({
        case Data.NA => Bson.Undefined.right
        case data    => NonRepresentableData(data).left
      },
        fromEJson),
      Data.toEJson[EJson].apply(_).right)

  def toData(bson: Bson): Data =
    bson.hylo[CoEnv[Bson, EJson, ?], Data](
      interpret(κ(Data.NA), Data.fromEJson),
      toEJson[EJson] ⋙ (CoEnv(_)))
}
