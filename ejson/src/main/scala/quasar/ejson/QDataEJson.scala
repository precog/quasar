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

package quasar.ejson

import slamdata.Predef.{Map => SMap, _}
import quasar.contrib.iota._

import scala.sys.error

import java.math.MathContext
import java.time.{
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  OffsetTime,
  ZoneOffset
}

import qdata.{QDataDecode, QDataEncode, QType}
import qdata.time.{DateTimeInterval, OffsetDate}
import matryoshka.{Corecursive, Embed, Recursive}
import matryoshka.implicits._
import spire.math.Real

private[ejson] object QDataEJson {
  import QType._

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  def decode[J](implicit J: Recursive.Aux[J, EJson]): QDataDecode[J] =
    new QDataDecode[J] {
      def tpe(a: J): QType =
        a.project match {
          case CommonEJson(Arr(_)) => QArray
          case CommonEJson(Null()) => QNull
          case CommonEJson(Bool(_)) => QBoolean
          case CommonEJson(Str(_)) => QString
          case CommonEJson(Dec(v)) =>
            if (v.isValidLong) QLong
            else if (v.isDecimalDouble) QDouble
            else QReal
          case ExtEJson(Map(_)) => QObject
          case ExtEJson(Char(_)) => QString
          case ExtEJson(Int(v)) =>
            if (v.isValidLong) QLong
            else QReal
          case ExtEJson(Meta(_, Embed(Type(TypeTag.LocalDateTime)))) => QLocalDateTime
          case ExtEJson(Meta(_, Embed(Type(TypeTag.LocalDate)))) => QLocalDate
          case ExtEJson(Meta(_, Embed(Type(TypeTag.LocalTime)))) => QLocalTime
          case ExtEJson(Meta(_, Embed(Type(TypeTag.OffsetDateTime)))) => QOffsetDateTime
          case ExtEJson(Meta(_, Embed(Type(TypeTag.OffsetDate)))) => QOffsetDate
          case ExtEJson(Meta(_, Embed(Type(TypeTag.OffsetTime)))) => QOffsetTime
          case ExtEJson(Meta(_, Embed(Type(TypeTag.Interval)))) => QInterval
          case ExtEJson(Meta(_, _)) => QMeta
        }

      def getLong(a: J): Long =
        a.project match {
          case CommonEJson(Dec(v)) => v.toLong
          case ExtEJson(Int(v)) => v.toLong
          case x => error(s"Expected `Common.Dec` or `Extension.Int`. Received $x")
        }

      def getDouble(a: J): Double =
        a.project match {
          case CommonEJson(Dec(v)) => v.toDouble
          case x => error(s"Expected `Common.Dec`. Received $x")
        }

      def getReal(a: J): Real =
        a.project match {
          case CommonEJson(Dec(v)) => Real(v)
          case ExtEJson(Int(v)) => Real(v)
          case x => error(s"Expected `Common.Dec` or `Extension.Int`. Received $x")
        }

      def getString(a: J): String =
        a.project match {
          case CommonEJson(Str(s)) => s
          case ExtEJson(Char(c)) => c.toString
          case x => error(s"Expected `Common.Str` or `Extension.Char`. Received $x")
        }

      def getBoolean(a: J): Boolean =
        a.project match {
          case CommonEJson(Bool(b)) => b
          case x => error(s"Expected `Common.Bool`. Received $x")
        }

      def getLocalDateTime(a: J): LocalDateTime =
        getMetaValue(a).project match {
          case ExtEJson(Map(as)) =>
            val t = for {
              y <- mapLookup(TemporalKeys.year, as)
              mo <- mapLookup(TemporalKeys.month, as)
              d <- mapLookup(TemporalKeys.day, as)
              h <- mapLookup(TemporalKeys.hour, as)
              m <- mapLookup(TemporalKeys.minute, as)
              s <- mapLookup(TemporalKeys.second, as)
              ns <- mapLookup(TemporalKeys.nanosecond, as)
            } yield LocalDateTime.of(
              getLong(y).toInt,
              getLong(mo).toInt,
              getLong(d).toInt,
              getLong(h).toInt,
              getLong(m).toInt,
              getLong(s).toInt,
              getLong(ns).toInt)

            t getOrElse error(s"Expected `LocalDateTime`. Received $as")

          case x => error(s"Expected `Extension.Map`. Received $x")
        }

      def getLocalDate(a: J): LocalDate =
        getMetaValue(a).project match {
          case ExtEJson(Map(as)) =>
            val t = for {
              y <- mapLookup(TemporalKeys.year, as)
              mo <- mapLookup(TemporalKeys.month, as)
              d <- mapLookup(TemporalKeys.day, as)
            } yield LocalDate.of(
              getLong(y).toInt,
              getLong(mo).toInt,
              getLong(d).toInt)

            t getOrElse error(s"Expected `LocalDate`. Received $as")

          case x => error(s"Expected `Extension.Map`. Received $x")
        }

      def getLocalTime(a: J): LocalTime =
        getMetaValue(a).project match {
          case ExtEJson(Map(as)) =>
            val t = for {
              h <- mapLookup(TemporalKeys.hour, as)
              m <- mapLookup(TemporalKeys.minute, as)
              s <- mapLookup(TemporalKeys.second, as)
              ns <- mapLookup(TemporalKeys.nanosecond, as)
            } yield LocalTime.of(
              getLong(h).toInt,
              getLong(m).toInt,
              getLong(s).toInt,
              getLong(ns).toInt)

            t getOrElse error(s"Expected `LocalTime`. Received $as")

          case x => error(s"Expected `Extension.Map`. Received $x")
        }

      def getOffsetDateTime(a: J): OffsetDateTime =
        getMetaValue(a).project match {
          case ExtEJson(Map(as)) =>
            val t = for {
              y <- mapLookup(TemporalKeys.year, as)
              mo <- mapLookup(TemporalKeys.month, as)
              d <- mapLookup(TemporalKeys.day, as)
              h <- mapLookup(TemporalKeys.hour, as)
              m <- mapLookup(TemporalKeys.minute, as)
              s <- mapLookup(TemporalKeys.second, as)
              ns <- mapLookup(TemporalKeys.nanosecond, as)
              o <- mapLookup(TemporalKeys.offset, as)
            } yield OffsetDateTime.of(
              getLong(y).toInt,
              getLong(mo).toInt,
              getLong(d).toInt,
              getLong(h).toInt,
              getLong(m).toInt,
              getLong(s).toInt,
              getLong(ns).toInt,
              ZoneOffset.ofTotalSeconds(getLong(o).toInt))

            t getOrElse error(s"Expected `OffsetDateTime`. Received $as")

          case x => error(s"Expected `Extension.Map`. Received $x")
        }

      def getOffsetDate(a: J): OffsetDate =
        getMetaValue(a).project match {
          case ExtEJson(Map(as)) =>
            val t = for {
              y <- mapLookup(TemporalKeys.year, as)
              mo <- mapLookup(TemporalKeys.month, as)
              d <- mapLookup(TemporalKeys.day, as)
              o <- mapLookup(TemporalKeys.offset, as)
            } yield OffsetDate(
              LocalDate.of(
                getLong(y).toInt,
                getLong(mo).toInt,
                getLong(d).toInt),
              ZoneOffset.ofTotalSeconds(getLong(o).toInt))

            t getOrElse error(s"Expected `OffsetDate`. Received $as")

          case x => error(s"Expected `Extension.Map`. Received $x")
        }

      def getOffsetTime(a: J): OffsetTime =
        getMetaValue(a).project match {
          case ExtEJson(Map(as)) =>
            val t = for {
              h <- mapLookup(TemporalKeys.hour, as)
              m <- mapLookup(TemporalKeys.minute, as)
              s <- mapLookup(TemporalKeys.second, as)
              ns <- mapLookup(TemporalKeys.nanosecond, as)
              o <- mapLookup(TemporalKeys.offset, as)
            } yield OffsetTime.of(
              getLong(h).toInt,
              getLong(m).toInt,
              getLong(s).toInt,
              getLong(ns).toInt,
              ZoneOffset.ofTotalSeconds(getLong(o).toInt))

            t getOrElse error(s"Expected `OffsetTime`. Received $as")

          case x => error(s"Expected `Extension.Map`. Received $x")
        }

      def getInterval(a: J): DateTimeInterval =
        getMetaValue(a).project match {
          case ExtEJson(Map(as)) =>
            val t = for {
              y <- mapLookup(TemporalKeys.year, as)
              mo <- mapLookup(TemporalKeys.month, as)
              d <- mapLookup(TemporalKeys.day, as)
              s <- mapLookup(TemporalKeys.second, as)
              ns <- mapLookup(TemporalKeys.nanosecond, as)
            } yield DateTimeInterval.make(
              getLong(y).toInt,
              getLong(mo).toInt,
              getLong(d).toInt,
              getLong(s),
              getLong(ns))

            t getOrElse error(s"Expected `DateTimeInterval`. Received $as")

          case x => error(s"Expected `Extension.Map`. Received $x")
        }

      type ArrayCursor = List[J]

      def getArrayCursor(a: J): ArrayCursor =
        a.project match {
          case CommonEJson(Arr(js)) => js
          case x => error(s"Expected `Common.Arr`. Received $x")
        }

      def hasNextArray(ac: ArrayCursor): Boolean =
        ac.nonEmpty

      def getArrayAt(ac: ArrayCursor): J =
        ac.head

      def stepArray(ac: ArrayCursor): ArrayCursor =
        ac.tail

      type ObjectCursor = List[(J, J)]

      def getObjectCursor(a: J): ObjectCursor =
        a.project match {
          case ExtEJson(Map(assocs)) => assocs
          case x => error(s"Expected `Extension.Map`. Received $x")
        }

      def hasNextObject(ac: ObjectCursor): Boolean =
        ac.nonEmpty

      def getObjectKeyAt(ac: ObjectCursor): String =
        ac.head._1.project match {
          case CommonEJson(Str(k)) => k
          case x => error(s"Expected `Common.Str`. Received $x")
        }

      def getObjectValueAt(ac: ObjectCursor): J =
        ac.head._2

      def stepObject(ac: ObjectCursor): ObjectCursor =
        ac.tail

      def getMetaValue(a: J): J =
        a.project match {
          case ExtEJson(Meta(v, _)) => v
          case x => error(s"Expected `Extension.Meta`. Received $x")
        }

      def getMetaMeta(a: J): J =
        a.project match {
          case ExtEJson(Meta(_, m)) => m
          case x => error(s"Expected `Extension.Meta`. Received $x")
        }

      private def mapLookup(k: String, m: List[(J, J)]): Option[J] = {
        val assoc = m find {
          case (Embed(CommonEJson(Str(`k`))), _) => true
          case _ => false
        }

        assoc.map(_._2)
      }
    }

  def encode[J](implicit J: Corecursive.Aux[J, EJson]): QDataEncode[J] =
    new QDataEncode[J] {
      def makeLong(l: Long): J =
        EJson.int(BigInt(l))

      def makeDouble(l: Double): J =
        EJson.dec(BigDecimal(l))

      def makeReal(l: Real): J =
        if (l.isWhole)
          EJson.int(l.toRational.toBigInt)
        else
          EJson.dec(l.toRational.toBigDecimal(MathContext.UNLIMITED))

      def makeString(l: String): J =
        EJson.str(l)

      def makeNull: J =
        EJson.nul[J]

      def makeBoolean(l: Boolean): J =
        EJson.bool(l)

      def makeLocalDateTime(l: LocalDateTime): J =
        temporalMap(TypeTag.LocalDateTime, SMap(
          TemporalKeys.year -> EJson.int(BigInt(l.getYear)),
          TemporalKeys.month -> EJson.int(BigInt(l.getMonth.getValue)),
          TemporalKeys.day -> EJson.int(BigInt(l.getDayOfMonth)),
          TemporalKeys.hour -> EJson.int(BigInt(l.getHour)),
          TemporalKeys.minute -> EJson.int(BigInt(l.getMinute)),
          TemporalKeys.second -> EJson.int(BigInt(l.getSecond)),
          TemporalKeys.nanosecond -> EJson.int(BigInt(l.getNano))))

      def makeLocalDate(l: LocalDate): J =
        temporalMap(TypeTag.LocalDate, SMap(
          TemporalKeys.year -> EJson.int(BigInt(l.getYear)),
          TemporalKeys.month -> EJson.int(BigInt(l.getMonth.getValue)),
          TemporalKeys.day -> EJson.int(BigInt(l.getDayOfMonth))))

      def makeLocalTime(l: LocalTime): J =
        temporalMap(TypeTag.LocalTime, SMap(
          TemporalKeys.hour -> EJson.int(BigInt(l.getHour)),
          TemporalKeys.minute -> EJson.int(BigInt(l.getMinute)),
          TemporalKeys.second -> EJson.int(BigInt(l.getSecond)),
          TemporalKeys.nanosecond -> EJson.int(BigInt(l.getNano))))

      def makeOffsetDateTime(l: OffsetDateTime): J =
        temporalMap(TypeTag.OffsetDateTime, SMap(
          TemporalKeys.year -> EJson.int(BigInt(l.getYear)),
          TemporalKeys.month -> EJson.int(BigInt(l.getMonth.getValue)),
          TemporalKeys.day -> EJson.int(BigInt(l.getDayOfMonth)),
          TemporalKeys.hour -> EJson.int(BigInt(l.getHour)),
          TemporalKeys.minute -> EJson.int(BigInt(l.getMinute)),
          TemporalKeys.second -> EJson.int(BigInt(l.getSecond)),
          TemporalKeys.nanosecond -> EJson.int(BigInt(l.getNano)),
          TemporalKeys.offset -> EJson.int(BigInt(l.getOffset.getTotalSeconds))))

      def makeOffsetDate(l: OffsetDate): J =
        temporalMap(TypeTag.OffsetDate, SMap(
          TemporalKeys.year -> EJson.int(BigInt(l.date.getYear)),
          TemporalKeys.month -> EJson.int(BigInt(l.date.getMonth.getValue)),
          TemporalKeys.day -> EJson.int(BigInt(l.date.getDayOfMonth)),
          TemporalKeys.offset -> EJson.int(BigInt(l.offset.getTotalSeconds))))

      def makeOffsetTime(l: OffsetTime): J =
        temporalMap(TypeTag.OffsetTime, SMap(
          TemporalKeys.hour -> EJson.int(BigInt(l.getHour)),
          TemporalKeys.minute -> EJson.int(BigInt(l.getMinute)),
          TemporalKeys.second -> EJson.int(BigInt(l.getSecond)),
          TemporalKeys.nanosecond -> EJson.int(BigInt(l.getNano)),
          TemporalKeys.offset -> EJson.int(BigInt(l.getOffset.getTotalSeconds))))

      def makeInterval(l: DateTimeInterval): J =
        temporalMap(TypeTag.Interval, SMap(
          TemporalKeys.year -> EJson.int(BigInt(l.period.getYears)),
          TemporalKeys.month -> EJson.int(BigInt(l.period.getMonths)),
          TemporalKeys.day -> EJson.int(BigInt(l.period.getDays)),
          TemporalKeys.second -> EJson.int(BigInt(l.duration.getSeconds)),
          TemporalKeys.nanosecond -> EJson.int(BigInt(l.duration.getNano))))

      type NascentArray = List[J]

      val prepArray: NascentArray = Nil

      def pushArray(a: J, na: NascentArray): NascentArray =
        a :: na

      def makeArray(na: NascentArray): J =
        EJson.fromCommon(Arr(na.reverse))

      type NascentObject = List[(J, J)]

      val prepObject: NascentObject = Nil

      def pushObject(key: String, a: J, na: NascentObject): NascentObject =
        (EJson.str(key), a) :: na

      def makeObject(na: NascentObject): J =
        EJson.fromExt(Map(na.reverse))

      def makeMeta(value: J, meta: J): J =
        EJson.meta(value, meta)

      private def temporalMap(tt: TypeTag, m: SMap[String, J]): J =
        EJson.meta(
          EJson.fromExt(Map(m.toList map { case (k, v) => (EJson.str(k), v) })),
          Type(tt))
    }
}
