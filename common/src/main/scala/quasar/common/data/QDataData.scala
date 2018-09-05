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

package quasar.common.data

import slamdata.Predef._

import qdata._
import qdata.time.{DateTimeInterval, OffsetDate}

import spire.math.{Natural, Real}

import java.math.MathContext
import java.time.{
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  OffsetTime,
}
import scala.sys.error

object QDataData extends QData[Data] {
  import QType._

  def tpe(a: Data): QType = a match {
    case Data.Null => QNull
    case Data.Str(_) => QString
    case Data.Bool(_) => QBoolean
    case Data.Dec(v) => if (v.isDecimalDouble) QDouble else QReal
    case Data.Int(v) => if (v.isValidLong) QLong else QReal
    case Data.OffsetDateTime(_) => QOffsetDateTime
    case Data.OffsetDate(_) => QOffsetDate
    case Data.OffsetTime(_) => QOffsetTime
    case Data.LocalDateTime(_) => QLocalDateTime
    case Data.LocalDate(_) => QLocalDate
    case Data.LocalTime(_) => QLocalTime
    case Data.Interval(_) => QInterval
    case Data.NA => error(s"Unable to represent `Data.NA`.")
    case Data.Obj(_) => QObject
    case Data.Arr(_) => QArray
  }

  def getLong(a: Data): Long = a match {
    case Data.Int(value) => value.toLong
    case _ => error(s"Expected `Data.Int`. Received $a")
  }
  def makeLong(l: Long): Data = Data.Int(l)

  def getDouble(a: Data): Double = a match {
    case Data.Dec(value) => value.toDouble
    case _ => error(s"Expected `Data.Dec`. Received $a")
  }
  def makeDouble(l: Double): Data = Data.Dec(l)

  def getReal(a: Data): Real = a match {
    case Data.Dec(value) => Real.algebra.fromBigDecimal(value)
    case Data.Int(value) => Real.algebra.fromBigDecimal(BigDecimal(value))
    case _ => error(s"Expected `Data.Dec`. Received $a")
  }
  def makeReal(l: Real): Data = Data.Dec(l.toRational.toBigDecimal(MathContext.UNLIMITED))

  def getString(a: Data): String = a match {
    case Data.Str(value) => value
    case _ => error(s"Expected `Data.Str`. Received $a")
  }
  def makeString(l: String): Data = Data.Str(l)

  def makeNull: Data = Data.Null

  def getBoolean(a: Data): Boolean = a match {
    case Data.Bool(value) => value
    case _ => error(s"Expected `Data.Bool`. Received $a")
  }
  def makeBoolean(l: Boolean): Data = Data.Bool(l)

  def getLocalDateTime(a: Data): LocalDateTime = a match {
    case Data.LocalDateTime(value) => value
    case _ => error(s"Expected `Data.LocalDateTime`. Received $a")
  }
  def makeLocalDateTime(l: LocalDateTime): Data = Data.LocalDateTime(l)

  def getLocalDate(a: Data): LocalDate = a match {
    case Data.LocalDate(value) => value
    case _ => error(s"Expected `Data.LocalDate`. Received $a")
  }
  def makeLocalDate(l: LocalDate): Data = Data.LocalDate(l)

  def getLocalTime(a: Data): LocalTime = a match {
    case Data.LocalTime(value) => value
    case _ => error(s"Expected `Data.LocalTime`. Received $a")
  }
  def makeLocalTime(l: LocalTime): Data = Data.LocalTime(l)

  def getOffsetDateTime(a: Data): OffsetDateTime = a match {
    case Data.OffsetDateTime(value) => value
    case _ => error(s"Expected `Data.OffsetDateTime`. Received $a")
  }
  def makeOffsetDateTime(l: OffsetDateTime): Data = Data.OffsetDateTime(l)

  def getOffsetDate(a: Data): OffsetDate = a match {
    case Data.OffsetDate(value) => value
    case _ => error(s"Expected `Data.OffsetDate`. Received $a")
  }
  def makeOffsetDate(l: OffsetDate): Data = Data.OffsetDate(l)

  def getOffsetTime(a: Data): OffsetTime = a match {
    case Data.OffsetTime(value) => value
    case _ => error(s"Expected `Data.OffsetTime`. Received $a")
  }
  def makeOffsetTime(l: OffsetTime): Data = Data.OffsetTime(l)

  def getInterval(a: Data): DateTimeInterval = a match {
    case Data.Interval(value) => value
    case _ => error(s"Expected `Data.Interval`. Received $a")
  }
  def makeInterval(l: DateTimeInterval): Data = Data.Interval(l)

  final case class ArrayCursor(index: Natural, values: List[Data])

  def getArrayCursor(a: Data): ArrayCursor = a match {
    case Data.Arr(arr) => ArrayCursor(Natural.zero, arr)
    case _ => error(s"Expected `Data.Arr`. Received $a")
  }
  def hasNextArray(ac: ArrayCursor): Boolean = ac.values.length > ac.index.toInt
  def getArrayAt(ac: ArrayCursor): Data = ac.values(ac.index.toInt)
  def stepArray(ac: ArrayCursor): ArrayCursor = ac.copy(index = ac.index + Natural.one)

  type NascentArray = List[Data]

  def prepArray: NascentArray = List[Data]()
  def pushArray(a: Data, na: NascentArray): NascentArray = a +: na // prepend
  def makeArray(na: NascentArray): Data = Data.Arr(na.reverse)

  final case class ObjectCursor(index: Natural, values: List[(String, Data)])

  def getObjectCursor(a: Data): ObjectCursor = a match {
    case Data.Obj(obj) => ObjectCursor(Natural.zero, obj.toList)
    case _ => error(s"Expected `Data.Obj`. Received $a")
  }
  def hasNextObject(ac: ObjectCursor): Boolean = ac.values.length > ac.index.toInt
  def getObjectKeyAt(ac: ObjectCursor): String = ac.values(ac.index.toInt)._1
  def getObjectValueAt(ac: ObjectCursor): Data = ac.values(ac.index.toInt)._2
  def stepObject(ac: ObjectCursor): ObjectCursor = ac.copy(index = ac.index + Natural.one)

  type NascentObject = List[(String, Data)]

  def prepObject: NascentObject = List[(String, Data)]()
  def pushObject(key: String, a: Data, na: NascentObject): NascentObject = (key, a) +: na // prepend
  def makeObject(na: NascentObject): Data = Data.Obj(ListMap(na.reverse: _*))

  def getMetaValue(a: Data): Data = error(s"Unable to represent metadata in `Data`.")
  def getMetaMeta(a: Data): Data = error(s"Unable to represent metadata in `Data`.")
  def makeMeta(value: Data, meta: Data): Data = error(s"Unable to create metadata in `Data`.")
}
