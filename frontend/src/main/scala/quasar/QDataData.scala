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

package quasar

import slamdata.Predef._

import quasar.qdata._
import quasar.time.{DateTimeInterval, OffsetDate}

import spire.math.Real

import java.time.{
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  OffsetTime,
}

@slamdata.Predef.SuppressWarnings(slamdata.Predef.Array("org.wartremover.warts.NonUnitStatements"))
object QDataData extends QData[Data] {

  def tpe(a: Data): QType = a match {
    case Data.Null => QNull
    case Data.Str(_) => QString
    case Data.Bool(_) => QBoolean
    case Data.Dec(v) => if (v.isExactDouble) QDouble else QReal
    case Data.Int(v) => if (v.isValidLong) QLong else QReal
    case Data.OffsetDateTime(_) => QOffsetDateTime
    case Data.OffsetDate(_) => QOffsetDate
    case Data.OffsetTime(_) => QOffsetTime
    case Data.LocalDateTime(_) => QLocalDateTime
    case Data.LocalDate(_) => QLocalDate
    case Data.LocalTime(_) => QLocalTime
    case Data.Interval(_) => QInterval
    case Data.Binary(_) => QByte // ???
    case Data.Id(_) => ???
    case Data.NA => ???
    case Data.Obj(_) => QObject
    case Data.Arr(_) => QArray
  }

  def getLong(a: Data): Long = a match {
    case Data.Int(value) => value.toLong
    case _ => scala.sys.error(s"Expected a Long, received $a")
  }
  def makeLong(l: Long): Data = Data.Int(l)

  def getDouble(a: Data): Double = a match {
    case Data.Dec(value) => value.toDouble
    case _ => scala.sys.error(s"Expected a Double, received $a")
  }
  def makeDouble(l: Double): Data = Data.Dec(l)

  def getReal(a: Data): Real = ???
  def makeReal(l: Real): Data = ???

  def getByte(a: Data): Byte = ???
  def makeByte(l: Byte): Data = ???

  def getString(a: Data): String = a match {
    case Data.Str(value) => value
    case _ => scala.sys.error(s"Expected a String, received $a")
  }
  def makeString(l: String): Data = Data.Str(l)

  def makeNull: Data = Data.Null

  def getBoolean(a: Data): Boolean = a match {
    case Data.Bool(value) => value
    case _ => scala.sys.error(s"Expected a Boolean, received $a")
  }
  def makeBoolean(l: Boolean): Data = Data.Bool(l)

  def getLocalDateTime(a: Data): LocalDateTime = a match {
    case Data.LocalDateTime(value) => value
    case _ => scala.sys.error(s"Expected a LocalDateTime, received $a")
  }
  def makeLocalDateTime(l: LocalDateTime): Data = Data.LocalDateTime(l)

  def getLocalDate(a: Data): LocalDate = a match {
    case Data.LocalDate(value) => value
    case _ => scala.sys.error(s"Expected a LocalDate, received $a")
  }
  def makeLocalDate(l: LocalDate): Data = Data.LocalDate(l)

  def getLocalTime(a: Data): LocalTime = a match {
    case Data.LocalTime(value) => value
    case _ => scala.sys.error(s"Expected a LocalTime, received $a")
  }
  def makeLocalTime(l: LocalTime): Data = Data.LocalTime(l)

  def getOffsetDateTime(a: Data): OffsetDateTime = a match {
    case Data.OffsetDateTime(value) => value
    case _ => scala.sys.error(s"Expected an OffsetDateTime, received $a")
  }
  def makeOffsetDateTime(l: OffsetDateTime): Data = Data.OffsetDateTime(l)

  def getOffsetDate(a: Data): OffsetDate = a match {
    case Data.OffsetDate(value) => value
    case _ => scala.sys.error(s"Expected an OffsetDate, received $a")
  }
  def makeOffsetDate(l: OffsetDate): Data = Data.OffsetDate(l)

  def getOffsetTime(a: Data): OffsetTime = a match {
    case Data.OffsetTime(value) => value
    case _ => scala.sys.error(s"Expected an OffsetTime, received $a")
  }
  def makeOffsetTime(l: OffsetTime): Data = Data.OffsetTime(l)

  def getInterval(a: Data): DateTimeInterval = a match {
    case Data.Interval(value) => value
    case _ => scala.sys.error(s"Expected an Interval, received $a")
  }
  def makeInterval(l: DateTimeInterval): Data = Data.Interval(l)

  // TODO use Natural for the index
  final case class ArrayCursor(index: Int, values: List[Data])

  def getArrayCursor(a: Data): ArrayCursor = a match {
    case Data.Arr(arr) => ArrayCursor(0, arr)
    case _ => scala.sys.error(s"Expected an Array, received $a")
  }
  def hasNextArray(ac: ArrayCursor): Boolean = ac.values.length > ac.index
  def getArrayAt(ac: ArrayCursor): Data = ac.values(ac.index)
  def stepArray(ac: ArrayCursor): ArrayCursor = ac.copy(index = ac.index + 1)

  type NascentArray = List[Data]

  def prepArray: NascentArray = List[Data]()
  def pushArray(a: Data, na: NascentArray): NascentArray = a +: na // prepend
  def makeArray(na: NascentArray): Data = Data.Arr(na)

  // TODO use Natural for the index
  final case class ObjectCursor(index: Int, values: List[(String, Data)])

  def getObjectCursor(a: Data): ObjectCursor = a match {
    case Data.Obj(obj) => ObjectCursor(0, obj.toList)
    case _ => scala.sys.error(s"Expected an Object, received $a")
  }
  def hasNextObject(ac: ObjectCursor): Boolean = ac.values.length > ac.index
  def getObjectKeyAt(ac: ObjectCursor): String = ac.values(ac.index)._1
  def getObjectValueAt(ac: ObjectCursor): Data = ac.values(ac.index)._2
  def stepObject(ac: ObjectCursor): ObjectCursor = ac.copy(index = ac.index + 1)

  type NascentObject = List[(String, Data)]

  def prepObject: NascentObject = List[(String, Data)]()
  def pushObject(key: String, a: Data, na: NascentObject): NascentObject = (key, a) +: na // prepend
  def makeObject(na: NascentObject): Data = Data.Obj(ListMap(na: _*))

  def getMetaValue(a: Data): Data = ???
  def getMetaMeta(a: Data): Data = ???
  def makeMeta(value: Data, meta: Data): Data = ???
}
