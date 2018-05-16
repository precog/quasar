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
    case Data.Dec(_) => QReal
    case Data.Bool(_) => QBoolean
    case Data.Str(_) => QString
    case _ => scala.sys.error(s"not implemented")
  }

  def getLong(a: Data): Long = ???
  def makeLong(l: Long): Data = ???

  def getDouble(a: Data): Double = ???
  def makeDouble(l: Double): Data = ???

  def getReal(a: Data): Real = ???
  def makeReal(l: Real): Data = ???

  def getByte(a: Data): Byte = ???
  def makeByte(l: Byte): Data = ???

  def getString(a: Data): String = a match {
    case Data.Str(value) => value
    case _ => scala.sys.error(s"Expected a string, received $a")
  }
  def makeString(l: String): Data = Data.Str(l)

  def makeNull: Data = Data.Null

  def getBoolean(a: Data): Boolean = ???
  def makeBoolean(l: Boolean): Data = ???

  def getLocalDateTime(a: Data): LocalDateTime = ???
  def makeLocalDateTime(l: LocalDateTime): Data = ???

  def getLocalDate(a: Data): LocalDate = ???
  def makeLocalDate(l: LocalDate): Data = ???

  def getLocalTime(a: Data): LocalTime = ???
  def makeLocalTime(l: LocalTime): Data = ???

  def getOffsetDateTime(a: Data): OffsetDateTime = ???
  def makeOffsetDateTime(l: OffsetDateTime): Data = ???

  def getOffsetDate(a: Data): OffsetDate = ???
  def makeOffsetDate(l: OffsetDate): Data = ???

  def getOffsetTime(a: Data): OffsetTime = ???
  def makeOffsetTime(l: OffsetTime): Data = ???

  def getInterval(a: Data): DateTimeInterval = ???
  def makeInterval(l: DateTimeInterval): Data = ???

  // TODO use Natural for the index
  final case class ArrayCursor(index: Int, values: List[Data])

  def getArrayCursor(a: Data): ArrayCursor = a match {
    case Data.Arr(arr) => ArrayCursor(0, arr)
    case _ => scala.sys.error(s"Expected an array, received $a")
  }
  def hasNextArray(ac: ArrayCursor): Boolean = ac.values.length < ac.index
  def getArrayAt(ac: ArrayCursor): Data = ac.values(ac.index)
  def stepArray(ac: ArrayCursor): ArrayCursor = ac.copy(index = ac.index + 1)

  type NascentArray = List[Data]

  def prepArray: NascentArray = List[Data]()
  def pushArray(a: Data, na: NascentArray): NascentArray = a +: na // prepend
  def makeArray(na: NascentArray): Data = Data.Arr(na)

  def MapCursor = ???

  def getMapCursor(a: Data): MapCursor = ???
  def hasNextMap(ac: MapCursor): Boolean = ???
  def getMapKeyAt(ac: MapCursor): String = ???
  def getMapValueAt(ac: MapCursor): Data = ???
  def stepMap(ac: MapCursor): MapCursor = ???

  def NascentMap = ???

  def prepMap: NascentMap = ???
  def pushMap(key: String, a: Data, na: NascentMap): NascentMap = ???
  def makeMap(na: NascentMap): Data = ???

  def getMetaValue(a: Data): Data = ???
  def getMetaMeta(a: Data): Data = ???
  def makeMeta(value: Data, meta: Data): Data = ???
}
