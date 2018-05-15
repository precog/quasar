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

import slamdata.Predef.{
  ???,
  Boolean,
  Byte,
  Double,
  Long,
  String
}

import quasar.qdata.{QData, QType}
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

  def tpe(a: Data): QType = ???

  def getLong(a: Data): Long = ???
  def makeLong(l: Long): Data = ???

  def getDouble(a: Data): Double = ???
  def makeDouble(l: Double): Data = ???

  def getReal(a: Data): Real = ???
  def makeReal(l: Real): Data = ???

  def getByte(a: Data): Byte = ???
  def makeByte(l: Byte): Data = ???

  def getString(a: Data): String = ???
  def makeString(l: String): Data = ???

  def makeNull: Data = ???

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

  def ArrayCursor = ???

  def getArrayCursor(a: Data): ArrayCursor = ???
  def hasNextArray(ac: ArrayCursor): Boolean = ???
  def getArrayAt(ac: ArrayCursor): Data = ???
  def stepArray(ac: ArrayCursor): ArrayCursor = ???

  def NascentArray = ???

  def prepArray: NascentArray = ???
  def pushArray(a: Data, na: NascentArray): NascentArray = ???
  def makeArray(na: NascentArray): Data = ???

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
