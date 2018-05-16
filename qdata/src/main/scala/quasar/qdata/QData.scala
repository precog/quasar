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

package quasar.qdata

import slamdata.Predef.{
  Boolean,
  Byte,
  Double,
  Long,
  String
}

import quasar.time.{DateTimeInterval, OffsetDate}

import spire.math.Real

import java.time.{
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  OffsetTime,
}

trait QData[A] {
  def tpe(a: A): QType

  def getLong(a: A): Long
  def makeLong(l: Long): A

  def getDouble(a: A): Double
  def makeDouble(l: Double): A

  def getReal(a: A): Real
  def makeReal(l: Real): A

  def getByte(a: A): Byte
  def makeByte(l: Byte): A

  def getString(a: A): String
  def makeString(l: String): A

  def makeNull: A

  def getBoolean(a: A): Boolean
  def makeBoolean(l: Boolean): A

  def getLocalDateTime(a: A): LocalDateTime
  def makeLocalDateTime(l: LocalDateTime): A

  def getLocalDate(a: A): LocalDate
  def makeLocalDate(l: LocalDate): A

  def getLocalTime(a: A): LocalTime
  def makeLocalTime(l: LocalTime): A

  def getOffsetDateTime(a: A): OffsetDateTime
  def makeOffsetDateTime(l: OffsetDateTime): A

  def getOffsetDate(a: A): OffsetDate
  def makeOffsetDate(l: OffsetDate): A

  def getOffsetTime(a: A): OffsetTime
  def makeOffsetTime(l: OffsetTime): A

  def getInterval(a: A): DateTimeInterval
  def makeInterval(l: DateTimeInterval): A

  type ArrayCursor

  def getArrayCursor(a: A): ArrayCursor
  def hasNextArray(ac: ArrayCursor): Boolean
  def getArrayAt(ac: ArrayCursor): A
  def stepArray(ac: ArrayCursor): ArrayCursor

  type NascentArray

  def prepArray: NascentArray
  def pushArray(a: A, na: NascentArray): NascentArray
  def makeArray(na: NascentArray): A

  type MapCursor

  def getMapCursor(a: A): MapCursor
  def hasNextMap(ac: MapCursor): Boolean
  def getMapKeyAt(ac: MapCursor): String
  def getMapValueAt(ac: MapCursor): A
  def stepMap(ac: MapCursor): MapCursor

  type NascentMap

  def prepMap: NascentMap
  def pushMap(key: String, a: A, na: NascentMap): NascentMap
  def makeMap(na: NascentMap): A

  def getMetaValue(a: A): A
  def getMetaMeta(a: A): A
  def makeMeta(value: A, meta: A): A

  ////

  // TODO write unit test for this
  def getArray(a: A): NascentArray = {
    @slamdata.Predef.SuppressWarnings(slamdata.Predef.Array("org.wartremover.warts.Recursion"))
    @scala.annotation.tailrec
    def iterate(cursor: ArrayCursor, nascent: NascentArray): NascentArray =
      if (hasNextArray(cursor))
        iterate(stepArray(cursor), pushArray(getArrayAt(cursor), nascent))
      else
        nascent

    iterate(getArrayCursor(a), prepArray)
  }
}
