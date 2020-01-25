/*
 * Copyright 2014–2019 SlamData Inc.
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

import cats.instances.string._
import cats.syntax.eq._

import monocle.Optional

import qdata._
import qdata.time.{DateTimeInterval, OffsetDate}

import spire.math.Real

import java.math.MathContext
import java.time.{
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  OffsetTime,
}
import scala.sys.error

@SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
object QDataRValue extends QDataEncode[RValue] with QDataDecode[RValue] {
  import QType._

  object MetaKey {
    val Type = "quasar.rvalue.type"
    val Size = "quasar.rvalue.size"
  }

  object MetaType {
    val Binary = "quasar.rvalue.type.binary"
  }

  val rMetaType: Optional[RValue, String] =
    RValue.rField1(MetaKey.Type).composePrism(RValue.rString)

  val rMetaSize: Optional[RValue, Long] =
    RValue.rField1(MetaKey.Size).composePrism(RValue.rLong)

  def tpe(a: RValue): QType = a match {
    case RObject(_) => QObject
    case RArray(_) => QArray
    case RMeta(_, _) => QMeta
    case CEmptyObject => QObject
    case CEmptyArray => QArray
    case CString(_) => QString
    case CBoolean(_) => QBoolean
    case CLong(_) => QLong
    case CDouble(_) => QDouble
    case CNum(_) => QReal
    case COffsetDateTime(_) => QOffsetDateTime
    case COffsetDate(_) => QOffsetDate
    case COffsetTime(_) => QOffsetTime
    case CLocalDateTime(_) => QLocalDateTime
    case CLocalDate(_) => QLocalDate
    case CLocalTime(_) => QLocalTime
    case CInterval(_) => QInterval
    case CBinary(_) => QMeta
    case CNull => QNull
    case CUndefined => error("Unable to represent `CUndefined`.")
    case CArray(_, _) => error("Unable to represent `CArray`.")
  }

  def getLong(a: RValue): Long = a match {
    case CLong(value) => value
    case _ => error(s"Expected `CLong`. Received $a")
  }
  def makeLong(l: Long): RValue = CLong(l)

  def getDouble(a: RValue): Double = a match {
    case CDouble(value) => value
    case _ => error(s"Expected `CDouble`. Received $a")
  }
  def makeDouble(l: Double): RValue = CDouble(l)

  def getReal(a: RValue): Real = a match {
    case CNum(value) => Real.algebra.fromBigDecimal(value)
    case _ => error(s"Expected `CNum`. Received $a")
  }
  def makeReal(l: Real): RValue = CNum(l.toRational.toBigDecimal(MathContext.UNLIMITED))

  def getString(a: RValue): String = a match {
    case CString(value) => value
    case _ => error(s"Expected `CString`. Received $a")
  }
  def makeString(l: String): RValue = CString(l)

  def makeNull: RValue = CNull

  def getBoolean(a: RValue): Boolean = a match {
    case CBoolean(value) => value
    case _ => error(s"Expected `CBoolean`. Received $a")
  }
  def makeBoolean(l: Boolean): RValue = CBoolean(l)

  def getLocalDateTime(a: RValue): LocalDateTime = a match {
    case CLocalDateTime(value) => value
    case _ => error(s"Expected `CLocalDateTime`. Received $a")
  }
  def makeLocalDateTime(l: LocalDateTime): RValue = CLocalDateTime(l)

  def getLocalDate(a: RValue): LocalDate = a match {
    case CLocalDate(value) => value
    case _ => error(s"Expected `CLocalDate`. Received $a")
  }
  def makeLocalDate(l: LocalDate): RValue = CLocalDate(l)

  def getLocalTime(a: RValue): LocalTime = a match {
    case CLocalTime(value) => value
    case _ => error(s"Expected `CLocalTime`. Received $a")
  }
  def makeLocalTime(l: LocalTime): RValue = CLocalTime(l)

  def getOffsetDateTime(a: RValue): OffsetDateTime = a match {
    case COffsetDateTime(value) => value
    case _ => error(s"Expected `COffsetDateTime`. Received $a")
  }
  def makeOffsetDateTime(l: OffsetDateTime): RValue = COffsetDateTime(l)

  def getOffsetDate(a: RValue): OffsetDate = a match {
    case COffsetDate(value) => value
    case _ => error(s"Expected `COffsetDate`. Received $a")
  }
  def makeOffsetDate(l: OffsetDate): RValue = COffsetDate(l)

  def getOffsetTime(a: RValue): OffsetTime = a match {
    case COffsetTime(value) => value
    case _ => error(s"Expected `COffsetTime`. Received $a")
  }
  def makeOffsetTime(l: OffsetTime): RValue = COffsetTime(l)

  def getInterval(a: RValue): DateTimeInterval = a match {
    case CInterval(value) => value
    case _ => error(s"Expected `CInterval`. Received $a")
  }
  def makeInterval(l: DateTimeInterval): RValue = CInterval(l)

  type ArrayCursor = List[RValue]

  def getArrayCursor(a: RValue): ArrayCursor = a match {
    case RArray(arr) => arr
    case CEmptyArray => List[RValue]()
    case _ => error(s"Expected `RArray` or `CEmptyArray`. Received $a")
  }
  def hasNextArray(ac: ArrayCursor): Boolean = !ac.isEmpty
  def getArrayAt(ac: ArrayCursor): RValue = ac.head
  def stepArray(ac: ArrayCursor): ArrayCursor = ac.tail

  type NascentArray = List[RValue]

  def prepArray: NascentArray = List[RValue]()
  def pushArray(a: RValue, na: NascentArray): NascentArray = a :: na // prepend
  def makeArray(na: NascentArray): RValue = if (na.isEmpty) CEmptyArray else RArray(na.reverse)

  type ObjectCursor = List[(String, RValue)]

  def getObjectCursor(a: RValue): ObjectCursor = a match {
    case RObject(obj) => obj.toList
    case CEmptyObject => List[(String, RValue)]()
    case _ => error(s"Expected `Data.Obj`. Received $a")
  }
  def hasNextObject(ac: ObjectCursor): Boolean = !ac.isEmpty
  def getObjectKeyAt(ac: ObjectCursor): String = ac.head._1
  def getObjectValueAt(ac: ObjectCursor): RValue = ac.head._2
  def stepObject(ac: ObjectCursor): ObjectCursor = ac.tail

  type NascentObject = Map[String, RValue]

  def prepObject: NascentObject = Map[String, RValue]()
  def pushObject(key: String, a: RValue, na: NascentObject): NascentObject = na + ((key, a))
  def makeObject(na: NascentObject): RValue = if (na.isEmpty) CEmptyObject else RObject(na)

  val metaKey: String = "meta"

  def getMetaValue(a: RValue): RValue = a match {
    case RMeta(value, _) => value
    case CBinary(bytes) => RValue.rString(CBinary.toBase64String(bytes))
    case _ => error(s"Expected `RMeta`. Received $a")
  }
  def getMetaMeta(a: RValue): RValue = a match {
    case RMeta(_, RObject(o)) => o.get(metaKey) match {
      case Some(meta) => meta
      case _ => error(s"Expected `RObject` with $metaKey in meta meta. Received $a")
    }
    case CBinary(bytes) =>
      RValue.rObject(Map(
        MetaKey.Type -> RValue.rString(MetaType.Binary),
        MetaKey.Size -> RValue.rLong(bytes.length.toLong)))
    case _ => error(s"Expected `RMeta`. Received $a")
  }
  def makeMeta(value: RValue, meta: RValue): RValue = {
    def attemptBinary: Option[Array[Byte]] =
      if (rMetaType.exist(_ === MetaType.Binary)(meta))
        RValue.rString
          .getOption(value)
          .flatMap(CBinary.fromBase64String)
      else
        None

    attemptBinary.fold(RValue.rMeta(value, RObject(Map(metaKey -> meta))))(RValue.rBinary(_))
  }
}
