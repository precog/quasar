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

package quasar.precog
package common

import quasar.blueeyes._, json._
import quasar.common.data.Data
import qdata.time.{DateTimeInterval, OffsetDate}

import monocle.{Optional, Prism, Traversal}
import monocle.function.{At, Each, Index}
import scalaz._, Scalaz._, Ordering._

import java.math.MathContext.UNLIMITED
import java.time._

import scala.reflect.ClassTag

sealed trait RValue { self =>
  def toJValue: JValue
  def toJValueRaw: JValue

  def unsafeInsert(path: CPath, value: RValue): RValue = {
    RValue.unsafeInsert(self, path, value)
  }

  def flattenWithPath: List[(CPath, CValue)] = {
    val lb = List.newBuilder[(CPath, CValue)]
    def flatten0(path: CPath, value: RValue): Unit = value match {
      case RObject(fields) if fields.isEmpty =>
        lb += ((path, CEmptyObject))

      case RArray(elems) if elems.isEmpty =>
        lb += ((path, CEmptyArray))

      case RObject(fields) =>
        val it = fields.iterator
        while (it.hasNext) {
          val (k, v) = it.next()
          flatten0(path \ k, v)
        }

      case RArray(elems) =>
        val it = elems.iterator
        var i = 0
        while (it.hasNext) {
          flatten0(path \ i, it.next())
          i = i + 1
        }

      case (v: CValue) =>
        lb += ((path, v))
    }

    flatten0(CPath.Identity, self)
    lb.result()
  }
}

object RValue extends RValueInstances {

  val rObject: Prism[RValue, Map[String, RValue]] =
    Prism.partial[RValue, Map[String, RValue]] {
      case RObject(fields) => fields
    } (RObject(_))

  def rField(field: String): Optional[RValue, Option[RValue]] =
    rObject composeLens At.at(field)

  def rField1(field: String): Optional[RValue, RValue] =
    rField(field) composePrism monocle.std.option.some

  val rFields: Traversal[RValue, RValue] =
    rObject composeTraversal Each.each

  val rArray: Prism[RValue, List[RValue]] =
    Prism.partial[RValue, List[RValue]] {
      case RArray(values) => values
    } (RArray(_))

  def rElement(index: Int): Optional[RValue, RValue] =
    rArray composeOptional Index.index(index)

  val rElements: Traversal[RValue, RValue] =
    rArray composeTraversal Each.each

  val rUndefined: Prism[RValue, Unit] =
    Prism.partial[RValue, Unit] {
      case CUndefined => ()
    } (_ => CUndefined)

  val rNull: Prism[RValue, Unit] =
    Prism.partial[RValue, Unit] {
      case CNull => ()
    } (_ => CNull)

  val rEmptyObject: Prism[RValue, Unit] =
    Prism.partial[RValue, Unit] {
      case CEmptyObject => ()
    } (_ => CEmptyObject)

  val rEmptyArray: Prism[RValue, Unit] =
    Prism.partial[RValue, Unit] {
      case CEmptyArray => ()
    } (_ => CEmptyArray)

  val rBoolean: Prism[RValue, Boolean] =
    Prism.partial[RValue, Boolean] {
      case CBoolean(b) => b
    } (CBoolean(_))

  val rString: Prism[RValue, String] =
    Prism.partial[RValue, String] {
      case CString(s) => s
    } (CString(_))

  val rLong: Prism[RValue, Long] =
    Prism.partial[RValue, Long] {
      case CLong(l) => l
    } (CLong(_))

  val rDouble: Prism[RValue, Double] =
    Prism.partial[RValue, Double] {
      case CDouble(d) => d
    } (CDouble(_))

  val rNum: Prism[RValue, BigDecimal] =
    Prism.partial[RValue, BigDecimal] {
      case CNum(n) => n
    } (CNum(_))

  val rOffsetDateTime: Prism[RValue, OffsetDateTime] =
    Prism.partial[RValue, OffsetDateTime] {
      case COffsetDateTime(t) => t
    } (COffsetDateTime(_))

  val rOffsetDate: Prism[RValue, OffsetDate] =
    Prism.partial[RValue, OffsetDate] {
      case COffsetDate(t) => t
    } (COffsetDate(_))

  val rOffsetTime: Prism[RValue, OffsetTime] =
    Prism.partial[RValue, OffsetTime] {
      case COffsetTime(t) => t
    } (COffsetTime(_))

  val rLocalDateTime: Prism[RValue, LocalDateTime] =
    Prism.partial[RValue, LocalDateTime] {
      case CLocalDateTime(t) => t
    } (CLocalDateTime(_))

  val rLocalDate: Prism[RValue, LocalDate] =
    Prism.partial[RValue, LocalDate] {
      case CLocalDate(t) => t
    } (CLocalDate(_))

  val rLocalTime: Prism[RValue, LocalTime] =
    Prism.partial[RValue, LocalTime] {
      case CLocalTime(t) => t
    } (CLocalTime(_))

  val rInterval: Prism[RValue, DateTimeInterval] =
    Prism.partial[RValue, DateTimeInterval] {
      case CInterval(i) => i
    } (CInterval(_))

  def toCValue(rvalue: RValue): Option[CValue] = rvalue match {
    case cvalue: CValue => Some(cvalue)
    case RArray.empty   => Some(CEmptyArray)
    case RObject.empty  => Some(CEmptyObject)
    case _              => None
  }

  def fromJValueRaw(jv: JValue): RValue = {
    def loop(jv: JValue): Option[RValue] = jv match {
      case JObject(fields) if fields.nonEmpty =>
        val transformed: Map[String, RValue] = fields.flatMap({
          case (key, value) => loop(value).map(key -> _).toList
        })(collection.breakOut)

        Some(RObject(transformed))

      case JArray(elements) if elements.nonEmpty =>
        Some(RArray(elements.flatMap(loop(_).toList)))

      case other =>
        CType.toCValueRaw(other)
    }
    loop(jv).getOrElse(CUndefined)
  }

  def fromJValue(jv: JValue): Option[RValue] = jv match {
    case JObject(fields) if !fields.isEmpty =>
      val transformed: Map[String, RValue] = fields.flatMap({
        case (key, value) => fromJValue(value).map(key -> _).toList
      })(collection.breakOut)

      Some(RObject(transformed))

    case JArray(elements) if !elements.isEmpty =>
      Some(RArray(elements.flatMap(fromJValue(_).toList)))

    case other =>
      CType.toCValue(other)
  }

  def unsafeInsert(rootTarget: RValue, rootPath: CPath, rootValue: RValue): RValue = {
    def rec(target: RValue, path: CPath, value: RValue): RValue = {
      if ((target == CNull || target == CUndefined) && path == CPath.Identity) value
      else {
        def arrayInsert(l: List[RValue], i: Int, rem: CPath, v: RValue): List[RValue] = {
          def update(l: List[RValue], j: Int): List[RValue] = l match {
            case x :: xs => (if (j == i) rec(x, rem, v) else x) :: update(xs, j + 1)
            case Nil     => Nil
          }

          update(l.padTo(i + 1, CUndefined), 0)
        }

        target match {
          case obj @ RObject(fields) =>
            path.nodes match {
              case CPathField(name) :: nodes =>
                val (child, rest) = (fields.get(name).getOrElse(CUndefined), fields - name)
                RObject(rest + (name -> rec(child, CPath(nodes), value)))

              case CPathIndex(_) :: _ => sys.error("Objects are not indexed: attempted to insert " + value + " at " + rootPath + " on " + rootTarget)
              case _ =>
                sys.error(
                  "RValue insert would overwrite existing data: " + target + " cannot be rewritten to " + value + " at " + path +
                    " in unsafeInsert of " + rootValue + " at " + rootPath + " in " + rootTarget)
            }

          case arr @ RArray(elements) =>
            path.nodes match {
              case CPathIndex(index) :: nodes => RArray(arrayInsert(elements, index, CPath(nodes), value))
              case CPathField(_) :: _         => sys.error("Arrays have no fields: attempted to insert " + value + " at " + rootPath + " on " + rootTarget)
              case _ =>
                sys.error(
                  "RValue insert would overwrite existing data: " + target + " cannot be rewritten to " + value + " at " + path +
                    " in unsafeInsert of " + rootValue + " at " + rootPath + " in " + rootTarget)
            }

          case CNull | CUndefined =>
            path.nodes match {
              case Nil                => value
              case CPathIndex(_) :: _ => rec(RArray.empty, path, value)
              case CPathField(_) :: _ => rec(RObject.empty, path, value)
              case CPathArray :: _    => sys.error("todo")
              case CPathMeta(_) :: _  => sys.error("todo")
            }

          case x =>
            sys.error(
              "RValue insert would overwrite existing data: " + x + " cannot be updated to " + value + " at " + path +
                " in unsafeInsert of " + rootValue + " at " + rootPath + " in " + rootTarget)
        }
      }
    }

    rec(rootTarget, rootPath, rootValue)
  }

  def fromData(data: Data): Option[RValue] = data match {
    case Data.Arr(d) => RArray(d.flatMap(fromData)).some
    case Data.Obj(o) => RObject(o.flatMap { case (k, v) => fromData(v).strengthL(k) }).some
    case Data.Null => CNull.some
    case Data.Bool(b) => CBoolean(b).some
    case Data.Str(s) => CString(s).some

    case Data.Dec(k) =>
      val back = if (k.isValidLong)
        CLong(k.toLong)
      else if (k.isDecimalDouble)
        CDouble(k.toDouble)
      else
        CNum(k)

      back.some

    case Data.Int(k) =>
      (if (k.isValidLong) CLong(k.toLong) else CNum(BigDecimal(k))).some

    case Data.OffsetDateTime(v) => COffsetDateTime(v).some
    case Data.OffsetDate(v) => COffsetDate(v).some
    case Data.OffsetTime(v) => COffsetTime(v).some
    case Data.LocalDateTime(v) => CLocalDateTime(v).some
    case Data.LocalDate(v) => CLocalDate(v).some
    case Data.LocalTime(v) => CLocalTime(v).some
    case Data.Interval(k) => CInterval(k).some
    case Data.NA => None
  }

  def toData(rvalue: RValue): Data = rvalue match {
    case RArray(a)           => Data.Arr(a.map(toData))
    case CArray(a, ty)       => Data.Arr(a.map(k => toData(ty.elemType(k))).toList)
    case RObject(a)          => Data.Obj(a.mapValues(toData).toList: _*)
    case CEmptyArray         => Data.Arr(Nil)
    case CEmptyObject        => Data.Obj()
    case CString(as)         => Data.Str(as)
    case CBoolean(ab)        => Data.Bool(ab)
    case CLong(al)           => Data.Int(BigInt(al))
    case CDouble(ad)         => Data.Dec(BigDecimal(ad))
    case CNum(an)            => Data.Dec(an)
    case CLocalDateTime(ad)  => Data.LocalDateTime(ad)
    case CLocalDate(ad)      => Data.LocalDate(ad)
    case CLocalTime(ad)      => Data.LocalTime(ad)
    case COffsetDateTime(ad) => Data.OffsetDateTime(ad)
    case COffsetDate(ad)     => Data.OffsetDate(ad)
    case COffsetTime(ad)     => Data.OffsetTime(ad)
    case CInterval(ad)       => Data.Interval(ad)
    case CUndefined          => Data.NA
    case CNull               => Data.Null
  }
}

sealed abstract class RValueInstances {
  implicit val equal: Equal[RValue] =
    Equal.equalA

  implicit val show: Show[RValue] =
    Show.showFromToString
}

case class RObject(fields: Map[String, RValue]) extends RValue {
  def toJValue = JObject(fields mapValues (_.toJValue) toMap)
  def toJValueRaw = JObject(fields mapValues (_.toJValueRaw) toMap)
}

object RObject {
  val empty = new RObject(Map.empty)
  def apply(fields: (String, RValue)*): RValue = new RObject(Map(fields: _*))
}

case class RArray(elements: List[RValue]) extends RValue {
  def toJValue = JArray(elements map { _.toJValue })
  def toJValueRaw = JArray(elements map { _.toJValueRaw })
}

object RArray {
  val empty = new RArray(Nil)
  def apply(elements: RValue*): RValue = new RArray(elements.toList)
}

sealed trait CValue extends RValue {
  def cType: CType
}

sealed trait CNullValue extends CValue { self: CNullType =>
  def cType: CNullType = self
}

sealed trait CWrappedValue[A] extends CValue {
  def cType: CValueType[A]
  def value: A
  def toJValue = cType.jValueFor(value)
  def toJValueRaw = cType.jValueForRaw(value)
}

sealed trait CNumericValue[A] extends CWrappedValue[A] {
  final override def hashCode = 12
  def cType: CNumericType[A]
  def toCNum: CNum = CNum(cType.bigDecimalFor(value))
}

object CValue {
  implicit val CValueOrder: scalaz.Order[CValue] = Order order {
    case (CString(as), CString(bs))                                                   => as ?|? bs
    case (CBoolean(ab), CBoolean(bb))                                                 => ab ?|? bb
    case (CLong(al), CLong(bl))                                                       => al ?|? bl
    case (CDouble(ad), CDouble(bd))                                                   => ad ?|? bd
    case (CNum(an), CNum(bn))                                                         => fromInt(an compare bn)
    case (CLocalDateTime(ad), CLocalDateTime(bd))                                     => fromInt(ad compareTo bd)
    case (CLocalDate(ad), CLocalDate(bd))                                             => fromInt(ad compareTo bd)
    case (CLocalTime(ad), CLocalTime(bd))                                             => fromInt(ad compareTo bd)
    case (COffsetDateTime(ad), COffsetDateTime(bd))                                   => fromInt(ad compareTo bd)
    case (COffsetDate(ad), COffsetDate(bd))                                           => fromInt(ad compareTo bd)
    case (COffsetTime(ad), COffsetTime(bd))                                           => fromInt(ad compareTo bd)
    case (CArray(as, CArrayType(atpe)), CArray(bs, CArrayType(btpe))) if atpe == btpe => as.toStream.map(x => atpe(x): CValue) ?|? bs.toStream.map(x => btpe(x))
    case (a: CNumericValue[_], b: CNumericValue[_])                                   => (a.toCNum: CValue) ?|? b.toCNum
    case (a, b)                                                                       => a.cType ?|? b.cType
  }
}

sealed trait CType extends Serializable {
  def readResolve(): CType
  def isNumeric: Boolean = false

  @inline
  private[common] final def typeIndex: Int = this match {
    case CUndefined      => 0
    case CBoolean        => 1
    case CString         => 2
    case CLong           => 3
    case CDouble         => 4
    case CNum            => 5
    case CEmptyObject    => 6
    case CEmptyArray     => 7
    case CNull           => 8
    case COffsetDateTime => 9
    case COffsetTime     => 10
    case COffsetDate     => 11
    case CLocalDateTime  => 12
    case CLocalTime      => 13
    case CLocalDate      => 14
    case CInterval       => 15
    case CArrayType(t)   => 100 + t.typeIndex
  }
}

sealed trait CNullType extends CType with CNullValue

sealed trait CValueType[A] extends CType { self =>
  def classTag: ClassTag[A]
  def readResolve(): CValueType[A]
  def apply(a: A): CWrappedValue[A]
  def order(a: A, b: A): scalaz.Ordering
  def jValueFor(a: A): JValue
  def jValueForRaw(a: A): JValue = jValueFor(a)
}

sealed trait CNumericType[A] extends CValueType[A] {
  override def isNumeric: Boolean = true
  def bigDecimalFor(a: A): BigDecimal
}

object CType {
  def nameOf(c: CType): String = c match {
    case CString              => "String"
    case CBoolean             => "Boolean"
    case CLong                => "Long"
    case CDouble              => "Double"
    case CNum                 => "Decimal"
    case CNull                => "Null"
    case CEmptyObject         => "EmptyObject"
    case CEmptyArray          => "EmptyArray"
    case CArrayType(elemType) => "Array[%s]" format nameOf(elemType)
    case COffsetDateTime      => "OffsetDateTime"
    case COffsetTime          => "OffsetTime"
    case COffsetDate          => "OffsetDate"
    case CLocalDateTime       => "LocalDateTime"
    case CLocalTime           => "LocalTime"
    case CLocalDate           => "LocalDate"
    case CInterval            => "Interval"
    case CUndefined           => sys.error("CUndefined cannot be serialized")
  }

  val ArrayName = """Array[(.*)]""".r

  def fromName(n: String): Option[CType] = n match {
    case "String"         => Some(CString)
    case "Boolean"        => Some(CBoolean)
    case "Long"           => Some(CLong)
    case "Double"         => Some(CDouble)
    case "Decimal"        => Some(CNum)
    case "Null"           => Some(CNull)
    case "EmptyObject"    => Some(CEmptyObject)
    case "EmptyArray"     => Some(CEmptyArray)
    case "OffsetDateTime" => Some(COffsetDateTime)
    case "OffsetDate"     => Some(COffsetDate)
    case "OffsetTime"     => Some(COffsetTime)
    case "LocalDateTime"  => Some(CLocalDateTime)
    case "LocalDate"      => Some(CLocalDate)
    case "LocalTime"      => Some(CLocalTime)
    case "Interval"       => Some(CInterval)
    case ArrayName(elem)  => fromName(elem) collect { case tp: CValueType[_] => CArrayType(tp) }
    case _                => None
  }

  def readResolve() = CType

  def of(v: CValue): CType = v.cType

  def canCompare(t1: CType, t2: CType): Boolean =
    (t1 == t2) || (t1.isNumeric && t2.isNumeric)

  def unify(t1: CType, t2: CType): Option[CType] = (t1, t2) match {
    case _ if t1 == t2                                    => Some(t1)
    case (CLong | CDouble | CNum, CLong | CDouble | CNum) => Some(CNum)
    case (CArrayType(et1), CArrayType(et2))               => unify(et1, et2) collect { case t: CValueType[_] => CArrayType(t) }
    case _                                                => None
  }

  @inline
  final def toCValueRaw(jval: JValue): Option[CValue] = jval match {
    case JString(s) =>
      Some(CString(s))

    case JBool(b) =>
      Some(CBoolean(b))

    case JNull =>
      Some(CNull)

    case JObject.empty =>
      Some(CEmptyObject)

    case JArray.empty =>
      Some(CEmptyArray)

    case JNumLong(d) =>
      Some(CLong(d))

    case JNumDouble(d) =>
      Some(CDouble(d))

    case JNumStr(d) =>
      Some(CNum(BigDecimal(d)))

    case JNumBigDec(d) =>
      Some(CNum(d))

    case JArray(_) =>
      sys.error("TODO: Allow for homogeneous JArrays -> CArray.")

    case JObject(_) =>
      sys.error("TODO: Not implemented.")

    case JUndefined => None
  }

  @inline
  final def toCValue(jval: JValue): Option[CValue] = jval match {
    case JString(s) =>
      Some(CString(s))

    case JBool(b) =>
      Some(CBoolean(b))

    case JNull =>
      Some(CNull)

    case JObject.empty =>
      Some(CEmptyObject)

    case JArray.empty =>
      Some(CEmptyArray)

    case JNum(d) =>
      forJValue(jval) match {
        case Some(CLong) => Some(CLong(d.toLong))
        case Some(CDouble) => Some(CDouble(d.toDouble))
        case _ => Some(CNum(d))
      }

    case JArray(_) =>
      sys.error("TODO: Allow for homogeneous JArrays -> CArray.")

    case JUndefined => None
  }

  @inline
  final def forJValue(jval: JValue): Option[CType] = jval match {
    case JBool(_) => Some(CBoolean)

    case JNum(d) => {
      lazy val isLong = try {
        d.toLongExact
        true
      } catch {
        case _: ArithmeticException => false
      }

      lazy val isDouble = (try decimal(d.toDouble.toString) == d
      catch { case _: NumberFormatException | _: ArithmeticException => false })

      if (isLong)
        Some(CLong)
      else if (isDouble)
        Some(CDouble)
      else
        Some(CNum)
    }

    case JString(_)    => Some(CString)
    case JNull         => Some(CNull)
    case JArray(Nil)   => Some(CEmptyArray)
    case JObject.empty => Some(CEmptyObject)
    case JArray.empty  => None // TODO Allow homogeneous JArrays -> CType
    case _             => None
  }

  final def forJValueRaw(jval: JValue): Option[CType] = jval match {
    case JBool(_) => Some(CBoolean)
    case JNumLong(_) => Some(CLong)
    case JNumDouble(_) => Some(CDouble)
    case JNumBigDec(_) | JNumStr(_) => Some(CNum)
    case JString(_)    => Some(CString)
    case JNull         => Some(CNull)
    case JArray(Nil)   => Some(CEmptyArray)
    case JObject.empty => Some(CEmptyObject)
    case JArray.empty  => None // TODO Allow homogeneous JArrays -> CType
    case _             => None
  }

  implicit val CTypeOrder: scalaz.Order[CType] = Order order {
    case (CArrayType(t1), CArrayType(t2)) => (t1: CType) ?|? t2
    case (x, y)                           => x.typeIndex ?|? y.typeIndex
  }
}

object CValueType {
  def apply[A](implicit A: CValueType[A]): CValueType[A] = A
  def apply[A](a: A)(implicit A: CValueType[A]): CWrappedValue[A] = A(a)

  // These let us do, def const[A: CValueType](a: A): CValue = CValueType[A](a)
  implicit def string: CValueType[String]                 = CString
  implicit def boolean: CValueType[Boolean]               = CBoolean
  implicit def long: CValueType[Long]                     = CLong
  implicit def double: CValueType[Double]                 = CDouble
  implicit def bigDecimal: CValueType[BigDecimal]         = CNum
  implicit def offsetDateTime: CValueType[OffsetDateTime] = COffsetDateTime
  implicit def offsetTime: CValueType[OffsetTime]         = COffsetTime
  implicit def offsetDate: CValueType[OffsetDate]         = COffsetDate
  implicit def localDateTime: CValueType[LocalDateTime]   = CLocalDateTime
  implicit def localTime: CValueType[LocalTime]           = CLocalTime
  implicit def localDate: CValueType[LocalDate]           = CLocalDate
  implicit def duration: CValueType[DateTimeInterval]     = CInterval
  implicit def array[A](implicit elemType: CValueType[A]) = CArrayType(elemType)
}

//
// Homogeneous arrays
//
case class CArray[A](value: Array[A], cType: CArrayType[A]) extends CWrappedValue[Array[A]] {
  private final def leafEquiv[X](as: Array[X], bs: Array[X]): Boolean = {
    var i      = 0
    var result = as.length == bs.length
    while (result && i < as.length) {
      result = as(i) == bs(i)
      i += 1
    }
    result
  }

  private final def equiv(a: Any, b: Any, elemType: CValueType[_]): Boolean = elemType match {
    case CBoolean =>
      leafEquiv(a.asInstanceOf[Array[Boolean]], b.asInstanceOf[Array[Boolean]])

    case CLong =>
      leafEquiv(a.asInstanceOf[Array[Long]], b.asInstanceOf[Array[Long]])

    case CDouble =>
      leafEquiv(a.asInstanceOf[Array[Double]], b.asInstanceOf[Array[Double]])

    case CArrayType(elemType) =>
      val as = a.asInstanceOf[Array[Array[_]]]
      val bs = b.asInstanceOf[Array[Array[_]]]
      var i      = 0
      var result = as.length == bs.length
      while (result && i < as.length) {
        result = equiv(as(i), bs(i), elemType)
        i += 1
      }
      result

    case _ =>
      leafEquiv(a.asInstanceOf[Array[AnyRef]], b.asInstanceOf[Array[AnyRef]])
  }

  override def equals(that: Any): Boolean = that match {
    case v @ CArray(_, thatCType) if cType == thatCType =>
      equiv(value, v.value, cType.elemType)

    case _ => false
  }

  override def toString: String = value.mkString("CArray(Array(", ", ", "), " + cType.toString + ")")
}

case object CArray {
  def apply[A](as: Array[A])(implicit elemType: CValueType[A]): CArray[A] =
    CArray(as, CArrayType(elemType))
}

case class CArrayType[A](elemType: CValueType[A]) extends CValueType[Array[A]] {
  // Spec. bug: Leave lazy here.
  lazy val classTag: ClassTag[Array[A]] = elemType.classTag.wrap //elemType.classTag.arrayClassTag

  type tpe = A

  def readResolve() = CArrayType(elemType.readResolve())

  def apply(value: Array[A]) = CArray(value, this)

  def order(as: Array[A], bs: Array[A]) =
    (as zip bs) map {
      case (a, b) =>
        elemType.order(a, b)
    } find (_ != EQ) getOrElse Ordering.fromInt(as.size - bs.size)

  def jValueFor(as: Array[A]) =
    sys.error("HOMOGENEOUS ARRAY ESCAPING! ALERT! ALERT!")
}

//
// Strings
//
case class CString(value: String) extends CWrappedValue[String] {
  val cType = CString
}

case object CString extends CValueType[String] {
  val classTag: ClassTag[String] = implicitly[ClassTag[String]]
  def readResolve()                 = CString
  def order(s1: String, s2: String) = stringInstance.order(s1, s2)
  def jValueFor(s: String)          = JString(s)
}

//
// Booleans
//
sealed abstract class CBoolean(val value: Boolean) extends CWrappedValue[Boolean] {
  val cType = CBoolean
}

case object CTrue  extends CBoolean(true)
case object CFalse extends CBoolean(false)

case object CBoolean extends CValueType[Boolean] {
  def apply(value: Boolean)    = if (value) CTrue else CFalse
  def unapply(cbool: CBoolean) = Some(cbool.value)
  val classTag: ClassTag[Boolean] = implicitly[ClassTag[Boolean]]
  def readResolve()                   = CBoolean
  def order(v1: Boolean, v2: Boolean) = booleanInstance.order(v1, v2)
  def jValueFor(v: Boolean)           = JBool(v)
}

//
// Numerics
//
case class CLong(value: Long) extends CNumericValue[Long] {
  val cType = CLong
}

case object CLong extends CNumericType[Long] {
  val classTag: ClassTag[Long] = implicitly[ClassTag[Long]]
  def readResolve()                          = CLong
  def order(v1: Long, v2: Long)              = longInstance.order(v1, v2)
  def jValueFor(v: Long): JValue             = JNum(bigDecimalFor(v))
  override def jValueForRaw(v: Long): JValue = JNumLong(v)
  def bigDecimalFor(v: Long)                 = BigDecimal(v, UNLIMITED)
}

case class CDouble(value: Double) extends CNumericValue[Double] {
  val cType = CDouble
}

case object CDouble extends CNumericType[Double] {
  val classTag: ClassTag[Double] = implicitly[ClassTag[Double]]
  def readResolve()                    = CDouble
  def order(v1: Double, v2: Double)    = doubleInstance.order(v1, v2)
  def jValueFor(v: Double)             = JNum(BigDecimal(v.toString, UNLIMITED))
  override def jValueForRaw(v: Double) = JNumDouble(v)
  def bigDecimalFor(v: Double)         = BigDecimal(v.toString, UNLIMITED)
}

case class CNum(value: BigDecimal) extends CNumericValue[BigDecimal] {
  val cType = CNum
}

case object CNum extends CNumericType[BigDecimal] {
  val classTag: ClassTag[BigDecimal] = implicitly[ClassTag[BigDecimal]]
  def readResolve()                         = CNum
  def order(v1: BigDecimal, v2: BigDecimal) = bigDecimalOrder.order(v1, v2)
  def jValueFor(v: BigDecimal)              = JNum(v)
  override def jValueForRaw(v: BigDecimal)  = JNumBigDec(v)
  def bigDecimalFor(v: BigDecimal)          = v
}

//
// Dates, Times, and Periods
//
case class COffsetDateTime(value: OffsetDateTime) extends CWrappedValue[OffsetDateTime] {
  val cType = COffsetDateTime
}

case object COffsetDateTime extends CValueType[OffsetDateTime] {
  val classTag: ClassTag[OffsetDateTime]            = implicitly[ClassTag[OffsetDateTime]]
  def readResolve()                                 = COffsetDateTime
  def order(v1: OffsetDateTime, v2: OffsetDateTime) = sys.error("todo")
  def jValueFor(v: OffsetDateTime)                  = JString(v.toString)
}

case class COffsetDate(value: OffsetDate) extends CWrappedValue[OffsetDate] {
  val cType = COffsetDate
}

case object COffsetDate extends CValueType[OffsetDate] {
  val classTag: ClassTag[OffsetDate]        = implicitly[ClassTag[OffsetDate]]
  def readResolve()                         = COffsetDate
  def order(v1: OffsetDate, v2: OffsetDate) = sys.error("todo")
  def jValueFor(v: OffsetDate)              = JString(v.toString)
}

case class COffsetTime(value: OffsetTime) extends CWrappedValue[OffsetTime] {
  val cType = COffsetTime
}

case object COffsetTime extends CValueType[OffsetTime] {
  val classTag: ClassTag[OffsetTime]        = implicitly[ClassTag[OffsetTime]]
  def readResolve()                         = COffsetTime
  def order(v1: OffsetTime, v2: OffsetTime) = sys.error("todo")
  def jValueFor(v: OffsetTime)              = JString(v.toString)
}

case class CLocalDateTime(value: LocalDateTime) extends CWrappedValue[LocalDateTime] {
  val cType = CLocalDateTime
}

case object CLocalDateTime extends CValueType[LocalDateTime] {
  val classTag: ClassTag[LocalDateTime]           = implicitly[ClassTag[LocalDateTime]]
  def readResolve()                               = CLocalDateTime
  def order(v1: LocalDateTime, v2: LocalDateTime) = sys.error("todo")
  def jValueFor(v: LocalDateTime)                 = JString(v.toString)
}

case class CLocalTime(value: LocalTime) extends CWrappedValue[LocalTime] {
  val cType = CLocalTime
}

case object CLocalTime extends CValueType[LocalTime] {
  val classTag: ClassTag[LocalTime]       = implicitly[ClassTag[LocalTime]]
  def readResolve()                       = CLocalTime
  def order(v1: LocalTime, v2: LocalTime) = sys.error("todo")
  def jValueFor(v: LocalTime)             = JString(v.toString)
}

case class CLocalDate(value: LocalDate) extends CWrappedValue[LocalDate] {
  val cType = CLocalDate
}

case object CLocalDate extends CValueType[LocalDate] {
  val classTag: ClassTag[LocalDate]       = implicitly[ClassTag[LocalDate]]
  def readResolve()                       = CLocalDate
  def order(v1: LocalDate, v2: LocalDate) = sys.error("todo")
  def jValueFor(v: LocalDate)             = JString(v.toString)
}

case class CInterval(value: DateTimeInterval) extends CWrappedValue[DateTimeInterval] {
  val cType = CInterval
}

case object CInterval extends CValueType[DateTimeInterval] {
  val classTag: ClassTag[DateTimeInterval]              = implicitly[ClassTag[DateTimeInterval]]
  def readResolve()                                     = CInterval
  def order(v1: DateTimeInterval, v2: DateTimeInterval) = sys.error("todo")
  def jValueFor(v: DateTimeInterval)                    = JString(v.toString)
}

//
// Nulls
//
case object CNull extends CNullType with CNullValue {
  def readResolve() = CNull
  def toJValue      = JNull
  def toJValueRaw   = JNull
}

case object CEmptyObject extends CNullType with CNullValue {
  def readResolve() = CEmptyObject
  def toJValue      = JObject(Nil)
  def toJValueRaw   = JObject(Nil)
}

case object CEmptyArray extends CNullType with CNullValue {
  def readResolve() = CEmptyArray
  def toJValue      = JArray(Nil)
  def toJValueRaw   = JArray(Nil)
}

//
// Undefined
//
case object CUndefined extends CNullType with CNullValue {
  def readResolve() = CUndefined
  def toJValue      = JUndefined
  def toJValueRaw   = JUndefined
}
