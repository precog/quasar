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

package quasar.precog
package common

import quasar.precog._
import quasar.blueeyes._, json._, serialization._
import DefaultSerialization._
import scalaz._, Scalaz._, Ordering._
import java.math.MathContext.UNLIMITED
import java.time.LocalDateTime

sealed trait RValue { self =>
  def toJValue: JValue

  def \(fieldName: String): RValue

  def unsafeInsert(path: CPath, value: RValue): RValue = {
    RValue.unsafeInsert(self, path, value)
  }

  def flattenWithPath: Vector[(CPath, CValue)] = {
    def flatten0(path: CPath)(value: RValue): Vector[(CPath, CValue)] = value match {
      case RObject(fields) if fields.isEmpty =>
        Vector((path, CEmptyObject))

      case RArray(elems) if elems.isEmpty =>
        Vector((path, CEmptyArray))

      case RObject(fields) =>
        fields.foldLeft(Vector.empty[(CPath, CValue)]) {
          case (acc, field) =>
            acc ++ flatten0(path \ field._1)(field._2)
        }

      case RArray(elems) =>
        Vector(elems: _*).zipWithIndex.flatMap { tuple =>
          val (elem, idx) = tuple

          flatten0(path \ idx)(elem)
        }

      case (v: CValue) => Vector((path, v))
    }

    flatten0(CPath.Identity)(self)
  }
}

object RValue {
  def fromJValue(jv: JValue): RValue = jv match {
    case JObject(fields)  => RObject(fields mapValues fromJValue toMap)
    case JArray(elements) => RArray(elements map fromJValue)
    case other            => CType.toCValue(other)
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
}

case class RObject(fields: Map[String, RValue]) extends RValue {
  def toJValue                     = JObject(fields mapValues (_.toJValue) toMap)
  def \(fieldName: String): RValue = fields(fieldName)
}

object RObject {
  val empty = new RObject(Map.empty)
  def apply(fields: (String, RValue)*): RValue = new RObject(Map(fields: _*))
}

case class RArray(elements: List[RValue]) extends RValue {
  def toJValue                     = JArray(elements map { _.toJValue })
  def \(fieldName: String): RValue = CUndefined
}

object RArray {
  val empty = new RArray(Nil)
  def apply(elements: RValue*): RValue = new RArray(elements.toList)
}

sealed trait CValue extends RValue {
  def cType: CType
  def \(fieldName: String): RValue = CUndefined
}

sealed trait CNullValue extends CValue { self: CNullType =>
  def cType: CNullType = self
}

sealed trait CWrappedValue[A] extends CValue {
  def cType: CValueType[A]
  def value: A
  def toJValue = cType.jValueFor(value)
}

sealed trait CNumericValue[A] extends CWrappedValue[A] {
  final override def hashCode = 12
  def cType: CNumericType[A]
  def toCNum: CNum = CNum(cType.bigDecimalFor(value))
}

object CValue {
  implicit val CValueOrder: ScalazOrder[CValue] = Order order {
    case (CString(as), CString(bs))                                                   => as ?|? bs
    case (CBoolean(ab), CBoolean(bb))                                                 => ab ?|? bb
    case (CLong(al), CLong(bl))                                                       => al ?|? bl
    case (CDouble(ad), CDouble(bd))                                                   => ad ?|? bd
    case (CNum(an), CNum(bn))                                                         => fromInt(an compare bn)
    case (CDate(ad), CDate(bd))                                                       => fromInt(ad compareTo bd)
    case (CPeriod(ad), CPeriod(bd))                                                   => ad.toDuration ?|? bd.toDuration
    case (CArray(as, CArrayType(atpe)), CArray(bs, CArrayType(btpe))) if atpe == btpe => as.toStream.map(x => atpe(x): CValue) ?|? bs.toStream.map(x => btpe(x))
    case (a: CNumericValue[_], b: CNumericValue[_])                                   => (a.toCNum: CValue) ?|? b.toCNum
    case (a, b)                                                                       => a.cType ?|? b.cType
  }
}

sealed trait CType extends Serializable {
  def readResolve(): CType
  def isNumeric: Boolean = false

  @inline
  private[common] final def typeIndex = this match {
    case CUndefined    => 0
    case CBoolean      => 1
    case CString       => 2
    case CLong         => 4
    case CDouble       => 6
    case CNum          => 7
    case CEmptyObject  => 8
    case CEmptyArray   => 9
    case CArrayType(_) => 10 // TODO: Should this account for the element type?
    case CNull         => 11
    case CDate         => 12
    case CPeriod       => 13
  }
}

sealed trait CNullType extends CType with CNullValue

sealed trait CValueType[A] extends CType { self =>
  def classTag: CTag[A]
  def readResolve(): CValueType[A]
  def apply(a: A): CWrappedValue[A]
  def order(a: A, b: A): ScalazOrdering
  def jValueFor(a: A): JValue
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
    case CDate                => "Timestamp"
    case CPeriod              => "Period"
    case CUndefined           => sys.error("CUndefined cannot be serialized")
  }

  val ArrayName = """Array[(.*)]""".r

  def fromName(n: String): Option[CType] = n match {
    case "String"        => Some(CString)
    case "Boolean"       => Some(CBoolean)
    case "Long"          => Some(CLong)
    case "Double"        => Some(CDouble)
    case "Decimal"       => Some(CNum)
    case "Null"          => Some(CNull)
    case "EmptyObject"   => Some(CEmptyObject)
    case "EmptyArray"    => Some(CEmptyArray)
    case "Timestamp"     => Some(CDate)
    case "Period"        => Some(CPeriod)
    case ArrayName(elem) => fromName(elem) collect { case tp: CValueType[_] => CArrayType(tp) }
    case _               => None
  }

  implicit val decomposer: Decomposer[CType] = new Decomposer[CType] {
    def decompose(ctype: CType): JValue = JString(nameOf(ctype))
  }

  implicit val extractor: Extractor[CType] = new Extractor[CType] {
    def validated(obj: JValue): Validation[Extractor.Error, CType] =
      obj.validated[String].map(fromName _) match {
        case Success(Some(t)) => Success(t)
        case Success(None)    => Failure(Extractor.Invalid("Unknown type."))
        case Failure(f)       => Failure(f)
      }
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

  // TODO Should return Option[CValue]... is this even used?
  // Yes; it is used only in RoutingTable.scala
  @inline
  final def toCValue(jval: JValue): CValue = (jval: @unchecked) match {
    case JString(s)    => CString(s)
    case JBool(b)      => CBoolean(b)
    case JNull         => CNull
    case JObject.empty => CEmptyObject
    case JArray.empty  => CEmptyArray
    case JNum(d)       =>
      forJValue(jval) match {
        case Some(CLong)   => CLong(d.toLong)
        case Some(CDouble) => CDouble(d.toDouble)
        case _             => CNum(d)
      }
    case JArray(_) =>
      sys.error("TODO: Allow for homogeneous JArrays -> CArray.")
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

  implicit val CTypeOrder: ScalazOrder[CType] = Order order {
    case (CArrayType(t1), CArrayType(t2)) => (t1: CType) ?|? t2
    case (x, y)                           => x.typeIndex ?|? y.typeIndex
  }
}

object CValueType {
  def apply[A](implicit A: CValueType[A]): CValueType[A]          = A
  def apply[A](a: A)(implicit A: CValueType[A]): CWrappedValue[A] = A(a)

  // These let us do, def const[A: CValueType](a: A): CValue = CValueType[A](a)
  implicit def string: CValueType[String]                 = CString
  implicit def boolean: CValueType[Boolean]               = CBoolean
  implicit def long: CValueType[Long]                     = CLong
  implicit def double: CValueType[Double]                 = CDouble
  implicit def bigDecimal: CValueType[BigDecimal]         = CNum
  implicit def dateTime: CValueType[LocalDateTime]             = CDate
  implicit def period: CValueType[Period]                 = CPeriod
  implicit def array[A](implicit elemType: CValueType[A]) = CArrayType(elemType)
}

//
// Homogeneous arrays
//
case class CArray[A](value: Array[A], cType: CArrayType[A]) extends CWrappedValue[Array[A]] {
  private final def leafEquiv[A](as: Array[A], bs: Array[A]): Boolean = {
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
  lazy val classTag: CTag[Array[A]] = elemType.classTag.wrap //elemType.classTag.arrayCTag

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
  val classTag: CTag[String] = implicitly[CTag[String]]
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

object CBoolean extends CValueType[Boolean] {
  def apply(value: Boolean)    = if (value) CTrue else CFalse
  def unapply(cbool: CBoolean) = Some(cbool.value)
  val classTag: CTag[Boolean] = implicitly[CTag[Boolean]]
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
  val classTag: CTag[Long] = implicitly[CTag[Long]]
  def readResolve()              = CLong
  def order(v1: Long, v2: Long)  = longInstance.order(v1, v2)
  def jValueFor(v: Long): JValue = JNum(bigDecimalFor(v))
  def bigDecimalFor(v: Long)     = BigDecimal(v, UNLIMITED)
}

case class CDouble(value: Double) extends CNumericValue[Double] {
  val cType = CDouble
}

case object CDouble extends CNumericType[Double] {
  val classTag: CTag[Double] = implicitly[CTag[Double]]
  def readResolve()                 = CDouble
  def order(v1: Double, v2: Double) = doubleInstance.order(v1, v2)
  def jValueFor(v: Double)          = JNum(BigDecimal(v.toString, UNLIMITED))
  def bigDecimalFor(v: Double)      = BigDecimal(v.toString, UNLIMITED)
}

case class CNum(value: BigDecimal) extends CNumericValue[BigDecimal] {
  val cType = CNum
}

case object CNum extends CNumericType[BigDecimal] {
  val classTag: CTag[BigDecimal] = implicitly[CTag[BigDecimal]]
  def readResolve()                         = CNum
  def order(v1: BigDecimal, v2: BigDecimal) = bigDecimalOrder.order(v1, v2)
  def jValueFor(v: BigDecimal)              = JNum(v)
  def bigDecimalFor(v: BigDecimal)          = v
}

//
// Dates and Periods
//
case class CDate(value: LocalDateTime) extends CWrappedValue[LocalDateTime] {
  val cType = CDate
}

case object CDate extends CValueType[LocalDateTime] {
  val classTag: CTag[LocalDateTime]      = implicitly[CTag[LocalDateTime]]
  def readResolve()                     = CDate
  def order(v1: LocalDateTime, v2: LocalDateTime) = sys.error("todo")
  def jValueFor(v: LocalDateTime)            = JString(v.toString)
}

case class CPeriod(value: Period) extends CWrappedValue[Period] {
  val cType = CPeriod
}

case object CPeriod extends CValueType[Period] {
  val classTag: CTag[Period]    = implicitly[CTag[Period]]
  def readResolve()                 = CPeriod
  def order(v1: Period, v2: Period) = sys.error("todo")
  def jValueFor(v: Period)          = JString(v.toString)
}

//
// Nulls
//
case object CNull extends CNullType with CNullValue {
  def readResolve() = CNull
  def toJValue      = JNull
}

case object CEmptyObject extends CNullType with CNullValue {
  def readResolve() = CEmptyObject
  def toJValue      = JObject(Nil)
}

case object CEmptyArray extends CNullType with CNullValue {
  def readResolve() = CEmptyArray
  def toJValue      = JArray(Nil)
}

//
// Undefined
//
case object CUndefined extends CNullType with CNullValue {
  def readResolve() = CUndefined
  def toJValue      = JUndefined
}
