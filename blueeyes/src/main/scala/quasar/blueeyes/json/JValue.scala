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

package quasar.blueeyes.json

import quasar.blueeyes._
import quasar.common.data.Data

import scalaz._, Scalaz._

import scala.annotation.{switch, tailrec}
import scala.collection.immutable.ListMap
import scala.util.Sorting.quickSort
import scala.util.Try

import java.lang.Double.isInfinite

/**
  * Data type for Json AST.
  */
sealed trait JValue extends Product with Ordered[JValue] {
  def compare(that: JValue): Int = this.typeIndex compare that.typeIndex
  override def toString = this.renderPretty
}

sealed trait JContainer extends JValue {
  assert(contained forall (_ != null), contained)

  def contained: Iterable[JValue]
  def hasDefinedChild: Boolean = contained exists (_ != JUndefined)
  def isNested: Boolean = contained exists {
    case _: JContainer => true
    case _             => false
  }
}

object JValue {
  sealed trait RenderMode
  case object Pretty    extends RenderMode
  case object Compact   extends RenderMode
  case object Canonical extends RenderMode

  def apply(p: JPath, v: JValue) = JUndefined.set(p, v)

  def coerceNumerics(v: JValue): JValue = v match {
    case JNum(d) =>
      val isLong = try {
        d.toLongExact
        true
      } catch {
        case _: ArithmeticException => false
      }

      lazy val isDouble =
        try decimal(d.toDouble.toString) == d
        catch { case _: NumberFormatException | _: ArithmeticException => false }

      if (isLong)
        JNumLong(d.toLong)
      else if (isDouble)
        JNumDouble(d.toDouble)
      else
        JNumBigDec(d)
    case JArray(vs) => JArray(vs.map(coerceNumerics))
    case JObject(vs) => JObject(vs.map { case (k, v) => k -> coerceNumerics(v) })
    case x => x
  }

  def fromData(data: Data): JValue = data match {
    case Data.Null => JNull
    case Data.Str(value) => JString(value)
    case Data.Bool(b) => JBool(b)
    case Data.Dec(value) => JNumStr(value.toString)
    case Data.Int(value) => JNumStr(value.toString)
    case Data.Obj(fields) => JObject(fields.mapValues(fromData))
    case Data.Arr(values) => JArray(values.map(fromData))
    case Data.OffsetDateTime(value) => JString(value.toString)
    case Data.OffsetDate(value) => JString(value.toString)
    case Data.OffsetTime(value) => JString(value.toString)
    case Data.LocalDateTime(value) => JString(value.toString)
    case Data.LocalDate(value) => JString(value.toString)
    case Data.LocalTime(value) => JString(value.toString)
    case Data.Interval(value) => JString(value.toString)
    case Data.NA => JUndefined
  }

  def toData(jv: JValue): Data = jv match {
    case JUndefined => Data.NA
    case JNull => Data.Null
    case JBool(value) => Data.Bool(value)
    case num: JNum => Data.Dec(num.toBigDecimal)
    case JString(value) => Data.Str(value)
    case JObject(fields) => Data.Obj(ListMap(fields.mapValues(toData).toSeq: _*))
    case JArray(values) => Data.Arr(values.map(toData))
  }

  private def unflattenArray(elements: Seq[(JPath, JValue)]): JArray = {
    elements.foldLeft(JArray(Nil)) { (arr, t) =>
      arr.set(t._1, t._2) --> classOf[JArray]
    }
  }

  private def unflattenObject(elements: Seq[(JPath, JValue)]): JObject = {
    elements.foldLeft(JObject(Nil)) { (obj, t) =>
      obj.set(t._1, t._2) --> classOf[JObject]
    }
  }

  def unflatten(elements: Seq[(JPath, JValue)]): JValue = {
    if (elements.isEmpty) JUndefined
    else {
      val sorted = elements.sortBy(_._1)

      val (xp, xv) = sorted.head

      if (xp == NoJPath && sorted.size == 1) xv
      else if (xp.path.startsWith("[")) unflattenArray(sorted)
      else unflattenObject(sorted)
    }
  }

  implicit val jvalueOrder: Order[JValue] = Order order ((x, y) => Ordering fromInt (x compare y))
  implicit val jvalueShow: Show[JValue]   = Show.showFromToString

  def unsafeInsert(rootTarget: JValue, rootPath: JPath, rootValue: JValue): JValue = {
    // println(s"$rootTarget \\ $rootPath := $rootValue")

    def rec(target: JValue, path: JPath, value: JValue): JValue = {
      if ((target == JNull || target == JUndefined) && path == NoJPath) value
      else {
        def arrayInsert(l: List[JValue], i: Int, rem: JPath, v: JValue): List[JValue] = {
          def update(l: List[JValue], j: Int): List[JValue] = l match {
            case x :: xs => (if (j == i) rec(x, rem, v) else x) :: update(xs, j + 1)
            case Nil     => Nil
          }

          update(l.padTo(i + 1, JUndefined), 0)
        }
        def fail(): Nothing = {
          val msg = s"""
            |JValue insert would overwrite existing data:
            |  $target \\ $path := $value
            |Initial call was
            |  $rootValue \\ $rootPath := $rootValue
            |""".stripMargin.trim
          sys error msg
        }

        target match {
          case obj @ JObject(fields) =>
            path.nodes match {
              case JPathField(name) :: nodes =>
                val (child, rest) = obj.partitionField(name)
                rest + JField(name, rec(child, JPath(nodes), value))

              case JPathIndex(_) :: _ => sys.error("Objects are not indexed: attempted to insert " + value + " at " + rootPath + " on " + rootTarget)
              case Nil                => fail()
            }

          case arr @ JArray(elements) =>
            path.nodes match {
              case JPathIndex(index) :: nodes => JArray(arrayInsert(elements, index, JPath(nodes), value))
              case JPathField(_) :: _         => sys.error("Arrays have no fields: attempted to insert " + value + " at " + rootPath + " on " + rootTarget)
              case Nil                        => fail()
            }

          case JNull | JUndefined =>
            path.nodes match {
              case Nil                => value
              case JPathIndex(_) :: _ => rec(JArray(Nil), path, value)
              case JPathField(_) :: _ => rec(JObject(Nil), path, value)
            }

          case x =>
            // println(s"Target is $x ${x.getClass}")
            fail()
        }
      }
    }

    rec(rootTarget, rootPath, rootValue)
  }
}

case object JUndefined extends JValue
case object JNull extends JValue

sealed abstract class JBool(val value: Boolean) extends JValue {
  override def compare(that: JValue) = that match {
    case JBool(v) => value compare v
    case _        => super.compare(that)
  }
}
final case object JTrue extends JBool(true)
final case object JFalse extends JBool(false)

object JBool {
  def apply(value: Boolean): JBool         = if (value) JTrue else JFalse
  def unapply(value: JBool): Some[Boolean] = Some(value.value)
}

sealed trait JNum extends JValue {
  def toBigDecimal: BigDecimal
  def toLong: Long
  def toDouble: Double
  def toRawString: String

  // SI-6173: to avoid hashCode on BigDecimal, we use a hardcoded hashcode to
  // ensure JNum("123.45").hashCode == JNum(123.45).hashCode
  override val hashCode = 6173

  override def compare(that: JValue): Int = that match {
    case num: JNum => numCompare(num)
    case _         => super.compare(that)
  }

  protected[json] def numCompare(other: JNum): Int
}

case class JNumStr private[json] (value: String) extends JNum {
  private lazy val dec: BigDecimal = BigDecimal(value)

  final def toBigDecimal: BigDecimal = dec
  final def toLong: Long             = value.toDouble.toLong
  final def toDouble: Double         = value.toDouble
  final def toRawString: String      = value

  override def equals(other: Any) = other match {
    case JNumLong(n) => value == n.toString
    case num: JNum   => toBigDecimal == num.toBigDecimal
    case _           => false
  }

  protected[json] def numCompare(other: JNum) = toBigDecimal compare other.toBigDecimal
}

case class JNumLong(value: Long) extends JNum {
  final def toBigDecimal: BigDecimal = BigDecimal(value)
  final def toLong: Long             = value
  final def toDouble: Double         = value.toDouble
  final def toRawString: String      = value.toString

  override def equals(other: Any) = other match {
    case JNumLong(n)   => value == n
    case JNumDouble(n) => value == n
    case JNumBigDec(n) => value == n
    case JNumStr(s)    => value.toString == s
    case _             => false
  }

  protected[json] def numCompare(other: JNum) = other match {
    case JNumLong(n)   => value compare n
    case JNumDouble(n) => value.toDouble compare n
    case JNumBigDec(n) => BigDecimal(value) compare n
    case _             => toBigDecimal compare other.toBigDecimal
  }
}

case class JNumDouble private[json] (value: Double) extends JNum {
  final def toBigDecimal: BigDecimal = BigDecimal(value)
  final def toLong: Long             = value.toLong
  final def toDouble: Double         = value
  final def toRawString: String      = value.toString

  override def equals(other: Any) = other match {
    case JNumLong(n)   => value == n
    case JNumDouble(n) => value == n
    case num: JNum     => toBigDecimal == num.toBigDecimal
    case _             => false
  }

  protected[json] def numCompare(other: JNum) = other match {
    case JNumLong(n)   => value compare n.toDouble
    case JNumDouble(n) => value compare n
    case JNumBigDec(n) => BigDecimal(value) compare n
    case _             => toBigDecimal compare other.toBigDecimal
  }
}

case class JNumBigDec(value: BigDecimal) extends JNum {
  final def toBigDecimal: BigDecimal = value
  final def toLong: Long             = value.toLong
  final def toDouble: Double         = value.toDouble
  final def toRawString: String      = value.toString

  override def equals(other: Any) = other match {
    case JNumLong(n)   => value == n
    case JNumDouble(n) => value == n
    case JNumBigDec(n) => value == n
    case num: JNum     => value == num.toBigDecimal
    case _             => false
  }

  protected[json] def numCompare(other: JNum) = other match {
    case JNumLong(n)   => value compare n
    case JNumDouble(n) => value compare n
    case JNumBigDec(n) => value compare n
    case _             => value compare other.toBigDecimal
  }
}

case object JNum {
  def apply(value: Double): JValue =
    if (value.isNaN || isInfinite(value)) JUndefined else JNumDouble(value)

  private[json] def apply(value: String): JNum = JNumStr(value)

  def apply(value: Long): JNum                 = JNumLong(value)
  def apply(value: BigDecimal): JNum           = JNumBigDec(value)
  def unapply(value: JNum): Option[BigDecimal] = Try(value.toBigDecimal).toOption
}

case class JString(value: String) extends JValue {
  override def compare(that: JValue): Int = that match {
    case JString(s) => value compare s
    case _          => super.compare(that)
  }
}

object JString {
  final def escape(s: String): String = buildString(internalEscape(_, s))

  protected[json] final def internalEscape(sb: StringBuilder, s: String) {
    sb.append('"')
    var i = 0
    val len = s.length
    while (i < len) {
      (s.charAt(i): @switch) match {
        case '"'  => sb.append("\\\"")
        case '\\' => sb.append("\\\\")
        case '\b' => sb.append("\\b")
        case '\f' => sb.append("\\f")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case c =>
          if (c < ' ')
            sb.append("\\u%04x" format c.toInt)
          else
            sb.append(c)
      }
      i += 1
    }
    sb.append('"')
  }
}

final case class JField(name: String, value: JValue) extends Product2[String, JValue] {
  def _1                        = name
  def _2                        = value
  def toTuple: (String, JValue) = name -> value
  def isUndefined               = value == JUndefined
}

final object JField {
  def apply(x: JFieldTuple): JField = JField(x._1, x._2)

  implicit def liftTuple(x: JFieldTuple): JField = apply(x)

  implicit final val jFieldOrder: Order[JField] = Order.orderBy(x => x._1 -> x._2)

  def liftFilter(f: JField => Boolean): JValue => JValue = {
    case JObjectFields(fields) => JObject(fields filter f)
    case value                 => value
  }
  def liftFind(f: JField => Boolean): JValue => Boolean = {
    case JObjectFields(fields) => fields exists f
    case _                     => false
  }
  def liftMap(f: JField => JField): JValue => JValue = {
    case JObjectFields(fields) => JObject(fields map f)
    case value                 => value
  }
  def liftCollect(f: PartialFunction[JField, JField]): PartialFunction[JValue, JValue] = {
    case JObjectFields(fields) if fields exists f.isDefinedAt => JObject(fields collect f)
  }
}

object JObjectFields {
  def unapply(m: JObject): Some[Vector[JField]] = Some(m.sortedFields)
}

case class JObject(fields: Map[String, JValue]) extends JContainer {
  def contained = fields.values

  def sortedFields: Vector[JField] = fields.toVector.sorted map (kv => JField(kv._1, kv._2)) filterNot (_.isUndefined)

  def get(name: String): JValue = fields.getOrElse(name, JUndefined)

  def +(field: JField): JObject                           = copy(fields = fields + field.toTuple)
  def -(name: String): JObject                            = copy(fields = fields - name)
  def partitionField(field: String): (JValue, JObject)    = get(field) -> JObject(fields - field)
  def partition(f: JField => Boolean): (JObject, JObject) = fields partition (x => f(JField(x._1, x._2))) bimap (JObject(_), JObject(_))

  private def fieldsCmp(m1: Map[String, JValue], m2: Map[String, JValue]): Int = {
    @tailrec def rec(fields: Array[String], i: Int): Int = {
      if (i < fields.length) {
        val key = fields(i)
        val v1  = m1.getOrElse(key, JUndefined)
        val v2  = m2.getOrElse(key, JUndefined)
        if (v1 == JUndefined && v2 == JUndefined) rec(fields, i + 1)
        else if (v1 == JUndefined) 1
        else if (v2 == JUndefined) -1
        else {
          val cres = (v1 compare v2)
          if (cres == 0) rec(fields, i + 1) else cres
        }
      }
      else 0
    }
    val arr: Array[String] = (m1.keySet ++ m2.keySet).toArray
    quickSort(arr)
    rec(arr, 0)
  }

  override def compare(that: JValue): Int = that match {
    case o: JObject => fieldsCmp(fields, o.fields)
    case _          => super.compare(that)
  }
  override def hashCode = fields.##
  override def equals(other: Any) = other match {
    case o: JObject => compare(o) == 0
    case _          => false
  }
}

final object JObject {
  final val empty = JObject(Nil)

  def apply(fields: Traversable[JField]): JObject      = JObject(fields.map(_.toTuple).toMap)
  def apply(fields: JField*): JObject                  = JObject(fields.map(_.toTuple).toMap)
  def unapplySeq(value: JObject): Some[Vector[JField]] = Some(value.sortedFields)
}

case class JArray(elements: List[JValue]) extends JContainer {
  def contained = elements
  override def compare(that: JValue): Int = that match {
    case JArray(xs) => elements ?|? xs toInt
    case _          => super.compare(that)
  }
}

case object JArray extends (List[JValue] => JArray) {
  final val empty = JArray(Nil)

  final def apply(vals: JValue*): JArray = JArray(vals.toList)
}
