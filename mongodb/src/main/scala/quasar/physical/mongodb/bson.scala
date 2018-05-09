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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.fp._
import quasar.javascript._
import quasar.jscore, jscore.JsFn

import java.time.Instant
import scala.Any
import scala.collection.JavaConverters._

import monocle.Prism
import org.bson._
import org.bson.types
import scalaz._, Scalaz._

/**
 * A type-safe ADT for Mongo's native data format. Note that this representation
 * is not suitable for efficiently storing large quantities of data.
 */
sealed abstract class Bson extends Product with Serializable {
  // TODO: Once Bson is fixpoint, this should be an algebra:
  //       BsonF[BsonValue] => BsonValue
  def repr: BsonValue
  def toJs: Js.Expr
}

object Bson {

  // TODO: Once Bson is fixpoint, this should be a coalgebra:
  //       BsonValue => BsonF[BsonValue]
  val fromRepr: BsonValue => Bson = {
    case arr:  BsonArray             => Arr(arr.getValues.asScala.toList ∘ fromRepr)
    case bin:  BsonBinary            => Binary.fromArray(bin.getData)
    case bool: BsonBoolean           => Bool(bool.getValue)
    case dt:   BsonDateTime          => Date(dt.getValue)
    case dec:  BsonDecimal128 if (dec.getValue.isNaN || dec.getValue.isInfinite)
                                     => Undefined
    case dec:  BsonDecimal128        => Dec128(dec.getValue.bigDecimalValue)
    case doc:  BsonDocument          => Doc(doc.asScala.toList.toListMap ∘ fromRepr)
    case dub:  BsonDouble            => Dec(dub.doubleValue)
    case i32:  BsonInt32             => Int32(i32.intValue)
    case i64:  BsonInt64             => Int64(i64.longValue)
    case _:    BsonMaxKey            => MaxKey
    case _:    BsonMinKey            => MinKey
    case _:    BsonNull              => Null
    case oid:  BsonObjectId          => ObjectId.fromArray(oid.getValue.toByteArray)
    case rex:  BsonRegularExpression => Regex(rex.getPattern, rex.getOptions)
    case str:  BsonString            => Text(str.getValue)
    case sym:  BsonSymbol            => Symbol(sym.getSymbol)
    case tms:  BsonTimestamp         => Timestamp(tms.getTime, tms.getInc)
    case _:    BsonUndefined         => Undefined
      // NB: These types we can’t currently translate back to Bson, but we don’t
      //     expect them to appear.
    case _: BsonDbPointer | _: BsonJavaScript | _: BsonJavaScriptWithScope => Undefined
  }

  final case class Dec(value: Double) extends Bson {
    def repr = new BsonDouble(value)

    def toJs = Js.Num(value, true)

    override def equals(that: Any): Boolean = that match {
      case Dec(value2) =>
        (value.isNaN && value2.isNaN) ||
        (value.isInfinity && value > 0 && value2.isInfinity && value2 > 0) ||
        (value.isInfinity && value < 0 && value2.isInfinity && value2 < 0) ||
        (value ≟ value2)
      case _ => false
    }

    override def hashCode =
      if (value.isNaN) "NaN".hashCode
      else if (value.isInfinity && value > 0) "+inf".hashCode
      else if (value.isInfinity && value < 0) "-inf".hashCode
      else value.hashCode
  }

  val _dec = Prism.partial[Bson, Double] { case Bson.Dec(v) => v } (Bson.Dec(_))

  final case class Text(value: String) extends Bson {
    def repr = new BsonString(value)

    def toJs = Js.Str(value)
  }

  val _text =
    Prism.partial[Bson, String] { case Bson.Text(v) => v } (Bson.Text(_))

  final case class Binary(value: ImmutableArray[Byte]) extends Bson {
    def repr = new BsonBinary(value.toArray[Byte])
    def toJs = Js.Call(Js.Ident("BinData"), List(Js.Num(0, false), Js.Str(new sun.misc.BASE64Encoder().encode(value.toArray))))

    override def toString = "Binary(Array[Byte](" + value.mkString(", ") + "))"

    override def equals(that: Any): Boolean = that match {
      case Binary(value2) => value ≟ value2
      case _ => false
    }

    override def hashCode = java.util.Arrays.hashCode(value.toArray[Byte])
  }

  object Binary {
    def fromArray(array: Array[Byte]): Binary = Binary(ImmutableArray.fromArray(array))
  }

  val _doc =
    Prism.partial[Bson, ListMap[String, Bson]] { case Bson.Doc(v) => v } (Bson.Doc(_))

  final case class Doc(value: ListMap[String, Bson])
      extends Bson with org.bson.conversions.Bson {
    def repr: BsonDocument =
      new BsonDocument(value.toList.map { case (k, v) => new BsonElement(k, v.repr) }.asJava)
    def toJs = Js.AnonObjDecl((value ∘ (_.toJs)).toList)

    def toBsonDocument[TDocument](
      documentClass: java.lang.Class[TDocument],
      codecRegistry: org.bson.codecs.configuration.CodecRegistry) =
      repr
  }

  object Doc {
    def apply(pairs: (String, Bson)*): Bson.Doc = Doc(ListMap(pairs: _*))

    def opt(value: ListMap[String, Option[Bson]]): Doc =
      Doc(value.foldLeft(ListMap[String, Bson]()){ case (acc, (k, v)) =>
        acc ++ v.map(x => ListMap(k -> x)).getOrElse(ListMap())
      })
  }

  final case class Arr(value: List[Bson]) extends Bson {
    def repr = new BsonArray((value ∘ (_.repr)).asJava)
    def toJs = Js.AnonElem(value ∘ (_.toJs))
  }

  object Arr {
    def apply(elems: Bson*): Bson.Arr = Arr(elems.toList)
  }

  final case class ObjectId(value: ImmutableArray[Byte]) extends Bson {
    private def oid = new org.bson.types.ObjectId(value.toArray[Byte])
    def repr = new BsonObjectId(oid)

    def str = oid.toHexString

    def toJs = Js.Call(Js.Ident("ObjectId"), List(Js.Str(str)))

    override def toString = "ObjectId(" + str + ")"

    override def equals(that: Any): Boolean = that match {
      case ObjectId(value2) => value ≟ value2
      case _ => false
    }

    override def hashCode = java.util.Arrays.hashCode(value.toArray[Byte])
  }

  object ObjectId {
    def fromArray(array: Array[Byte]): ObjectId = ObjectId(ImmutableArray.fromArray(array))

    def fromString(str: String): Option[ObjectId] = {
      \/.fromTryCatchNonFatal(new types.ObjectId(str)).toOption.map(oid => ObjectId.fromArray(oid.toByteArray))
    }
  }

  final case class Bool(value: Boolean) extends Bson {
    def repr = new BsonBoolean(value)
    def toJs = Js.Bool(value)
  }

  /** NB: Can’t use `Instant`, because it encodes values outside the range of
    *     Bson DateTimes.
    */
  final case class Date(millis: Long) extends Bson {
    def repr = new BsonDateTime(millis)
    def toJs = Js.Call(Js.Ident("ISODate"), List(Js.Str(Instant.ofEpochMilli(millis).toString)))
  }

  object Date {
    val minInstant = Instant.ofEpochMilli(Long.MinValue)
    val maxInstant = Instant.ofEpochMilli(Long.MaxValue)
    def fromInstant(inst: Instant) =
      (minInstant.compareTo(inst) <= 0 && inst.compareTo(maxInstant) <= 0)
        .option(Date(inst.toEpochMilli))
  }

  final case object Null extends Bson {
    def repr = new BsonNull
    override def toJs = Js.Null
  }

  /** DEPRECATED in the spec, but the 3.0 Mongo driver returns it to us. */
  final case object Undefined extends Bson {
    def repr = new BsonUndefined
    override def toJs = Js.Undefined
  }

  final case class Regex(value: String, options: String) extends Bson {
    def repr = new BsonRegularExpression(value, options)
    def toJs = Js.New(Js.Call(Js.Ident("RegExp"), List(Js.Str(value), Js.Str(options))))
  }

  final case class JavaScript(value: Js.Expr) extends Bson {
    def repr = new BsonJavaScript(value.pprint(0))
    def toJs = value
  }

  final case class JavaScriptScope(code: Js.Expr, doc: ListMap[String, Bson])
      extends Bson {
    def repr =
      new BsonJavaScriptWithScope(
        code.pprint(0),
        new BsonDocument(doc.toList.map { case (k, v) => new BsonElement(k, v.repr) }.asJava))
    // FIXME: this loses scope, but I don’t know what it should look like
    def toJs = code
  }

  final case class Symbol(value: String) extends Bson {
    def repr = new BsonSymbol(value)
    def toJs = Js.Ident(value)
  }

  final case class Int32(value: Int) extends Bson {
    def repr = new BsonInt32(value)
    def toJs = Js.Call(Js.Ident("NumberInt"), List(Js.Str(value.shows)))
  }

  val _int32 =
    Prism.partial[Bson, Int] { case Bson.Int32(v) => v } (Bson.Int32(_))

  final case class Int64(value: Long) extends Bson {
    def repr = new BsonInt64(value)
    def toJs = Js.Call(Js.Ident("NumberLong"), List(Js.Str(value.shows)))
  }

  val _dec128 =
    Prism.partial[Bson, BigDecimal] { case Bson.Dec128(v) => v } (Bson.Dec128(_))

  final case class Dec128(value: BigDecimal) extends Bson {
    private val mc =
      if (value.mc == java.math.MathContext.UNLIMITED) java.math.MathContext.DECIMAL128
      else value.mc

    private val decimal128 = new types.Decimal128(value(mc).bigDecimal)
    def repr = new BsonDecimal128(decimal128)
    def toJs = Js.Call(Js.Ident("NumberDecimal"), List(Js.Str(decimal128.toString)))
  }

  final case class Timestamp private (epochSecond: Int, ordinal: Int) extends Bson {
    def repr = new BsonTimestamp(epochSecond, ordinal)
    def toJs = Js.Call(Js.Ident("Timestamp"),
      List(Js.num(epochSecond.toLong), Js.num(ordinal.toLong)))
    override def toString = s"Timestamp(${Instant.ofEpochSecond(epochSecond.toLong)}, ${ordinal.shows})"
  }

  final case object MinKey extends Bson {
    def repr = new BsonMinKey()
    def toJs = Js.Ident("MinKey")
  }

  final case object MaxKey extends Bson {
    def repr = new BsonMaxKey()
    def toJs = Js.Ident("MaxKey")
  }

  implicit val equal: Equal[Bson] = Equal.equalA

  implicit val show: Show[Bson] = Show.showFromToString

  def isInt(bson: Bson, i: Int): Boolean =
    List(
      Bson.Int32(i),
      Bson.Int64(i.toLong),
      Bson.Dec(i.toDouble),
      Bson.Dec128(i)).contains(bson)
}

sealed abstract class BsonType(val ordinal: Int)

object BsonType {
  final case object Dec extends BsonType(1)
  final case object Text extends BsonType(2)
  final case object Doc extends BsonType(3)
  final case object Arr extends BsonType(4)
  final case object Binary extends BsonType(5)
  final case object Undefined extends BsonType(6)
  final case object ObjectId extends BsonType(7)
  final case object Bool extends BsonType(8)
  final case object Date extends BsonType(9)
  final case object Null extends BsonType(10)
  final case object Regex extends BsonType(11)
  final case object JavaScript extends BsonType(13)
  final case object JavaScriptScope extends BsonType(15)
  final case object Symbol extends BsonType(14)
  final case object Int32 extends BsonType(16)
  final case object Int64 extends BsonType(18)
  final case object Timestamp extends BsonType(17)
  final case object Dec128 extends BsonType(19)
  final case object MinKey extends BsonType(255)
  final case object MaxKey extends BsonType(127)
}

sealed abstract class BsonField {
  def asText  : String
  def asField : String = "$" + asText
  def asVar   : String = "$$" + asText

  def bson = Bson.Text(asText)

  import BsonField._

  def \ (that: BsonField): BsonField = (this, that) match {
    case (Path(x),     Path(y))     => Path(x ⊹ y)
    case (Path(x),     y @ Name(_)) => Path(x ⊹ NonEmptyList(y))
    case (x @ Name(_), Path(y))     => Path(x <:: y)
    case (x @ Name(_), y @ Name(_)) => Path(NonEmptyList(x, y))
  }

  def \\ (tail: List[BsonField]): BsonField =
    if (tail.isEmpty) this
    else {
      val t = IList.fromList(tail.flatMap(_.flatten.toList))
      this match {
        case Path(p)     => Path(p :::> t)
        case l @ Name(_) => Path(NonEmptyList.nel(l, t))
    }
  }

  def flatten: NonEmptyList[Name]

  def parent: Option[BsonField] =
    BsonField(flatten.toList.dropRight(1))

  def startsWith(that: BsonField) =
    this.flatten.toList.startsWith(that.flatten.toList)

  def relativeTo(that: BsonField): Option[BsonField] = {
    val f1 = flatten.toList
    val f2 = that.flatten.toList
    if (f1 startsWith f2) BsonField(f1.drop(f2.length))
    else None
  }

  def toJs: JsFn =
    this.flatten.foldLeft(JsFn.identity)((acc, leaf) =>
      leaf match {
        case Name(v)  => JsFn(JsFn.defaultName, jscore.Access(acc(jscore.Ident(JsFn.defaultName)), jscore.Literal(Js.Str(v))))
      })

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  override def hashCode = this match {
    case Name(v) => v.hashCode
    case Path(v) if (v.tail.length ≟ 0) => v.head.hashCode
    case p @ Path(_) => p.flatten.hashCode
  }

  override def equals(that: Any): Boolean = (this, that) match {
    case (Name(v1),      Name(v2))      => v1 ≟ v2
    case (v1: BsonField, v2: BsonField) => v1.flatten.equals(v2.flatten)
    case _                              => false
  }
}

object BsonField {
  sealed abstract class Root
  final case object Root extends Root

  def apply(v: NonEmptyList[BsonField.Name]): BsonField = v match {
    case NonEmptyList(head, INil()) => head
    case _ => Path(v)
  }

  def apply(v: List[BsonField.Name]): Option[BsonField] = v.toNel.map(apply)

  final case class Name(value: String) extends BsonField {
    def asText = Path(NonEmptyList(this)).asText

    def flatten = NonEmptyList(this)
  }

  object Name {
    implicit val equal: Equal[Name] = Equal.equalA

    implicit val show: Show[Name] =
      Show.shows(n => s"""BsonField.Name("${n.value}")""")
  }

  final case class Path(values: NonEmptyList[Name]) extends BsonField {
    def flatten = values

    def asText = (values.list.zipWithIndex.map {
      case (Name(value), 0) => value
      case (Name(value), _) => "." + value
    }).toList.mkString("")
  }

  implicit val equal: Equal[BsonField] = Equal.equalA

  implicit val show: Show[BsonField] = Show.shows {
    case n @ Name(_)  => n.shows
    case Path(values) => values.list.toList.mkString(" \\ ")
  }
}
