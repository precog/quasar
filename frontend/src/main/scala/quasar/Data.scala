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
import quasar.ejson.{
  Common,
  EJson,
  Extension,
  SizedType,
  Type => EType,
  TypeTag,
  ExtEJson,
  CommonEJson
}
import quasar.fp._
import quasar.contrib.iota.{:<<:, ACopK}
import quasar.javascript.Js
import quasar.time.{DateTimeInterval, OffsetDate => QOffsetDate}

import java.time.{
  LocalDate => JLocalDate,
  LocalDateTime => JLocalDateTime,
  LocalTime => JLocalTime,
  OffsetDateTime => JOffsetDateTime,
  OffsetTime => JOffsetTime,
  ZoneOffset
}
import java.time.format.DateTimeFormatter
import scala.Any

import jawn._
import matryoshka._
import matryoshka.patterns._
import monocle.{Iso, Optional, Prism}
import scalaz.{Lens => _, Optional=>_, _}, Scalaz._
import scodec.bits.ByteVector

sealed abstract class Data extends Product with Serializable {
  def dataType: Type
  def toJs: Option[jscore.JsCore]
}

object Data {
  final case object Null extends Data {
    def dataType = Type.Null
    def toJs = jscore.Literal(Js.Null).some
  }

  final case class Str(value: String) extends Data {
    def dataType = Type.Str
    def toJs = jscore.Literal(Js.Str(value)).some
  }

  val _str = Prism.partial[Data, String] { case Data.Str(s) => s } (Data.Str(_))

  final case class Bool(value: Boolean) extends Data {
    def dataType = Type.Bool
    def toJs = jscore.Literal(Js.Bool(value)).some
  }
  val True = Bool(true)
  val False = Bool(false)

  val _bool =
    Prism.partial[Data, Boolean] { case Data.Bool(b) => b } (Data.Bool(_))

  sealed abstract class Number extends Data {
    override def equals(other: Any) = (this, other) match {
      case (Int(v1), Number(v2)) => BigDecimal(v1) ≟ v2
      case (Dec(v1), Number(v2)) => v1 ≟ v2
      case _                     => false
    }
  }
  object Number {
    def unapply(value: Data): Option[BigDecimal] = value match {
      case Int(value) => Some(BigDecimal(value))
      case Dec(value) => Some(value)
      case _ => None
    }
  }
  final case class Dec(value: BigDecimal) extends Number {
    def dataType = Type.Dec
    def toJs = jscore.Literal(Js.Num(value.doubleValue, true)).some
  }

  val _dec =
    Prism.partial[Data, BigDecimal] { case Data.Dec(i) => i } (Data.Dec(_))

  final case class Int(value: BigInt) extends Number {
    def dataType = Type.Int
    def toJs = jscore.Literal(Js.Num(value.doubleValue, false)).some
  }

  val _int = Prism.partial[Data, BigInt] { case Data.Int(i) => i } (Data.Int(_))

  object Obj {
    @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
    def apply(xs: (String -> Data)*): Obj = new Obj(ListMap(xs: _*))
  }

  final case class Obj(value: ListMap[String, Data]) extends Data {
    def dataType = Type.Obj(value ∘ (Type.Const(_)), None)

   def toJs =
     value.toList.map(_.bimap(jscore.Name(_), _.toJs))
          .toListMap.sequence.map(jscore.Obj(_))
  }

  val _obj =
    Prism.partial[Data, ListMap[String, Data]] { case Data.Obj(m) => m } (Data.Obj(_))

  def singletonObj(k: String, v: Data): Data =
    Obj(ListMap(k -> v))

  final case class Arr(value: List[Data]) extends Data {
    def dataType = Type.Arr(value ∘ (Type.Const(_)))
    def toJs = value.traverse(_.toJs).map(jscore.Arr(_))
  }

  val _arr =
    Prism.partial[Data, List[Data]] { case Data.Arr(l) => l } (Data.Arr(_))

  final case class OffsetDateTime(value: JOffsetDateTime) extends Data {
    def dataType = Type.OffsetDateTime
    def toJs = jscore.Call(
      jscore.ident("ISODate"),
      List(jscore.Literal(Js.Str(value.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))))).some
  }

  val _offsetDateTime =
    Prism.partial[Data, JOffsetDateTime] { case Data.OffsetDateTime(ts) => ts } (Data.OffsetDateTime)

  final case class OffsetTime(value: JOffsetTime) extends Data {
    def dataType = Type.OffsetTime
    def toJs = jscore.Call(
      jscore.ident("ISODate"),
      List(jscore.Literal(Js.Str(value.format(DateTimeFormatter.ISO_OFFSET_TIME))))).some
  }

  val _offsetTime =
    Prism.partial[Data, JOffsetTime] { case Data.OffsetTime(ts) => ts } (Data.OffsetTime)

  final case class OffsetDate(value: QOffsetDate) extends Data {
    def dataType = Type.OffsetDate
    def toJs = jscore.Call(
      jscore.ident("ISODate"),
      List(jscore.Literal(Js.Str(value.date.atStartOfDay(value.offset).format(DateTimeFormatter.ISO_OFFSET_DATE))))).some
  }

  val _offsetDate =
    Prism.partial[Data, QOffsetDate] { case Data.OffsetDate(ts) => ts } (Data.OffsetDate)

  final case class LocalDateTime(value: JLocalDateTime) extends Data {
    def dataType = Type.LocalDateTime
    def toJs = jscore.Call(
      jscore.ident("ISODate"),
      List(jscore.Literal(Js.Str(value.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))))).some
  }

  val _localDateTimeOptional = Optional[Data, JLocalDateTime] { case LocalDateTime(k) => Some(k); case _ => None }(ldt => _ => LocalDateTime(ldt))
  val _localDateTime =
    Prism.partial[Data, JLocalDateTime] { case Data.LocalDateTime(ts) => ts } (Data.LocalDateTime)

  final case class LocalTime(value: JLocalTime) extends Data {
    def dataType = Type.LocalTime
    def toJs = jscore.Call(
      jscore.ident("ISODate"),
      List(jscore.Literal(Js.Str(value.format(DateTimeFormatter.ISO_LOCAL_TIME))))).some
  }

  val _localTimeIso = Iso[LocalTime, JLocalTime](_.value)(LocalTime(_))
  val _localTime =
    Prism.partial[Data, JLocalTime] { case Data.LocalTime(t) => t } (Data.LocalTime)

  final case class LocalDate(value: JLocalDate) extends Data {
    def dataType = Type.LocalDate
    def toJs = jscore.Call(
      jscore.ident("ISODate"),
      List(jscore.Literal(Js.Str(value.format(DateTimeFormatter.ISO_LOCAL_DATE))))).some
  }

  val _localDate =
    Prism.partial[Data, JLocalDate] { case Data.LocalDate(d) => d } (Data.LocalDate)

  final case class Interval(value: DateTimeInterval) extends Data {
    def dataType = Type.Interval
    def toJs = if (value.isTimeLike) {
      jscore.Literal(Js.Num(value.duration.getSeconds*1000 + value.duration.getNano*1e-6, true)).some
    } else {
      None // TODO support intervals with dates in them
    }
  }

  val _interval =
    Prism.partial[Data, DateTimeInterval]
      { case Data.Interval(dti) => dti }(Data.Interval)

  final case class Binary(value: ImmutableArray[Byte]) extends Data {
    def dataType = Type.Binary
    def toJs = jscore.Call(jscore.ident("BinData"), List(
      jscore.Literal(Js.Num(0, false)),
      jscore.Literal(Js.Str(base64)))).some

    def base64: String = new sun.misc.BASE64Encoder().encode(value.toArray)

    override def toString = "Binary(Array[Byte](" + value.mkString(", ") + "))"

    /**
      * scala equality needs to remain for Spark to work
      * @see Planner.qscriptCore
      */
    override def equals(that: Any): Boolean = that match {
      case Binary(value2) => value ≟ value2
      case _ => false
    }
    override def hashCode = java.util.Arrays.hashCode(value.toArray[Byte])
  }
  object Binary {
    def fromArray(array: Array[Byte]): Binary = Binary(ImmutableArray.fromArray(array))
  }

  val _binary =
    Prism.partial[Data, ImmutableArray[Byte]] { case Data.Binary(bs) => bs } (Data.Binary(_))

  final case class Id(value: String) extends Data {
    def dataType = Type.Id
    def toJs = jscore.Call(jscore.ident("ObjectId"), List(jscore.Literal(Js.Str(value)))).some
  }

  val _id =
    Prism.partial[Data, String] { case Data.Id(id) => id } (Data.Id(_))

  /**
   An object to represent any value that might come from a backend, but that
   we either don't know about or can't represent in this ADT. We represent it
   with JS's `undefined`, just because no other value will ever be translated
   that way.
   */
  final case object NA extends Data {
    def dataType = Type.Bottom
    def toJs = jscore.ident(Js.Undefined.ident).some
  }

  object DateTimeLike {
    def unapply(data: Data): Option[Data] = data match {
      case d @ Data.OffsetDateTime(_) => d.some
      case d @ Data.OffsetDate(_) => d.some
      case d @ Data.OffsetTime(_) => d.some
      case d @ Data.LocalDateTime(_) => d.some
      case d @ Data.LocalDate(_) => d.some
      case d @ Data.LocalTime(_) => d.some
      case d @ Data.Interval(_) => d.some
      case _ => none
    }
  }

  object OffsetLike {
    def unapply(data: Data): Option[Data] = data match {
      case d @ Data.OffsetDateTime(_) => d.some
      case d @ Data.OffsetDate(_) => d.some
      case d @ Data.OffsetTime(_) => d.some
      case _ => none
    }
  }

  object LocalLike {
    def unapply(data: Data): Option[Data] = data match {
      case d @ Data.LocalDateTime(_) => d.some
      case d @ Data.LocalDate(_) => d.some
      case d @ Data.LocalTime(_) => d.some
      case _ => none
    }
  }

  final class Comparable private (val value: Data) extends scala.AnyVal

  object Comparable {
    def apply(data: Data): Option[Comparable] =
      some(data)
        .filter(d => Type.Comparable contains d.dataType)
        .map(new Comparable(_))

    def partialCompare(a: Comparable, b: Comparable): Option[Ordering] = {
      (a.value, b.value) match {
        case (Int(x), Int(y)) => Some(x cmp y)
        case (Dec(x), Dec(y)) => Some(x cmp y)
        case (Str(x), Str(y)) => Some(x cmp y)
        case (Bool(x), Bool(y)) => Some(x cmp y)
        case (OffsetDateTime(x), OffsetDateTime(y)) => Some(Ordering.fromInt(x compareTo y))
        case (OffsetDate(x), OffsetDate(y)) => Some(Ordering.fromInt(x compareTo y))
        case (OffsetTime(x), OffsetTime(y)) => Some(Ordering.fromInt(x compareTo y))
        case (LocalDateTime(x), LocalDateTime(y)) => Some(Ordering.fromInt(x compareTo y))
        case (LocalDate(x), LocalDate(y)) => Some(Ordering.fromInt(x compareTo y))
        case (LocalTime(x), LocalTime(y)) => Some(Ordering.fromInt(x compareTo y))
        case _ => None
      }
    }

    def min(a: Comparable, b: Comparable): Option[Comparable] = {
      partialCompare(a, b) map {
        case Ordering.LT => a
        case Ordering.EQ => a
        case Ordering.GT => b
      }
    }

    def max(a: Comparable, b: Comparable): Option[Comparable] = {
      partialCompare(a, b) map {
        case Ordering.LT => b
        case Ordering.EQ => a
        case Ordering.GT => a
      }
    }
  }

  implicit val dataShow: Show[Data] = Show.showFromToString

  implicit val dataEqual: Equal[Data] = Equal.equalA

  implicit val dataCorecursive: Corecursive.Aux[Data, EJson] =
    new Corecursive[Data] {
      type Base[T] = EJson[T]
      def embed(t: EJson[Data])(implicit BF: Functor[Base]) = fromEJson(t)
    }

  /** NB: For parsing arbitrary JSON into `Data`, _not_ for deserializing `Data`
    *     previously serialized as JSON. For that, see `DataCodec`.
    */
  val jsonParser: SupportParser[Data] =
    new SupportParser[Data] {
      implicit val facade: Facade[Data] =
        new SimpleFacade[Data] {
          def jarray(arr: List[Data])         = Arr(arr)
          def jobject(obj: Map[String, Data]) = Obj(ListMap(obj.toList: _*))
          def jnull()                         = Null
          def jfalse()                        = False
          def jtrue()                         = True
          def jnum(n: String)                 = Dec(BigDecimal(n))
          def jint(n: String)                 = Int(BigInt(n))
          def jstring(s: String)              = Str(s)
        }
    }

  object DateTimeConstants {
    val year: String = "year"
    val month: String = "month"
    val day: String = "day" // day of month
    val hour: String = "hour"
    val minute: String = "minute"
    val second: String = "second"
    val nanosecond: String = "nanosecond"
    val offset: String = "offset"
  }

  object EJsonType {
    def apply(tag: TypeTag): Data =
      Data.Obj(ListMap(EType.TypeKey -> Data.Str(tag.value)))

    def unapply(data: Data) = data match {
      case Data.Obj(map) =>
        map.get(EType.TypeKey) >>= {
          case Data.Str(str) => TypeTag(str).some
          case _             => None
        }
      case _ => None
    }
  }

  object EJsonTypeSize {
    def apply(tag: TypeTag, size: BigInt): Data =
      Obj(ListMap(
        EType.TypeKey     -> Data.Str(tag.value),
        SizedType.SizeKey -> Data.Int(size)))

    def unapply(data: Data) = data match {
      case Data.Obj(map) =>
        ((map.get(EType.TypeKey) ⊛ map.get(SizedType.SizeKey)) {
          case (Data.Str(str), Data.Int(size)) => (TypeTag(str), size).some
          case _                               => None
        }).join
      case _ => None
    }
  }

  val fromCommon: Algebra[Common, Data] = {
    case ejson.Arr(value)  => Arr(value)
    case ejson.Null()      => Null
    case ejson.Bool(value) => Bool(value)
    case ejson.Str(value)  => Str(value)
    case ejson.Dec(value)  => Dec(value)
  }

  def extract[A, B](fa: Option[Data], p: Prism[Data, A])(f: A => B): Option[B] =
    fa.flatMap(p.getOption).map(f)

  val nanosPerSec = 1000000000L

  val fromExtension: Algebra[Extension, Data] = {
    case ejson.Meta(value, meta) => (meta, value) match {
      case (EJsonType(TypeTag("_bson.oid")), Str(oid)) => Id(oid)

      case (EJsonTypeSize(TypeTag.Binary, size), Str(data)) =>
        if (size.isValidInt)
          ejson.z85.decode(data).fold[Data](
            NA)(
            bv => Binary(ImmutableArray.fromArray(bv.take(size.toLong).toArray)))
        else NA

      case (EJsonType(TypeTag.OffsetDateTime), Obj(map)) =>
        (extract(map.get(DateTimeConstants.year), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.month), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.day), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.hour), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.minute), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.second), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.nanosecond), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.offset), _int)(_.toInt)) {
            (y, mo, d, h, mi, s, n, o) =>
              OffsetDateTime(JOffsetDateTime.of(y, mo, d, h, mi, s, n,
                ZoneOffset.ofTotalSeconds(o)))
          }.getOrElse(NA)

      case (EJsonType(TypeTag.OffsetTime), Obj(map)) =>
        (extract(map.get(DateTimeConstants.hour), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.minute), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.second), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.nanosecond), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.offset), _int)(_.toInt)) {
            (h, m, s, n, o) =>
              OffsetTime(JOffsetTime.of(h, m, s, n, ZoneOffset.ofTotalSeconds(o)))
          }.getOrElse(NA)

      case (EJsonType(TypeTag.OffsetDate), Obj(map)) =>
        (extract(map.get(DateTimeConstants.year), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.month), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.day), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.offset), _int)(_.toInt)) {
            (y, m, d, o) =>
              OffsetDate(QOffsetDate(JLocalDate.of(y, m, d), ZoneOffset.ofTotalSeconds(o)))
          }.getOrElse(NA)

      case (EJsonType(TypeTag.LocalDateTime), Obj(map)) =>
        (extract(map.get(DateTimeConstants.year), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.month), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.day), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.hour), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.minute), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.second), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.nanosecond), _int)(_.toInt)) {
            (y, mo, d, h, mi, s, n) =>
              LocalDateTime(JLocalDateTime.of(y, mo, d, h, mi, s, n))
          }.getOrElse(NA)

      case (EJsonType(TypeTag.LocalTime), Obj(map)) =>
        (extract(map.get(DateTimeConstants.hour), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.minute), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.second), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.nanosecond), _int)(_.toInt)) {
            (h, m, s, n) =>
              LocalTime(JLocalTime.of(h, m, s, n))
          }.getOrElse(NA)

      case (EJsonType(TypeTag.LocalDate), Obj(map)) =>
        (extract(map.get(DateTimeConstants.year), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.month), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.day), _int)(_.toInt)) {
            (y, m, d) =>
              LocalDate(JLocalDate.of(y, m, d))
          }.getOrElse(NA)

      case (EJsonType(TypeTag.Interval), Obj(map)) =>
        (extract(map.get(DateTimeConstants.year), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.month), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.day), _int)(_.toInt) ⊛
          extract(map.get(DateTimeConstants.second), _int)(_.toLong) ⊛
          extract(map.get(DateTimeConstants.nanosecond), _int)(_.toLong)) {
            (y, m, d, s, n) =>
              Interval(DateTimeInterval.make(y, m, d, s, n))
          }.getOrElse(NA)

      case (_, _) => value
    }

    case ejson.Map(value)       =>
      value.traverse(Bitraverse[(?, ?)].leftTraverse.traverse(_) {
        case Str(key) => key.some
        case _        => None
      }).fold[Data](NA)(pairs => Obj(ListMap(pairs: _*)))

    case ejson.Int(value)       => Int(value)

    // FIXME: cheating, but it’s what we’re already doing in the SQL parser
    case ejson.Byte(value)      => Binary.fromArray(Array[Byte](value))

    case ejson.Char(value)      => Str(value.toString)
  }

  // TODO: Data should be replaced with EJson. These just exist to bridge the
  //       gap in the meantime.
  val fromEJson: Algebra[EJson, Data] = {
    case ExtEJson(ext) => fromExtension(ext)
    case CommonEJson(com) => fromCommon(com)
  }

  /** Converts the parts of `Data` that it can, then stores the rest in,
    * effectively, `Free.Pure`.
    */
  def toEJson[F[a] <: ACopK[a]](implicit C: Common :<<: F, E: Extension :<<: F):
      Coalgebra[CoEnv[Data, F, ?], Data] =
    ed => CoEnv(ed match {
      case Arr(value)       => C.inj(ejson.Arr(value)).right
      case Obj(value)       =>
        E.inj(ejson.Map(value.toList.map(_.leftMap(Str(_))))).right
      case Null             => C.inj(ejson.Null()).right
      case Bool(value)      => C.inj(ejson.Bool(value)).right
      case Str(value)       => C.inj(ejson.Str(value)).right
      case Dec(value)       => C.inj(ejson.Dec(value)).right
      case Int(value)       => E.inj(ejson.Int(value)).right

      case OffsetDateTime(value) => E.inj(ejson.Meta(
        Obj(ListMap(
          DateTimeConstants.year       -> Int(value.getYear),
          DateTimeConstants.month      -> Int(value.getMonth.getValue),
          DateTimeConstants.day        -> Int(value.getDayOfMonth),
          DateTimeConstants.hour       -> Int(value.getHour),
          DateTimeConstants.minute     -> Int(value.getMinute),
          DateTimeConstants.second     -> Int(value.getSecond),
          DateTimeConstants.nanosecond -> Int(value.getNano),
          DateTimeConstants.offset     -> Int(value.getOffset.getTotalSeconds))),
        EJsonType(TypeTag.OffsetDateTime))).right

      case OffsetTime(value) => E.inj(ejson.Meta(
        Obj(ListMap(
          DateTimeConstants.hour       -> Int(value.getHour),
          DateTimeConstants.minute     -> Int(value.getMinute),
          DateTimeConstants.second     -> Int(value.getSecond),
          DateTimeConstants.nanosecond -> Int(value.getNano),
          DateTimeConstants.offset     -> Int(value.getOffset.getTotalSeconds))),
        EJsonType(TypeTag.OffsetTime))).right

      case OffsetDate(value) => E.inj(ejson.Meta(
        Obj(ListMap(
          DateTimeConstants.year   -> Int(value.date.getYear),
          DateTimeConstants.month  -> Int(value.date.getMonth.getValue),
          DateTimeConstants.day    -> Int(value.date.getDayOfMonth),
          DateTimeConstants.offset -> Int(value.offset.getTotalSeconds))),
        EJsonType(TypeTag.OffsetDate))).right

      case LocalDateTime(value) => E.inj(ejson.Meta(
        Obj(ListMap(
          DateTimeConstants.year       -> Int(value.getYear),
          DateTimeConstants.month      -> Int(value.getMonth.getValue),
          DateTimeConstants.day        -> Int(value.getDayOfMonth),
          DateTimeConstants.hour       -> Int(value.getHour),
          DateTimeConstants.minute     -> Int(value.getMinute),
          DateTimeConstants.second     -> Int(value.getSecond),
          DateTimeConstants.nanosecond -> Int(value.getNano))),
        EJsonType(TypeTag.LocalDateTime))).right

      case LocalTime(value)      => E.inj(ejson.Meta(
        Obj(ListMap(
          DateTimeConstants.hour       -> Int(value.getHour),
          DateTimeConstants.minute     -> Int(value.getMinute),
          DateTimeConstants.second     -> Int(value.getSecond),
          DateTimeConstants.nanosecond -> Int(value.getNano))),
        EJsonType(TypeTag.LocalTime))).right

      case LocalDate(value) => E.inj(ejson.Meta(
        Obj(ListMap(
          DateTimeConstants.year  -> Int(value.getYear),
          DateTimeConstants.month -> Int(value.getMonth.getValue),
          DateTimeConstants.day   -> Int(value.getDayOfMonth))),
        EJsonType(TypeTag.LocalDate))).right

      case Interval(value)  =>
        E.inj(ejson.Meta(
          Obj(ListMap(
            DateTimeConstants.year       -> Int(value.period.getYears),
            DateTimeConstants.month      -> Int(value.period.getMonths),
            DateTimeConstants.day        -> Int(value.period.getDays),
            DateTimeConstants.second     -> Int(value.duration.getSeconds),
            DateTimeConstants.nanosecond -> Int(value.duration.getNano))),
          EJsonType(TypeTag.Interval))).right

      case Binary(value)    =>
        E.inj(ejson.Meta(
          Str(ejson.z85.encode(ByteVector.view(value.toArray))),
          EJsonTypeSize(TypeTag.Binary, value.size))).right

      case Id(value)        =>
        // FIXME: This evilly guesses the backend-specific OID formats
        E.inj(ejson.Meta(Str(value), EJsonType(TypeTag("_bson.oid")))).right

      case data             => data.left
    })
}
