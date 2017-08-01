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

package quasar.physical.marklogic.fs

import slamdata.Predef._
import quasar.Data
import quasar.fp.ski._
import quasar.physical.marklogic.{ErrorMessages, MonadErrMsgs}
import quasar.physical.marklogic.optics._
import quasar.physical.marklogic.qscript.{EJsonTypeKey, EJsonValueKey}
import quasar.physical.marklogic.xml._
import quasar.physical.marklogic.xml.namespaces._

import scala.xml._
import scala.collection.immutable.Seq

import argonaut._, Argonaut._
import eu.timepit.refined.auto._
import monocle.Prism
import scalaz.{Node => _, _}, Scalaz._
import xml.name._

object data {
  val encodeJson: Data => Json = {
    def typedObj[A: EncodeJson](typ: DataType, value: A): Json =
      jObjectFields(EJsonTypeKey -> jString(typ.shows), EJsonValueKey -> value.asJson)

    {
      case Data.Binary(bytes) => typedObj(DT.Binary   , base64Bytes(bytes))
      case Data.Bool(b)       => jBool(b)
      case Data.Date(d)       => typedObj(DT.Date     , isoLocalDate(d))
      case Data.Dec(d)        => typedObj(DT.Decimal  , d)
      case Data.Id(id)        => typedObj(DT.Id       , id)
      case Data.Int(i)        => typedObj(DT.Integer  , i)
      case Data.Interval(d)   => typedObj(DT.Interval , isoDuration(d))
      case Data.NA            => jSingleObject(EJsonTypeKey, jString((DT.NA: DataType).shows))
      case Data.Null          => jNull
      case Data.Str(s)        => jString(s)
      case Data.Time(t)       => typedObj(DT.Time     , isoLocalTime(t))
      case Data.Timestamp(ts) => typedObj(DT.Timestamp, isoInstant(ts))

      case Data.Arr(elements) => jArray(elements map encodeJson)
      case Data.Obj(entries)  => jObject(JsonObject.fromTraversableOnce(entries mapValues encodeJson))
      case Data.Set(elements) => typedObj(DT.Set      , encodeJson(Data.Arr(elements)))
    }
  }

  def encodeXml[F[_]: MonadErrMsgs](data: Data): F[Elem] = {
    def typeAttr(tpe: DataType): Attribute =
      Attribute(ejsBinding.prefix, ejsonType.localPart.shows, tpe.shows, Null)

    def ejsElem(name: QName, tpe: DataType, ns: NamespaceBinding, children: Seq[Node]): Elem =
      Elem(name.prefix.map(_.shows).orNull, name.localPart.shows, typeAttr(tpe), ns, true, children: _*)

    def keyElem(label: String): (QName, Option[Attribute]) =
      NCName.fromString(label).fold(κ(wrappedKey(label)), unwrappedKey(_))

    def unwrappedKey(label: NCName): (QName, Option[Attribute]) =
      (QName.unprefixed(label), None)

    def wrappedKey(unwrappedLabel: String): (QName, Option[Attribute]) = {
      (ejsonEncodedName, Attribute(
        ejsonNs.prefix.shows,
        ejsonEncodedAttr.localPart.shows,
        unwrappedLabel, Null).some)
    }

    def innerElem(name: QName, tpe: DataType, children: Seq[Node]): Elem =
      ejsElem(name, tpe, TopScope, children)

    def rootElem(name: QName, tpe: DataType, children: Seq[Node]): Elem =
      ejsElem(name, tpe, ejsBinding, children)

    def encodeXml0(
      elem: (QName, DataType, Seq[Node]) => Elem,
      loop: QName => Data => Validation[ErrorMessages, Elem]
    ): QName => Data => Validation[ErrorMessages, Elem] = {
      val mapEntryToXml: ((String, Data)) => ErrorMessages \/ Elem = {
        case (k, v) => {
          val (name, attr) = keyElem(k)
          loop(name)(v).map(_ % attr.getOrElse(Null)).disjunction
        }
      }

      elementName => {
        case Data.Binary(bytes) => elem(elementName, DT.Binary   , Text(base64Bytes(bytes))     ).success
        case Data.Bool(b)       => elem(elementName, DT.Boolean  , Text(b.fold("true", "false"))).success
        case Data.Date(d)       => elem(elementName, DT.Date     , Text(isoLocalDate(d))        ).success
        case Data.Dec(d)        => elem(elementName, DT.Decimal  , Text(d.toString)             ).success
        case Data.Id(id)        => elem(elementName, DT.Id       , Text(id)                     ).success
        case Data.Int(i)        => elem(elementName, DT.Integer  , Text(i.toString)             ).success
        case Data.Interval(d)   => elem(elementName, DT.Interval , Text(isoDuration(d))         ).success
        case Data.NA            => elem(elementName, DT.NA       , Nil                          ).success
        case Data.Null          => elem(elementName, DT.Null     , Nil                          ).success
        case Data.Str(s)        => elem(elementName, DT.String   , Text(s)                      ).success
        case Data.Time(t)       => elem(elementName, DT.Time     , Text(isoLocalTime(t))        ).success
        case Data.Timestamp(ts) => elem(elementName, DT.Timestamp, Text(isoInstant(ts))         ).success

        case Data.Arr(elements) =>
          elements traverse loop(ejsonArrayElt) map (elem(elementName, DT.Array, _))

        case Data.Obj(entries)  =>
          entries.toList traverse (mapEntryToXml andThen (_.validation)) map (elem(elementName, DT.Object, _))

        case other              => s"No representation for '$other' in XML.".failureNel[Elem]
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def inner: QName => Data => Validation[ErrorMessages, Elem] =
      name => encodeXml0(innerElem, inner)(name)

    encodeXml0(rootElem, inner)(ejsonEjson)(data)
      .fold(_.raiseError[F, Elem], _.point[F])
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def decodeJson[F[_]: MonadErrMsgs](json: Json): F[Data] = {
    val error: String => F[Data] = _.wrapNel.raiseError[F, Data]

    val decodeNumber: JsonNumber => Data = {
      case JsonLong(i) => Data.Int(i)
      case other       => Data.Dec(other.toBigDecimal)
    }

    val decodeObject: JsonObject => F[Data] = {
      case EJsonTyped(DT.Binary, b64) =>
        b64.string.flatMap(base64Bytes.getOption(_)).map(Data._binary(_)).cata(
          _.point[F], error(s"Expected Base64-encoded binary data, found: $b64"))

      case EJsonTyped(DT.Date, dt) =>
        dt.string.flatMap(isoLocalDate.getOption(_)).map(Data._date(_)).cata(
          _.point[F], error(s"Expected ISO-8601 formatted local date, found: $dt"))

      case EJsonTyped(DT.Decimal, d) =>
        d.as[BigDecimal].toOption.map(Data._dec(_)).cata(
          _.point[F], error(s"Expected a decimal number, found: $d"))

      case EJsonTyped(DT.Id, id) =>
        id.string.map(Data._id(_)).cata(
          _.point[F], error(s"Expected an identifier string, found: $id"))

      case EJsonTyped(DT.Integer, i) =>
        i.as[BigInt].toOption.map(Data._int(_)).cata(
          _.point[F], error(s"Expected an integer number, found: $i"))

      case EJsonTyped(DT.Interval, ivl) =>
        ivl.string.flatMap(isoDuration.getOption(_)).map(Data._interval(_)).cata(
          _.point[F], error(s"Expected an ISO-8601 formatted duration, found: $ivl"))

      case EJsonTyped(DT.Time, t) =>
        t.string.flatMap(isoLocalTime.getOption(_)).map(Data._time(_)).cata(
          _.point[F], error(s"Expected an ISO-8601 formatted local time, found: $t"))

      case EJsonTyped(DT.Timestamp, ts) =>
        ts.string.flatMap(isoInstant.getOption(_)).map(Data._timestamp(_)).cata(
          _.point[F], error(s"Expected an ISO-8601 formatted datetime, found: $ts"))

      case EJsType(DT.NA) => (Data.NA: Data).point[F]

      // NB: Because sometimes we need to encode `Null` as an object due to
      //     MarkLogic losing map entries where the value is a literal `null`.
      case EJsType(DT.Null) => (Data.Null: Data).point[F]

      case other =>
        other.toList traverse { case (k, v) => decodeJson[F](v) strengthL k } map (Data.Obj(_: _*))
    }

    json.fold(
      (Data.Null: Data).point[F],
      b => Data._bool(b).point[F],
      decodeNumber andThen (_.point[F]),
      s => Data._str(s).point[F],
      _.traverse(decodeJson[F](_)).map(Data.Arr(_)),
      decodeObject)
  }

  /** Attempts to decode an XML `Elem` as `Data` using the provided `recover`
    * function to handle untyped child `Node`s.
    *
    * The computation fails when `Elem` purports to be encoded `Data` but is
    * malformed. `None` is returned when `Elem` does not purport to be `Data`.
    */
  def decodeXml[F[_]: MonadErrMsgs](recover: Node => F[Data])(elem: Elem): F[Option[Data]] = {
    type V[A] = Validation[ErrorMessages, A]
    type G[A] = F[V[A]]

    implicit val G: Applicative[G] = Applicative[F].compose[V]

    def qname[A](e: Elem): G[String] =
      encodedQualifiedName(e)
        .toSuccessNel(s"Expected a ${ejsonEncodedAttr.shows} for an ${ejsonEncodedName.shows} element")
        .point[F]

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def decodeXml0: Node => G[Data] = {
      case DataNode(DT.Array, children) =>
        elements(children).toList traverse decodeXml0 map (Data._arr(_))

      case DataNode(DT.Binary, LeafText(b64)) =>
        base64Bytes.getOption(b64)
          .map(bytes => Data._binary(bytes))
          .toSuccessNel(s"Expected Base64-encoded binary data, found: $b64")
          .point[F]

      case DataNode(DT.Binary, Seq()   ) =>
        Data._binary(ImmutableArray.fromArray(Array())).success.point[F]

      case DataNode(DT.Boolean, LeafText("true"))  => Data._bool(true).success.point[F]
      case DataNode(DT.Boolean, LeafText("false")) => Data._bool(false).success.point[F]

      case DataNode(DT.Date, LeafText(d)) =>
        isoLocalDate.getOption(d)
          .map(Data._date(_))
          .toSuccessNel(s"Expected an ISO-8601 formatted local date, found: $d")
          .point[F]

      case DataNode(DT.Decimal, LeafText(d)) =>
        Validation.fromTryCatchNonFatal(BigDecimal(d))
          .map(Data._dec(_))
          .leftAs(s"Expected a decimal number, found: $d".wrapNel)
          .point[F]

      case DataNode(DT.Id, LeafText(id)) => Data._id(id).success.point[F]

      case DataNode(DT.Integer, LeafText(n)) =>
        Validation.fromTryCatchNonFatal(BigInt(n))
          .map(Data._int(_))
          .leftAs(s"Expected an integral number, found: $n".wrapNel)
          .point[F]

      case DataNode(DT.Interval, LeafText(ivl)) =>
        isoDuration.getOption(ivl)
          .map(Data._interval(_))
          .toSuccessNel(s"Expected an ISO-8601 formatted duration, found: $ivl")
          .point[F]

      case DataNode(DT.NA, Seq()) => (Data.NA: Data).success.point[F]

      case DataNode(DT.Null, Seq()) => (Data.Null: Data).success.point[F]

      case DataNode(DT.Object, children) =>
        elements(children).toList
          .traverse(el => (qname(el) |@| decodeXml0(el))((_, _)))
          .map(entries => Data._obj(ListMap(entries: _*)))

      case DataNode(DT.String, LeafText(s)) => Data._str(s).success.point[F]

      case DataNode(DT.Time, LeafText(t)) =>
        isoLocalTime.getOption(t)
          .map(Data._time(_))
          .toSuccessNel(s"Expected an ISO-8601 formatted local time, found: $t")
          .point[F]

      case DataNode(DT.Timestamp, LeafText(ts)) =>
        isoInstant.getOption(ts)
          .map(Data._timestamp(_))
          .toSuccessNel(s"Expected an ISO-8601 formatted datetime, found: $ts")
          .point[F]

      case untyped =>
        recover(untyped) map (_.point[V]) handleError (_.failure[Data].point[F])
    }

    elem match {
      case DataNode(_, _) => (decodeXml0(elem): F[V[Data]]) flatMap (_.fold(
                               _.raiseError[F, Option[Data]],
                               _.some.point[F]))
      case other          => none.point[F]
    }
  }

  /** Decodes the given `Elem` as `Data` strictly: any untyped `Node`s
    * encountered results in an error.
    */
  def decodeXmlStrict[F[_]: MonadErrMsgs](elem: Elem): F[Option[Data]] =
    decodeXml(node => s"Unrecognized Data XML: $node.".wrapNel.raiseError[F, Data])(elem)

  ////

  private sealed abstract class DataType {
    override def toString = Show[DataType].shows(this)
  }

  private object DataType {
    case object Array     extends DataType
    case object Binary    extends DataType
    case object Boolean   extends DataType
    case object Date      extends DataType
    case object Decimal   extends DataType
    case object Id        extends DataType
    case object Integer   extends DataType
    case object Interval  extends DataType
    case object NA        extends DataType
    case object Null      extends DataType
    case object Object    extends DataType
    case object Set       extends DataType
    case object String    extends DataType
    case object Time      extends DataType
    case object Timestamp extends DataType

    val stringCodec = Prism.partial[String, DataType] {
      case "array"     => Array
      case "binary"    => Binary
      case "boolean"   => Boolean
      case "date"      => Date
      case "decimal"   => Decimal
      case "id"        => Id
      case "integer"   => Integer
      case "interval"  => Interval
      case "na"        => NA
      case "null"      => Null
      case "object"    => Object
      case "set"       => Set
      case "string"    => String
      case "time"      => Time
      case "timestamp" => Timestamp
    } {
      case Array       => "array"
      case Binary      => "binary"
      case Boolean     => "boolean"
      case Date        => "date"
      case Decimal     => "decimal"
      case Id          => "id"
      case Integer     => "integer"
      case Interval    => "interval"
      case NA          => "na"
      case Null        => "null"
      case Object      => "object"
      case Set         => "set"
      case String      => "string"
      case Time        => "time"
      case Timestamp   => "timestamp"
    }

    implicit val show: Show[DataType] =
      Show.shows(stringCodec(_))
  }

  private val DT = DataType

  private object DataNode {
    def unapply(node: Node): Option[(DataType, Seq[Node])] = {
      attribute(node, ejsonType) flatMap (DataType.stringCodec.getOption(_)) strengthR node.child
    }
  }

  private object EJsType {
    def unapply(jobj: JsonObject): Option[DataType] =
      jobj(EJsonTypeKey) >>= (_.string) >>= (DataType.stringCodec.getOption(_))
  }

  private object EJsValue {
    def unapply(jobj: JsonObject): Option[Json] =
      jobj(EJsonValueKey)
  }

  private object EJsonTyped {
    def unapply(jobj: JsonObject): Option[(DataType, Json)] =
      (EJsType.unapply(jobj) |@| EJsValue.unapply(jobj)).tupled
  }

  private object ElemName {
    def unapply(elem: Elem): Option[QName] =
      (Option(elem.prefix), elem.label) match {
        case (Some(prefix), label) =>
          (NCName.fromString(prefix) ⊛ NCName.fromString(label))((p, l) =>
            QName(NSPrefix(p).some, l)).toOption
        case (None, label) =>
          NCName.fromString(label).toOption.flatMap(QName.unprefixed(_).some)
      }
  }

  private val ejsBinding: NamespaceBinding =
    NamespaceBinding(ejsonNs.prefix.shows, ejsonNs.uri.shows, TopScope)

  private def attribute(elem: Node, qn: QName): Option[String] =
    elem.attributes collectFirst {
      case PrefixedAttribute(p, n, Seq(Text(label)), _) if s"$p:$n" === qn.shows => label
    }

  private def encodedQualifiedName(elem: Elem): Option[String] = {
    elem match {
      case ElemName(qn) if qn === ejsonEncodedName => attribute(elem, ejsonEncodedAttr)
      case _                                       => qualifiedName(elem).some
    }
  }
}
