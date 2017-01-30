/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.Data
import quasar.physical.marklogic.MonadErrMsgs
import quasar.physical.marklogic.optics._
import quasar.physical.marklogic.xml
import quasar.physical.marklogic.xml.SecureXML

import scala.collection.JavaConverters._
import scala.xml.Elem

import argonaut._
import com.marklogic.xcc.types.{Duration => _, _}
import java.time._
import scalaz._, Scalaz._

object xdmitem {
  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  def toData[F[_]: MonadErrMsgs](xdm: XdmItem): F[Data] = xdm match {
    case item: CtsBox                   =>
      Data.singletonObj("cts:box", Data.Obj(ListMap(
        "east"  -> Data._str(item.getEast),
        "north" -> Data._str(item.getNorth),
        "south" -> Data._str(item.getSouth),
        "west"  -> Data._str(item.getWest)
      ))).point[F]

    case item: CtsCircle                =>
      toData[F](item.getCenter) map { center =>
        Data.singletonObj("cts:circle", Data.Obj(ListMap(
           "center" -> center,
           "radius" -> Data._str(item.getRadius)
         )))
      }

    case item: CtsPoint                 =>
      Data.singletonObj("cts:point", Data.Obj(ListMap(
        "latitude"  -> Data._str(item.getLatitude),
        "longitude" -> Data._str(item.getLongitude)
      ))).point[F]

    case item: CtsPolygon               =>
      item.getVertices.asScala.toList traverse toData[F] map { verts =>
        Data.singletonObj("cts:polygon", Data.singletonObj("vertices", Data.Arr(verts)))
      }

    case item: JSArray                  => jsonToData[F](item.asString)
    case item: JSObject                 => jsonToData[F](item.asString)
    case item: JsonItem                 => jsonToData[F](item.asString)

    case item: XdmAttribute             =>
      val attr = item.asW3cAttr
      Data.singletonObj(attr.getName, Data._str(attr.getValue)).point[F]

    // TODO: Inefficient for large data as it must be buffered into memory
    case item: XdmBinary                => bytesToData[F](item.asBinaryData)
    case item: XdmComment               => Data.singletonObj("xdm:comment"  , Data._str(item.asString)).point[F]
    case item: XdmDocument              => xmlToData[F](item.asString)
    case item: XdmElement               => xmlToData[F](item.asString)
    case item: XdmProcessingInstruction => Data.singletonObj("xdm:processingInstruction", Data._str(item.asString)).point[F]
    case item: XdmText                  => Data._str(item.asString).point[F]
    case item: XSAnyURI                 => Data._str(item.asString).point[F]
    case item: XSBase64Binary           => bytesToData[F](item.asBinaryData)
    case item: XSBoolean                => Data._bool(item.asPrimitiveBoolean).point[F]
    case item: XSDate                   => parseLocalDate[F](item.asString) map (Data._date(_))
    case item: XSDateTime               => Data._timestamp(ZonedDateTime.parse(item.asString).toInstant).point[F]
    case item: XSDecimal                => Data._dec(item.asBigDecimal).point[F]
    case item: XSDouble                 => Data._dec(item.asBigDecimal).point[F]
    case item: XSDuration               => Data._interval(convertDuration(item)).point[F]
    case item: XSFloat                  => Data._dec(item.asBigDecimal).point[F]
    case item: XSGDay                   => Data.singletonObj("xs:gDay"      , Data._str(item.asString)).point[F]
    case item: XSGMonth                 => Data.singletonObj("xs:gMonth"    , Data._str(item.asString)).point[F]
    case item: XSGMonthDay              => Data.singletonObj("xs:gMonthDay" , Data._str(item.asString)).point[F]
    case item: XSGYear                  => Data.singletonObj("xs:gYear"     , Data._str(item.asString)).point[F]
    case item: XSGYearMonth             => Data.singletonObj("xs:gYearMonth", Data._str(item.asString)).point[F]
    case item: XSHexBinary              => bytesToData[F](item.asBinaryData)
    case item: XSInteger                => Data._int(item.asBigInteger).point[F]
    case item: XSQName                  => Data._str(item.asString).point[F]
    case item: XSString                 => Data._str(item.asString).point[F]
    case item: XSTime                   => parseLocalTime[F](item.asString) map (Data._time(_))
    case item: XSUntypedAtomic          => Data._str(item.asString).point[F]
    case other                          => noReprError[F, Data](other.toString)
  }

  ////

  private def bytesToData[F[_]: Applicative](bytes: Array[Byte]): F[Data] =
    Data._binary(ImmutableArray.fromArray(bytes)).point[F]

  private def jsonToData[F[_]: MonadErrMsgs](jsonString: String): F[Data] =
    Parse.decodeWithMessage(jsonString, data.decodeJson[F], _.wrapNel.raiseError[F, Data])

  private def convertDuration(xsd: XSDuration): Duration = {
    val xdd   = xsd.asDuration
    val days  = (xdd.getYears * 365L) + (xdd.getMonths * 30) + xdd.getDays
    val rsecs = BigDecimal(xdd.getSeconds)
    val secs  = rsecs.toLong
    val nanos = (rsecs - secs) / 1000000000
    val dur   = Duration.ofDays(days)
                  .plusHours(xdd.getHours.toLong)
                  .plusMinutes(xdd.getMinutes.toLong)
                  .plusSeconds(secs)
                  .plusNanos(nanos.toLong)

    if (xdd.isPositive) dur else dur.negated
  }

  private def parseLocalDate[F[_]: MonadErrMsgs](s: String): F[LocalDate] =
    parseTemporal[F, LocalDate]("LocalDate", isoLocalDate.getOption)(s)

  private def parseLocalTime[F[_]: MonadErrMsgs](s: String): F[LocalTime] =
    parseTemporal[F, LocalTime]("LocalTime", isoLocalTime.getOption)(s)

  private def parseTemporal[F[_]: MonadErrMsgs, A](name: String, parse: String => Option[A])(str: String): F[A] =
    parse(str).fold(s"Failed to parse '$str' as a $name.".wrapNel.raiseError[F, A])(_.point[F])

  private def xmlToData[F[_]: MonadErrMsgs](xmlString: String): F[Data] = {
    val elem = SecureXML.loadString(xmlString).fold(
                 _.toString.wrapNel.raiseError[F, Elem],
                 _.point[F])

    def singletonValue(d: Data): Data =
      Data._obj.getOption(d) map (_.toList) collect {
        case (_, value) :: Nil => value
      } getOrElse d

    elem flatMap (e => OptionT(data.decodeXml[F]({
      case n: Elem => singletonValue(xml.toEJsonData(n)).point[F]
      case other   => noReprError[F, Data](other.toString)
    })(e)) getOrElse xml.toEJsonData(e))
  }

  private def noReprError[F[_]: MonadErrMsgs, A](form: String): F[A] =
    s"No Data representation for '$form'.".wrapNel.raiseError[F, A]
}
