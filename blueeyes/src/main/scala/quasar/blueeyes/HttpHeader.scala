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

package quasar.blueeyes

import quasar.precog.ProductPrefixUnmangler

import scalaz.Scalaz._

// FIXME Just needed for tests. Move to test directory.
sealed trait HttpHeaderField[T <: HttpHeader] extends ProductPrefixUnmangler {
  lazy val name: String = unmangledName

  def parse(s: String): Option[T]
  def parse(t: (String, String)): Option[T] =
    if (t._1.equalsIgnoreCase(name)) parse(t._2) else None

  override def toString = name
}

object HttpHeaderField {
  import HttpHeaders._

  val All: List[HttpHeaderField[_ <: HttpHeader]] = List(
    `Content-Length`,
    `Cache-Control`,
    Trailer,
    `Transfer-Encoding`)

  val ByName: Map[String, HttpHeaderField[_]] = All.map(v => (v.name.toLowerCase -> v)).toMap

  def parseAll(inString: String, parseType: String): List[HttpHeaderField[_]] = {
    val fields = inString.toLowerCase.split("""\s*,\s*""").toList.flatMap(HttpHeaderField.ByName.get)

    if (parseType == "trailer") fields.filterNot {
      case Trailer => true
      case `Transfer-Encoding` => true
      case `Content-Length` => true
      case NullHeader => true
      case _ => false
    } else fields.filterNot {
      case NullHeader => true
      case _ => false
    }
  }
}

sealed trait HttpHeader extends ProductPrefixUnmangler {
  def name: String = unmangledName
  def value: String

  def header = name + ": " + value

  def canEqual(any: Any) = any match {
    case header: HttpHeader => true
    case _ => false
  }

  override def productPrefix = this.getClass.getSimpleName

  override def toString = header

  def tuple = (name, value)

  override def equals(any: Any) = any match {
    case that: HttpHeader => name.equalsIgnoreCase(that.name) && value.equalsIgnoreCase(that.value)
    case _ => false
  }
}

trait HttpHeaderImplicits {
  implicit def tuple2HttpHeader(t: (String, String)): HttpHeader = HttpHeader(t)
}

object HttpHeader extends HttpHeaderImplicits {
  private val All = Map(HttpHeaderField.All.map(field => (field.name.toLowerCase, field)): _*)
  def apply(t: (String, String)): HttpHeader = All.get(t._1.toLowerCase).flatMap(_.parse(t)).getOrElse(HttpHeaders.CustomHeader(t._1, t._2))
}

sealed trait HttpHeaderRequest extends HttpHeader
sealed trait HttpHeaderResponse extends HttpHeader

case class HttpHeaders private (val raw: Map[String, String]) {
  def + (kv: (String, String)): HttpHeaders = this + HttpHeader(kv)
  def + (header: HttpHeader): HttpHeaders = new HttpHeaders(raw + header.tuple)
  def ++ (that: HttpHeaders) = new HttpHeaders(raw ++ that.raw)
  def ++ (that: Iterable[(HttpHeader)]) = new HttpHeaders(raw ++ that.map(_.tuple))

  def - (key: String): HttpHeaders = new HttpHeaders(raw - key)

  def get(key: String): Option[String] = raw.get(key)

  def header[T <: HttpHeader](implicit field: HttpHeaderField[T]): Option[T] = {
    raw.get(field.name).flatMap(field.parse)
  }
}

trait HttpHeadersImplicits extends HttpHeaderImplicits {
  implicit def iterableOfTuple2ToHttpHeaders(i: Iterable[(String, String)]): HttpHeaders = HttpHeaders(i)
  implicit def iterableToHttpHeaders[A <: HttpHeader](i: Iterable[A]): HttpHeaders = HttpHeaders(i)
}

object HttpHeaders {
  def apply(headers: HttpHeader*): HttpHeaders = 
    apply(headers.map(_.tuple))
    
  def apply(i: Iterable[(String, String)]): HttpHeaders = 
    apply(i.map(HttpHeader(_)))

  def apply[A](i: Iterable[A])(implicit ev: A <:< HttpHeader): HttpHeaders = 
    new HttpHeaders(i.map(ev(_).tuple)(collection.breakOut))

  val Empty: HttpHeaders = new HttpHeaders(Map.empty[String, String])

  // Requests
  case class `Content-Length`(length: Long) extends HttpHeaderRequest with HttpHeaderResponse {
    def value = length.toString
  }
  implicit case object `Content-Length` extends HttpHeaderField[`Content-Length`] {
    override def parse(s: String) = s.parseLong.toOption.map(apply)
    def unapply(t: (String, String)) = parse(t).map(_.length)
  }

  // Responses

  case class `Cache-Control`(directives: CacheDirective*) extends HttpHeaderResponse {
    def value = directives.map(_.toString).mkString(", ")
  }
  implicit case object `Cache-Control` extends HttpHeaderField[`Cache-Control`] {
    override def parse(s: String) = Some(apply(CacheDirectives.parseCacheDirectives(s): _*))
    def unapply(t: (String, String)) = parse(t).map(_.directives)
  }

  /* Will take a while to implement */
  case class Trailer(fields: HttpHeaderField[_]*) extends HttpHeaderResponse {
    def value = fields.map(_.toString).mkString(", ")
  }
  implicit case object Trailer extends HttpHeaderField[Trailer] {
    override def parse(s: String) = Some(apply(HttpHeaderField.parseAll(s, "trailer"): _*))
    def unapply(t: (String, String)) = parse(t).map(_.fields)
  }

  case class `Transfer-Encoding`(encodings: Encoding*) extends HttpHeaderResponse {
    def value = encodings.map(_.toString).mkString(", ")
  }
  implicit case object `Transfer-Encoding` extends HttpHeaderField[`Transfer-Encoding`] {
    override def parse(s: String) = Some(apply(Encodings.parseEncodings(s): _*))
    def unapply(t: (String, String)) = parse(t).map(_.encodings)
  }

  case class CustomHeader(override val name: String, val value: String) extends HttpHeaderRequest with HttpHeaderResponse

  class NullHeader extends HttpHeader {
    def value = ""
  }
  case object NullHeader extends HttpHeaderField[NullHeader] {
    override def parse(s: String) = Some(new NullHeader)
  }
}
