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

import scala.util.parsing.combinator._
import scala.util.parsing.input._

sealed trait CacheDirective extends ProductPrefixUnmangler {

  def name: String = unmangledName
  def delta: Option[HttpNumber] = None
  def fieldNames: Option[String] = None
  def value = (List(name) ++ delta.toList.map(_.value) ++ fieldNames.toList).mkString("=")
  override def toString = value;
}

object CacheDirectives extends RegexParsers with HttpNumberImplicits{

  private def literalValueParser = regex("\"([a-zA-Z-])+\"".r)
  private def digitalValueParser = regex("""[\d]+""".r)

  private def elementParser: Parser[Option[CacheDirective]] =
  (
    "private="          ~> literalValueParser ^^ {case v => `private`(Some(v))} |
    "no-cache="         ~> literalValueParser ^^ {case v => `no-cache`(Some(v))} |
    "private"                                 ^^^ `private`(None) |
    "no-cache"                                ^^^ `no-cache` |
    "no-store"                                ^^^ `no-store` |
    "no-transform"                            ^^^ `no-transform` |
    "only-if-cached"                          ^^^ `only-if-cached` |
    "public"                                  ^^^ `public` |
    "must-revalidate"                         ^^^ `must-revalidate` |
    "proxy-revalidate"                        ^^^ `proxy-revalidate` |
    "max-age="          ~> digitalValueParser ^^ {case v => `max-age`(Some(v.toLong))} |
    "max-stale="        ~> digitalValueParser ^^ {case v => `max-stale`(Some(v.toLong))} |
    "min-fresh="        ~> digitalValueParser ^^ {case v => `min-fresh`(Some(v.toLong))} |
    "s-maxage="         ~> digitalValueParser ^^ {case v => `s-maxage`(Some(v.toLong))}
  )?

  private def parser = repsep(elementParser, regex("""[ ]*,[ ]*""".r)) ^^ { _.flatten }

  def parseCacheDirectives(inString: String) = parser(new CharSequenceReader(inString)) match {
    case Success(result, _) => result

    case Failure(msg, _) => sys.error("The CacheDirectives " + inString + " has a syntax error: " + msg)

    case Error(msg, _) => sys.error("There was an error parsing \"" + inString + "\": " + msg)
  }

  sealed abstract class RequestDirective extends CacheDirective 

  sealed abstract class ResponseDirective extends CacheDirective 

  /* Requests */

  case object `no-cache` extends RequestDirective 
  case object `no-store` extends RequestDirective 
  case class `max-age`(inDelta: Option[HttpNumber]) extends RequestDirective {
    override def delta = inDelta
  }
  case class `max-stale`(inDelta: Option[HttpNumber]) extends RequestDirective {
    override def delta = inDelta
  }
  case class `min-fresh`(inDelta: Option[HttpNumber]) extends RequestDirective {
    override def delta = inDelta
  }
  case object `no-transform` extends RequestDirective 
  case object `only-if-cached` extends RequestDirective
  case class CustomRequestDirective(inName: String)  extends RequestDirective {
    override def name = inName
  }

  /* Responses */

  case object `public` extends ResponseDirective   
  case class `private` (inFieldNames: Option[String]) extends ResponseDirective {
    override def fieldNames = inFieldNames
  }
  case class `no-cache`(inFieldNames: Option[String]) extends ResponseDirective {
    override def fieldNames = inFieldNames
  }
  //case object `no-store` extends ResponseDirective       //Probably should use some implicits here 
  //case object `no-transform` extends ResponseDirective  
  case object `must-revalidate` extends ResponseDirective  
  case object `proxy-revalidate` extends ResponseDirective
  //case class `max-age`(delta: Option[HttpNumber])  extends ResponseDirective  with NoFieldName 
  case class `s-maxage`(inDelta: Option[HttpNumber])  extends ResponseDirective { 
    override def delta = inDelta
  }

  case class CustomResponseDirective(inName: String) extends ResponseDirective {
    override def name = inName
  }

  case class NullDirective(inName: String) extends CacheDirective {
    override def name = inName
    override def value = ""
  }
}
