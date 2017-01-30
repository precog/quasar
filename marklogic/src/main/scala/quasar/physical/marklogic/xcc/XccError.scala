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

package quasar.physical.marklogic.xcc

import quasar.Predef._

import com.marklogic.xcc.exceptions._
import monocle.{Getter, Prism}
import scalaz._
import scalaz.std.string._
import scalaz.syntax.std.option._

sealed abstract class XccError

object XccError {
  /** Indicates a problem with an XCC request. */
  final case class RequestError(cause: RequestException) extends XccError

  /** Indicates a problem (syntactic or semantic) with some XQuery submitted for evaluation. */
  final case class XQueryError(xqy: String, cause: XQueryException) extends XccError

  val cause: Getter[XccError, RequestException] =
    Getter {
      case RequestError(ex)   => ex
      case XQueryError(_, ex) => ex
    }

  val requestError = Prism.partial[XccError, RequestException] {
    case RequestError(ex) => ex
  } (RequestError)

  val xqueryError = Prism.partial[XccError, (String, XQueryException)] {
    case XQueryError(xqy, cause) => (xqy, cause)
  } (XQueryError.tupled)

  implicit val show: Show[XccError] = Show.shows {
    case RequestError(cause) =>
      cause.toString

    case XQueryError(xqy, cause) =>
      val lineNum = cause.getStack.headOption map (sf => s" on line ${sf.getLineNumber}")
      s"${cause.getFormatString}${~lineNum}\n---BEGIN QUERY---\n$xqy\n--- END QUERY ---"
  }
}
