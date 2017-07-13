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

package quasar.physical.marklogic.xcc

import slamdata.Predef._
import quasar.fp.ski.ι
import quasar.physical.marklogic.xquery.MainModule

import com.marklogic.xcc.exceptions._
import monocle.{Getter, Prism}
import scalaz._, Scalaz._

sealed abstract class XccError

object XccError {
  /** Indicates a problem with an XCC request. */
  final case class RequestError(cause: RequestException)
    extends XccError

  /** Indicates a problem (syntactic or semantic) with some XQuery submitted for
    * evaluation.
    */
  final case class XQueryError(
    module: MainModule,
    cause: RetryableXQueryException \/ XQueryException
  ) extends XccError

  object QueryError {
    def unapply(xerr: XccError): Option[(MainModule, QueryException)] =
      Functor[Option].compose[(MainModule, ?)]
        .map(xqueryError.getOption(xerr))(widenXQueryCause)
  }

  sealed abstract class Code

  object Code {
    case object DirExists extends Code

    val string = Prism.partial[String, Code] {
      case "XDMP-DIREXISTS" => DirExists
    } {
      case DirExists        => "XDMP-DIREXISTS"
    }

    implicit val equal: Equal[Code] =
      Equal.equalA

    implicit val show: Show[Code] =
      Show.shows(string(_))
  }

  val widenXQueryCause: RetryableXQueryException \/ XQueryException => QueryException =
    _.fold[QueryException](ι, ι)

  val cause: Getter[XccError, RequestException] =
    Getter {
      case RequestError(ex)  => ex
      case XQueryError(_, c) => widenXQueryCause(c)
    }

  val requestError = Prism.partial[XccError, RequestException] {
    case RequestError(ex) => ex
  } (RequestError)

  val xqueryError = Prism.partial[XccError, (MainModule, RetryableXQueryException \/ XQueryException)] {
    case XQueryError(xqy, cause) => (xqy, cause)
  } (XQueryError.tupled)

  implicit val show: Show[XccError] = Show.shows {
    case RequestError(cause) =>
      cause.toString

    case XQueryError(main, c) =>
      val error   = widenXQueryCause(c)
      val lineNum = error.getStack.headOption map (sf => s" on line ${sf.getLineNumber}")
      s"${error.getFormatString}${~lineNum}\n---BEGIN QUERY---\n${main.render}\n--- END QUERY ---"
  }
}
