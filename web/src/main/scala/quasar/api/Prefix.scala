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

package quasar.api

import slamdata.Predef._

import monocle.Lens
import monocle.macros.GenLens
import org.http4s.{HttpService, Pass, Request, Service, Uri}
import scalaz.std.option._
import scalaz.syntax.std.boolean._

object Prefix {
  def apply(prefix: String)(service: HttpService): HttpService = {
    val stripChars = prefix match {
      case "/"                    => 0
      case x if x.startsWith("/") => x.length
      case x                      => x.length + 1
    }

    def rewrite(path: String): Option[String] =
      path.startsWith(prefix) option path.substring(stripChars)

    Service.lift { req: Request =>
      _uri_path.modifyF(rewrite)(req) match {
        case Some(req1) => service(req1)
        case None       => Pass.now
      }
    }
  }

  private val uriLens = Lens[Request, Uri](_.uri)(uri => req => req.withUri(uri))
  private val _uri_path = uriLens composeLens GenLens[Uri](_.path)
}
