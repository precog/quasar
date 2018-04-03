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

import slamdata.Predef.{Option, String}

import org.http4s.{Header, HeaderKey, ParseResult}
import org.http4s.util.CaseInsensitiveString
import scalaz.syntax.equal._
import scalaz.syntax.std.boolean._

object XFileName extends HeaderKey.Singleton {
  type HeaderT = Header

  val name = CaseInsensitiveString("X-File-Name")

  override def matchHeader(header: Header): Option[HeaderT] =
    (header.name ≟ name) option header

  override def parse(s: String): ParseResult[Header] =
    ParseResult.success(Header.Raw(name, s))
}
