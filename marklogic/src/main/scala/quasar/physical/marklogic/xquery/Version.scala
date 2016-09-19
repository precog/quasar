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

package quasar.physical.marklogic.xquery

import quasar.Predef._
import quasar.physical.marklogic.xquery.syntax._

import scalaz.{Show, Order}
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.std.option._

final case class Version(version: String, encoding: Option[String]) {
  def render: String = {
    val enc = encoding.map(e => s" ($e)")
    s"xquery version ${version.xs}${~enc}"
  }
}

object Version {
  val `1.0-ml` = Version("1.0-ml", None)

  implicit val order: Order[Version] =
    Order.orderBy(v => (v.version, v.encoding))

  implicit val show: Show[Version] =
    Show.showFromToString
}
