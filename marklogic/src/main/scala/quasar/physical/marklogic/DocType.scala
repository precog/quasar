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

package quasar.physical.marklogic

import slamdata.Predef._

import monocle.Prism
import scalaz._

sealed abstract class DocType {
  def fold[A](json: => A, xml: => A): A =
    this match {
      case DocType.JsonDoc => json
      case DocType.XmlDoc  => xml
    }
}

object DocType {
  case object JsonDoc extends DocType
  case object XmlDoc  extends DocType

  type Json = JsonDoc.type
  type Xml  = XmlDoc.type

  val json: DocType = JsonDoc
  val xml:  DocType = XmlDoc

  val name: Prism[String, DocType] =
    Prism.partial[String, DocType] {
      case "json"  => json
      case "xml"   => xml
    } {
      case JsonDoc => "json"
      case XmlDoc  => "xml"
    }

  implicit val equal: Equal[DocType] =
    Equal.equalA

  implicit val show: Show[DocType] =
    Show.showFromToString
}
