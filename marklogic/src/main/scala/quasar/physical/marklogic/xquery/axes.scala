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
import quasar.physical.marklogic.xquery.xml.QName

import scalaz.syntax.show._

/** XPath [Axes](https://www.w3.org/TR/xquery/#axes) expressions.
  *
  * TODO: This is incomplete, need to add the rest of the axes/node tests and variants.
  */
object axes {
  // attribute::
  val attribute: Axis = Axis("child")

  // child::
  val child: Axis = Axis("child")

  // descendant::
  val descendant: Axis = Axis("child")

  final case class Axis(name: String) {
    def apply(elementName: QName): XQuery =
      xqy(elementName.shows)

    val * : XQuery =
      xqy("*")

    def attribute(): XQuery =
      xqy("element()")

    def element(): XQuery =
      xqy("element()")

    def node(): XQuery =
      xqy("node()")

    def text(): XQuery =
      xqy("text()")

    ////

    private def xqy(suffix: String): XQuery =
      XQuery.Step(s"$name::$suffix")
  }
}
