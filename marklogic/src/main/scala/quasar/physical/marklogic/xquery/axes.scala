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

package quasar.physical.marklogic.xquery

import slamdata.Predef._
import quasar.physical.marklogic.xquery.syntax._

import scalaz.syntax.show._
import xml.name._

/** XPath [Axes](https://www.w3.org/TR/xquery/#axes) expressions.
  *
  * TODO: This is incomplete, need to add the rest of the axes/node tests and variants.
  */
object axes {
  // attribute::
  val attribute: Axis = Axis("attribute")

  // child::
  val child: Axis = Axis("child")

  // descendant::
  val descendant: Axis = Axis("descendant")

  final case class Axis(name: String) {
    def apply(name: QName): XQuery =
      xqy(name.shows)

    val * : XQuery =
      xqy("*")

    def attribute(): XQuery =
      xqy("attribute()")

    def attributeNamed(name: String): XQuery =
      xqy(s"attribute(${name})")

    def element(): XQuery =
      xqy("element()")

    def elementNamed(name: String): XQuery =
      xqy(s"element(${name})")

    def node(): XQuery =
      xqy("node()")

    def nodeNamed(name: String): XQuery =
      xqy(s"node(${name.xs})")

    def text(): XQuery =
      xqy("text()")

    ////

    private def xqy(suffix: String): XQuery =
      XQuery.Step(s"$name::$suffix")
  }
}
