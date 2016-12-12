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

package quasar.physical.marklogic

import quasar.Predef._
import quasar.Data

import scala.collection.immutable.Seq
import scala.xml._

import scalaz.{Node => _, _}, Scalaz._

package object xml {

  final case class KeywordConfig(attributesKeyName: String, textKeyName: String)

  object KeywordConfig {
    val ejsonCompliant =
      KeywordConfig(
        attributesKeyName = "_xml.attributes",
        textKeyName       = "_xml.text")
  }

  import Data._

  /** Example
    *
    * <foo type="baz" id="1">
    *   <bar>
    *     <baz>37</baz>
    *     <bat>one</bat>
    *     <bat>two</bat>
    *   </bar>
    *   <quux>lorem ipsum</quux>
    * </foo>
    *
    * {
    *   "foo": {
    *     "_attributes": {
    *       "type": "baz",
    *       "id": "1"
    *     },
    *     "bar": {
    *       "baz": "37",
    *       "bat": ["one", "two"]
    *     },
    *     "quux": "lorem ipsum"
    *   }
    * }
    */
  def toData(elem: Elem, config: KeywordConfig): Data = {
    def impl(nodes: Seq[Node], m: Option[MetaData]): Data = nodes match {
      case Seq() =>
        m.cata(attrsAndText(_,  ""), _str(""))
      case LeafText(txt) =>
        m.cata(attrsAndText(_, txt), _str(txt))
      case xs =>
        val childrenByName = elements(xs) groupBy qualifiedName
        val childrenData = childrenByName.mapValues {
          case Seq(single) => impl(single.child, single.attributes.some)
          case xs          => Arr(xs.map(x => impl(x.child, x.attributes.some)).toList)
        }
        val attributeData = m.flatMap(attrToData).strengthL(config.attributesKeyName)
        Obj(ListMap((attributeData.toList ++ childrenData): _*))
    }

    def attrToData(meta: MetaData): Option[Data] = meta match {
      case scala.xml.Null => none
      case m              => some(Obj(meta.map(m => m.key -> impl(m.value, none)).toSeq: _*))
    }

    def attrsAndText(attrs: MetaData, txt: String): Data =
      attrToData(attrs).fold(_str(txt))(d => _obj(ListMap(
        config.attributesKeyName -> d,
        config.textKeyName       -> _str(txt))))

    Obj(ListMap(qualifiedName(elem) -> impl(elem.child, elem.attributes.some)))
  }

  /** Converts the given element to `Data` using EJson-compliant synthetic keys. */
  def toEJsonData(elem: Elem): Data =
    toData(elem, KeywordConfig.ejsonCompliant)

  def elements(nodes: Seq[Node]): Seq[Elem] =
    nodes.collect { case e: Elem => e }

  def qualifiedName(elem: Elem): String =
    Option(elem.prefix).fold("")(_ + ":") + elem.label

  /** Extract the child sequence from a node. */
  object Children {
    def unapply(node: Node): Option[Seq[Node]] =
      some(node.child)
  }

  /** Matches a sequence devoid of `Elem` nodes. */
  object Leaf {
    def unapply(nodes: Seq[Node]): Option[Seq[Node]] =
      nodes.forall({
        case _: Elem => false
        case _       => true
      }) option nodes
  }

  /** Extracts all of the text from a leaf sequence. */
  object LeafText {
    def unapply(nodes: Seq[Node]): Option[String] =
      Leaf.unapply(nodes) map (_.collect({
        case Text(s)   => s
        case PCData(s) => s
      }).mkString)
  }
}
