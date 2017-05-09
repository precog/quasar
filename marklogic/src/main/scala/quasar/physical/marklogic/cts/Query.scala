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

package quasar.physical.marklogic.cts

import slamdata.Predef._

import quasar.{NonTerminal, Terminal, RenderTree}, RenderTree.ops._

import eu.timepit.refined.scalaz._
import matryoshka._
import scalaz._, Scalaz._
import xml.name._

/** cts:query AST
  *
  * NB: Currently can only support literal conditions.
  *
  * @tparam V the type of literal values
  * @tparam A recursive position
  */
sealed abstract class Query[V, A]

object Query extends QueryInstances {
  final case class AndNot[V, A](positive: A, negative: A) extends Query[V, A]

  // TODO: Options
  final case class And[V, A](queries: IList[A]) extends Query[V, A]

  final case class Collection[V, A](uris: IList[Uri]) extends Query[V, A]

  final case class Directory[V, A](uris: IList[Uri], depth: MatchDepth) extends Query[V, A]

  final case class DocumentFragment[V, A](query: A) extends Query[V, A]

  final case class Document[V, A](uris: IList[Uri]) extends Query[V, A]

  // TODO: Options
  final case class ElementAttributeRange[V, A](
    elements: IList[QName],
    attributes: IList[QName],
    op: ComparisonOp,
    values: IList[V]
  ) extends Query[V, A]

  // TODO: Options
  final case class ElementAttributeValue[V, A](
    elements: IList[QName],
    attributes: IList[QName],
    values: IList[String]
  ) extends Query[V, A]

  // TODO: Options
  final case class ElementAttributeWord[V, A](
    elements: IList[QName],
    attributes: IList[QName],
    words: IList[String]
  ) extends Query[V, A]

  final case class Element[V, A](
    elements: IList[QName],
    query: A
  ) extends Query[V, A]

  // TODO: Options
  final case class ElementRange[V, A](
    elements: IList[QName],
    op: ComparisonOp,
    values: IList[V]
  ) extends Query[V, A]

  // TODO: Options
  final case class ElementValue[V, A](
    elements: IList[QName],
    values: IList[String]
  ) extends Query[V, A]

  // TODO: Options
  final case class ElementWord[V, A](
    elements: IList[QName],
    words: IList[String]
  ) extends Query[V, A]

  final case class False[V, A]() extends Query[V, A]

  // TODO: Field queries?

  // TODO: Options
  final case class JsonPropertyRange[V, A](
    properties: IList[String],
    op: ComparisonOp,
    values: IList[V]
  ) extends Query[V, A]

  final case class JsonPropertyScope[V, A](
    properties: IList[String],
    query: A
  ) extends Query[V, A]

  // TODO: Options
  final case class JsonPropertyValue[V, A](
    properties: IList[String],
    values: IList[V]
  ) extends Query[V, A]

  // TODO: Options
  final case class JsonPropertyWord[V, A](
    properties: IList[String],
    words: IList[String]
  ) extends Query[V, A]

  // TODO: Options
  // NB: http://blog.davidcassel.net/2012/08/a-trick-with-ctsnear-query/
  final case class Near[V, A](queries: IList[A], weight: Double) extends Query[V, A]

  final case class Not[V, A](query: A) extends Query[V, A]

  // TODO: Options
  final case class Or[V, A](queries: IList[A]) extends Query[V, A]

  // TODO: Options
  final case class PathRange[V, A](
    paths: IList[String],
    op: ComparisonOp,
    values: IList[V]
  ) extends Query[V, A]

  final case class True[V, A]() extends Query[V, A]

  // TODO: Options
  final case class Word[V, A](words: IList[String]) extends Query[V, A]
}

sealed abstract class QueryInstances {
  import Query._

  def traverse[V]: Traverse[Query[V, ?]] =
    new Traverse[Query[V, ?]] {
      def traverseImpl[F[_]: Applicative, A, B](qa: Query[V, A])(f: A => F[B]): F[Query[V, B]] = {
        def F(qb: Query[V, B]): F[Query[V, B]] = qb.point[F]

        qa match {
          case AndNot(positive, negative) => (f(positive) |@| f(negative))(AndNot(_, _))
          case And(queries) => (queries traverse f) map (And(_))
          case Collection(uris) => F(Collection(uris))
          case Directory(uris, depth) => F(Directory(uris, depth))
          case DocumentFragment(query) => f(query) map (DocumentFragment(_))
          case Document(uris) => F(Document(uris))
          case ElementAttributeRange(elements, attributes, op, values) => F(ElementAttributeRange(elements, attributes, op, values))
          case ElementAttributeValue(elements, attributes, values) => F(ElementAttributeValue(elements, attributes, values))
          case ElementAttributeWord(elements, attributes, words) => F(ElementAttributeWord(elements, attributes, words))
          case Element(elements, query) => f(query) map (Element(elements, _))
          case ElementRange(elements, op, values) => F(ElementRange(elements, op, values))
          case ElementValue(elements, values) => F(ElementValue(elements, values))
          case ElementWord(elements, words) => F(ElementWord(elements, words))
          case False() => F(False())
          case JsonPropertyRange(properties, op, values) => F(JsonPropertyRange(properties, op, values))
          case JsonPropertyScope(properties, query) => f(query) map (JsonPropertyScope(properties, _))
          case JsonPropertyValue(properties, values) => F(JsonPropertyValue(properties, values))
          case JsonPropertyWord(properties, words) => F(JsonPropertyWord(properties, words))
          case Near(queries, weight) => (queries traverse f) map (Near(_, weight))
          case Not(query) => f(query) map (Not(_))
          case Or(queries) => (queries traverse f) map (Or(_))
          case PathRange(paths, op, values) => F(PathRange(paths, op, values))
          case True() => F(True())
          case Word(words) => F(Word(words))
        }
      }
    }

  implicit def equal[V: Equal]: Delay[Equal, Query[V, ?]] =
    new Delay[Equal, Query[V, ?]] {
      def apply[A](eql: Equal[A]): Equal[Query[V, A]] = {
        implicit val eqlA = eql

        Equal.equal {
          case (AndNot(p1, n1), AndNot(p2, n2)) =>
            p1 === p2 && n1 === n2
          case (And(q1), And(q2)) =>
            q1 === q2
          case (Collection(u1), Collection(u2)) =>
            u1 === u2
          case (Directory(u1, d1), Directory(u2, d2)) =>
            u1 === u2 && d1 === d2
          case (DocumentFragment(q1), DocumentFragment(q2)) =>
            q1 === q2
          case (Document(u1), Document(u2)) =>
            u1 === u2
          case (ElementAttributeRange(e1, a1, o1, v1), ElementAttributeRange(e2, a2, o2, v2)) =>
            e1 === e2 && a1 === a2 && o1 === o2 && v1 === v2
          case (ElementAttributeValue(e1, a1, v1), ElementAttributeValue(e2, a2, v2)) =>
            e1 === e2 && a1 === a2 && v1 === v2
          case (ElementAttributeWord(e1, a1, w1), ElementAttributeWord(e2, a2, w2)) =>
            e1 === e2 && a1 === a2 && w1 === w2
          case (Element(e1, q1), Element(e2, q2)) =>
            e1 === e2 && q1 === q2
          case (ElementRange(e1, o1, v1), ElementRange(e2, o2, v2)) =>
            e1 === e2 && o1 === o2 && v1 === v2
          case (ElementValue(e1, v1), ElementValue(e2, v2)) =>
            e1 === e2 && v1 === v2
          case (ElementWord(e1, w1), ElementWord(e2, w2)) =>
            e1 === e2 && w1 === w2
          case (False(), False()) =>
            true
          case (JsonPropertyRange(p1, o1, v1), JsonPropertyRange(p2, o2, v2)) =>
            p1 === p2 && o1 === o2 && v1 === v2
          case (JsonPropertyScope(p1, q1), JsonPropertyScope(p2, q2)) =>
            p1 === p2 && q1 === q2
          case (JsonPropertyValue(p1, v1), JsonPropertyValue(p2, v2)) =>
            p1 === p2 && v1 === v2
          case (JsonPropertyWord(p1, w1), JsonPropertyWord(p2, w2)) =>
            p1 === p2 && w1 === w2
          case (Near(q1, w1), Near(q2, w2)) =>
            q1 === q2 && w1 === w2
          case (Not(q1), Not(q2)) =>
            q1 === q2
          case (Or(q1), Or(q2)) =>
            q1 === q2
          case (PathRange(p1, o1, v1), PathRange(p2, o2, v2)) =>
            p1 === p2 && o1 === o2 && v1 === v2
          case (True(), True()) =>
            true
          case (Word(w1), Word(w2)) =>
            w1 === w2

          case (_, _) =>
            false
        }
      }
    }

    def renderTree[V: RenderTree]: Delay[RenderTree, Query[V, ?]] =
      new Delay[RenderTree, Query[V, ?]] {
        def apply[A](rt: RenderTree[A]) = {
          implicit val rta = rt

          val nt = List("cts:query")

          def leaf(t: String, l: Option[String]) =
            Terminal(t :: nt, l)

          def node[X: RenderTree](t: String, xs: X*) =
            NonTerminal(t :: nt, none, xs.map(_.render).toList)

          def nodeL[X: RenderTree](t: String, xs: IList[X]) =
            node(t, xs.toList: _*)

          def us(xs: IList[Uri]) = nodeL("Uris", xs)
          def es(xs: IList[QName]) = nodeL("Elements", xs)
          def as(xs: IList[QName]) = nodeL("Attributes", xs)
          def vs(xs: IList[V]) = nodeL("Values", xs)
          def ws(xs: IList[String]) = nodeL("Words", xs)
          def ps(xs: IList[String]) = nodeL("Properties", xs)

          RenderTree.make[Query[V, A]] {
            case AndNot(positive, negative) =>
              node("cts:and-not-query", positive, negative)
            case And(queries) =>
              nodeL("cts:and-query", queries)
            case Collection(uris) =>
              node("cts:collection-query", us(uris))
            case Directory(uris, depth) =>
              node("cts:directory-query", us(uris), depth.render)
            case DocumentFragment(query) =>
              node("cts:document-fragment-query", query)
            case Document(uris) =>
              node("cts:document-query", us(uris))
            case ElementAttributeRange(elements, attributes, op, values) =>
              node(
                "cts:element-attribute-range-query",
                es(elements),
                as(attributes),
                op.render,
                vs(values))
            case ElementAttributeValue(elements, attributes, values) =>
              node(
                "cts:element-attribute-value-query",
                es(elements),
                as(attributes),
                nodeL("Values", values))
            case ElementAttributeWord(elements, attributes, words) =>
              node(
                "cts:element-attribute-word-query",
                es(elements),
                as(attributes),
                ws(words))
            case Element(elements, query) =>
              node("cts:element-query", es(elements), query.render)
            case ElementRange(elements, op, values) =>
              node(
                "cts:element-range-query",
                es(elements),
                op.render,
                vs(values))
            case ElementValue(elements, values) =>
              node(
                "cts:element-value-query",
                es(elements),
                nodeL("Values", values))
            case ElementWord(elements, words) =>
              node(
                "cts:element-word-query",
                es(elements),
                ws(words))
            case False() =>
              leaf("cts:false-query", none)
            case JsonPropertyRange(properties, op, values) =>
              node(
                "cts:json-property-range-query",
                ps(properties),
                op.render,
                vs(values))
            case JsonPropertyScope(properties, query) =>
              node(
                "cts:json-property-scope-query",
                ps(properties),
                query.render)
            case JsonPropertyValue(properties, values) =>
              node(
                "cts:json-property-value-query",
                ps(properties),
                vs(values))
            case JsonPropertyWord(properties, words) =>
              node(
                "cts:json-property-word-query",
                ps(properties),
                ws(words))
            case Near(queries, weight) =>
              nodeL(
                "cts:near-query",
                queries.map(_.render) ::: IList(leaf("Weight", weight.shows.some)))
            case Not(query) =>
              node("cts:not-query", query)
            case Or(queries) =>
              nodeL("cts:or-query", queries)
            case PathRange(paths, op, values) =>
              node(
                "cts:path-range-query",
                nodeL("Paths", paths),
                op.render,
                vs(values))
            case True() =>
              leaf("cts:true-query", none)
            case Word(words) =>
              node("cts:word-query", ws(words))
          }
        }
      }
}
