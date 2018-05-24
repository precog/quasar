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

package quasar.physical.mongodb.selector

import slamdata.Predef._
import quasar._
import quasar.fp._
import quasar.javascript._
import quasar.physical.mongodb.{Bson, BsonField, BsonType}

import scala.Any

import matryoshka._
import scalaz._, Scalaz._

sealed abstract class Selector {
  def bson: Bson.Doc

  import Selector._

  // TODO: Replace this with fixplate!!!

  def mapUpFields(f0: PartialFunction[BsonField, BsonField]): Selector = {
    val f0l = f0.lift

    mapUp0(s => f0l(s).getOrElse(s))
  }

  private def mapUp0(f: BsonField => BsonField): Selector = mapUpFieldsM[Id](f(_).point[Id])

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def mapUpFieldsM[M[_]: Monad](f: BsonField => M[BsonField]): M[Selector] =
    this match {
      case Doc(pairs)       => pairs.toList.traverse { case (field, expr) => f(field).map(_ -> expr) }.map(ps => Doc(ps.toListMap))
      case And(left, right) => (left.mapUpFieldsM(f) |@| right.mapUpFieldsM(f))(And(_, _))
      case Or(left, right)  => (left.mapUpFieldsM(f) |@| right.mapUpFieldsM(f))(Or(_, _))
      case Nor(left, right) => (left.mapUpFieldsM(f) |@| right.mapUpFieldsM(f))(Nor(_, _))
      case Where(_)         => this.point[M] // FIXME: need to rename fields referenced in the JS (#383)
    }

  def negate: Selector = {
    def expr(x: Selector.CondExpr): Selector.CondExpr = x match {
      case Selector.CExpr(cond) => Selector.NotCExpr(cond)
      case Selector.NotCExpr(cond) => Selector.CExpr(cond)
    }

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def loop(s: Selector): Selector = s match {
      case Selector.Doc(pairs) =>
        pairs.toList
          .map { case (f, x) => Selector.Doc(ListMap(f -> expr(x))) }
          .reduceOption[Selector](Selector.Or(_, _))
          .getOrElse(Selector.Where(Js.Bool(false)))
      case Selector.And(l, r) => Selector.Or(loop(l), loop(r))
      case Selector.Or(l, r)  => Selector.Nor(l, r)
      case Selector.Nor(l, r) => Selector.Or(l, r)
      case Selector.Where(x)  => Selector.Where(Js.UnOp("!", x))
    }

    loop(this)
  }
}

object Selector {
  implicit def SelectorRenderTree[S <: Selector]: RenderTree[Selector] =
    new RenderTree[Selector] {
      val SelectorNodeType = List("Selector")

      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def render(sel: Selector) = sel match {
        case and: And     => NonTerminal("And" :: SelectorNodeType, None, and.flatten.map(render))
        case or: Or       => NonTerminal("Or" :: SelectorNodeType, None, or.flatten.map(render))
        case nor: Nor     => NonTerminal("Nor" :: SelectorNodeType, None, nor.flatten.map(render))
        case where: Where => Terminal("Where" :: SelectorNodeType, Some(where.bson.toJs.pprint(0)))
        case Doc(pairs)   => {
          val children = pairs.map {
            case (field, CExpr(expr)) =>
              Terminal("CExpr" :: SelectorNodeType, Some(field.asField + " -> " + expr.shows))
            case (field, NotCExpr(expr)) =>
              Terminal("NotCExpr" :: SelectorNodeType, Some(field.asField + " -> " + expr.shows))
          }
          NonTerminal("Doc" :: SelectorNodeType, None, children.toList)
        }
      }
    }

  sealed abstract class Condition {
    def bson: Bson
  }

  implicit val showCondition: Show[Condition] = Show.showFromToString

  private[Selector] abstract sealed class SimpleCondition(val op: String) extends Condition {
    protected def rhs: Bson

    def bson = Bson.Doc(ListMap(op -> rhs))
  }

  sealed trait Comparison extends Condition
  final case class Eq(bson: Bson) extends Condition with Comparison
  final case class Gt(rhs: Bson) extends SimpleCondition("$gt") with Comparison
  final case class Gte(rhs: Bson) extends SimpleCondition("$gte") with Comparison
  final case class In(rhs: Bson) extends SimpleCondition("$in") with Comparison
  final case class Lt(rhs: Bson) extends SimpleCondition("$lt") with Comparison
  final case class Lte(rhs: Bson) extends SimpleCondition("$lte") with Comparison
  final case class Neq(rhs: Bson) extends SimpleCondition("$ne") with Comparison
  final case class Nin(rhs: Bson) extends SimpleCondition("$nin") with Comparison

  sealed trait Element extends Condition

  final case class Exists(exists: Boolean) extends SimpleCondition("$exists") with Element {
    protected def rhs = Bson.Bool(exists)
  }

  final case class Type(bsonType: BsonType) extends SimpleCondition("$type") with Element {
    protected def rhs = Bson.Int32(bsonType.ordinal)
  }

  sealed trait Evaluation extends Condition

  final case class Mod(divisor: Int, remainder: Int) extends SimpleCondition("$mod") with Evaluation {
    protected def rhs = Bson.Arr(Bson.Int32(divisor) :: Bson.Int32(remainder) :: Nil)
  }

  final case class Regex(pattern: String, caseInsensitive: Boolean, multiLine: Boolean, extended: Boolean, dotAll: Boolean) extends Evaluation {
    def bson = {
      val options = (if (caseInsensitive) "i" else "") +
                    (if (multiLine)       "m" else "") +
                    (if (extended)        "x" else "") +
                    (if (dotAll)          "s" else "")
      Bson.Regex(pattern, options)
    }
  }

  // Note: $where can actually appear within a Doc (as in
  //     {foo: 1, $where: "this.bar < this.baz"}),
  // but the same thing can be accomplished with $and, so we always wrap $where
  // in its own Bson.Doc.
  final case class Where(code: Js.Expr) extends Selector {
    def bson =
      Bson.Doc(ListMap(
        "$where" -> Bson.JavaScript(Js.AnonFunDecl(Nil, List(Js.Return(code))))))
  }

  sealed trait Geospatial extends Condition

  final case class GeoWithin(geometry: String, coords: List[List[(Double, Double)]]) extends SimpleCondition("$geoWithin") with Geospatial {
    protected def rhs = Bson.Doc(ListMap(
      "$geometry" -> Bson.Doc(ListMap(
        "type"        -> Bson.Text(geometry),
        "coordinates" -> Bson.Arr(coords.map(v => Bson.Arr(v.map(t => Bson.Arr(Bson.Dec(t._1) :: Bson.Dec(t._2) :: Nil)))))))))
  }

  final case class GeoIntersects(geometry: String, coords: List[List[(Double, Double)]]) extends SimpleCondition("$geoIntersects") with Geospatial {
    protected def rhs = Bson.Doc(ListMap(
      "$geometry" -> Bson.Doc(ListMap(
        "type"        -> Bson.Text(geometry),
        "coordinates" -> Bson.Arr(coords.map(v => Bson.Arr(v.map(t => Bson.Arr(Bson.Dec(t._1) :: Bson.Dec(t._2) :: Nil)))))))))
  }

  final case class Near(lat: Double, long: Double, maxDistance: Double) extends SimpleCondition("$near") with Geospatial {
    protected def rhs = Bson.Doc(ListMap(
      "$geometry" -> Bson.Doc(ListMap(
        "type"        -> Bson.Text("Point"),
        "coordinates" -> Bson.Arr(Bson.Dec(long) :: Bson.Dec(lat) :: Nil)))))
  }

  final case class NearSphere(lat: Double, long: Double, maxDistance: Double) extends SimpleCondition("$nearSphere") with Geospatial {
    protected def rhs = Bson.Doc(ListMap(
      "$geometry" -> Bson.Doc(ListMap(
        "type"        -> Bson.Text("Point"),
        "coordinates" -> Bson.Arr(Bson.Dec(long) :: Bson.Dec(lat) :: Nil))),
      "$maxDistance" -> Bson.Dec(maxDistance)))
  }

  sealed trait Arr extends Condition

  final case class All(selectors: List[Selector]) extends SimpleCondition("$all") with Arr {
    protected def rhs = Bson.Arr(selectors.map(_.bson))
  }

  final case class ElemMatch(selector: Selector \/ SimpleCondition)
      extends SimpleCondition("$elemMatch") with Arr {
    protected def rhs = selector.fold(_.bson, _.bson)
  }

  final case class Size(size: Int) extends SimpleCondition("$size") with Arr {
    protected def rhs = Bson.Int32(size)
  }

  sealed abstract class CondExpr {
    def bson: Bson
  }

  implicit val showCondExpr: Show[CondExpr] = Show.showFromToString

  final case class CExpr(value: Condition) extends CondExpr {
    def bson = value.bson
  }

  final case class NotCExpr(value: Condition) extends CondExpr {
    def bson = value match {
      // NB: there is no $eq operator, and MongoDB does not allow $not around
      // a simple value, so this pattern _must_ be rewritten with $ne.
      case Eq(bson) => Neq(bson).bson
      case _        => Bson.Doc(ListMap("$not" -> value.bson))
    }
  }

  final case class Doc(pairs: ListMap[BsonField, CondExpr]) extends Selector {
    def bson = Bson.Doc(pairs.map { case (f, e) => f.asText -> e.bson })

    override def toString = {
      val children = pairs.map {
        case (field, expr) => field.shows + " -> " + expr.shows
      }
      "Selector.Doc(" + children.mkString(", ") + ")"
    }
  }

  object Doc {
    def apply(pairs: (BsonField, Condition)*): Doc =
      Doc(ListMap(pairs.map(t => t._1 -> CExpr(t._2)): _*))
  }

  sealed abstract class CompoundSelector extends Selector {
    protected def op: String
    def left: Selector
    def right: Selector

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def flatten: List[Selector] = {
      def loop(sel: Selector) = sel match {
        case sel: CompoundSelector if (this.op == sel.op) => sel.flatten
        case _ => sel :: Nil
      }
      loop(left) ++ loop(right)
    }
  }

  private[Selector] abstract sealed class Abstract(val op: String) extends CompoundSelector {
    def bson = Bson.Doc(ListMap(op -> Bson.Arr(flatten.map(_.bson))))
  }

  final case class And(left: Selector, right: Selector) extends Abstract("$and") {
    override def equals(that: Any) = that match {
      case that @ And(_, _) => flatten == that.flatten
      case _ => false
    }
    override def hashCode = flatten.hashCode
  }

  object And {
    def apply(first: Selector, rest: Selector*): Selector =
      rest.foldLeft(first)(And(_, _))
  }

  final case class Or(left: Selector, right: Selector) extends Abstract("$or") {
    override def equals(that: Any) = that match {
      case that @ Or(_, _) => flatten == that.flatten
      case _ => false
    }
    override def hashCode = flatten.hashCode
  }

  object Or {
    def apply(first: Selector, rest: Selector*): Selector =
      rest.foldLeft(first)(Or(_, _))
  }

  final case class Nor(left: Selector, right: Selector) extends Abstract("$nor") {
    override def equals(that: Any) = that match {
      case that @ Nor(_, _) => flatten == that.flatten
      case _ => false
    }
    override def hashCode = flatten.hashCode
  }

  object Nor {
    def apply(first: Selector, rest: Selector*): Selector =
      rest.foldLeft(first)(Nor(_, _))
  }

  implicit val andSemigroup: Semigroup[Selector] = new Semigroup[Selector] {
    def append(s1: Selector, s2: => Selector): Selector = {
      def overlapping[A](s1: Set[A], s2: Set[A]) = !(s1 & s2).isEmpty

      (s1, s2) match {
        case _ if (s1 == s2)  => s1
        case (Doc(pairs1), Doc(pairs2)) if (!overlapping(pairs1.keySet, pairs2.keySet))
                              => Doc(pairs1 ++ pairs2)
        case _                => And(s1, s2)
      }
    }
  }

  implicit val showSelector: Show[Selector] = Show.showFromToString
}
