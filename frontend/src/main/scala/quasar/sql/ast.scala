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

package quasar.sql

import quasar.Predef._

import scala.Any

import matryoshka._
import monocle.macros.Lenses
import scalaz._, Scalaz._
import pathy.Path._

trait IsDistinct
final case object SelectDistinct extends IsDistinct
final case object SelectAll extends IsDistinct

@Lenses final case class Proj[A](expr: A, alias: Option[String])

sealed trait Sql[A]
object Sql {
  implicit val traverse: Traverse[Sql] = new Traverse[Sql] {
    def traverseImpl[G[_], A, B](
      fa: Sql[A])(
      f: A => G[B])(
      implicit G: Applicative[G]):
        G[Sql[B]] = {
      def traverseCase(c: Case[A]): G[Case[B]] =
        (f(c.cond) ⊛ f(c.expr))(Case(_, _))

      fa match {
        case Select(dist, proj, rel, filter, group, order) =>
          (proj.traverse(p => f(p.expr).map(Proj(_, p.alias))) ⊛
            rel.traverse(traverseRelation(_, f)) ⊛
            filter.traverse(f) ⊛
            group.traverse(g =>
              (g.keys.traverse(f) ⊛ g.having.traverse(f))(GroupBy(_, _))) ⊛
            order.traverse(_.keys.traverse(_.traverse(f)).map(OrderBy(_))))(
            Select(dist, _, _, _, _, _))
        case Vari(symbol) => vari(symbol).point[G]
        case SetLiteral(exprs) => exprs.traverse(f).map(setLiteral(_))
        case ArrayLiteral(exprs) => exprs.traverse(f).map(arrayLiteral(_))
        case MapLiteral(exprs) =>
          exprs.traverse(_.bitraverse(f, f)).map(mapLiteral(_))
        case Splice(expr) => expr.traverse(f).map(splice(_))
        case Binop(lhs, rhs, op) => (f(lhs) ⊛ f(rhs))(binop(_, _, op))
        case Unop(expr, op) => f(expr).map(unop(_, op))
        case Ident(name) => G.point(ident(name))
        case InvokeFunction(name, args) =>
          args.traverse(f).map(invokeFunction(name, _))
        case Match(expr, cases, default) =>
          (f(expr) ⊛ cases.traverse(traverseCase) ⊛ default.traverse(f))(
            matc(_, _, _))
        case Switch(cases, default) =>
          (cases.traverse(traverseCase) ⊛ default.traverse(f))(
            switch(_, _))
        case Let(name, form, body) => (f(form) ⊛ f(body))(Let(name, _, _))
        case IntLiteral(v) => intLiteral(v).point[G]
        case FloatLiteral(v) => floatLiteral(v).point[G]
        case StringLiteral(v) => stringLiteral(v).point[G]
        case NullLiteral() => nullLiteral().point[G]
        case BoolLiteral(v) => boolLiteral(v).point[G]
      }
    }
  }

  implicit def toExprAlgebraOps[A](a: Algebra[Sql, A]): AlgebraOps[Sql, A] =
    toAlgebraOps[Sql, A](a)
}

@Lenses final case class Select[A] private[sql] (
  isDistinct:  IsDistinct,
  projections: List[Proj[A]],
  relations:   Option[SqlRelation[A]],
  filter:      Option[A],
  groupBy:     Option[GroupBy[A]],
  orderBy:     Option[OrderBy[A]])
    extends Sql[A]
@Lenses final case class Vari[A] private[sql] (symbol: String) extends Sql[A]
@Lenses final case class SetLiteral[A] private[sql] (exprs: List[A]) extends Sql[A]
@Lenses final case class ArrayLiteral[A] private[sql] (exprs: List[A]) extends Sql[A]
/** Can’t be a Map, because we need to arbitrarily transform the key */
@Lenses final case class MapLiteral[A] private[sql] (exprs: List[(A, A)]) extends Sql[A]
/** Represents the wildcard in a select projection
  * For instance:
  *  "select foo.* from example" => ...(Splice(Some(Ident("foo"))))...
  *  "select * from example"     => ...(Splice(None))...
  */
@Lenses final case class Splice[A] private[sql] (expr: Option[A]) extends Sql[A]
@Lenses final case class Binop[A] private[sql] (lhs: A, rhs: A, op: BinaryOperator)
    extends Sql[A]
@Lenses final case class Unop[A] private[sql] (expr: A, op: UnaryOperator) extends Sql[A]
@Lenses final case class Ident[A] private[sql] (name: String) extends Sql[A]
@Lenses final case class InvokeFunction[A] private[sql] (name: String, args: List[A])
    extends Sql[A]
@Lenses final case class Match[A] private[sql] (expr: A, cases: List[Case[A]], default: Option[A])
    extends Sql[A]
@Lenses final case class Switch[A] private[sql] (cases: List[Case[A]], default: Option[A])
    extends Sql[A]
@Lenses final case class Let[A](name: String, form: A, body: A) extends Sql[A]
@Lenses final case class IntLiteral[A] private[sql] (v: Long) extends Sql[A]
@Lenses final case class FloatLiteral[A] private[sql] (v: Double) extends Sql[A]
@Lenses final case class StringLiteral[A] private[sql] (v: String) extends Sql[A]
@Lenses final case class NullLiteral[A] private[sql] () extends Sql[A]
@Lenses final case class BoolLiteral[A] private[sql] (value: Boolean) extends Sql[A]

sealed abstract class BinaryOperator(val sql: String) {
  def apply[A](lhs: A, rhs: A): Sql[A] = binop(lhs, rhs, this)

  val name = "(" + sql + ")"

  override def equals(that: Any) = that match {
    case x: BinaryOperator => sql == x.sql
    case _                 => false
  }

  override def hashCode = sql.hashCode

  override def toString = sql
}

final case object IfUndefined  extends BinaryOperator("??")
final case object Range        extends BinaryOperator("..")
final case object Or           extends BinaryOperator("or")
final case object And          extends BinaryOperator("and")
final case object Eq           extends BinaryOperator("=")
final case object Neq          extends BinaryOperator("<>")
final case object Ge           extends BinaryOperator(">=")
final case object Gt           extends BinaryOperator(">")
final case object Le           extends BinaryOperator("<=")
final case object Lt           extends BinaryOperator("<")
final case object Concat       extends BinaryOperator("||")
final case object Plus         extends BinaryOperator("+")
final case object Minus        extends BinaryOperator("-")
final case object Mult         extends BinaryOperator("*")
final case object Div          extends BinaryOperator("/")
final case object Mod          extends BinaryOperator("%")
final case object Pow          extends BinaryOperator("^")
final case object In           extends BinaryOperator("in")
final case object FieldDeref   extends BinaryOperator("{}")
final case object IndexDeref   extends BinaryOperator("[]")
final case object Limit        extends BinaryOperator("limit")
final case object Offset       extends BinaryOperator("offset")
final case object Union        extends BinaryOperator("union")
final case object UnionAll     extends BinaryOperator("union all")
final case object Intersect    extends BinaryOperator("intersect")
final case object IntersectAll extends BinaryOperator("intersect all")
final case object Except       extends BinaryOperator("except")
final case object UnshiftMap   extends BinaryOperator("{...}")

sealed abstract class UnaryOperator(val sql: String) {
  def apply[A](expr: A): Sql[A] = unop(expr, this)

  val name = sql

  override def equals(that: Any) = that match {
    case x: UnaryOperator => sql == x.sql
    case _                => false
  }

  override def hashCode = sql.hashCode

  override def toString = sql
}

final case object Not                 extends UnaryOperator("not")
final case object Exists              extends UnaryOperator("exists")
final case object Positive            extends UnaryOperator("+")
final case object Negative            extends UnaryOperator("-")
final case object Distinct            extends UnaryOperator("distinct")
final case object FlattenMapKeys      extends UnaryOperator("{*:}")
final case object FlattenMapValues    extends UnaryOperator("flatten_map")
final case object ShiftMapKeys        extends UnaryOperator("{_:}")
final case object ShiftMapValues      extends UnaryOperator("shift_map")
final case object FlattenArrayIndices extends UnaryOperator("[*:]")
final case object FlattenArrayValues  extends UnaryOperator("flatten_array")
final case object ShiftArrayIndices   extends UnaryOperator("[_:]")
final case object ShiftArrayValues    extends UnaryOperator("shift_array")
final case object UnshiftArray        extends UnaryOperator("[...]")

@Lenses final case class Case[A](cond: A, expr: A)

sealed trait SqlRelation[A] {
  def namedRelations: Map[String, List[NamedRelation[A]]] = {
    def collect(n: SqlRelation[A]): List[(String, NamedRelation[A])] =
      n match {
        case JoinRelation(left, right, _, _) => collect(left) ++ collect(right)
        case t: NamedRelation[A] => (t.aliasName -> t) :: Nil
    }

    collect(this).groupBy(_._1).mapValues(_.map(_._2))
  }

  def mapPathsM[F[_]: Monad](f: FUPath => F[FUPath]): F[SqlRelation[A]] = this match {
    case IdentRelationAST(_, _) => this.point[F]
    case VariRelationAST(_, _) => this.point[F]
    case TableRelationAST(path, alias) => f(path).map(TableRelationAST(_, alias))
    case rel @ JoinRelation(left, right, _, _) =>
      (left.mapPathsM(f) |@| right.mapPathsM(f))((l,r) => rel.copy(left = l, right = r))
    case ExprRelationAST(_,_) => this.point[F]
  }

  def transformM[F[_]: Monad, B](f: SqlRelation[A] => F[SqlRelation[B]], g: A => F[B]): F[SqlRelation[B]] = this match {
    case JoinRelation(left, right, tpe, clause) =>
      (left.transformM[F, B](f, g) |@|
        right.transformM[F, B](f, g) |@|
        g(clause))((l,r,c) =>
          JoinRelation(l, r, tpe, c))
    case rel => f(rel)
  }
}

sealed trait NamedRelation[A] extends SqlRelation[A] {
  def aliasName: String
}

/**
 * IdentRelationAST allows us to reference a let binding in relation (i.e. table)
 * context. ExprF.IdentF allows us to reference a let binding in expression context.
 * Ideally we can unify these two contexts, providing a single way to reference a
 * let binding.
 */
@Lenses final case class IdentRelationAST[A](name: String, alias: Option[String])
    extends NamedRelation[A] {
  def aliasName = alias.getOrElse(name)
}

@Lenses final case class VariRelationAST[A](vari: Vari[A], alias: Option[String])
    extends NamedRelation[A] {
  def aliasName = alias.getOrElse(vari.symbol)
}


@Lenses final case class TableRelationAST[A](tablePath: FUPath, alias: Option[String])
    extends NamedRelation[A] {
  def aliasName = alias.getOrElse(fileName(tablePath).value)
}
@Lenses final case class ExprRelationAST[A](expr: A, aliasName: String)
    extends NamedRelation[A]

@Lenses final case class JoinRelation[A](left: SqlRelation[A], right: SqlRelation[A], tpe: JoinType, clause: A)
    extends SqlRelation[A]

sealed abstract class JoinType(val sql: String)
final case object LeftJoin extends JoinType("left join")
final case object RightJoin extends JoinType("right join")
final case object InnerJoin extends JoinType("inner join")
final case object FullJoin extends JoinType("full join")

sealed trait OrderType
final case object ASC extends OrderType
final case object DESC extends OrderType

@Lenses final case class GroupBy[A](keys: List[A], having: Option[A])

@Lenses final case class OrderBy[A](keys: List[(OrderType, A)])
