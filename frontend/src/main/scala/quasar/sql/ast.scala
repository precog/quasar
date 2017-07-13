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

package quasar.sql

import slamdata.Predef._
import quasar._, RenderTree.ops._
import quasar.fp._
import quasar.fp.ski._

import matryoshka._
import matryoshka.implicits._
import monocle.macros.Lenses
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.Liskov._

sealed abstract class Sql[A]
object Sql {

  implicit val equal: Delay[Equal, Sql] =
    new Delay[Equal, Sql] {
      def apply[A](fa: Equal[A]) = {
        implicit val eqA: Equal[A] = fa
        Equal.equal {
          case (Select(d1, p1, r1, f1, g1, o1), Select(d2, p2, r2, f2, g2, o2)) =>
            d1 ≟ d2 && p1 ≟ p2 && r1 ≟ r2 && f1 ≟ f2 && g1 ≟ g2 && o1 ≟ o2

          case (Vari(s1), Vari(s2))                 => s1 ≟ s2
          case (SetLiteral(e1), SetLiteral(e2))     => e1 ≟ e2
          case (ArrayLiteral(e1), ArrayLiteral(e2)) => e1 ≟ e2
          case (MapLiteral(e1), MapLiteral(e2))     => e1 ≟ e2
          case (Splice(e1), Splice(e2))             => e1 ≟ e2

          case (Binop(l1, r1, o1), Binop(l2, r2, o2)) => l1 ≟ l2 && r1 ≟ r2 && o1 ≟ o2
          case (Unop(e1, o1), Unop(e2, o2))           => e1 ≟ e2 && o1 ≟ o2
          case (Ident(n1), Ident(n2))                 => n1 ≟ n2

          case (InvokeFunction(n1, a1), InvokeFunction(n2, a2)) => n1 ≟ n2 && a1 ≟ a2
          case (Match(e1, c1, d1), Match(e2, c2, d2))           => e1 ≟ e2 && c1 ≟ c2 && d1 ≟ d2
          case (Switch(c1, d1), Switch(c2, d2))                 => c1 ≟ c2 && d1 ≟ d2
          case (Let(n1, f1, b1), Let(n2, f2, b2))               => n1 ≟ n2 && f1 ≟ f2 && b1 ≟ b2

          case (IntLiteral(v1), IntLiteral(v2))       => v1 ≟ v2
          case (FloatLiteral(v1), FloatLiteral(v2))   => v1 ≟ v2
          case (StringLiteral(v1), StringLiteral(v2)) => v1 ≟ v2
          case (NullLiteral(), NullLiteral())         => true
          case (BoolLiteral(v1), BoolLiteral(v2))     => v1 ≟ v2

          case (_, _) => false
        }
      }
    }

  private val astType = "AST" :: Nil

  implicit val SqlRenderTree: Delay[RenderTree, Sql] =
    new Delay[RenderTree, Sql] {
      def apply[A](ra: RenderTree[A]): RenderTree[Sql[A]] = new RenderTree[Sql[A]] {
        implicit val rtA: RenderTree[A] = ra

        def renderCase(c: Case[A]): RenderedTree =
          NonTerminal("Case" :: astType, None, ra.render(c.cond) :: ra.render(c.expr) :: Nil)

        def render(n: Sql[A]) = n match {
          case Select(isDistinct, projections, relations, filter, groupBy, orderBy) =>
            val nt = "Select" :: astType
            NonTerminal(nt,
              isDistinct match { case `SelectDistinct` => Some("distinct"); case _ => None },
              projections.map { p =>
                NonTerminal("Proj" :: astType, p.alias, ra.render(p.expr) :: Nil)
              } ⊹
                (relations.map(SqlRelation.renderTree(ra).render) ::
                  filter.map(ra.render) ::
                  groupBy.map {
                    case GroupBy(keys, Some(having)) => NonTerminal("GroupBy" :: astType, None, keys.map(ra.render) :+ ra.render(having))
                    case GroupBy(keys, None)         => NonTerminal("GroupBy" :: astType, None, keys.map(ra.render))
                  } ::
                  orderBy.map {
                    case OrderBy(keys) =>
                      val nt = "OrderBy" :: astType
                      NonTerminal(nt, None,
                        keys.map { case (t, x) => NonTerminal("OrderType" :: nt, Some(t.shows), ra.render(x) :: Nil) }.toList)
                  } ::
                  Nil).foldMap(_.toList))

          case SetLiteral(exprs) => NonTerminal("Set" :: astType, None, exprs.map(ra.render))
          case ArrayLiteral(exprs) => NonTerminal("Array" :: astType, None, exprs.map(ra.render))
          case MapLiteral(exprs) => NonTerminal("Map" :: astType, None, exprs.map(_.render))

          case InvokeFunction(name, args) => NonTerminal("InvokeFunction" :: astType, Some(name.value), args.map(ra.render))

          case Match(expr, cases, Some(default)) => NonTerminal("Match" :: astType, None, ra.render(expr) :: (cases.map(renderCase) :+ ra.render(default)))
          case Match(expr, cases, None)          => NonTerminal("Match" :: astType, None, ra.render(expr) :: cases.map(renderCase))

          case Switch(cases, Some(default)) => NonTerminal("Switch" :: astType, None, cases.map(renderCase) :+ ra.render(default))
          case Switch(cases, None)          => NonTerminal("Switch" :: astType, None, cases.map(renderCase))

          case Binop(lhs, rhs, op) => NonTerminal("Binop" :: astType, Some(op.sql), ra.render(lhs) :: ra.render(rhs) :: Nil)

          case Unop(expr, op) => NonTerminal("Unop" :: astType, Some(op.sql), ra.render(expr) :: Nil)

          case Splice(expr) => NonTerminal("Splice" :: astType, None, expr.toList.map(ra.render))

          case Ident(name) => Terminal("Ident" :: astType, Some(name))

          case Vari(name) => Terminal("Variable" :: astType, Some(":" + name))

          case Let(name, form, body) =>
            NonTerminal("Let" :: astType, Some(name.value), ra.render(form) :: ra.render(body) :: Nil)

          case IntLiteral(v) => Terminal("LiteralExpr" :: astType, Some(v.shows))
          case FloatLiteral(v) => Terminal("LiteralExpr" :: astType, Some(v.shows))
          case StringLiteral(v) => Terminal("LiteralExpr" :: astType, Some(v.shows))
          case NullLiteral() => Terminal("LiteralExpr" :: astType, None)
          case BoolLiteral(v) => Terminal("LiteralExpr" :: astType, Some(v.shows))
        }
      }
    }

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
}

@Lenses final case class Select[A] private[sql] (
  isDistinct:  IsDistinct,
  projections: List[Proj[A]],
  relation:    Option[SqlRelation[A]],
  filter:      Option[A],
  groupBy:     Option[GroupBy[A]],
  orderBy:     Option[OrderBy[A]])
    extends Sql[A] {
  def substituteRelationVariable[M[_]: Monad, T](mapping: Vari[A] => M[A])(implicit
    T0: Recursive.Aux[T, Sql],
    T1: Corecursive.Aux[T, Sql],
    ev: A <~< T
  ): M[SemanticError \/ Select[A]] = {
      val newRelation = relation.traverse(_.transformM[EitherT[M, SemanticError, ?], A]({
        case VariRelationAST(vari, alias) =>
          EitherT(mapping(vari).map(ev(_).project match {
            case Ident(name) =>
              posixCodec.parsePath(Some(_), Some(_), κ(None), κ(None))(name).cata(
                TableRelationAST(_, alias).right,
                SemanticError.GenericError(s"bad path: $name (note: absolute file path required)").left) // FIXME
            // If the variable points to another variable, substitute the old one for the new one
            case Vari(symbol) => VariRelationAST(Vari(symbol), alias).right
            case x =>
              SemanticError.GenericError(s"not a valid table name: ${pprint(x.embed)}").left // FIXME
          }))
        case otherRelation => otherRelation.point[EitherT[M, SemanticError, ?]]
      }, _.point[EitherT[M, SemanticError, ?]]))
      newRelation.map(r => this.copy(relation = r)).run
  }
}
@Lenses final case class Vari[A] private[sql] (symbol: String) extends Sql[A] {
  def map[B](f: A => B): Vari[B] = Vari(symbol)
}
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
@Lenses final case class InvokeFunction[A] private[sql] (name: CIName, args: List[A])
    extends Sql[A]
@Lenses final case class Match[A] private[sql] (expr: A, cases: List[Case[A]], default: Option[A])
    extends Sql[A]
@Lenses final case class Switch[A] private[sql] (cases: List[Case[A]], default: Option[A])
    extends Sql[A]
@Lenses final case class Let[A](ident: CIName, bindTo: A, in: A) extends Sql[A]
@Lenses final case class IntLiteral[A] private[sql] (v: Long) extends Sql[A]
@Lenses final case class FloatLiteral[A] private[sql] (v: Double) extends Sql[A]
@Lenses final case class StringLiteral[A] private[sql] (v: String) extends Sql[A]
@Lenses final case class NullLiteral[A] private[sql] () extends Sql[A]
@Lenses final case class BoolLiteral[A] private[sql] (value: Boolean) extends Sql[A]

@Lenses final case class Case[A](cond: A, expr: A)
object Case {
  implicit val equal: Delay[Equal, Case] =
    new Delay[Equal, Case] {
      def apply[A](fa: Equal[A]) = {
        implicit val eqA: Equal[A] = fa
        Equal.equal {
          case (Case(c1, e1), Case(c2, e2)) => c1 ≟ c2 && e1 ≟ e2
          case (_, _)                       => false
        }
      }
    }
}

@Lenses final case class GroupBy[A](keys: List[A], having: Option[A])
object GroupBy {
  implicit val equal: Delay[Equal, GroupBy] =
    new Delay[Equal, GroupBy] {
      def apply[A](fa: Equal[A]) = {
        implicit val eqA: Equal[A] = fa
        Equal.equal {
          case (GroupBy(k1, h1), GroupBy(k2, h2)) => k1 ≟ k2 && h1 ≟ h2
          case (_, _)                             => false
        }
      }
    }
}

@Lenses final case class OrderBy[A](keys: NonEmptyList[(OrderType, A)])
object OrderBy {
  implicit val equal: Delay[Equal, OrderBy] =
    new Delay[Equal, OrderBy] {
      def apply[A](fa: Equal[A]) = {
        implicit val eqA: Equal[A] = fa
        Equal.equal {
          case (OrderBy(k1), OrderBy(k2)) => k1 ≟ k2
          case (_, _)                     => false
        }
      }
    }
}

@Lenses final case class Proj[A](expr: A, alias: Option[String])
object Proj {
  implicit val equal: Delay[Equal, Proj] =
    new Delay[Equal, Proj] {
      def apply[A](fa: Equal[A]) = {
        implicit val eqA: Equal[A] = fa
        Equal.equal {
          case (Proj(e1, a1), Proj(e2, a2)) => e1 ≟ e2 && a1 ≟ a2
          case (_, _)                       => false
        }
      }
    }

  implicit val traverse1: Traverse1[Proj] = new Traverse1[Proj] {
    def foldMapRight1[A, B](fa: Proj[A])(z: (A) ⇒ B)(f: (A, ⇒ B) ⇒ B): B =
      z(fa.expr)

    def traverse1Impl[G[_]: Apply, A, B](fa: Proj[A])(f: (A) ⇒ G[B]) =
      f(fa.expr).map(Proj(_, fa.alias))
  }
}
