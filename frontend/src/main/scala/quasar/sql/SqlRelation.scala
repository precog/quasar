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
import quasar._
import quasar.common.JoinType
import quasar.contrib.pathy._

import matryoshka._
import monocle.macros.Lenses
import pathy.Path._
import scalaz._, Scalaz._

sealed abstract class SqlRelation[A] extends Product with Serializable {
  def namedRelations: Map[String, List[NamedRelation[A]]] = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def collect(n: SqlRelation[A]): List[(String, NamedRelation[A])] =
      n match {
        case JoinRelation(left, right, _, _) => collect(left) ++ collect(right)
        case t: NamedRelation[A] => (t.aliasName -> t) :: Nil
    }

    collect(this).groupBy(_._1).mapValues(_.map(_._2))
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def mapPathsM[F[_]: Monad](f: FUPath => F[FUPath]): F[SqlRelation[A]] = this match {
    case IdentRelationAST(_, _) => this.point[F]
    case VariRelationAST(_, _) => this.point[F]
    case TableRelationAST(path, alias) => f(path).map(TableRelationAST(_, alias))
    case rel @ JoinRelation(left, right, _, _) =>
      (left.mapPathsM(f) |@| right.mapPathsM(f))((l,r) => rel.copy(left = l, right = r))
    case ExprRelationAST(_,_) => this.point[F]
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def transformM[F[_]: Monad, B](f: SqlRelation[A] => F[SqlRelation[B]], g: A => F[B]): F[SqlRelation[B]] = this match {
    case JoinRelation(left, right, tpe, clause) =>
      (left.transformM[F, B](f, g) |@|
        right.transformM[F, B](f, g) |@|
        g(clause))((l,r,c) =>
          JoinRelation(l, r, tpe, c))
    case rel => f(rel)
  }
}

sealed abstract class NamedRelation[A] extends SqlRelation[A] {
  def aliasName: String
}

/**
 * IdentRelationAST allows us to reference a let binding in a relation (i.e. table)
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

object SqlRelation {
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  implicit val equal: Delay[Equal, SqlRelation] =
    new Delay[Equal, SqlRelation] {
      def apply[A](fa: Equal[A]) = {
        implicit val eqA: Equal[A] = fa
        Equal.equal {
          case (IdentRelationAST(n1, a1), IdentRelationAST(n2, a2)) =>
            n1 ≟ n2 && a1 ≟ a2
          case (VariRelationAST(v1, a1), VariRelationAST(v2, a2)) =>
            Equal[Sql[A]].equal(v1, v2) && a1 ≟ a2
          case (TableRelationAST(t1, a1), TableRelationAST(t2, a2)) =>
            t1 == t2 && a1 ≟ a2  // TODO use scalaz equal for the `FUPath`
          case (ExprRelationAST(e1, a1), ExprRelationAST(e2, a2)) =>
            e1 ≟ e2 && a1 ≟ a2
          case (JoinRelation(l1, r1, t1, c1), JoinRelation(l2, r2, t2, c2)) =>
            l1 ≟ l2 && r1 ≟ r2 && t1 ≟ t2 && c1 ≟ c2
          case (_, _) =>
            false
        }
      }
    }

  implicit def show[A: Show]: Show[SqlRelation[A]] = Show.showFromToString

  private val astType = "AST" :: Nil

  implicit def renderTree: Delay[RenderTree, SqlRelation] =
    new Delay[RenderTree, SqlRelation] {
      def apply[A](ra: RenderTree[A]) = new RenderTree[SqlRelation[A]] {
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def render(r: SqlRelation[A]): RenderedTree = r match {
          case IdentRelationAST(name, alias) =>
            val aliasString = alias.cata(" as " + _, "")
            Terminal("IdentRelation" :: astType, Some(name + aliasString))
          case VariRelationAST(vari, alias) =>
            val aliasString = alias.cata(" as " + _, "")
            Terminal("VariRelation" :: astType, Some(":" + vari.symbol + aliasString))
          case ExprRelationAST(select, alias) =>
            NonTerminal("ExprRelation" :: astType, Some("Expr as " + alias), ra.render(select) :: Nil)
          case TableRelationAST(name, alias) =>
            val aliasString = alias.cata(" as " + _, "")
            Terminal("TableRelation" :: astType, Some(prettyPrint(name) + aliasString))
          case JoinRelation(left, right, jt, clause) =>
            NonTerminal("JoinRelation" :: astType, Some(jt.shows),
              List(render(left), render(right), ra.render(clause)))
        }
      }
    }

  implicit val functor: Functor[SqlRelation] = new Functor[SqlRelation] {
    def map[A, B](fa: SqlRelation[A])(f: A => B) = fa match {
      case IdentRelationAST(name, alias)  => IdentRelationAST(name, alias)
      case VariRelationAST(vari, alias)   => VariRelationAST(vari.map(f), alias)
      case ExprRelationAST(select, alias) => ExprRelationAST(f(select), alias)
      case TableRelationAST(name, alias)  => TableRelationAST(name, alias)
      case JoinRelation(left, right, jt, clause) => JoinRelation(left.map(f), right.map(f), jt, f(clause))
    }
  }
}
