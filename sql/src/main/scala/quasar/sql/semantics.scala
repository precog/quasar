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
import quasar.{NonTerminal, RenderTree, RenderedTree, SemanticError, Terminal, VarName},
  RenderTree.ops._,
  SemanticError._
import quasar.contrib.pathy.prettyPrint

import scala.AnyRef

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._, Validation.{success, failure}

object SemanticAnalysis {
  type Failure = NonEmptyList[SemanticError]

  private def fail[A](e: SemanticError) = Validation.failure[Failure, A](NonEmptyList(e))

  sealed abstract class Synthetic
  object Synthetic {
    final case object SortKey extends Synthetic

    implicit val show: Show[Synthetic] = Show.showFromToString

    implicit val renderTree: RenderTree[Synthetic] =
      RenderTree.fromShow[Synthetic]("Synthetic")
  }

  def normalizeProjectionsƒ[T](implicit TR: Recursive.Aux[T, Sql], TC: Corecursive.Aux[T, Sql])
      : CoalgebraM[State[Option[String], ?], Sql, T] =
    _.project match {
      case sel @ Select(_, _, Some(TableRelationAST(table, alias @ Some(_))), _, _, _) =>
        constantState(sel, alias)
      case sel @ Select(_, _, Some(TableRelationAST(table, None)), _, _, _) =>
        val head: Option[String] = pathy.Path.peel(table).map {
          case (_, -\/(pathy.Path.DirName(dir))) => dir
          case (_, \/-(pathy.Path.FileName(file))) => file
        }
        constantState(sel, head)
      case op @ Binop(Embed(Ident(name)), Embed(StringLiteral(expr)), FieldDeref) =>
        gets {
          case Some(head) => (head === name).fold(ident[T](expr), op)
          case None => op
        }
      case other => state(other)
    }

  private val syntheticPrefix = "__sd__"

  /** Inserts synthetic fields into the projections of each `select` stmt to
    * hold the values that will be used in sorting, and annotates each new
    * projection with Synthetic.SortKey. The compiler will generate a step to
    * remove these fields after the sort operation.
    */
  def projectSortKeysƒ[T](implicit TR: Recursive.Aux[T, Sql], TC: Corecursive.Aux[T, Sql])
      : Sql[T] => Option[Sql[T]] = {
    case Select(d, projections, r, f, g, Some(OrderBy(keys))) => {
      def matches(key: T): PartialFunction[Proj[T], T] =
        key.project match {
          case Ident(keyName) => {
            case Proj(_, Some(alias))               if keyName == alias    => key
            case Proj(Embed(Ident(projName)), None) if keyName == projName => key
            case Proj(Embed(Splice(_)), _)                                 => key
          }
          case _ => {
            case Proj(expr2, Some(alias)) if key == expr2 => ident[T](alias).embed
          }
        }

      // NB: order of the keys has to be preserved, so this complex fold
      //     seems to be the best way.
      type Target = (List[Proj[T]], List[(OrderType, T)], Int)

      val (projs2, keys2, _) = keys.foldRight[Target]((Nil, Nil, 0)) {
        case ((orderType, expr), (projs, keys, index)) =>
          projections.collectFirst(matches(expr)).fold {
            val name  = syntheticPrefix + index.toString()
            val proj2 = Proj(expr, Some(name))
            val key2  = (orderType, ident[T](name).embed)
            (proj2 :: projs, key2 :: keys, index + 1)
          } (kExpr => (projs, (orderType, kExpr) :: keys, index))
      }
      select(d, projections ⊹ projs2, r, f, g, keys2.toNel map (OrderBy(_))).some
    }
    case _ => None
  }

  private val identifySyntheticsƒ: Algebra[Sql, List[Option[Synthetic]]] = {
    case Select(_, projections, _, _, _, _) =>
      projections.map(_.alias match {
        case Some(name) if name.startsWith(syntheticPrefix) =>
          Some(Synthetic.SortKey)
        case _ => None
      })
    case _ => Nil
  }

  final case class BindingScope(scope: Map[String, SqlRelation[Unit]])

  implicit val ShowBindingScope: Show[BindingScope] = new Show[BindingScope] {
    override def show(v: BindingScope) = v.scope.toString
  }

  final case class TableScope(scope: Map[String, SqlRelation[Unit]])

  implicit def ShowTableScope: Show[TableScope] =
    new Show[TableScope] {
      override def show(v: TableScope) = v.scope.toString
    }

  final case class Scope(tableScope: TableScope, bindingScope: BindingScope)

  import Validation.FlatMap._

  type ValidSem[A] = ValidationNel[SemanticError, A]

  /** This analysis identifies all the named tables within scope at each node in
    * the tree. If two tables are given the same name within the same scope,
    * then because this leads to an ambiguity, an error is produced containing
    * details on the duplicate name.
    */
  def scopeTablesƒ[T](implicit T: Recursive.Aux[T, Sql]):
      CoalgebraM[ValidSem, Sql, (Scope, T)] = {
    case (Scope(ts, bs), Embed(expr)) => expr match {
      case Select(_, _, relations, _, _, _) =>
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def findRelations(r: SqlRelation[T]): ValidSem[Map[String, SqlRelation[Unit]]] =
          r match {
            case IdentRelationAST(name, aliasOpt) =>
              success(Map(aliasOpt.getOrElse(name) ->
                IdentRelationAST(name, aliasOpt)))
            case VariRelationAST(vari, aliasOpt) =>
              failure((UnboundVariable(VarName(vari.symbol)): SemanticError).wrapNel)
            case TableRelationAST(file, aliasOpt) =>
              success(Map(aliasOpt.getOrElse(prettyPrint(file)) -> TableRelationAST(file, aliasOpt)))
            case ExprRelationAST(_, alias) =>
              success(Map(alias -> ExprRelationAST((), alias)))
            case JoinRelation(l, r, _, _) => for {
              rels <- findRelations(l) tuple findRelations(r)
              (left, right) = rels
              rez <- (left.keySet intersect right.keySet).toList.toNel.cata(
                nel => failure(nel.map(DuplicateRelationName(_):SemanticError)),
                success(left ++ right))
            } yield rez
          }

        relations.fold[ValidSem[Map[String, SqlRelation[Unit]]]](
          success(Map[String, SqlRelation[Unit]]()))(
          findRelations)
          .map(m => expr.map((Scope(TableScope(m), bs), _)))

      case Let(name, form, body) => {
        val bs2: BindingScope =
          BindingScope(bs.scope ++ Map(name.value -> ExprRelationAST((), name.value)))

        success(Let(name, (Scope(ts, bs), form), (Scope(ts, bs2), body)))
      }

      case x => success(x.map((Scope(ts, bs), _)))
    }
  }

  sealed abstract class Provenance {
    import Provenance._

    def & (that: Provenance): Provenance = Both(this, that)

    def | (that: Provenance): Provenance = Either(this, that)

    @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Recursion"))
    def simplify: Provenance = this match {
      case x : Either => anyOf(x.flatten.map(_.simplify).filterNot(_.equals(Empty)))
      case x : Both => allOf(x.flatten.map(_.simplify).filterNot(_.equals(Empty)))
      case _ => this
    }

    def namedRelations: Map[String, List[NamedRelation[Unit]]] =
      relations.foldMap(_.namedRelations)

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def relations: List[SqlRelation[Unit]] = this match {
      case Empty => Nil
      case Value => Nil
      case Relation(value) => value :: Nil
      case Either(v1, v2) => v1.relations ++ v2.relations
      case Both(v1, v2) => v1.relations ++ v2.relations
    }

    def flatten: Set[Provenance] = Set(this)

    // TODO: Implement Order for all sorts of types so we can get Equal (well,
    //       Order, even) defined properly for Provenance.
    @SuppressWarnings(Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals"))
    override def equals(that: scala.Any): Boolean = (this, that) match {
      case (x, y) if (x.eq(y.asInstanceOf[AnyRef])) => true
      case (Relation(v1), Relation(v2))             => v1 ≟ v2
      case (Either(_, _), that @ Either(_, _))      => this.simplify.flatten == that.simplify.flatten
      case (Both(_, _), that @ Both(_, _))          => this.simplify.flatten == that.simplify.flatten
      case (_, _)                                   => false
    }

    override def hashCode = this match {
      case Either(_, _) => this.simplify.flatten.hashCode
      case Both(_, _) => this.simplify.flatten.hashCode
      case _ => super.hashCode
    }
  }
  trait ProvenanceInstances {
    import Provenance._

    implicit val renderTree: RenderTree[Provenance] =
      new RenderTree[Provenance] { self =>
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def render(v: Provenance) = {
          val ProvenanceNodeType = List("Provenance")

          def nest(l: RenderedTree, r: RenderedTree, sep: String) = (l, r) match {
            case (RenderedTree(_, ll, Nil), RenderedTree(_, rl, Nil)) =>
              Terminal(ProvenanceNodeType, Some((ll.toList ++ rl.toList).mkString("(", s" $sep ", ")")))
            case _ => NonTerminal(ProvenanceNodeType, Some(sep), l :: r :: Nil)
          }

          v match {
            case Empty               => Terminal(ProvenanceNodeType, Some("Empty"))
            case Value               => Terminal(ProvenanceNodeType, Some("Value"))
            case Relation(value)     => value.render.copy(nodeType = ProvenanceNodeType)
            case Either(left, right) => nest(self.render(left), self.render(right), "|")
            case Both(left, right)   => nest(self.render(left), self.render(right), "&")
        }
      }
    }

    implicit val orMonoid: Monoid[Provenance] =
      new Monoid[Provenance] {
        def zero = Empty

        def append(v1: Provenance, v2: => Provenance) = (v1, v2) match {
          case (Empty, that) => that
          case (this0, Empty) => this0
          case _ => v1 | v2
        }
      }

    implicit val andMonoid: Monoid[Provenance] =
      new Monoid[Provenance] {
        def zero = Empty

        def append(v1: Provenance, v2: => Provenance) = (v1, v2) match {
          case (Empty, that) => that
          case (this0, Empty) => this0
          case _ => v1 & v2
        }
      }
  }
  object Provenance extends ProvenanceInstances {
    case object Empty extends Provenance
    case object Value extends Provenance
    final case class Relation(value: SqlRelation[Unit]) extends Provenance
    final case class Either(left: Provenance, right: Provenance)
        extends Provenance {
      override def flatten: Set[Provenance] = {
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def flatten0(x: Provenance): Set[Provenance] = x match {
          case Either(left, right) => flatten0(left) ++ flatten0(right)
          case _ => Set(x)
        }
        flatten0(this)
      }
    }
    final case class Both(left: Provenance, right: Provenance)
        extends Provenance {
      override def flatten: Set[Provenance] = {
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def flatten0(x: Provenance): Set[Provenance] = x match {
          case Both(left, right) => flatten0(left) ++ flatten0(right)
          case _ => Set(x)
        }
        flatten0(this)
      }
    }

    def allOf[F[_]: Foldable](xs: F[Provenance]): Provenance =
      xs.concatenate(andMonoid)

    def anyOf[F[_]: Foldable](xs: F[Provenance]): Provenance =
      xs.concatenate(orMonoid)
  }

  /** This phase infers the provenance of every expression, issuing errors
    * if identifiers are used with unknown provenance. The phase requires
    * TableScope and BindingScope annotations on the tree.
    */
  def inferProvenanceƒ[T](implicit T: Corecursive.Aux[T, Sql]):
      ElgotAlgebraM[(Scope, ?), ValidSem, Sql, Provenance] = {
    case (Scope(ts, bs), expr) => expr match {
      case Select(_, projections, _, _, _, _) =>
        success(Provenance.allOf(projections.map(_.expr)))

      case SetLiteral(_)  => success(Provenance.Value)
      case ArrayLiteral(_) => success(Provenance.Value)
      case MapLiteral(_) => success(Provenance.Value)
      case Splice(expr)       => success(expr.getOrElse(Provenance.Empty))
      case Vari(_)        => success(Provenance.Value)
      case Binop(left, right, _) => success(left & right)
      case Unop(expr, _) => success(expr)
      case Ident(name) =>
        val scope = bs.scope.get(name) match {
          case None => ts.scope.get(name)
          case s => s
        }
        scope.fold({
          // If `name` matches neither table nor binding scope we default to table scope.
          // For example:
          // `mystate := "CO"; select * from zips where state = mystate`
          val localScope = if (ts.scope.isEmpty) bs.scope else ts.scope
          Provenance.anyOf[Map[String, ?]]((localScope) ∘ (Provenance.Relation(_))) match {
            case Provenance.Empty => fail(NoTableDefined(Ident[Fix[Sql]](name).embed))
            case x                => success(x)
          }})(
          (Provenance.Relation(_)) ⋙ success)
      case InvokeFunction(_, args) => success(Provenance.allOf(args))
      case Match(_, cases, _)      =>
        success(Provenance.allOf(cases.map(_.expr)))
      case Switch(cases, _)        =>
        success(Provenance.allOf(cases.map(_.expr)))
      case Let(_, form, body)      => success(form & body)
      case IntLiteral(_)           => success(Provenance.Value)
      case FloatLiteral(_)         => success(Provenance.Value)
      case StringLiteral(_)        => success(Provenance.Value)
      case BoolLiteral(_)          => success(Provenance.Value)
      case NullLiteral()           => success(Provenance.Value)
    }
  }

  type Annotations = (List[Option[Synthetic]], Provenance)

  val synthElgotMƒ:
      ElgotAlgebraM[(Scope, ?), ValidSem, Sql, List[Option[Synthetic]]] =
    (identifySyntheticsƒ: Algebra[Sql, List[Option[Synthetic]]]).generalizeElgot[(Scope, ?)] ⋙ (_.point[ValidSem])

  def addAnnotations[T](implicit T: Corecursive.Aux[T, Sql]):
      ElgotAlgebraM[
        ((Scope, T), ?),
        NonEmptyList[SemanticError] \/ ?,
        Sql,
        Cofree[Sql, Annotations]] =
    e => attributeElgotM[(Scope, ?), ValidSem][Sql, Annotations](
      ElgotAlgebraMZip[(Scope, ?), ValidSem, Sql].zip(
        synthElgotMƒ,
        inferProvenanceƒ)).apply(e.leftMap(_._1)).disjunction

  def normalizeProjections[T](expr: T)(implicit TR: Recursive.Aux[T, Sql], TC: Corecursive.Aux[T, Sql]): T =
    expr.anaM[T](normalizeProjectionsƒ[T]).run(None)._2

  def projectSortKeys[T](expr: T)(implicit TR: Recursive.Aux[T, Sql], TC: Corecursive.Aux[T, Sql]): T =
    expr.transCata[T](orOriginal(projectSortKeysƒ))

  // NB: identifySynthetics &&& (scopeTables >>> inferProvenance)
  def annotate[T](expr: T)(implicit TR: Recursive.Aux[T, Sql], TC: Corecursive.Aux[T, Sql])
      : NonEmptyList[SemanticError] \/ Cofree[Sql, Annotations] = {
    val emptyScope: Scope = Scope(TableScope(Map()), BindingScope(Map()))
    (emptyScope, expr).coelgotM[NonEmptyList[SemanticError] \/ ?](
      addAnnotations,
      scopeTablesƒ.apply(_).disjunction)
  }
}
