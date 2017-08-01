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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.{NonTerminal, RenderTree, Terminal}, RenderTree.ops._
import quasar.common.SortDir
import quasar.fp._
import quasar.fp.ski._
import quasar.namegen._
import quasar._, Planner._
import quasar.javascript._
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.workflow._
import quasar.std.StdLib._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

sealed abstract class WorkflowBuilderF[F[_], +A] extends Product with Serializable

object WorkflowBuilderF {
  import WorkflowBuilder._

  // NB: This instance can’t be derived, because of `dummyOp`.
  implicit def equal[F[_]: Coalesce](implicit ev: WorkflowOpCoreF :<: F): Delay[Equal, WorkflowBuilderF[F, ?]] =
    new Delay[Equal, WorkflowBuilderF[F, ?]] {
      def apply[A](eq: Equal[A]) = {
        implicit val eqA: Equal[A] = eq

        Equal.equal((v1: WorkflowBuilderF[F, A], v2: WorkflowBuilderF[F, A]) => (v1, v2) match {
          case (CollectionBuilderF(g1, b1, s1), CollectionBuilderF(g2, b2, s2)) =>
            g1 == g2 && b1 == b2 && s1 ≟ s2
          case (v1 @ ShapePreservingBuilderF(s1, i1, _), v2 @ ShapePreservingBuilderF(s2, i2, _)) =>
            eq.equal(s1, s2) && i1 ≟ i2 && v1.dummyOp == v2.dummyOp
          case (ValueBuilderF(v1), ValueBuilderF(v2)) => v1 == v2
          case (ExprBuilderF(s1, e1), ExprBuilderF(s2, e2)) =>
            eq.equal(s1, s2) && e1 == e2
          case (DocBuilderF(s1, e1), DocBuilderF(s2, e2)) =>
            eq.equal(s1, s2) && e1 == e2
          case (ArrayBuilderF(s1, e1), ArrayBuilderF(s2, e2)) =>
            eq.equal(s1, s2) && e1 == e2
          case (GroupBuilderF(s1, k1, c1), GroupBuilderF(s2, k2, c2)) =>
            eq.equal(s1, s2) && k1 ≟ k2 && c1 == c2
          case (FlatteningBuilderF(s1, f1), FlatteningBuilderF(s2, f2)) =>
            eq.equal(s1, s2) && f1 == f2
          case (SpliceBuilderF(s1, i1), SpliceBuilderF(s2, i2)) =>
            eq.equal(s1, s2) && i1 == i2
          case (ArraySpliceBuilderF(s1, i1), ArraySpliceBuilderF(s2, i2)) =>
            eq.equal(s1, s2) && i1 == i2
          case _ => false
        })
      }
    }

  implicit def traverse[F[_]]: Traverse[WorkflowBuilderF[F, ?]] =
    new Traverse[WorkflowBuilderF[F, ?]] {
      def traverseImpl[G[_], A, B](
        fa: WorkflowBuilderF[F, A])(
        f: A => G[B])(
        implicit G: Applicative[G]):
          G[WorkflowBuilderF[F, B]] =
        fa match {
          case x @ CollectionBuilderF(_, _, _) => G.point(x)
          case ShapePreservingBuilderF(src, inputs, op) =>
            (f(src) |@| inputs.traverse(f))(ShapePreservingBuilderF(_, _, op))
          case x @ ValueBuilderF(_) => G.point(x)
          case ExprBuilderF(src, expr) => f(src).map(ExprBuilderF(_, expr))
          case DocBuilderF(src, shape) => f(src).map(DocBuilderF(_, shape))
          case ArrayBuilderF(src, shape) => f(src).map(ArrayBuilderF(_, shape))
          case GroupBuilderF(src, keys, contents) =>
            (f(src) |@| keys.traverse(f))(GroupBuilderF(_, _, contents))
          case FlatteningBuilderF(src, fields) =>
            f(src).map(FlatteningBuilderF(_, fields))
          case SpliceBuilderF(src, structure) =>
            f(src).map(SpliceBuilderF(_, structure))
          case ArraySpliceBuilderF(src, structure) =>
            f(src).map(ArraySpliceBuilderF(_, structure))
        }
    }

  implicit def renderTree[F[_]: Coalesce: Functor](implicit
      RG: RenderTree[Contents[GroupValue[Fix[ExprOp]]]],
      RC: RenderTree[Contents[Expr]],
      RF: RenderTree[Fix[F]],
      ev: WorkflowOpCoreF :<: F
    ): Delay[RenderTree, WorkflowBuilderF[F, ?]] =
    Delay.fromNT(λ[RenderTree ~> (RenderTree ∘ WorkflowBuilderF[F, ?])#λ](rt => {
      val nodeType = "WorkflowBuilder" :: Nil
      import fixExprOp._

      RenderTree.make {
        case CollectionBuilderF(graph, base, struct) =>
          NonTerminal("CollectionBuilder" :: nodeType, Some(base.shows),
            graph.render ::
              Terminal("Schema" :: "CollectionBuilder" :: nodeType, struct ∘ (_.shows)) ::
              Nil)
        case spb @ ShapePreservingBuilderF(src, inputs, op) =>
          val nt = "ShapePreservingBuilder" :: nodeType
          NonTerminal(nt, None,
            rt.render(src) :: (inputs.map(rt.render) :+ spb.dummyOp.render))
        case ValueBuilderF(value) =>
          Terminal("ValueBuilder" :: nodeType, Some(value.shows))
        case ExprBuilderF(src, expr) =>
          NonTerminal("ExprBuilder" :: nodeType, None,
            rt.render(src) :: expr.render :: Nil)
        case DocBuilderF(src, shape) =>
          val nt = "DocBuilder" :: nodeType
          NonTerminal(nt, None,
            rt.render(src) ::
              NonTerminal("Shape" :: nt, None,
                shape.toList.map {
                  case (name, expr) =>
                    NonTerminal("Name" :: nodeType, Some(name.value), List(expr.render))
                }) ::
              Nil)
        case ArrayBuilderF(src, shape) =>
          val nt = "ArrayBuilder" :: nodeType
          NonTerminal(nt, None,
            rt.render(src) ::
              NonTerminal("Shape" :: nt, None, shape.map(_.render)) ::
              Nil)
        case GroupBuilderF(src, keys, content) =>
          val nt = "GroupBuilder" :: nodeType
          NonTerminal(nt, None,
            rt.render(src) ::
              NonTerminal("By" :: nt, None, keys.map(rt.render)) ::
              RG.render(content).copy(nodeType = "Content" :: nt) ::
              Nil)
        case FlatteningBuilderF(src, fields) =>
          val nt = "FlatteningBuilder" :: nodeType
          NonTerminal(nt, None,
            rt.render(src) ::
              fields.toList.map(x => $var(x.field).render.copy(nodeType = (x match {
                case StructureType.Array(_) => "Array"
                case StructureType.Object(_) => "Object"
              }) :: nt)))
        case SpliceBuilderF(src, structure) =>
          NonTerminal("SpliceBuilder" :: nodeType, None,
            rt.render(src) :: structure.map(RC.render))
        case ArraySpliceBuilderF(src, structure) =>
          NonTerminal("ArraySpliceBuilder" :: nodeType, None,
            rt.render(src) :: structure.map(RC.render))
      }
    }))
}

object WorkflowBuilder {
  /** A partial description of a query that can be run on an instance of MongoDB */
  type WorkflowBuilder[F[_]] = Fix[WorkflowBuilderF[F, ?]]
  /** If we know what the shape is, represents the list of Fields. */
  type Schema = Option[NonEmptyList[BsonField.Name]]

  /** Either arbitrary javascript expression or Pipeline expression
    * An arbitrary javascript is more powerful but less performant because it
    * gets materialized into a Map/Reduce operation.
    */
  type Expr = JsFn \/ Fix[ExprOp]

  private def exprToJs(expr: Expr)(implicit ev: ExprOpOps.Uni[ExprOp])
      : PlannerError \/ JsFn =
    expr.fold(\/-(_), _.para(toJs))

  implicit val ExprRenderTree: RenderTree[Expr] = new RenderTree[Expr] {
    def render(x: Expr) = x.fold(_.render, _.render)
  }

  import fixExprOp._

  /** Like ValueBuilder, this is a Leaf node which can be used to construct a
    * more complicated WorkflowBuilder. Takes a value resulting from a Workflow
    * and wraps it in a WorkflowBuilder. For example: If you want to read from
    * MongoDB and then project on a field, the read would be the
    * CollectionBuilder.
    * @param base Name, or names under which the values produced by the src will
    *             be found. It's most often `Root`, or else it's probably a
    *             temporary `Field`.
    * @param struct In the case of read, it's None. In the case where we are
    *               converting a WorkflowBuilder into a Workflow, we have access
    *               to the shape of this Workflow and encode it in `struct`.
    */
  final case class CollectionBuilderF[F[_]](
    src: Fix[F],
    base: Base,
    struct: Schema) extends WorkflowBuilderF[F, Nothing]
  object CollectionBuilder {
    def apply[F[_]](graph: Fix[F], base: Base, struct: Schema) =
      Fix[WorkflowBuilderF[F, ?]](new CollectionBuilderF(graph, base, struct))
  }

  /** For instance, \$match, \$skip, \$limit, \$sort */
  final case class ShapePreservingBuilderF[F[_], A](
    src: A,
    inputs: List[A],
    op: PartialFunction[List[BsonField], FixOp[F]])
      extends WorkflowBuilderF[F, A]
  {
    def dummyOp(implicit ev0: WorkflowOpCoreF :<: F, ev1: Coalesce[F]): Fix[F] =
      op(
        inputs.zipWithIndex.map {
          case (_, index) => BsonField.Name("_" + index.toString)
        })(
        // Nb. This read is an arbitrary value that allows us to compare the partial function
        $read[F](Collection(DatabaseName(""), CollectionName(""))))
  }
  object ShapePreservingBuilder {
    def apply[F[_]: Coalesce](
      src: WorkflowBuilder[F],
      inputs: List[WorkflowBuilder[F]],
      op: PartialFunction[List[BsonField], FixOp[F]])
      (implicit ev: WorkflowOpCoreF :<: F) =
      Fix[WorkflowBuilderF[F, ?]](new ShapePreservingBuilderF(src, inputs, op))
  }

  /** A query that produces a constant value. */
  final case class ValueBuilderF[F[_]](value: Bson) extends WorkflowBuilderF[F, Nothing]
  object ValueBuilder {
    def apply[F[_]](value: Bson) = Fix[WorkflowBuilderF[F, ?]](new ValueBuilderF(value))
  }

  /** A query that applies an `Expr` operator to a source (which could be
    * multiple values). You can think of `Expr` as a function application in
    * MongoDB that accepts values and produces new values. It's kind of like a
    * map. The shape coming out of an `ExprBuilder` is unknown because of the
    * fact that the expression can be arbitrary.
    * @param src The values on which to apply the `Expr`
    * @param expr The expression that produces a new set of values given a set
    *             of values.
    */
  final case class ExprBuilderF[F[_], A](src: A, expr: Expr) extends WorkflowBuilderF[F, A]
  object ExprBuilder {
    def apply[F[_]](src: WorkflowBuilder[F], expr: Expr) =
      Fix[WorkflowBuilderF[F, ?]](new ExprBuilderF(src, expr))
  }

  /** Same as an `ExprBuilder` but contains the shape of the resulting query.
    * The result is a document that maps the field Name to the resulting values
    * from applying the `Expr` associated with that name.
    * NB: The shape is more restrictive than \$project because we may need to
    *     convert it to a `GroupBuilder`, and a nested `Reshape` can be realized
    *     with a chain of DocBuilders, leaving the collapsing to
    *     Workflow.coalesce.
    */
  final case class DocBuilderF[F[_], A](src: A, shape: ListMap[BsonField.Name, Expr])
      extends WorkflowBuilderF[F, A]
  object DocBuilder {
    def apply[F[_]](src: WorkflowBuilder[F], shape: ListMap[BsonField.Name, Expr]) =
      Fix[WorkflowBuilderF[F, ?]](new DocBuilderF(src, shape))
  }

  final case class ArrayBuilderF[F[_], A](src: A, shape: List[Expr])
      extends WorkflowBuilderF[F, A]
  object ArrayBuilder {
    def apply[F[_]](src: WorkflowBuilder[F], shape: List[Expr]) =
      Fix[WorkflowBuilderF[F, ?]](new ArrayBuilderF(src, shape))
  }

  sealed abstract class Contents[+A] extends Product with Serializable
  sealed abstract class DocContents[+A] extends Contents[A]
  sealed trait ArrayContents[+A] extends Contents[A]
  object Contents {
    final case class Expr[A](contents: A) extends DocContents[A] with ArrayContents[A]
    final case class Doc[A](contents: ListMap[BsonField.Name, A]) extends DocContents[A]
    final case class Array[A](contents: List[A]) extends ArrayContents[A]

    implicit def ContentsRenderTree[A: RenderTree]: RenderTree[Contents[A]] =
      new RenderTree[Contents[A]] {
        val nodeType = "Contents" :: Nil

        def render(v: Contents[A]) =
          v match {
            case Expr(a)   => NonTerminal("Expr" :: nodeType, None, a.render :: Nil)
            case Doc(b)    => NonTerminal("Doc" :: nodeType, None, b.render :: Nil)
            case Array(as) => NonTerminal("Array" :: nodeType, None, as.map(_.render))
          }
      }
  }
  import Contents._

  def contentsToBuilder[F[_]]: Contents[Expr] => WorkflowBuilder[F] => WorkflowBuilder[F] = {
    case Expr(expr) => ExprBuilder(_, expr)
    case Doc(doc)   => DocBuilder(_, doc)
    case Array(arr) => ArrayBuilder(_, arr)
  }

  type GroupValue[A] = AccumOp[A] \/ A
  type GroupContents = DocContents[GroupValue[Fix[ExprOp]]]

  final case class GroupBuilderF[F[_], A](
    src: A, keys: List[A], contents: GroupContents)
      extends WorkflowBuilderF[F, A]
  object GroupBuilder {
    def apply[F[_]](
      src: WorkflowBuilder[F],
      keys: List[WorkflowBuilder[F]],
      contents: GroupContents) =
      Fix[WorkflowBuilderF[F, ?]](new GroupBuilderF(src, keys, contents))
  }

  sealed abstract class StructureType[A] {
    val field: A
  }
  object StructureType {
    final case class Array[A](field: A) extends StructureType[A]
    final case class Object[A](field: A) extends StructureType[A]

    implicit val StructureTypeTraverse: Traverse[StructureType] =
      new Traverse[StructureType] {
        def traverseImpl[G[_], A, B](fa: StructureType[A])(f: A => G[B])(implicit G: Applicative[G]):
            G[StructureType[B]] =
          fa match {
            case Array(field) => f(field).map(Array(_))
              case Object(field) => f(field).map(Object(_))
          }
      }
  }

  final case class FlatteningBuilderF[F[_], A](src: A, fields: Set[StructureType[DocVar]])
      extends WorkflowBuilderF[F, A]
  object FlatteningBuilder {
    def apply[F[_]](src: WorkflowBuilder[F], fields: Set[StructureType[DocVar]]) =
      Fix[WorkflowBuilderF[F, ?]](new FlatteningBuilderF(src, fields))
  }

  /** Holds a partially-unknown structure. `Expr` entries are unknown and `Doc`
    * entries are known. There should be at least one Expr in the list,
    * otherwise it should be a DocBuilder.
    */
  final case class SpliceBuilderF[F[_], A](src: A, structure: List[DocContents[Expr]])
      extends WorkflowBuilderF[F, A] {
    def toJs(implicit ev: ExprOpOps.Uni[ExprOp]): PlannerError \/ JsFn =
      structure.traverse {
        case Expr(unknown) => exprToJs(unknown)
        case Doc(known)    => known.toList.traverse { case (k, v) =>
          exprToJs(v).map(k.asText -> _)
        }.map(ms => JsFn(jsBase, jscore.Obj(ms.map { case (k, v) => jscore.Name(k) -> v(jscore.Ident(jsBase)) }.toListMap)))
      }.map(srcs =>
        JsFn(jsBase, jscore.SpliceObjects(srcs.map(_(jscore.Ident(jsBase))))))
  }
  object SpliceBuilder {
    def apply[F[_]](src: WorkflowBuilder[F], structure: List[DocContents[Expr]]) =
      Fix[WorkflowBuilderF[F, ?]](new SpliceBuilderF(src, structure))
  }

  final case class ArraySpliceBuilderF[F[_], A](src: A, structure: List[ArrayContents[Expr]])
      extends WorkflowBuilderF[F, A] {
    def toJs(implicit ev: ExprOpOps.Uni[ExprOp]): PlannerError \/ JsFn =
      structure.traverse {
        case Expr(unknown) => exprToJs(unknown)
        case Array(known)  => known.traverse(exprToJs).map(
            ms => JsFn(jsBase, jscore.Arr(ms.map(_(jscore.Ident(jsBase))))))
      }.map(srcs =>
        JsFn(jsBase, jscore.SpliceArrays(srcs.map(_(jscore.Ident(jsBase))))))
  }
  object ArraySpliceBuilder {
    def apply[F[_]](src: WorkflowBuilder[F], structure: List[ArrayContents[Expr]]) =
      Fix[WorkflowBuilderF[F, ?]](new ArraySpliceBuilderF(src, structure))
  }

  /** Simplify/coalesce certain shapes, eliminating extra layers that make it
    * harder to pattern match. Should be applied before `objectConcat`,
    * `arrayConcat`, or `merge`.
    */
  def normalizeƒ[F[_]: Coalesce](implicit ev0: WorkflowOpCoreF :<: F, exprOps: ExprOpOps.Uni[ExprOp])
    : WorkflowBuilderF[F, Fix[WorkflowBuilderF[F, ?]]] => Option[WorkflowBuilderF[F, Fix[WorkflowBuilderF[F, ?]]]] = {

    def collapse(outer: Expr, inner: ListMap[BsonField.Name, Expr]): Option[Expr] = {
      def rewriteExpr(t: Fix[ExprOp])(applyExpr: PartialFunction[ExprOp[Expr], Option[Expr]]): Option[Expr] =
        t.cataM[Option, Expr] { x =>
          applyExpr.lift(x).getOrElse {
            x.sequence.fold(
              κ(for {
                op <- x.traverse(exprToJs).toOption
                js <- exprOps.toJsSimple(op).toOption
              } yield -\/(js)),
              {
                case $varF(_) => None
                case op => \/-(Fix(op)).some
              })
          }
        }

      outer.fold(
        js =>
          for {
            xs <- inner.map { case (n, x) =>
                    jscore.Select(jscore.Ident(js.param), n.value) -> exprToJs(x)
                  }.sequence.toOption
            expr1 <- js.expr.apoM[JsCore] {
                    case t @ jscore.Access(b, _) if b == jscore.Ident(js.param) =>
                      xs.get(t).map(_(jscore.Ident(js.param)).project.map(_.left[JsCore]))
                    case t => t.project.map(_.right[JsCore]).some
                  }
          } yield -\/(JsFn(js.param, expr1)),
        expr =>
          rewriteExpr(expr) {
            case $varF(DocVar(_, Some(f))) =>
              f.flatten match {
                case NonEmptyList(h @ BsonField.Name(_), INil()) => inner.get(h)
                case ls =>
                  ls.head match {
                    case n @ BsonField.Name(_) => inner.get(n).flatMap {
                      case \/-($var(DocVar(b, None))) => BsonField(ls.tail.toList).map(f => \/-($var(DocVar(b, Some(f)))))
                      case \/-($var(DocVar(b, Some(f)))) => \/-($var(DocVar(b, Some(BsonField(f.flatten :::> ls.tail))))).some
                      case _ => None
                    }
                    case _ => None
                  }
              }
          })
    }

    def inln(outerExpr: Expr, cont: DocContents[_ \/ Fix[ExprOp]]) =
      outerExpr.fold(
        κ(None),
        expr => (cont match {
          case Expr(\/-($var(dv))) =>
            Some(expr.cata(exprOps.rewriteRefs(prefixBase(dv))))
          case Expr(\/-(ex)) => expr.cataM[Option, Fix[ExprOp]] {
            case $varF(DocVar.ROOT(None)) => ex.some
            case $varF(_)                 => None
            case x                        => Fix(x).some
          }
          case Doc(map) => expr.cataM[Option, Fix[ExprOp]] {
            case $varF(DocField(field)) =>
              field.flatten.toList match {
                case (name @ BsonField.Name(_)) :: Nil =>
                  map.get(name).flatMap(_.toOption)
                case (name @ BsonField.Name(_)) :: tail =>
                  map.get(name).flatMap {
                    case \/-($var(dv)) => BsonField(tail).map(p => $var(dv \ p))
                    case _ => None
                  }
                case _ => None
              }
            case $varF(_) => None
            case x => Some(Fix(x))
          }
          case _ => None
        }))

    {
      case ExprBuilderF(src, \/-($$ROOT)) => src.project.some
      case ExprBuilderF(src, outerExpr) =>
        src.unFix match {
          case ExprBuilderF(wb0, -\/(js1)) =>
            exprToJs(outerExpr).map(js => ExprBuilderF[F, WorkflowBuilder[F]](wb0, -\/(js1 >>> js))).toOption
          case ExprBuilderF(src0, contents) =>
            inln(outerExpr, Expr(contents)).map(expr => ExprBuilderF(src0, \/-(expr)))
          case DocBuilderF(src, innerShape) =>
            collapse(outerExpr, innerShape).map(ExprBuilderF(src, _))
          case ShapePreservingBuilderF(src0, inputs, op) =>
            ShapePreservingBuilderF(
              Fix(normalize[F].apply(ExprBuilderF(src0, outerExpr))),
              inputs,
              op).some
          case GroupBuilderF(wb0, key, Expr(\/-($var(DocVar.ROOT(None))))) =>
            GroupBuilderF(
              Fix(normalize[F].apply(ExprBuilderF(wb0, outerExpr))),
              key,
              Expr(\/-($$ROOT))).some
          case GroupBuilderF(wb0, key, contents) =>
            inln(outerExpr, contents).map(expr => GroupBuilderF(Fix(normalize[F].apply(ExprBuilderF(wb0, \/-(expr)))), key, Expr(\/-($$ROOT))))
          case _ => None
        }
      case DocBuilderF(Fix(DocBuilderF(src, innerShape)), outerShape) =>
        outerShape.traverse(collapse(_, innerShape)).map(DocBuilderF(src, _))
      case DocBuilderF(Fix(ExprBuilderF(src, innerExpr)), outerShape) =>
        outerShape.traverse(inln(_, Expr(innerExpr))).map(exes => DocBuilderF(src, exes ∘ (_.right)))
      case DocBuilderF(Fix(ShapePreservingBuilderF(src, inputs, op)), shape) =>
        ShapePreservingBuilderF(Fix(normalize[F].apply(DocBuilderF(src, shape))), inputs, op).some
      case _ => None
    }
  }

  def normalize[F[_]: Coalesce](implicit ev0: WorkflowOpCoreF :<: F, exprOps: ExprOpOps.Uni[ExprOp]) =
    repeatedly(normalizeƒ[F]) _

  private def rewriteObjRefs
    (obj: ListMap[BsonField.Name, GroupValue[Fix[ExprOp]]])
    (f: PartialFunction[DocVar, DocVar])
    (implicit exprOps: ExprOpOps.Uni[ExprOp]) =
    obj ∘ (_.bimap(accumulator.rewriteGroupRefs(_)(f), _.cata(exprOps.rewriteRefs(f))))

  private def rewriteGroupRefs
    (contents: GroupContents)
    (f: PartialFunction[DocVar, DocVar])
    (implicit exprOps: ExprOpOps.Uni[ExprOp]) =
    contents match {
      case Expr(expr) =>
        Expr(expr.bimap(accumulator.rewriteGroupRefs(_)(f), _.cata(exprOps.rewriteRefs(f))))
      case Doc(doc)   => Doc(rewriteObjRefs(doc)(f))
    }

  private def rewriteDocPrefix(doc: ListMap[BsonField.Name, Expr], base: Base)
      (implicit exprOps: ExprOpOps.Uni[ExprOp]): ListMap[BsonField.Name, Expr] =
    doc ∘ (rewriteExprPrefix(_, base))

  private def rewriteExprPrefix(expr: Expr, base: Base)
      (implicit exprOps: ExprOpOps.Uni[ExprOp]): Expr =
    expr.bimap(base.toDocVar.toJs >>> _, _.cata(exprOps.rewriteRefs(prefixBase0(base))))

  private def prefixBase0(base: Base): PartialFunction[DocVar, DocVar] =
    prefixBase(base.toDocVar)

  type EitherE[X] = PlannerError \/ X
  type M[X] = StateT[EitherE, NameGen, X]

  // Wrappers for results that don't use state:
  def emit[A](a: A): M[A] = quasar.namegen.emit[EitherE, A](a)
  def fail[A](e: PlannerError): M[A] = lift(-\/(e))
  def lift[A](v: EitherE[A]): M[A] = quasar.namegen.lift[EitherE](v)
  def emitSt[A](v: State[NameGen, A]): M[A] = emitName[EitherE, A](v)
  def swapM[A](v: State[NameGen, PlannerError \/ A]): M[A] =
    StateT[EitherE, NameGen, A](s => { val (s1, x) = v.run(s); x.map(s1 -> _) })

  def commonMap[K, A, B](m: ListMap[K, A \/ B])(f: B => PlannerError \/ A):
      PlannerError \/ (ListMap[K, A] \/ ListMap[K, B]) = {
    m.sequence.fold(
      κ((m ∘ (_.fold(\/.right, f))).sequence.map(\/.left)),
      l => \/-(\/-(l)))
  }

  private def commonShape(shape: ListMap[BsonField.Name, Expr])(implicit ev: ExprOpOps.Uni[ExprOp]) =
    commonMap(shape)(_.para(toJs[Fix[ExprOp], ExprOp]))

  private val jsBase = jscore.Name("__val")

  @SuppressWarnings(scala.Array("org.wartremover.warts.Recursion"))
  private def toCollectionBuilder[F[_]: Coalesce]
    (wb: WorkflowBuilder[F])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: RenderTree[WorkflowBuilder[F]], exprOps: ExprOpOps.Uni[ExprOp])
    : M[CollectionBuilderF[F]] =
    wb.unFix match {
      case cb @ CollectionBuilderF(_, _, _) => emit(cb)
      case ValueBuilderF(value) =>
        emit(CollectionBuilderF($pure[F](value), Root(), None))
      case ShapePreservingBuilderF(src, inputs, op) =>
        // At least one argument has no deref (e.g. $$ROOT)
        def case1(src: WorkflowBuilder[F], input: WorkflowBuilder[F], op: PartialFunction[List[BsonField], FixOp[F]], fields: List[Base]): M[CollectionBuilderF[F]] = {
          emitSt(freshName).flatMap(name =>
            fields.traverse(f => (DocField(name) \\ f.toDocVar).deref).fold(
              scala.sys.error("prefixed ${name}, but still no field"))(
              op.lift(_).fold(
                fail[CollectionBuilderF[F]](UnsupportedFunction(set.Filter, Some("failed to build operation"))))(
                op =>
                (toCollectionBuilder(src) |@| toCollectionBuilder(DocBuilder(input, ListMap(name -> \/-($$ROOT))))) {
                  case (
                    CollectionBuilderF(_, _, srcStruct),
                    CollectionBuilderF(graph, _, _)) =>
                    CollectionBuilderF(
                      chain(graph, op),
                      Field(name),
                      srcStruct)
                })))
        }
        // Every argument has a deref (so, a BsonField that can be given to the op)
        def case2(
          src: WorkflowBuilder[F],
          input: WorkflowBuilder[F],
          base: Base,
          op: PartialFunction[List[BsonField], FixOp[F]],
          fields: List[BsonField]):
            M[CollectionBuilderF[F]] = {
          ((toCollectionBuilder(src) |@| toCollectionBuilder(input)) {
            case (
              CollectionBuilderF(_, _, srcStruct),
              CollectionBuilderF(graph, base0, bothStruct)) =>
              op.lift(fields.map(f => base0.toDocVar.deref.map(_ \ f).getOrElse(f))).fold[M[CollectionBuilderF[F]]](
                fail[CollectionBuilderF[F]](UnsupportedFunction(set.Filter, Some("failed to build operation"))))(
                { op =>
                  val g = chain(graph, op)
                  if (srcStruct ≟ bothStruct)
                    emit(CollectionBuilderF(g, base0 \ base, srcStruct))
                  else {
                    val (g1, base1) = shift(base0 \ base, srcStruct, g)
                    emit(CollectionBuilderF(g1, base1, srcStruct))
                  }
                })
          }).join
        }
        inputs match {
          case Nil =>
            toCollectionBuilder(src).map {
              case CollectionBuilderF(g, b, s) =>
                CollectionBuilderF(op(Nil)(g), b, s)
            }
          case _ =>
            foldBuilders(src, inputs).flatMap { case (input1, base, fields) =>
              fields.traverse(_.toDocVar.deref).fold(
                case1(src, input1, op, fields))(
                case2(src, input1, base, op, _))
            }
        }
      case ExprBuilderF(src, \/-($var(d))) =>
        toCollectionBuilder(src).map {
          case CollectionBuilderF(graph, base, _) =>
            CollectionBuilderF(graph, base \ fromDocVar(d), None)
        }
      case ExprBuilderF(src, expr) =>
        (toCollectionBuilder(src) ⊛ emitSt(freshName))((cb, name) =>
          cb match {
            case CollectionBuilderF(graph, base, _) =>
              CollectionBuilderF(
                chain(graph,
                  rewriteExprPrefix(expr, base).fold(
                    js => $simpleMap[F](NonEmptyList(MapExpr(JsFn(jsBase, jscore.Obj(ListMap(jscore.Name(name.asText) -> js(jscore.Ident(jsBase))))))), ListMap()),
                    op => $project[F](Reshape(ListMap(name -> \/-(op)))))),
                Field(name),
                None)
          })
      case DocBuilderF(src, shape) =>
        generateWorkflow(src).flatMap { case (wf, base) =>
          commonShape(rewriteDocPrefix(shape, base)).fold(
            fail(_),
            s => shape.keys.toList.toNel.fold[M[CollectionBuilderF[F]]](
              fail(InternalError fromMsg "A shape with no fields does not make sense"))(
              fields => emit(CollectionBuilderF(
                chain(wf,
                  s.fold(
                    jsExprs => $simpleMap[F](NonEmptyList(
                      MapExpr(JsFn(jsBase,
                        jscore.Obj(jsExprs.map {
                          case (name, expr) => jscore.Name(name.asText) -> expr(jscore.Ident(jsBase))
                        })))),
                      ListMap()),
                    exprOps => $project[F](Reshape(exprOps ∘ \/.right)))),
                Root(),
                fields.some))))
        }
      case ArrayBuilderF(src, shape) =>
        generateWorkflow(src).flatMap { case (wf, base) =>
          lift(shape.traverse(exprToJs).map(jsExprs =>
            CollectionBuilderF(
              chain(wf,
                $simpleMap[F](NonEmptyList(
                  MapExpr(JsFn(jsBase,
                    jscore.Arr(jsExprs.map(_(base.toDocVar.toJs(jscore.Ident(jsBase))
                    )).toList)))),
                  ListMap())),
              Root(),
              None)))
        }
      case GroupBuilderF(src, keys, content) =>
        (foldBuilders(src, keys) |@| toCollectionBuilder(src)){ case ((wb, base, fields), CollectionBuilderF(_, _, struct)) =>
          def key(base: Base) = keys.zip(fields) match {
            case Nil        => \/-($literal(Bson.Null))
            case (key, field) :: Nil => key.unFix match {
              // NB: normalize to Null, to ease merging
              case ValueBuilderF(_) | ExprBuilderF(_, \/-($literal(_))) =>
                \/-($literal(Bson.Null))
              case _ =>
                field match {
                  // NB: MongoDB doesn’t allow arrays _as_ `_id`, but it allows
                  //     them _in_ `_id`, so we wrap any field in a document to
                  //     protect against arrays.
                  // TODO: Once we have type information available to the
                  //       planner, don’t wrap fields that can’t be arrays.
                  case Root() | Field(_) => -\/(Reshape(ListMap(
                    BsonField.Name("0") -> \/-($var(field.toDocVar).cata(exprOps.rewriteRefs(prefixBase0(base)))))))
                  case Subset(fields) => -\/(Reshape(fields.toList.map(fld =>
                    fld -> \/-($var(DocField(fld)))).toListMap))
                }
            }
            case _ => -\/(Reshape(fields.map(_.toDocVar).zipWithIndex.map {
              case (field, index) =>
                BsonField.Name(index.toString) -> \/-($var(field))
            }.toListMap).rewriteRefs(prefixBase0(base)))
          }

          content match {
            case Expr(-\/(grouped)) =>
              (toCollectionBuilder(wb) ⊛ emitSt(freshName))((cb, rootName) =>
                cb match {
                  case CollectionBuilderF(wf, base0, _) =>
                    CollectionBuilderF(
                      chain(wf,
                        $group[F](Grouped(ListMap(rootName -> grouped)).rewriteRefs(prefixBase0(base0 \ base)),
                          key(base0))),
                      Field(rootName),
                      struct)
                })
            case Expr(\/-(expr)) =>
              // NB: This case just winds up a single value, then unwinds it.
              //     It’s effectively a no-op, so we just use the src and expr.
              toCollectionBuilder(ExprBuilder(src, \/-(expr)))
            case Doc(obj) =>
              val (grouped, ungrouped) =
                obj.foldLeft[(ListMap[BsonField.Name, AccumOp[Fix[ExprOp]]], ListMap[BsonField.Name, Fix[ExprOp]])]((ListMap.empty[BsonField.Name, AccumOp[Fix[ExprOp]]], ListMap.empty[BsonField.Name, Fix[ExprOp]]))((acc, item) =>
                  item match {
                    case (k, -\/(v)) =>
                      ((x: ListMap[BsonField.Name, AccumOp[Fix[ExprOp]]]) => x + (k -> v)).first(acc)
                    case (k, \/-(v)) =>
                      ((x: ListMap[BsonField.Name, Fix[ExprOp]]) => x + (k -> v)).second(acc)
                  })
              if (grouped.isEmpty && !ungrouped.isEmpty)
                toCollectionBuilder(DocBuilder(src, ungrouped ∘ (_.right)))
              else
                obj.keys.toList.toNel.fold[M[CollectionBuilderF[F]]](
                  generateWorkflow(wb) ∘ { case (wf, base0) =>
                    CollectionBuilderF(
                      chain(wf, $group(Grouped(ListMap()), key(base0))),
                      Field(BsonField.Name("_id")),
                      none)
                  })(
                  fields => generateWorkflow(wb).flatMap { case (wf, base0) =>
                    emitSt(ungrouped.toList match {
                      case Nil =>
                        state[NameGen, Fix[F]](chain(wf,
                          $group[F](Grouped(grouped).rewriteRefs(prefixBase0(base0 \ base)), key(base0))))
                      case (name -> _) :: Nil =>
                        state[NameGen, Fix[F]](chain(wf,
                          $group[F](Grouped(
                            obj.transform {
                              case (_, -\/(v)) =>
                                accumulator.rewriteGroupRefs(v)(prefixBase0(base0 \ base))
                              case (_, \/-(v)) =>
                                $push(v.cata(exprOps.rewriteRefs(prefixBase0(base0 \ base))))
                            }),
                            key(base0)),
                          $unwind[F](DocField(name))))
                      case _ => for {
                        ungroupedName <- freshName
                        groupedName <- freshName
                      } yield
                        chain(wf,
                          $project[F](Reshape(ListMap(
                            ungroupedName -> -\/(Reshape(ungrouped.map {
                              case (k, v) => k -> \/-(v.cata(exprOps.rewriteRefs(prefixBase0(base0 \ base))))
                            })),
                            groupedName -> \/-($$ROOT)))),
                          $group[F](Grouped(
                            (grouped ∘ (accumulator.rewriteGroupRefs(_)(prefixBase0(Field(groupedName) \ base0)))) +
                              (ungroupedName -> $push($var(DocField(ungroupedName))))),
                            key(Field(groupedName) \ base0)),
                          $unwind[F](DocField(ungroupedName)),
                          $project[F](Reshape(obj.transform {
                            case (k, -\/ (_)) => \/-($var(DocField(k)))
                            case (k,  \/-(_)) => \/-($var(DocField(ungroupedName \ k)))
                          })))
                    }).map(CollectionBuilderF(
                      _,
                      Root(),
                      fields.some))
                  })
          }
        }.join
      case FlatteningBuilderF(src, fields) =>
        toCollectionBuilder(src).map {
          case CollectionBuilderF(graph, base, struct) =>
            CollectionBuilderF(fields.foldRight(graph) {
              case (StructureType.Array(field), acc) => $unwind[F](base.toDocVar \\ field).apply(acc)
              case (StructureType.Object(field), acc) =>
                $simpleMap[F](NonEmptyList(FlatExpr(JsFn(jsBase, (base.toDocVar \\ field).toJs(jscore.Ident(jsBase))))), ListMap()).apply(acc)
            }, base, struct)
        }
      case sb @ SpliceBuilderF(_, _) =>
        generateWorkflow(sb.src).flatMap { case (wf, base) =>
          lift(
            sb.toJs.map { splice =>
              CollectionBuilderF(
                chain(wf,
                  $simpleMap[F](NonEmptyList(MapExpr(JsFn(jsBase, (base.toDocVar.toJs >>> splice)(jscore.Ident(jsBase))))), ListMap())),
                Root(),
                None)
            })
        }
      case sb @ ArraySpliceBuilderF(_, _) =>
        generateWorkflow(sb.src).flatMap { case (wf, base) =>
          lift(
            sb.toJs.map { splice =>
              CollectionBuilderF(
                chain(wf,
                  $simpleMap[F](NonEmptyList(MapExpr(JsFn(jsBase, (base.toDocVar.toJs >>> splice)(jscore.Ident(jsBase))))), ListMap())),
                Root(),
                None)
            })
        }
    }

  def generateWorkflow[F[_]: Coalesce](wb: WorkflowBuilder[F])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: RenderTree[WorkflowBuilder[F]], ev2: ExprOpOps.Uni[ExprOp])
      : M[(Fix[F], Base)] =
    toCollectionBuilder(wb).map(x => (x.src, x.base))

  def shift[F[_]: Coalesce](base: Base, struct: Schema, graph: Fix[F])
    (implicit ev: WorkflowOpCoreF :<: F)
    : (Fix[F], Base) = {
    (base, struct) match {
      case (Field(ExprName), None) => (graph, Field(ExprName))
      case (_,       None)         =>
        (chain(graph,
          $project[F](Reshape(ListMap(ExprName -> \/-($var(base.toDocVar)))),
            ExcludeId)),
          Field(ExprName))
      case (_,       Some(fields)) =>
        (chain(graph,
          $project[F](
            Reshape(fields.map(name =>
              name -> \/-($var((base \ name).toDocVar))).toList.toListMap),
            if (fields.element(IdName)) IncludeId else ExcludeId)),
          Root())
    }
  }

  def build[F[_]: Coalesce](wb: WorkflowBuilder[F])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: RenderTree[WorkflowBuilder[F]], ev2: ExprOpOps.Uni[ExprOp])
    : M[Fix[F]] =
    toCollectionBuilder(wb).map {
      case CollectionBuilderF(graph, base, struct) =>
        if (base == Root()) graph
        else shift(base, struct, graph)._1
    }


  def asLiteral[F[_]](wb: WorkflowBuilder[F]): Option[Bson] = wb.unFix match {
    case ValueBuilderF(value)                  => Some(value)
    case ExprBuilderF(_, \/-($literal(value))) => Some(value)
    case _                                     => None
  }

  @SuppressWarnings(scala.Array("org.wartremover.warts.Recursion"))
  private def fold1Builders[F[_]: Coalesce](builders: List[WorkflowBuilder[F]])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: RenderTree[WorkflowBuilder[F]], exprOps: ExprOpOps.Uni[ExprOp])
    : Option[M[(WorkflowBuilder[F], List[Fix[ExprOp]])]] =
    builders match {
      case Nil             => None
      case builder :: Nil  => Some(emit((builder, List($$ROOT))))
      case Fix(ValueBuilderF(bson)) :: rest =>
        fold1Builders(rest).map(_.map { case (builder, fields) =>
          (builder, $literal(bson) +: fields)
        })
      case builder :: rest =>
        Some(rest.foldLeftM[M, (WorkflowBuilder[F], List[Fix[ExprOp]])](
          (builder, List($$ROOT))) {
          case ((wf, fields), Fix(ValueBuilderF(bson))) =>
            emit((wf, fields :+ $literal(bson)))
          case ((wf, fields), x) =>
            merge(wf, x).map { case (lbase, rbase, src) =>
              (src, fields.map(_.cata(exprOps.rewriteRefs(prefixBase0(lbase)))) :+ $var(rbase.toDocVar))
            }
        })
    }

  private def foldBuilders[F[_]: Coalesce](src: WorkflowBuilder[F], others: List[WorkflowBuilder[F]])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: RenderTree[WorkflowBuilder[F]], ev2: ExprOpOps.Uni[ExprOp])
    : M[(WorkflowBuilder[F], Base, List[Base])] =
    others.foldLeftM[M, (WorkflowBuilder[F], Base, List[Base])](
      (src, Root(), Nil)) {
      case ((wf, base, fields), x) =>
        merge(wf, x).map { case (lbase, rbase, src) =>
          (src, lbase \ base, fields.map(lbase \ _) :+ rbase)
        }
    }

  /** The location of the desired content relative to the current $$ROOT.
    *
    * Various transformations (merging, conversion to Workflow, etc.) combine
    * structures that we need to be able to extract later. This tells us how to
    * extract them.
    */
  sealed abstract class Base {
    def \ (that: Base): Base = (this, that) match {
      case (Root(),      _)            => that
      case (_,           Root())       => this
      case (Subset(_),   _)            => that // TODO: can we do better?
      case (_,           Subset(_))    => this // TODO: can we do better?
      case (Field(name), Field(name2)) => Field(name \ name2)
    }

    def \ (that: BsonField): Base = this \ Field(that)

    // NB: This is a lossy conversion.
    val toDocVar: DocVar = this match {
      case Root()      => DocVar.ROOT()
      case Field(name) => DocField(name)
      case Subset(_)   => DocVar.ROOT()
    }
  }

  object Base {
    implicit val show: Show[Base] = Show.showFromToString
  }

  /** The content is already at $$ROOT. */
  final case class Root()                              extends Base
  /** The content is nested in a field under $$ROOT. */
  final case class Field(name: BsonField)              extends Base
  /** The content is a subset of the document at $$ROOT. */
  final case class Subset(fields: Set[BsonField.Name]) extends Base

  val fromDocVar: DocVar => Base = {
    case DocVar.ROOT(None) => Root()
    case DocField(name)   => Field(name)
  }

  def mergeContents[A](c1: DocContents[A], c2: DocContents[A]):
      M[((Base, Base), DocContents[A])] = {
    def documentize(c: DocContents[A]):
        State[NameGen, (Base, ListMap[BsonField.Name, A])] =
      c match {
        case Doc(d) => state(Subset(d.keySet) -> d)
        case Expr(cont) =>
          freshName.map(name => (Field(name), ListMap(name -> cont)))
      }

    lazy val doc =
      swapM((documentize(c1) |@| documentize(c2)) {
        case ((lb, lshape), (rb, rshape)) =>
          Reshape.mergeMaps(lshape, rshape).fold[PlannerError \/ ((Base, Base), DocContents[A])](
            -\/(InternalError.fromMsg(s"conflicting fields when merging contents: $lshape, $rshape")))(
            map => {
              val llb = if (Subset(map.keySet) == lb) Root() else lb
              val rrb = if (Subset(map.keySet) == rb) Root() else rb
              \/-((llb, rrb) -> Doc(map))
            })
      })

    if (c1 == c2)
      emit((Root(), Root()) -> c1)
    else
      (c1, c2) match {
        case (Expr(v), Doc(o)) =>
          o.find { case (_, e) => v == e }.fold(doc) {
            case (lField, _) =>
              emit((Field(lField), Root()) -> Doc(o))
          }
        case (Doc(o), Expr(v)) =>
          o.find { case (_, e) => v == e }.fold(doc) {
            case (rField, _) =>
              emit((Root(), Field(rField)) -> Doc(o))
          }
        case _ => doc
      }
  }

  trait Combine {
    def apply[A, B](a1: A, a2: A)(f: (A, A) => B): B
    def flip: Combine
  }
  val unflipped: Combine = new Combine { outer =>
    def apply[A, B](a1: A, a2: A)(f: (A, A) => B) = f(a1, a2)
    val flip = new Combine {
      def apply[A, B](a1: A, a2: A)(f: (A, A) => B) = f(a2, a1)
      val flip = outer
    }
  }

  // TODO: Cases that match on `$$ROOT` should be generalized to look up the
  //       shape of any DocVar in the source.
  @tailrec def findKeys[F[_]](wb: WorkflowBuilder[F]): Option[Base] = {
    wb.unFix match {
      case CollectionBuilderF(_, _, s2)             => s2.map(s => Subset(s.toSet))
      case DocBuilderF(_, shape)                    => Subset(shape.keySet).some
      case FlatteningBuilderF(src, _)               => findKeys(src)
      case GroupBuilderF(src, _, Expr(\/-($$ROOT))) => findKeys(src)
      case GroupBuilderF(_, _, Doc(obj))            => Subset(obj.keySet).some
      case ShapePreservingBuilderF(src, _, _)       => findKeys(src)
      case ExprBuilderF(src, \/-($$ROOT))           => findKeys(src)
      case ExprBuilderF(_, _)                       => Root().some
      case ValueBuilderF(Bson.Doc(shape))           => Subset(shape.keySet
                                                        .map(BsonField.Name(_))).some
      case _                                        => None
    }
  }

  private def findSort[F[_]: Coalesce]
    (src: WorkflowBuilder[F])
    (distincting: WorkflowBuilder[F] => M[WorkflowBuilder[F]])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: RenderTree[WorkflowBuilder[F]], ev2: ExprOpOps.Uni[ExprOp])
    : M[WorkflowBuilder[F]] = {
    @tailrec
    def loop(wb: WorkflowBuilder[F]): M[WorkflowBuilder[F]] =
      wb.unFix match {
        case spb @ ShapePreservingBuilderF(spbSrc, sortKeys, f) =>
          spb.dummyOp.unFix match {
            case ev0($SortF(_, _)) =>
              foldBuilders(src, sortKeys).flatMap {
                case (newSrc, dv, ks) =>
                  distincting(newSrc).map { dist =>
                    val spb = ShapePreservingBuilder(
                      Fix(normalize[F].apply(ExprBuilderF(dist, \/-($var(dv.toDocVar))))),
                      ks.map(k => Fix(normalize[F].apply(ExprBuilderF(dist, \/-($var(k.toDocVar)))))), f)
                    dv match {
                      case Subset(ks) => DocBuilder(spb, ks.toList.map(k => k -> \/-($var(DocField(k)))).toListMap)
                      case _ => spb
                    }
                  }
              }
            case _ => loop(spbSrc)
          }
        case _ => distincting(src)
      }
    loop(src)
  }

  @SuppressWarnings(scala.Array("org.wartremover.warts.Recursion", "org.wartremover.warts.ToString"))
  private def merge[F[_]: Coalesce]
    (left: Fix[WorkflowBuilderF[F, ?]], right: Fix[WorkflowBuilderF[F, ?]])
    (implicit I: WorkflowOpCoreF :<: F, ev0: RenderTree[Fix[WorkflowBuilderF[F, ?]]], ev1: ExprOpOps.Uni[ExprOp])
    : M[(Base, Base, Fix[WorkflowBuilderF[F, ?]])] = {
    def delegate =
      merge(right, left).map { case (r, l, merged) => (l, r, merged) }

    (left.unFix, right.unFix) match {
      case (
        ExprBuilderF(src1, \/-($var(DocField(base1)))),
        ExprBuilderF(src2, \/-($var(DocField(base2)))))
          if src1 ≟ src2 =>
        emit((Field(base1), Field(base2), src1))

      case _ if left ≟ right => emit((Root(), Root(), left))

      case (ValueBuilderF(bson), ExprBuilderF(src, expr)) =>
        mergeContents(Expr(\/-($literal(bson))), Expr(expr)).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase, contentsToBuilder(cont)(src))
        }
      case (ExprBuilderF(_, _), ValueBuilderF(_)) => delegate

      case (ValueBuilderF(bson), DocBuilderF(src, shape)) =>
        mergeContents(Expr(\/-($literal(bson))), Doc(shape)).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase, contentsToBuilder(cont)(src))
        }
      case (DocBuilderF(_, _), ValueBuilderF(_)) => delegate

      case (ValueBuilderF(bson), _) =>
        mergeContents(Expr(\/-($literal(bson))), Expr(\/-($$ROOT))).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase, contentsToBuilder(cont)(right))
        }
      case (_, ValueBuilderF(_)) => delegate

      case (ExprBuilderF(src1, expr1), ExprBuilderF(src2, expr2)) if src1 ≟ src2 =>
        mergeContents(Expr(expr1), Expr(expr2)).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase, contentsToBuilder(cont)(src1))
        }
      case (ExprBuilderF(src, \/-($var(DocField(base)))), _) if src ≟ right =>
        emit((Field(base), Root(), right))
      case (_, ExprBuilderF(src, \/-($var(DocField(_))))) if left ≟ src =>
        delegate

      case (DocBuilderF(src1, shape1), ExprBuilderF(src2, expr2)) =>
        merge(src1, src2).flatMap { case (lb, rb, wb) =>
          mergeContents(Doc(rewriteDocPrefix(shape1, lb)), Expr(rewriteExprPrefix(expr2, rb))).map {
            case ((lbase, rbase), cont) =>
              (lbase, rbase, contentsToBuilder(cont)(wb))
          }
        }
      case (ExprBuilderF(_, _), DocBuilderF(_, _)) =>
        delegate

      case (DocBuilderF(src1, shape1), DocBuilderF(src2, shape2)) =>
        merge(src1, src2).flatMap { case (lbase, rbase, wb) =>
          mergeContents(
            Doc(rewriteDocPrefix(shape1, lbase)),
            Doc(rewriteDocPrefix(shape2, rbase))).map {
            case ((lbase, rbase), cont) =>
              (lbase, rbase, contentsToBuilder(cont)(wb))
          }
        }

      // NB: The SPB cases need to be handled fairly early, because it allows
      //     them to stay closer to the root of the Builder.
      case (
        spb1 @ ShapePreservingBuilderF(src1, inputs1, op1),
        spb2 @ ShapePreservingBuilderF(src2, inputs2, _))
          if inputs1 ≟ inputs2 && spb1.dummyOp == spb2.dummyOp =>
        merge(src1, src2).map { case (lbase, rbase, wb) =>
          (lbase, rbase, ShapePreservingBuilder(wb, inputs1, op1))
        }
      case (ShapePreservingBuilderF(src, inputs, op), _) =>
        merge(src, right).map { case (lbase, rbase, wb) =>
          (lbase, rbase, ShapePreservingBuilder(wb, inputs, op))
        }
      case (_, ShapePreservingBuilderF(src, inputs, op)) => delegate

      case (ExprBuilderF(src, expr), _) =>
        merge(src, right).flatMap { case (lbase, rbase, wb) =>
          mergeContents(Expr(rewriteExprPrefix(expr, lbase)), Expr(\/-($var(rbase.toDocVar)))).map {
            case ((lbase, rbase), cont) =>
              (lbase, rbase, contentsToBuilder(cont)(wb))
          }
        }
      case (_, ExprBuilderF(src, _)) => delegate

      case (DocBuilderF(src1, shape1), _) =>
        merge(src1, right).flatMap { case (lbase, rbase, wb) =>
          mergeContents(Doc(rewriteDocPrefix(shape1, lbase)), Expr(\/-($var(rbase.toDocVar)))).map {
            case ((lbase, rbase), cont) =>
              (lbase, rbase, contentsToBuilder(cont)(wb))
          }
        }
      case (_, DocBuilderF(_, _)) => delegate

      case (sb @ SpliceBuilderF(_, _), _) =>
        merge(sb.src, right).flatMap { case (lbase, rbase, wb) =>
          for {
            lName  <- emitSt(freshName)
            rName  <- emitSt(freshName)
            splice <- lift(sb.toJs)
          } yield (Field(lName), Field(rName),
            DocBuilder(wb, ListMap(
              lName -> -\/ (lbase.toDocVar.toJs >>> splice),
              rName ->  \/-($var(rbase.toDocVar)))))
        }
      case (_, SpliceBuilderF(_, _)) => delegate

      case (sb @ ArraySpliceBuilderF(_, _), _) =>
        merge(sb.src, right).flatMap { case (lbase, rbase, wb) =>
          for {
            lName  <- emitSt(freshName)
            rName  <- emitSt(freshName)
            splice <- lift(sb.toJs)
          } yield (Field(lName), Field(rName),
            DocBuilder(wb, ListMap(
              lName -> -\/ (lbase.toDocVar.toJs >>> splice),
              rName ->  \/-($var(rbase.toDocVar)))))
        }
      case (_, ArraySpliceBuilderF(_, _)) => delegate

      case (
        FlatteningBuilderF(src0, fields0),
        FlatteningBuilderF(src1, fields1)) =>
        left.cata(height) cmp right.cata(height) match {
          case Ordering.LT =>
            merge(left, src1).map { case (lbase, rbase, wb) =>
              (lbase, rbase, FlatteningBuilder(wb, fields1.map(_.map(rbase.toDocVar \\ _))))
            }
          case Ordering.EQ =>
            merge(src0, src1).map { case (lbase, rbase, wb) =>
              val lfields = fields0.map(_.map(lbase.toDocVar \\ _))
              val rfields = fields1.map(_.map(rbase.toDocVar \\ _))
              (lbase, rbase, FlatteningBuilder(wb, lfields union rfields))
            }
          case Ordering.GT =>
            merge(src0, right).map { case (lbase, rbase, wb) =>
              (lbase, rbase, FlatteningBuilder(wb, fields0.map(_.map(lbase.toDocVar \\ _))))
            }
        }
      case (FlatteningBuilderF(src, fields), _) =>
        merge(src, right).flatMap { case (lbase, rbase, wb) =>
          val lfields = fields.map(_.map(lbase.toDocVar \\ _))
          if (lfields.exists(x => x.field.startsWith(rbase.toDocVar) || rbase.toDocVar.startsWith(x.field)))
            for {
              lName <- emitSt(freshName)
              rName <- emitSt(freshName)
            } yield
              (Field(lName), Field(rName),
                FlatteningBuilder(
                  DocBuilder(wb, ListMap(
                    lName -> \/-($var(lbase.toDocVar)),
                    rName -> \/-($var(rbase.toDocVar)))),
                  fields.map(_.map(DocField(lName) \\ _))))
          else emit((lbase, rbase, FlatteningBuilder(wb, lfields)))
        }
      case (_, FlatteningBuilderF(_, _)) => delegate

      case (GroupBuilderF(src1, key1, cont1), GroupBuilderF(src2, key2, cont2))
          if key1 ≟ key2 =>
        merge(src1, src2).flatMap { case (lbase, rbase, wb) =>
          mergeContents(rewriteGroupRefs(cont1)(prefixBase0(lbase)), rewriteGroupRefs(cont2)(prefixBase0(rbase))).map {
            case ((lb, rb), contents) =>
              (lb, rb, GroupBuilder(wb, key1, contents))
          }
        }

      case (ArrayBuilderF(src, shape), _) =>
        merge(src, right).flatMap { case (lbase, rbase, wb) =>
          generateWorkflow(ArrayBuilder(wb, shape.map(rewriteExprPrefix(_, lbase)))).flatMap { case (wf, base) =>
            I.prj(wf.unFix).cata(
              {
                case $ProjectF(psrc, Reshape(shape), idx) =>
                  emitSt(freshName.map(rName =>
                    (lbase, Field(rName),
                      CollectionBuilder(
                        chain(psrc,
                          $project[F](Reshape(shape + (rName -> \/-($var(rbase.toDocVar)))))),
                        Root(),
                        None))))
                case _ => fail(InternalError fromMsg "couldn’t merge array")
              },
              {
                // TODO: Find a way to print this without using toString
                val msg = "couldn’t merge unrecognized op: " + wf.toString
                fail(InternalError.fromMsg(msg))
              })
          }
        }
      case (_, ArrayBuilderF(_, _)) => delegate

      case _ =>
        fail(InternalError.fromMsg("failed to merge:\n" + left.render.shows + "\n" + right.render.shows))
    }
  }

  final class Ops[F[_]: Coalesce](implicit ev0: WorkflowOpCoreF :<: F, ev1: ExprOpOps.Uni[ExprOp]) {
    def read(coll: Collection): WorkflowBuilder[F] =
      CollectionBuilder($read[F](coll), Root(), None)

    def pure(bson: Bson): Fix[WorkflowBuilderF[F, ?]] = ValueBuilder(bson)

    def limit(wb: WorkflowBuilder[F], count: Long): WorkflowBuilder[F] =
      ShapePreservingBuilder(wb, Nil, { case Nil => $limit[F](count) })

    def skip(wb: WorkflowBuilder[F], count: Long): WorkflowBuilder[F] =
      ShapePreservingBuilder(wb, Nil, { case Nil => $skip[F](count) })

    def filter
      (src: WorkflowBuilder[F],
        those: List[WorkflowBuilder[F]],
        sel: PartialFunction[List[BsonField], Selector])
      : WorkflowBuilder[F] =
      ShapePreservingBuilder(src, those, PartialFunction(fields => $match[F](sel(fields))))

    def expr1
      (wb: WorkflowBuilder[F])
      (f: Fix[ExprOp] => Fix[ExprOp])
      (implicit ev0: WorkflowOpCoreF :<: F, ev1: RenderTree[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] =
      expr(List(wb)) { case List(e) => f(e) }

    def expr2
      (wb1: WorkflowBuilder[F], wb2: WorkflowBuilder[F])
      (f: (Fix[ExprOp], Fix[ExprOp]) => Fix[ExprOp])
      (implicit ev2: RenderTree[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] =
      expr(List(wb1, wb2)) { case List(e1, e2) => f(e1, e2) }

    def expr
      (wbs: List[WorkflowBuilder[F]])
      (f: List[Fix[ExprOp]] => Fix[ExprOp])
      (implicit ev2: RenderTree[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] = {
      fold1Builders(wbs).fold[M[WorkflowBuilder[F]]](
        fail(InternalError fromMsg "impossible – no arguments"))(
        _.map { case (wb, exprs) => Fix(normalize[F].apply(ExprBuilderF(wb, \/-(f(exprs))))) })
    }

    // FIXME: no constraints
    def jsExpr1(wb: WorkflowBuilder[F], js: JsFn): WorkflowBuilder[F] =
      Fix(normalize[F].apply(ExprBuilderF(wb, -\/(js))))

    // Like JsExpr, but accepts a JsFn that expects to receive an array of values.
    def jsArrayExpr(wbs: List[WorkflowBuilder[F]], js: JsFn)
      (implicit ev2: RenderTree[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] =
      fold1Builders(wbs).fold[M[WorkflowBuilder[F]]](
        fail(InternalError.fromMsg("impossible – no arguments")))(
        _.flatMap { case (wb, exprs) =>
          lift(exprs.traverse[PlannerError \/ ?, JsFn](_.para(toJs)).map(jses =>
            jsExpr1(jsExpr1(wb, JsFn(jsBase, jscore.Arr(jses.map(_(jscore.Ident(jsBase)))))), js)))
        })

    def jsExpr(wbs: List[WorkflowBuilder[F]], f: List[JsCore] => JsCore)
      (implicit ev2: RenderTree[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] =
      fold1Builders(wbs).fold[M[WorkflowBuilder[F]]](
        fail(InternalError fromMsg "impossible – no arguments"))(
        _.flatMap { case (wb, exprs) =>
          lift(exprs.traverse[PlannerError \/ ?, JsFn](_.para(toJs)).map(jses =>
            jsExpr1(wb, JsFn(jsBase, f(jses.map(_(jscore.Ident(jsBase))))))))
        })

    @SuppressWarnings(scala.Array("org.wartremover.warts.Recursion"))
    def makeObject(wb: WorkflowBuilder[F], name: String): WorkflowBuilder[F] =
      wb.unFix match {
        case ValueBuilderF(value) =>
          ValueBuilder(Bson.Doc(ListMap(name -> value)))
        case GroupBuilderF(src, key, Expr(cont)) =>
          GroupBuilder(src, key, Doc(ListMap(BsonField.Name(name) -> cont)))
        case ExprBuilderF(src, expr) =>
          DocBuilder(src, ListMap(BsonField.Name(name) -> expr))
        case ShapePreservingBuilderF(src, inputs, op) =>
          ShapePreservingBuilder(makeObject(src, name), inputs, op)
        case _ =>
          DocBuilder(wb, ListMap(BsonField.Name(name) -> \/-($$ROOT)))
      }

    def makeArray(wb: WorkflowBuilder[F]): WorkflowBuilder[F] = wb.unFix match {
      case ValueBuilderF(value) => ValueBuilder(Bson.Arr(List(value)))
      case _ => ArrayBuilder(wb, List(\/-($$ROOT)))
    }

    @SuppressWarnings(scala.Array("org.wartremover.warts.Recursion"))
    def flattenMap(wb: WorkflowBuilder[F]): WorkflowBuilder[F] =
      wb.unFix match {
        case ShapePreservingBuilderF(src, inputs, op) =>
          ShapePreservingBuilder(flattenMap(src), inputs, op)
        case GroupBuilderF(src, keys, Expr(\/-($var(DocVar.ROOT(None))))) =>
          GroupBuilder(flattenMap(src), keys, Expr(\/-($$ROOT)))
        case _ => FlatteningBuilder(wb, Set(StructureType.Object(DocVar.ROOT())))
      }

    @SuppressWarnings(scala.Array("org.wartremover.warts.Recursion"))
    def flattenArray(wb: WorkflowBuilder[F]): WorkflowBuilder[F] =
      wb.unFix match {
        case ShapePreservingBuilderF(src, inputs, op) =>
          ShapePreservingBuilder(flattenArray(src), inputs, op)
        case GroupBuilderF(src, keys, Expr(\/-($$ROOT))) =>
          GroupBuilder(flattenArray(src), keys, Expr(\/-($$ROOT)))
        case _ => FlatteningBuilder(wb, Set(StructureType.Array(DocVar.ROOT())))
      }

    @SuppressWarnings(scala.Array("org.wartremover.warts.Recursion"))
    def projectField(wb: WorkflowBuilder[F], name: String): PlannerError \/ WorkflowBuilder[F] =
      wb.unFix match {
        case ShapePreservingBuilderF(src, inputs, op) =>
          projectField(src, name).map(ShapePreservingBuilder(_, inputs, op))
        case ValueBuilderF(Bson.Doc(fields)) =>
          fields.get(name).fold[PlannerError \/ WorkflowBuilder[F]](
            -\/(UnsupportedFunction(structural.ObjectProject, Some("value does not contain a field ‘" + name + "’."))))(
            x => \/-(ValueBuilder(x)))
        case ValueBuilderF(_) =>
          -\/(UnsupportedFunction(structural.ObjectProject, Some("value is not a document.")))
        case GroupBuilderF(wb0, key, Expr(\/-($var(dv)))) =>
          projectField(wb0, name).map(GroupBuilder(_, key, Expr(\/-($var(dv)))))
        case GroupBuilderF(wb0, key, Doc(doc)) =>
          doc.get(BsonField.Name(name)).fold[PlannerError \/ WorkflowBuilder[F]](
            -\/(UnsupportedFunction(structural.ObjectProject, Some("group does not contain a field ‘" + name + "’."))))(
            x => \/-(GroupBuilder(wb0, key, Expr(x))))
        case DocBuilderF(wb, doc) =>
          doc.get(BsonField.Name(name)).fold[PlannerError \/ WorkflowBuilder[F]](
            -\/(UnsupportedFunction(structural.ObjectProject, Some("document does not contain a field ‘" + name + "’."))))(
            expr => \/-(ExprBuilder(wb, expr)))
        case ExprBuilderF(wb0,  -\/(js1)) =>
          \/-(ExprBuilder(wb0,
            -\/(JsFn(jsBase, DocField(BsonField.Name(name)).toJs(js1(jscore.Ident(jsBase)))))))
        case ExprBuilderF(wb, \/-($var(DocField(field)))) =>
          \/-(ExprBuilder(wb, \/-($var(DocField(field \ BsonField.Name(name))))))
        case _ => \/-(ExprBuilder(wb, \/-($var(DocField(BsonField.Name(name))))))
      }

    def projectIndex(wb: WorkflowBuilder[F], index: Int): PlannerError \/ WorkflowBuilder[F] =
      wb.unFix match {
        case ValueBuilderF(Bson.Arr(elems)) =>
          if (index < elems.length) // UGH!
            \/-(ValueBuilder(elems(index)))
          else
            -\/(UnsupportedFunction(
              structural.ArrayProject,
              Some(s"value does not contain index ‘$index’.")))
        case ArrayBuilderF(wb0, elems) =>
          if (index < elems.length) // UGH!
            \/-(ExprBuilder(wb0, elems(index)))
          else
            -\/(UnsupportedFunction(
              structural.ArrayProject,
              Some(s"array does not contain index ‘$index’.")))
        case ValueBuilderF(_) =>
          -\/(UnsupportedFunction(
            structural.ArrayProject,
            Some("value is not an array.")))
        case DocBuilderF(_, _) =>
          -\/(UnsupportedFunction(
            structural.ArrayProject,
            Some("value is not an array.")))
        case _ =>
          jsExpr1(wb, JsFn(jsBase,
            jscore.Access(jscore.Ident(jsBase), jscore.Literal(Js.num(index.toLong))))).right
      }

    @SuppressWarnings(scala.Array("org.wartremover.warts.Recursion"))
    def deleteField(wb: WorkflowBuilder[F], name: String): PlannerError \/ WorkflowBuilder[F] =
      wb.unFix match {
        case ShapePreservingBuilderF(src, inputs, op) =>
          deleteField(src, name).map(ShapePreservingBuilder(_, inputs, op))
        case ValueBuilderF(Bson.Doc(fields)) =>
          \/-(ValueBuilder(Bson.Doc(fields - name)))
        case ValueBuilderF(_) =>
          -\/(UnsupportedFunction(
            structural.DeleteField,
            Some("value is not a document.")))
        case GroupBuilderF(wb0, key, Expr(\/-($$ROOT))) =>
          deleteField(wb0, name).map(GroupBuilder(_, key, Expr(\/-($$ROOT))))
        case GroupBuilderF(wb0, key, Doc(doc)) =>
          \/-(GroupBuilder(wb0, key, Doc(doc - BsonField.Name(name))))
        case DocBuilderF(wb0, doc) =>
          \/-(DocBuilder(wb0, doc - BsonField.Name(name)))
        case _ => jsExpr1(wb, JsFn(jsBase,
          // FIXME: Need to pull this back up from the top level (SD-665)
          jscore.Call(jscore.ident("remove"),
            List(jscore.Ident(jsBase), jscore.Literal(Js.Str(name)))))).right
      }

    def groupBy(src: WorkflowBuilder[F], keys: List[WorkflowBuilder[F]])
      : WorkflowBuilder[F] =
      GroupBuilder(src, keys, Expr(\/-($$ROOT)))

    @SuppressWarnings(scala.Array("org.wartremover.warts.Recursion"))
    def reduce(wb: WorkflowBuilder[F])(f: Fix[ExprOp] => AccumOp[Fix[ExprOp]]): WorkflowBuilder[F] =
      wb.unFix match {
        case GroupBuilderF(wb0, keys, Expr(\/-(expr))) =>
          GroupBuilder(wb0, keys, Expr(-\/(f(expr))))
        case ShapePreservingBuilderF(src @ Fix(GroupBuilderF(_, _, Expr(\/-(_)))), inputs, op) =>
          ShapePreservingBuilder(reduce(src)(f), inputs, op)
        case _ =>
          GroupBuilder(wb, Nil, Expr(-\/(f($$ROOT))))
      }

    def sortBy
      (src: WorkflowBuilder[F], keys: List[WorkflowBuilder[F]], sortTypes: List[SortDir])
        : WorkflowBuilder[F] =
      ShapePreservingBuilder(
        src,
        keys,
        // TODO[ASP]: This pattern match is non total!
        // possible solution: make sortTypes and the argument to this partial function NonEmpty
        _.zip(sortTypes) match {
          case x :: xs => $sort[F](NonEmptyList.nel(x, IList.fromList(xs)))
        })

    def distinct(src: WorkflowBuilder[F])
      (implicit ev2: RenderTree[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] =
      findKeys(src).fold(
        lift(deleteField(src, "_id")).flatMap(del => distinctBy(del, List(del))))(
        ks => ks match {
          case Root() => distinctBy(src, List(src))
          case Field(k) =>
            distinctBy(src, List(Fix(normalize[F].apply(ExprBuilderF(src, \/-($var(DocField(k))))))))
          case Subset(ks) =>
            val keys = ks.toList.map(k => Fix(normalize[F].apply(ExprBuilderF(src, \/-($var(DocField(k)))))))
            findSort(src) { newSrc =>
              findKeys(newSrc)
                .cata(
                  {
                    case Subset(fields) => fields.toList.map(k => k -> $first($var(DocField(k))).left[Fix[ExprOp]]).right
                    case b => InternalError.fromMsg(s"Expected a Subset but found $b").left
                  },
                  List().right)
                .fold(fail, i => GroupBuilder(newSrc, keys, Doc(i.toListMap)).point[M])
            }
        })

    def distinctBy(src: WorkflowBuilder[F], keys: List[WorkflowBuilder[F]])
      (implicit ev2: RenderTree[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] =
      findSort(src)(s => reduce(groupBy(s, keys))($first(_)).point[M])

    // TODO: handle concating value, expr, or collection with group (#439)
    def objectConcat(wb1: WorkflowBuilder[F], wb2: WorkflowBuilder[F])
      (implicit ev2: RenderTree[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] = {
      @SuppressWarnings(scala.Array("org.wartremover.warts.Recursion"))
      def impl(wb1: WorkflowBuilder[F], wb2: WorkflowBuilder[F], combine: Combine): M[WorkflowBuilder[F]] = {
        def delegate = impl(wb2, wb1, combine.flip)

        def mergeGroups(s1: WorkflowBuilder[F], s2: WorkflowBuilder[F], c1: GroupContents, c2: GroupContents, keys: List[WorkflowBuilder[F]]):
            M[((Base, Base), WorkflowBuilder[F])] =
          merge(s1, s2).flatMap { case (lbase, rbase, src) =>
            combine(
              rewriteGroupRefs(c1)(prefixBase0(lbase)),
              rewriteGroupRefs(c2)(prefixBase0(rbase)))(mergeContents(_, _)).map {
              case ((lb, rb), contents) =>
                combine(lb, rb)((_, _) -> GroupBuilder(src, keys, contents))
            }
          }

        (wb1.unFix, wb2.unFix) match {
          case (ShapePreservingBuilderF(s1, i1, o1), ShapePreservingBuilderF(s2, i2, o2))
              if i1 == i2 && o1 == o2 =>
            impl(s1, s2, combine).map(ShapePreservingBuilder(_, i1, o1))

          case (
            v1 @ ShapePreservingBuilderF(
              Fix(DocBuilderF(_, shape1)), inputs1, _),
            GroupBuilderF(
              Fix(v2 @ ShapePreservingBuilderF(_, inputs2, _)),
              Nil, _))
            if inputs1 ≟ inputs2 && v1.dummyOp == v2.dummyOp =>
            impl(GroupBuilder(wb1, Nil, Doc(shape1.keys.toList.map(n => n -> \/-($var(DocField(n)))).toListMap)), wb2, combine)
          case (
            GroupBuilderF(
            Fix(v1 @ ShapePreservingBuilderF(_, inputs1, op1)),
              Nil, _),
            v2 @ ShapePreservingBuilderF(Fix(DocBuilderF(_, _)), inputs2, op2))
            if inputs1 ≟ inputs2 && v1.dummyOp == v2.dummyOp => delegate

          case (
            v1 @ ShapePreservingBuilderF(
              Fix(DocBuilderF(_, shape1)), inputs1, _),
            DocBuilderF(
              Fix(GroupBuilderF(
                Fix(v2 @ ShapePreservingBuilderF(_, inputs2, _)),
                Nil, _)),
             shape2))
            if inputs1 ≟ inputs2 && v1.dummyOp == v2.dummyOp =>
            impl(GroupBuilder(wb1, Nil, Doc(shape1.keys.toList.map(n => n -> \/-($var(DocField(n)))).toListMap)), wb2, combine)
          case (
            DocBuilderF(
              Fix(GroupBuilderF(
                Fix(v1 @ ShapePreservingBuilderF(_, inputs1, _)),
                Nil, _)),
              shape2),
            v2 @ ShapePreservingBuilderF(Fix(DocBuilderF(_, _)), inputs2, _))
            if inputs1 ≟ inputs2 && v1.dummyOp == v2.dummyOp => delegate

          case (ShapePreservingBuilderF(s, i, o), _) =>
            impl(s, wb2, combine).map(ShapePreservingBuilder(_, i, o))
          case (_, ShapePreservingBuilderF(_, _, _)) => delegate

          case (ValueBuilderF(Bson.Doc(map1)), ValueBuilderF(Bson.Doc(map2))) =>
            emit(ValueBuilder(Bson.Doc(combine(map1, map2)(_ ++ _))))

          case (ValueBuilderF(Bson.Doc(map1)), DocBuilderF(s2, shape2)) =>
            emit(DocBuilder(s2,
              combine(
                map1.map { case (k, v) => BsonField.Name(k) -> \/-($literal(v)) },
                shape2)(_ ++ _)))
          case (DocBuilderF(_, _), ValueBuilderF(Bson.Doc(_))) => delegate

          case (ValueBuilderF(Bson.Doc(map1)), GroupBuilderF(s1, k1, Doc(c2))) =>
            val content = combine(
              map1.map { case (k, v) => BsonField.Name(k) -> -\/($first($literal(v))) },
              c2)(_ ++ _)
            emit(GroupBuilder(s1, k1, Doc(content)))
          case (GroupBuilderF(_, _, Doc(_)), ValueBuilderF(_)) => delegate

          case (
            GroupBuilderF(src1, keys1, Expr(\/-($$ROOT))),
            GroupBuilderF(src2, keys2, Expr(\/-($$ROOT))))
              if keys1 ≟ keys2 =>
            impl(src1, src2, combine).map(GroupBuilder(_, keys1, Expr(\/-($$ROOT))))

          case (
            GroupBuilderF(s1, keys1, c1 @ Doc(_)),
            GroupBuilderF(s2, keys2,    c2 @ Doc(_)))
              if keys1 ≟ keys2 =>
            mergeGroups(s1, s2, c1, c2, keys1).map(_._2)

          case (
            GroupBuilderF(s1, keys1, c1 @ Doc(d1)),
            DocBuilderF(Fix(GroupBuilderF(s2, keys2, c2)), shape2))
              if keys1 ≟ keys2 =>
            mergeGroups(s1, s2, c1, c2, keys1).map { case ((glbase, grbase), g) =>
              DocBuilder(g, combine(
                d1.transform { case (n, _) => \/-($var(DocField(n))) },
                shape2 ∘ (rewriteExprPrefix(_, grbase)))(_ ++ _))
            }
          case (
            DocBuilderF(Fix(GroupBuilderF(_, k1, _)), _),
            GroupBuilderF(_, k2, Doc(_)))
              if k1 ≟ k2 =>
            delegate

          case (
            DocBuilderF(Fix(GroupBuilderF(s1, keys1, c1)), shape1),
            DocBuilderF(Fix(GroupBuilderF(s2, keys2, c2)), shape2))
              if keys1 ≟ keys2 =>
            mergeGroups(s1, s2, c1, c2, keys1).flatMap {
              case ((glbase, grbase), g) =>
                emit(DocBuilder(g, combine(
                  shape1 ∘ (rewriteExprPrefix(_, glbase)),
                  shape2 ∘ (rewriteExprPrefix(_, grbase)))(_ ++ _)))
            }

          case (
            DocBuilderF(_, shape),
            GroupBuilderF(_, Nil, _)) =>
            impl(
              GroupBuilder(wb1, Nil, Doc(shape.map { case (n, _) => n -> \/-($var(DocField(n))) })),
              wb2,
              combine)
          case (
            GroupBuilderF(_, Nil, _),
            DocBuilderF(_, _)) =>
            delegate

          case (
            GroupBuilderF(_, _, Doc(cont1)),
            GroupBuilderF(_, Nil, _)) =>
            impl(
              GroupBuilder(wb1, Nil, Doc(cont1.map { case (n, _) => n -> \/-($var(DocField(n))) })),
              wb2,
              combine)
          case (
            GroupBuilderF(_, Nil, _),
            GroupBuilderF(_, _, _)) =>
            delegate

          case (
            DocBuilderF(_, shape),
            DocBuilderF(Fix(GroupBuilderF(_, Nil, _)), _)) =>
            impl(
              GroupBuilder(wb1, Nil, Doc(shape.map { case (n, _) => n -> \/-($var(DocField(n))) })),
              wb2,
              combine)
          case (
            DocBuilderF(Fix(GroupBuilderF(_, Nil, _)), _),
            DocBuilderF(_, _)) =>
            delegate

          case (DocBuilderF(s1, shape1), DocBuilderF(s2, shape2)) =>
            merge(s1, s2).map { case (lbase, rbase, src) =>
              DocBuilder(src, combine(
                rewriteDocPrefix(shape1, lbase),
                rewriteDocPrefix(shape2, rbase))(_ ++ _))
            }

          case (DocBuilderF(src1, shape), ExprBuilderF(src2, expr)) =>
            merge(src1, src2).map { case (left, right, list) =>
              SpliceBuilder(list, combine(
                Doc(rewriteDocPrefix(shape, left)),
                Expr(rewriteExprPrefix(expr, right)))(List(_, _)))
            }
          case (ExprBuilderF(_, _), DocBuilderF(_, _)) => delegate

          case (ExprBuilderF(src1, expr1), ExprBuilderF(src2, expr2)) =>
            merge(src1, src2).map { case (left, right, list) =>
              SpliceBuilder(list, combine(
                Expr(rewriteExprPrefix(expr1, left)),
                Expr(rewriteExprPrefix(expr2, right)))(List(_, _)))
            }

          case (SpliceBuilderF(src1, structure1), DocBuilderF(src2, shape2)) =>
            merge(src1, src2).map { case (left, right, list) =>
              SpliceBuilder(list, combine(
                structure1,
                List(Doc(rewriteDocPrefix(shape2, right))))(_ ++ _))
            }
          case (DocBuilderF(_, _), SpliceBuilderF(_, _)) => delegate

          case (SpliceBuilderF(src1, structure1), ExprBuilderF(src2, expr2)) =>
            merge(src1, src2).map { case (left, right, list) =>
              SpliceBuilder(list, combine(
                structure1,
                List(Expr(rewriteExprPrefix(expr2, right))))(_ ++ _))
            }
          case (ExprBuilderF(_, _), SpliceBuilderF(_, _)) => delegate

          case (SpliceBuilderF(src1, structure1), CollectionBuilderF(_, _, _)) =>
            merge(src1, wb2).map { case (_, right, list) =>
              SpliceBuilder(list, combine(
                structure1,
                List(Expr(\/-($var(right.toDocVar)))))(_ ++ _))
            }
          case (CollectionBuilderF(_, _, _), SpliceBuilderF(_, _)) => delegate

          case (DocBuilderF(src, shape), CollectionBuilderF(_, _, _)) =>
            merge(src, wb2).map { case (left, right, list) =>
              SpliceBuilder(list, combine(
                Doc(rewriteDocPrefix(shape, left)),
                Expr(\/-($var(right.toDocVar))))(List(_, _)))
            }
          case (CollectionBuilderF(_, _, _), DocBuilderF(_, _)) => delegate

          case (ValueBuilderF(Bson.Doc(map1)), CollectionBuilderF(_, base, _)) =>
            emit(SpliceBuilder(wb2,
              combine(
                Doc(map1.map { case (k, v) => BsonField.Name(k) -> \/-($literal(v)) }),
                Expr(\/-($$ROOT)))(List(_, _))))
          case (CollectionBuilderF(_, _, _), ValueBuilderF(Bson.Doc(_))) =>
            delegate

          case (ValueBuilderF(Bson.Doc(map1)), SpliceBuilderF(src, structure)) =>
            emit(SpliceBuilder(src,
              combine(
                List(Doc(map1.map { case (k, v) => BsonField.Name(k) -> \/-($literal(v)) })),
                structure)(_ ++ _)))
          case (SpliceBuilderF(_, _), ValueBuilderF(Bson.Doc(_))) =>
            delegate

          case (
            DocBuilderF(s1 @ Fix(
              ArraySpliceBuilderF(_, _)),
              shape1),
            GroupBuilderF(_, _, Doc(c2))) =>
            merge(s1, wb2).map { case (lbase, rbase, src) =>
              DocBuilder(src,
                combine(
                  rewriteDocPrefix(shape1, lbase),
                  c2.map { case (n, _) => (n, rewriteExprPrefix(\/-($var(DocField(n))), rbase)) })(_ ++ _))
            }
          case (GroupBuilderF(_, _, _), DocBuilderF(Fix(ArraySpliceBuilderF(_, _)), _)) => delegate

          case _ => fail(UnsupportedFunction(
            structural.ObjectConcat,
            Some("unrecognized shapes:\n" + wb1.render.shows + "\n" + wb2.render.shows)))
        }
      }

      impl(wb1, wb2, unflipped)
    }

    def arrayConcat(left: WorkflowBuilder[F], right: WorkflowBuilder[F])
      (implicit ev2: RenderTree[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] = {
      @SuppressWarnings(scala.Array("org.wartremover.warts.Recursion"))
      def impl(wb1: WorkflowBuilder[F], wb2: WorkflowBuilder[F], combine: Combine):
          M[WorkflowBuilder[F]] = {
        def delegate = impl(wb2, wb1, combine.flip)

        (wb1.unFix, wb2.unFix) match {
          case (ValueBuilderF(Bson.Arr(seq1)), ValueBuilderF(Bson.Arr(seq2))) =>
            emit(ValueBuilder(Bson.Arr(seq1 ++ seq2)))

          case (ValueBuilderF(Bson.Arr(seq)), ArrayBuilderF(src, shape)) =>
            emit(ArrayBuilder(src,
              combine(seq.map(x => \/-($literal(x))), shape)(_ ++ _)))
          case (ArrayBuilderF(_, _), ValueBuilderF(Bson.Arr(_))) => delegate

          case (
            ArrayBuilderF(Fix(
              v1 @ ShapePreservingBuilderF(src1, inputs1, op1)), shape1),
            v2 @ ShapePreservingBuilderF(src2, inputs2, op2))
            if inputs1 ≟ inputs2 && v1.dummyOp == v2.dummyOp =>
            merge(src1, src2).map { case (lbase, rbase, wb) =>
              ShapePreservingBuilder(
                ArraySpliceBuilder(wb, combine(
                  Array(shape1.map(rewriteExprPrefix(_, lbase))),
                  Expr(\/-($var(rbase.toDocVar))))(List(_, _))),
                inputs1, op1)
            }
          case (ShapePreservingBuilderF(_, in1, op1), ArrayBuilderF(Fix(ShapePreservingBuilderF(_, in2, op2)), _)) => delegate

          case (
            v1 @ ShapePreservingBuilderF(src1, inputs1, op1),
            ArrayBuilderF(Fix(
              GroupBuilderF(Fix(
                v2 @ ShapePreservingBuilderF(src2, inputs2, _)),
                Nil, cont2)),
              shape2))
              if inputs1 ≟ inputs2 && v1.dummyOp == v2.dummyOp =>
            merge(src1, src2).flatMap { case (lbase, rbase, wb) =>
              combine(Expr(\/-($var(lbase.toDocVar))), rewriteGroupRefs(cont2)(prefixBase0(rbase)))(mergeContents(_, _)).map { case ((lbase1, rbase1), cont) =>
                ShapePreservingBuilder(
                  ArraySpliceBuilder(
                    GroupBuilder(wb, Nil, cont),
                    combine(
                      Expr(\/-($var(lbase1.toDocVar))),
                      Array(shape2.map(rewriteExprPrefix(_, rbase1))))(List(_, _))),
                  inputs1, op1)
              }
            }
          case (
            ArrayBuilderF(Fix(
              GroupBuilderF(Fix(
                v1 @ ShapePreservingBuilderF(_, inputs1, _)),
                Nil, _)), _),
            v2 @ ShapePreservingBuilderF(_, inputs2, _))
              if inputs1 ≟ inputs2 && v1.dummyOp == v2.dummyOp => delegate

          case (ShapePreservingBuilderF(s, i, o), _) =>
            impl(s, wb2, combine).map(ShapePreservingBuilder(_, i, o))
          case (_, ShapePreservingBuilderF(_, _, _)) => delegate

          case (ArrayBuilderF(src1, shape1), ArrayBuilderF(src2, shape2)) =>
            merge(src1, src2).map { case (lbase, rbase, wb) =>
              ArrayBuilder(wb,
                shape1.map(rewriteExprPrefix(_, lbase)) ++
                  shape2.map(rewriteExprPrefix(_, rbase)))
            }
          case (ArrayBuilderF(src1, shape1), ExprBuilderF(src2, expr2)) =>
            merge(src1, src2).map { case (left, right, list) =>
              ArraySpliceBuilder(list, combine(
                Array(shape1.map(rewriteExprPrefix(_, left))),
                Expr(rewriteExprPrefix(expr2, right)))(List(_, _)))
            }
          case (ExprBuilderF(_, _), ArrayBuilderF(_, _)) => delegate

          case (ArrayBuilderF(src1, shape1), GroupBuilderF(_, _, _)) =>
            merge(src1, wb2).map { case (left, right, wb) =>
              ArraySpliceBuilder(wb, combine(
                Array(shape1.map(rewriteExprPrefix(_, left))),
                Expr(\/-($var(right.toDocVar))))(List(_, _)))
            }
          case (GroupBuilderF(_, _, _), ArrayBuilderF(_, _)) => delegate

          case (ValueBuilderF(Bson.Arr(seq1)), ExprBuilderF(src2, expr2)) =>
            emit(ArraySpliceBuilder(src2, combine(
              Array(seq1.map(x => \/-($literal(x)))),
              Expr(expr2))(List(_, _))))
          case (ExprBuilderF(_, _), ValueBuilderF(Bson.Arr(_))) => delegate

          case (ArraySpliceBuilderF(src1, structure1), ArrayBuilderF(src2, shape2)) =>
            merge(src1, src2).map { case (left, right, list) =>
              ArraySpliceBuilder(list, combine(
                structure1,
                List(Array(shape2.map(rewriteExprPrefix(_, right)))))(_ ++ _))
            }
          case (ArrayBuilderF(_, _), ArraySpliceBuilderF(_, _)) => delegate

          case (ArraySpliceBuilderF(src1, structure1), ExprBuilderF(src2, expr2)) =>
            merge(src1, src2).map { case (_, right, list) =>
              ArraySpliceBuilder(list, combine(
                structure1,
                List(Expr(rewriteExprPrefix(expr2, right))))(_ ++ _))
            }
          case (ExprBuilderF(_, _), ArraySpliceBuilderF(_, _)) => delegate

          case (ArraySpliceBuilderF(src1, structure1), ValueBuilderF(bson2)) =>
            emit(ArraySpliceBuilder(src1, combine(structure1, List(Expr(\/-($literal(bson2)))))(_ ++ _)))
          case (ValueBuilderF(_), ArraySpliceBuilderF(_, _)) => delegate

          case _ =>
            fail(UnsupportedFunction(
              structural.ArrayConcat,
              Some("values are not both arrays")))
        }
      }

      impl(left, right, unflipped)
    }

    def unionAll
      (left: WorkflowBuilder[F], right: WorkflowBuilder[F])
      (implicit ev2: RenderTree[WorkflowBuilder[F]])
        : M[WorkflowBuilder[F]] =
      (generateWorkflow(left) |@| generateWorkflow(right)) { case ((l, _), (r, _)) =>
        CollectionBuilder(
          $foldLeft(
            l,
            chain(r,
              $map($MapF.mapFresh, ListMap()),
              $reduce($ReduceF.reduceNOP, ListMap()))),
          Root(),
          None)
      }

    def union
      (left: WorkflowBuilder[F], right: WorkflowBuilder[F])
      (implicit ev2: RenderTree[WorkflowBuilder[F]])
        : M[WorkflowBuilder[F]] =
      (generateWorkflow(left) |@| generateWorkflow(right)) { case ((l, _), (r, _)) =>
        CollectionBuilder(
          $foldLeft(
            chain(l, $map($MapF.mapValKey, ListMap())),
            chain(r,
              $map($MapF.mapValKey, ListMap()),
              $reduce($ReduceF.reduceNOP, ListMap()))),
          Root(),
          None)
      }
  }
  object Ops {
    implicit def apply[F[_]: Coalesce](implicit ev0: WorkflowOpCoreF :<: F, ev1: ExprOpOps.Uni[ExprOp]): Ops[F] =
      new Ops[F]
  }
}
