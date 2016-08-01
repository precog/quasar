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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.{NonTerminal, RenderTree, Terminal}, RenderTree.ops._
import quasar.fp._
import quasar.namegen._
import quasar._, Planner._
import quasar.javascript._
import quasar.std.StdLib._
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.qscript._

import matryoshka._, Recursive.ops._, FunctorT.ops._
import scalaz._, Scalaz._

sealed trait WorkflowBuilderF[F[_], +A]

object WorkflowBuilder {
  import quasar.physical.mongodb.accumulator._
  import quasar.physical.mongodb.expression._
  import Workflow._
  import IdHandling._

  /** A partial description of a query that can be run on an instance of MongoDB */
  type WorkflowBuilder[F[_]] = Fix[WorkflowBuilderF[F, ?]]
  /** If we know what the shape is, represents the list of Fields. */
  type Schema = Option[NonEmptyList[BsonField.Name]]

  /** Either arbitrary javascript expression or Pipeline expression
    * An arbitrary javascript is more powerful but less performant because it
    * gets materialized into a Map/Reduce operation.
    */
  type Expr = JsFn \/ Expression
  private def exprToJs(expr: Expr) = expr.fold(\/-(_), toJs)
  implicit val ExprRenderTree: RenderTree[Expr] = new RenderTree[Expr] {
    def render(x: Expr) = x.fold(_.render, _.render)
  }

  /**
   * Like ValueBuilder, this is a Leaf node which can be used to construct a more complicated WorkflowBuilder.
   * Takes a value resulting from a Workflow and wraps it in a WorkflowBuilder.
   * For example: If you want to read from MongoDB and then project on a field, the read would be the
   * CollectionBuilder.
   * @param base Name, or names under which the values produced by the src will be found.
   *             It's most often `Root`, or else it's probably a temporary `Field`
   * @param struct In the case of read, it's None. In the case where we are converting a WorkflowBuilder into
   *               a Workflow, we have access to the shape of this Workflow and encode it in `struct`.
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
          case (_, index) => BsonField.Name("_" + index)
        })(
        // Nb. This read is an arbitrary value that allows us to compare the partial function
        $read[F](Collection("", "")))
  }
  object ShapePreservingBuilder {
    def apply[F[_]: Coalesce](
      src: WorkflowBuilder[F],
      inputs: List[WorkflowBuilder[F]],
      op: PartialFunction[List[BsonField], FixOp[F]])
      (implicit ev: WorkflowOpCoreF :<: F) =
      Fix[WorkflowBuilderF[F, ?]](new ShapePreservingBuilderF(src, inputs, op))
  }

  /**
   * A query that produces a constant value.
   */
  final case class ValueBuilderF[F[_]](value: Bson) extends WorkflowBuilderF[F, Nothing]
  object ValueBuilder {
    def apply[F[_]](value: Bson) = Fix[WorkflowBuilderF[F, ?]](new ValueBuilderF(value))
  }

  /**
   * A query that applies an Expression operator to a source (which could be multiple values)
   * You can think of Expression as a function application in MongoDB that accepts values and produces
   * new values. It's kind of like a map.
   * The shape coming out of an ExprBuilder is unknown because of the fact that the Expression can be arbitrary.
   * @param src The values on which to apply the Expression
   * @param expr The expression that procudes a new set of values given a set of values.
   */
  final case class ExprBuilderF[F[_], A](src: A, expr: Expr) extends WorkflowBuilderF[F, A]
  object ExprBuilder {
    def apply[F[_]](src: WorkflowBuilder[F], expr: Expr) =
      Fix[WorkflowBuilderF[F, ?]](new ExprBuilderF(src, expr))
  }

  /**
   * Same as an ExprBuilder but contains the shape of the resulting query.
   * The result is a document that maps the field Name to the resulting values
   * from applying the Expression associated with that name.
   * NB: The shape is more restrictive than \$project because we may need to
   * convert it to a GroupBuilder, and a nested Reshape can be realized with
   * a chain of DocBuilders, leaving the collapsing to Workflow.coalesce.
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

  sealed trait Contents[+A]
  sealed trait DocContents[+A] extends Contents[A]
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
  type GroupContents = DocContents[GroupValue[Expression]]

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

  sealed trait StructureType[A] {
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

  /**
    Holds a partially-unknown structure. `Expr` entries are unknown and `Doc`
    entries are known. There should be at least one Expr in the list, otherwise
    it should be a DocBuilder.
    */
  final case class SpliceBuilderF[F[_], A](src: A, structure: List[DocContents[Expr]])
      extends WorkflowBuilderF[F, A] {
    def toJs: PlannerError \/ JsFn =
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
    def toJs: PlannerError \/ JsFn =
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

  // NB: This instance can’t be derived, because of `dummyOp`.
  implicit def WorkflowBuilderEqualF[F[_]: Coalesce](implicit ev: WorkflowOpCoreF :<: F)
    : EqualF[WorkflowBuilderF[F, ?]] =
    new EqualF[WorkflowBuilderF[F, ?]] {
      def equal[A: Equal](v1: WorkflowBuilderF[F, A], v2: WorkflowBuilderF[F, A]) = (v1, v2) match {
        case (CollectionBuilderF(g1, b1, s1), CollectionBuilderF(g2, b2, s2)) =>
          g1 == g2 && b1 == b2 && s1 ≟ s2
        case (v1 @ ShapePreservingBuilderF(s1, i1, _), v2 @ ShapePreservingBuilderF(s2, i2, _)) =>
          s1 ≟ s2 && i1 ≟ i2 && v1.dummyOp == v2.dummyOp
        case (ValueBuilderF(v1), ValueBuilderF(v2)) => v1 == v2
        case (ExprBuilderF(s1, e1), ExprBuilderF(s2, e2)) =>
          s1 ≟ s2 && e1 == e2
        case (DocBuilderF(s1, e1), DocBuilderF(s2, e2)) =>
          s1 ≟ s2 && e1 == e2
        case (ArrayBuilderF(s1, e1), ArrayBuilderF(s2, e2)) =>
          s1 ≟ s2 && e1 == e2
        case (GroupBuilderF(s1, k1, c1), GroupBuilderF(s2, k2, c2)) =>
          s1 ≟ s2 && k1 ≟ k2 && c1 == c2
        case (FlatteningBuilderF(s1, f1), FlatteningBuilderF(s2, f2)) =>
          s1 ≟ s2 && f1 == f2
        case (SpliceBuilderF(s1, i1), SpliceBuilderF(s2, i2)) =>
          s1 ≟ s2 && i1 == i2
        case (ArraySpliceBuilderF(s1, i1), ArraySpliceBuilderF(s2, i2)) =>
          s1 ≟ s2 && i1 == i2
        case _ => false
      }
    }

  implicit def WorkflowBuilderTraverse[F[_]: Coalesce]
    (implicit ev: WorkflowOpCoreF :<: F)
    : Traverse[WorkflowBuilderF[F, ?]] =
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

  def branchLengthƒ[F[_]]: WorkflowBuilderF[F, Int] => Int = {
    case CollectionBuilderF(_, _, _) => 0
    case ShapePreservingBuilderF(src, inputs, _) => 1 + src
    case ValueBuilderF(_) => 0
    case ExprBuilderF(src, _) => 1 + src
    case DocBuilderF(src, _) => 1 + src
    case ArrayBuilderF(src, _) => 1 + src
    case GroupBuilderF(src, keys, _) => 1 + src
    case FlatteningBuilderF(src, _) => 1 + src
    case SpliceBuilderF(src, _) => 1 + src
    case ArraySpliceBuilderF(src, _) => 1 + src
  }

  /** Simplify/coalesce certain shapes, eliminating extra layers that make it
    * harder to pattern match. Should be applied before `objectConcat`,
    * `arrayConcat`, or `merge`.
    */
  def normalizeƒ[F[_]: Coalesce](implicit ev: WorkflowOpCoreF :<: F)
    : WorkflowBuilderF[F, Fix[WorkflowBuilderF[F, ?]]] => Option[WorkflowBuilderF[F, Fix[WorkflowBuilderF[F, ?]]]] = {

    def collapse(outer: Expr, inner: ListMap[BsonField.Name, Expr]): Option[Expr] = {
      def rewriteExpr(t: Expression)(applyExpr: PartialFunction[ExprOp[Expr], Option[Expr]]): Option[Expr] =
        t.cataM[Option, Expr] { x =>
          applyExpr.lift(x).getOrElse {
            x.sequence.fold(
              κ(for {
                op <- x.traverse(exprToJs).toOption
                js <- toJsSimpleƒ(op).toOption
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
            expr1 <- js.expr.apoM[Fix, Option, jscore.JsCoreF] {
                    case t @ jscore.Access(b, _) if b == jscore.Ident(js.param) =>
                      xs.get(t).map(_(jscore.Ident(js.param)).unFix.map(_.left))
                    case t => t.unFix.map(_.right[JsCore]).some
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

    def inln(outerExpr: Expr, cont: DocContents[_ \/ Expression]) =
      outerExpr.fold(
        κ(None),
        expr => (cont match {
          case Expr(\/-($var(dv))) =>
            Some(rewriteExprRefs(expr)(prefixBase(dv)))
          case Expr(\/-(ex)) => expr.cataM[Option, Expression] {
            case $varF(DocVar.ROOT(None)) => ex.some
            case $varF(_)                 => None
            case x                        => Fix(x).some
          }
          case Doc(map) => expr.cataM[Option, Expression] {
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

  def normalize[F[_]: Coalesce](implicit ev: WorkflowOpCoreF :<: F) =
    repeatedly(normalizeƒ[F])

  private def rewriteObjRefs(
    obj: ListMap[BsonField.Name, GroupValue[Expression]])(
    f: PartialFunction[DocVar, DocVar]) =
    obj ∘ (_.bimap(accumulator.rewriteGroupRefs(_)(f), rewriteExprRefs(_)(f)))

  private def rewriteGroupRefs(
    contents: GroupContents)(
    f: PartialFunction[DocVar, DocVar]) =
    contents match {
      case Expr(expr) =>
        Expr(expr.bimap(accumulator.rewriteGroupRefs(_)(f), rewriteExprRefs(_)(f)))
      case Doc(doc)   => Doc(rewriteObjRefs(doc)(f))
    }

  private def rewriteDocPrefix(doc: ListMap[BsonField.Name, Expr], base: Base) =
    doc ∘ (rewriteExprPrefix(_, base))

  private def rewriteExprPrefix(expr: Expr, base: Base): Expr =
    expr.bimap(base.toDocVar.toJs >>> _, rewriteExprRefs(_)(prefixBase0(base)))

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

  private def commonShape(shape: ListMap[BsonField.Name, Expr]) =
    commonMap(shape)(toJs)

  private val jsBase = jscore.Name("__val")

  private def toCollectionBuilder[F[_]: Coalesce]
    (wb: WorkflowBuilder[F])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: Show[WorkflowBuilder[F]])
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
                fail[CollectionBuilderF[F]](UnsupportedFunction(set.Filter.name, Some("failed to build operation"))))(
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
                fail[CollectionBuilderF[F]](UnsupportedFunction(set.Filter.name, Some("failed to build operation"))))(
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
        workflow(src).flatMap { case (wf, base) =>
          commonShape(rewriteDocPrefix(shape, base)).fold(
            fail(_),
            s => shape.keys.toList.toNel.fold[M[CollectionBuilderF[F]]](
              fail(InternalError("A shape with no fields does not make sense")))(
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
        workflow(src).flatMap { case (wf, base) =>
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
                    BsonField.Name("0") -> \/-(rewriteExprRefs($var(field.toDocVar))(prefixBase0(base))))))
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
                obj.foldLeft[(ListMap[BsonField.Name, Accumulator], ListMap[BsonField.Name, Expression])]((ListMap.empty[BsonField.Name, Accumulator], ListMap.empty[BsonField.Name, Expression]))((acc, item) =>
                  item match {
                    case (k, -\/(v)) =>
                      ((x: ListMap[BsonField.Name, Accumulator]) => x + (k -> v)).first(acc)
                    case (k, \/-(v)) =>
                      ((x: ListMap[BsonField.Name, Expression]) => x + (k -> v)).second(acc)
                  })
              if (grouped.isEmpty)
                toCollectionBuilder(DocBuilder(src, ungrouped ∘ (_.right)))
              else
                obj.keys.toList.toNel.fold[M[CollectionBuilderF[F]]](
                  fail(InternalError("A shape with no fields does not make sense")))(
                  fields => workflow(wb).flatMap { case (wf, base0) =>
                    emitSt(ungrouped.size match {
                      case 0 =>
                        state[NameGen, Fix[F]](chain(wf,
                          $group[F](Grouped(grouped).rewriteRefs(prefixBase0(base0 \ base)), key(base0))))
                      case 1 =>
                        state[NameGen, Fix[F]](chain(wf,
                          $group[F](Grouped(
                            obj.transform {
                              case (_, -\/(v)) =>
                                accumulator.rewriteGroupRefs(v)(prefixBase0(base0 \ base))
                              case (_, \/-(v)) =>
                                $push(rewriteExprRefs(v)(prefixBase0(base0 \ base)))
                            }),
                            key(base0)),
                          $unwind[F](DocField(ungrouped.head._1))))
                      case _ => for {
                        ungroupedName <- freshName
                        groupedName <- freshName
                      } yield
                        chain(wf,
                          $project[F](Reshape(ListMap(
                            ungroupedName -> -\/(Reshape(ungrouped.map {
                              case (k, v) => k -> \/-(rewriteExprRefs(v)(prefixBase0(base0 \ base)))
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
        workflow(sb.src).flatMap { case (wf, base) =>
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
        workflow(sb.src).flatMap { case (wf, base) =>
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

  def workflow[F[_]: Coalesce](wb: WorkflowBuilder[F])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: Show[WorkflowBuilder[F]])
    : M[(Fix[F], Base)] =
    toCollectionBuilder(wb).map(x => (x.src, x.base))

  def shift[F[_]: Coalesce](base: Base, struct: Schema, graph: Fix[F])
    (implicit ev: WorkflowOpCoreF :<: F)
    : (Fix[F], Base) = {
    (base, struct) match {
      case (Field(ExprName), None) => (graph, Field(ExprName))
      case (_,       None)         =>
        (chain(graph,
          Workflow.$project[F](Reshape(ListMap(ExprName -> \/-($var(base.toDocVar)))),
            ExcludeId)),
          Field(ExprName))
      case (_,       Some(fields)) =>
        (chain(graph,
          Workflow.$project[F](
            Reshape(fields.map(name =>
              name -> \/-($var((base \ name).toDocVar))).toList.toListMap),
            if (fields.element(IdName)) IncludeId else ExcludeId)),
          Root())
    }
  }

  def build[F[_]: Coalesce](wb: WorkflowBuilder[F])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: Show[WorkflowBuilder[F]])
    : M[Fix[F]] =
    toCollectionBuilder(wb).map {
      case CollectionBuilderF(graph, base, struct) =>
        if (base == Root()) graph
        else shift(base, struct, graph)._1
    }

  private def $project[F[_]: Coalesce](shape: Reshape)
    (implicit ev: WorkflowOpCoreF :<: F)
    : FixOp[F] =
    Workflow.$project[F](
      shape,
      shape.get(IdName).fold[IdHandling](IgnoreId)(κ(IncludeId)))

  def asLiteral[F[_]](wb: WorkflowBuilder[F]): Option[Bson] = wb.unFix match {
    case ValueBuilderF(value)                  => Some(value)
    case ExprBuilderF(_, \/-($literal(value))) => Some(value)
    case _                                     => None
  }

  private def fold1Builders[F[_]: Coalesce](builders: List[WorkflowBuilder[F]])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: Show[WorkflowBuilder[F]])
    : Option[M[(WorkflowBuilder[F], List[Expression])]] =
    builders match {
      case Nil             => None
      case builder :: Nil  => Some(emit((builder, List($$ROOT))))
      case Fix(ValueBuilderF(bson)) :: rest =>
        fold1Builders(rest).map(_.map { case (builder, fields) =>
          (builder, $literal(bson) +: fields)
        })
      case builder :: rest =>
        Some(rest.foldLeftM[M, (WorkflowBuilder[F], List[Expression])](
          (builder, List($$ROOT))) {
          case ((wf, fields), Fix(ValueBuilderF(bson))) =>
            emit((wf, fields :+ $literal(bson)))
          case ((wf, fields), x) =>
            merge(wf, x).map { case (lbase, rbase, src) =>
              (src, fields.map(rewriteExprRefs(_)(prefixBase0(lbase))) :+ $var(rbase.toDocVar))
            }
        })
    }

  private def foldBuilders[F[_]: Coalesce](src: WorkflowBuilder[F], others: List[WorkflowBuilder[F]])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: Show[WorkflowBuilder[F]])
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
  sealed trait Base {
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
            -\/(InternalError("conflicting fields when merging contents: " + lshape + ", " + rshape)))(
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


  // TODO: This is an approximation. If we could postpone this decision until
  //      `Workflow.crush`, when we actually have a task (whether aggregation or
  //       mapReduce) in hand, we would know for sure.
  def preferMapReduce[F[_]: Coalesce: Crush: Crystallize: Functor](wb: WorkflowBuilder[F])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: Show[WorkflowBuilder[F]])
    : Boolean = {
    // TODO: Get rid of this when we functorize WorkflowTask
    def checkTask(wt: workflowtask.WorkflowTask): Boolean = wt match {
      case workflowtask.FoldLeftTask(_, _)     => true
      case workflowtask.MapReduceTask(_, _, _) => true
      case workflowtask.PipelineTask(src, _)   => checkTask(src)
      case _                                   => false
    }

    workflow[F](wb).evalZero.fold(
      κ(false),
      wf => checkTask(task(Crystallize[F].crystallize(wf._1))))
  }

  def join[F[_]: Functor: Coalesce: Crush: Crystallize](left0: WorkflowBuilder[F], right0: WorkflowBuilder[F],
    tpe: GenericFunc[_],
    leftKey0: List[WorkflowBuilder[F]], leftJs0: Option[List[JsFn]],
    rightKey0: List[WorkflowBuilder[F]], rightJs0: Option[List[JsFn]])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: Show[WorkflowBuilder[F]])
    : M[WorkflowBuilder[F]] = {

    import Js._

    val ops = Ops[F]
    import ops._

    // FIXME: these have to match the names used in the logical plan. Should
    //        change this to ensure left0/right0 are `Free` and pull the names
    //        from those.
    val leftField0: BsonField.Name = BsonField.Name("left")
    val rightField0: BsonField.Name = BsonField.Name("right")

    def keyMap(keyExpr: List[JsFn], rootField: BsonField.Name, otherField: BsonField.Name): AnonFunDecl =
      $MapF.mapKeyVal(("key", "value"),
        keyExpr match {
          case Nil => Js.Null
          case _   =>
            jscore.Obj(keyExpr.map(_(jscore.ident("value"))).zipWithIndex.foldLeft[ListMap[jscore.Name, JsCore]](ListMap[jscore.Name, JsCore]()) {
              case (acc, (j, i)) => acc + (jscore.Name(i.toString) -> j)
            }).toJs
        },
        AnonObjDecl(List(
          (otherField.asText, AnonElem(Nil)),
          (rootField.asText, AnonElem(List(Ident("value")))))))

    def jsReduce(src: WorkflowBuilder[F], key: List[JsFn],
                 rootField: BsonField.Name, otherField: BsonField.Name):
        (WorkflowBuilder[F], FixOp[F]) =
      (src, $map[F](keyMap(key, rootField, otherField), ListMap()))

    def wbReduce(src: WorkflowBuilder[F], key: List[WorkflowBuilder[F]],
                 rootField: BsonField.Name, otherField: BsonField.Name):
        (WorkflowBuilder[F], FixOp[F]) =
      (DocBuilder(
        reduce(groupBy(src, key))($push(_)),
        ListMap(
          rootField             -> \/-($$ROOT),
          otherField            -> \/-($literal(Bson.Arr(Nil))),
          BsonField.Name("_id") -> \/-($include()))),
        ι)

    val (left, right, leftField, rightField) =
      (leftJs0, rightJs0) match {
        case (Some(js), _)
            if preferMapReduce(left0) && !preferMapReduce(right0) =>
          (wbReduce(right0, rightKey0, rightField0, leftField0),
            jsReduce(left0, js, leftField0, rightField0),
            rightField0, leftField0)
        case (_, Some(js)) =>
          (wbReduce(left0, leftKey0, leftField0, rightField0),
            jsReduce(right0, js, rightField0, leftField0),
            leftField0, rightField0)
        case (Some(js), _) =>
          (wbReduce(right0, rightKey0, rightField0, leftField0),
            jsReduce(left0, js, leftField0, rightField0),
            rightField0, leftField0)
        case (None, None) =>
          (wbReduce(left0, leftKey0, leftField0, rightField0),
            wbReduce(right0, rightKey0, rightField0, leftField0),
            leftField0, rightField0)
      }

    val nonEmpty: Selector.SelectorExpr = Selector.NotExpr(Selector.Size(0))

    def padEmpty(side: BsonField): Expression =
      $cond($eq($size($var(DocField(side))), $literal(Bson.Int32(0))),
        $literal(Bson.Arr(List(Bson.Doc(ListMap())))),
        $var(DocField(side)))

    def buildProjection(l: Expression, r: Expression): FixOp[F] =
      $project[F](Reshape(ListMap(leftField -> \/-(l), rightField -> \/-(r)))).apply(_)

    // TODO exhaustive pattern match
    def buildJoin(src: Fix[F], tpe: GenericFunc[_]): Fix[F] =
      tpe match {
        case set.FullOuterJoin =>
          chain(src,
            buildProjection(padEmpty(leftField), padEmpty(rightField)))
        case set.LeftOuterJoin =>
          chain(src,
            $match[F](Selector.Doc(ListMap(
              leftField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection($var(DocField(leftField)), padEmpty(rightField)))
        case set.RightOuterJoin =>
          chain(src,
            $match[F](Selector.Doc(ListMap(
              rightField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection(padEmpty(leftField), $var(DocField(rightField))))
        case set.InnerJoin =>
          chain(
            src,
            $match[F](
              Selector.Doc(ListMap(
                leftField.asInstanceOf[BsonField] -> nonEmpty,
                rightField -> nonEmpty))))
        case _ => scala.sys.error("How did this get here?")
      }

    val rightReduce =
      AnonFunDecl(List("key", "values"),
        List(
          VarDef(List(("result",
            AnonObjDecl(List(
              (leftField.asText, AnonElem(Nil)),
              (rightField.asText, AnonElem(Nil))))))),
          Call(Select(Ident("values"), "forEach"),
            List(AnonFunDecl(List("value"),
              // TODO: replace concat here with a more efficient operation
              //      (push or unshift)
              List(
                BinOp("=",
                  Select(Ident("result"), leftField.asText),
                  Call(Select(Select(Ident("result"), leftField.asText), "concat"),
                    List(Select(Ident("value"), leftField.asText)))),
                BinOp("=",
                  Select(Ident("result"), rightField.asText),
                  Call(Select(Select(Ident("result"), rightField.asText), "concat"),
                    List(Select(Ident("value"), rightField.asText)))))))),
          Return(Ident("result"))))

    (workflow(left._1) |@| workflow(right._1)) { case ((l, _), (r, _)) =>
      CollectionBuilder(
        chain(
          $foldLeft[F](
            left._2(l),
            chain(r, right._2, $reduce[F](rightReduce, ListMap()))),
          (op: Fix[F]) => buildJoin(op, tpe),
          $unwind[F](DocField(leftField)),
          $unwind[F](DocField(rightField))),
        Root(),
        None)
    }
  }

  // TODO: Cases that match on `$$ROOT` should be generalized to look up the
  //       shape of any DocVar in the source.
  @tailrec def findKeys[F[_]](wb: WorkflowBuilder[F]): Option[Base] = {
    wb.unFix match {
      case CollectionBuilderF(_, _, s2) => s2.map(s => Subset(s.toSet))
      case DocBuilderF(_, shape) => Subset(shape.keySet).some
      case FlatteningBuilderF(src, _) => findKeys(src)
      case GroupBuilderF(src, _, Expr(\/-($$ROOT))) => findKeys(src)
      case GroupBuilderF(_, _, Doc(obj)) => Subset(obj.keySet).some
      case ShapePreservingBuilderF(src, _, _) => findKeys(src)
      case ExprBuilderF(src, \/-($$ROOT)) => findKeys(src)
      case ExprBuilderF(_, _) => Root().some
      case ValueBuilderF(Bson.Doc(shape)) =>
        Subset(shape.keySet.map(BsonField.Name(_))).some
      case _ => None
    }
  }

  private def findSort[F[_]: Coalesce]
    (src: WorkflowBuilder[F])
    (distincting: WorkflowBuilder[F] => M[WorkflowBuilder[F]])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: Show[WorkflowBuilder[F]])
    : M[WorkflowBuilder[F]] = {
    @tailrec
    def loop(wb: WorkflowBuilder[F]): M[WorkflowBuilder[F]] =
      wb.unFix match {
        case spb @ ShapePreservingBuilderF(spbSrc, sortKeys, f) =>
          spb.dummyOp.unFix match {
            case $sort(_, _) =>
              foldBuilders(src, sortKeys).flatMap {
                case (newSrc, dv, ks) =>
                  distincting(Fix(normalize[F].apply(ExprBuilderF(newSrc, \/-($var(dv.toDocVar)))))).map { dist =>
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

  private def merge[F[_]: Coalesce](left: Fix[WorkflowBuilderF[F, ?]], right: Fix[WorkflowBuilderF[F, ?]])
    (implicit I: WorkflowOpCoreF :<: F, ev: Show[Fix[WorkflowBuilderF[F, ?]]])
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
        left.cata(branchLengthƒ[F]) cmp right.cata(branchLengthƒ[F]) match {
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
          workflow(ArrayBuilder(wb, shape.map(rewriteExprPrefix(_, lbase)))).flatMap { case (wf, base) =>
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
                case _ => fail(InternalError("couldn’t merge array"))
              },
              fail(InternalError("couldn’t merge unrecognized op: " + wf)))
          }
        }
      case (_, ArrayBuilderF(_, _)) => delegate

      case _ =>
        fail(InternalError("failed to merge:\n" + left.show + "\n" + right.show))
    }
  }

  final class Ops[F[_]: Coalesce](implicit ev: WorkflowOpCoreF :<: F) {
    def read(coll: Collection): WorkflowBuilder[F] =
      CollectionBuilder($read[F](coll), Root(), None)

    def pure(bson: Bson): WorkflowBuilder[F] = ValueBuilder(bson)

    def limit(wb: WorkflowBuilder[F], count: Long): WorkflowBuilder[F] =
      ShapePreservingBuilder(wb, Nil, { case Nil => $limit[F](count) })

    def skip(wb: WorkflowBuilder[F], count: Long): WorkflowBuilder[F] =
      ShapePreservingBuilder(wb, Nil, { case Nil => $skip[F](count) })

    def squash[F[_]](wb: WorkflowBuilder[F]): WorkflowBuilder[F] = wb

    def filter
      (src: WorkflowBuilder[F],
        those: List[WorkflowBuilder[F]],
        sel: PartialFunction[List[BsonField], Selector])
      : WorkflowBuilder[F] =
      ShapePreservingBuilder(src, those, PartialFunction(fields => $match[F](sel(fields))))

    def expr1
      (wb: WorkflowBuilder[F])
      (f: Expression => Expression)
      (implicit ev0: WorkflowOpCoreF :<: F, ev1: Show[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] =
      expr(List(wb)) { case List(e) => f(e) }

    def expr2
      (wb1: WorkflowBuilder[F], wb2: WorkflowBuilder[F])
      (f: (Expression, Expression) => Expression)
      (implicit ev2: Show[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] =
      expr(List(wb1, wb2)) { case List(e1, e2) => f(e1, e2) }

    def expr
      (wbs: List[WorkflowBuilder[F]])
      (f: List[Expression] => Expression)
      (implicit ev2: Show[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] = {
      fold1Builders(wbs).fold[M[WorkflowBuilder[F]]](
        fail(InternalError("impossible – no arguments")))(
        _.map { case (wb, exprs) => Fix(normalize[F].apply(ExprBuilderF(wb, \/-(f(exprs))))) })
    }

    // FIXME: no constraints
    def jsExpr1(wb: WorkflowBuilder[F], js: JsFn): WorkflowBuilder[F] =
      ExprBuilder(wb, -\/(js))

    def jsExpr(wbs: List[WorkflowBuilder[F]], f: List[JsCore] => JsCore)
      (implicit ev2: Show[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] =
      fold1Builders(wbs).fold[M[WorkflowBuilder[F]]](
        fail(InternalError("impossible – no arguments")))(
        _.flatMap { case (wb, exprs) =>
          lift(exprs.traverse(toJs).map(jses => Fix(normalize[F].apply(ExprBuilderF(wb, -\/(JsFn(jsBase, f(jses.map(_(jscore.Ident(jsBase)))))))))))
        })

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

    def flattenMap(wb: WorkflowBuilder[F]): WorkflowBuilder[F] =
      wb.unFix match {
        case ShapePreservingBuilderF(src, inputs, op) =>
          ShapePreservingBuilder(flattenMap(src), inputs, op)
        case GroupBuilderF(src, keys, Expr(\/-($var(DocVar.ROOT(None))))) =>
          GroupBuilder(flattenMap(src), keys, Expr(\/-($$ROOT)))
        case _ => FlatteningBuilder(wb, Set(StructureType.Object(DocVar.ROOT())))
      }

    def flattenArray(wb: WorkflowBuilder[F]): WorkflowBuilder[F] =
      wb.unFix match {
        case ShapePreservingBuilderF(src, inputs, op) =>
          ShapePreservingBuilder(flattenArray(src), inputs, op)
        case GroupBuilderF(src, keys, Expr(\/-($var(DocVar.ROOT(None))))) =>
          GroupBuilder(flattenArray(src), keys, Expr(\/-($$ROOT)))
        case _ => FlatteningBuilder(wb, Set(StructureType.Array(DocVar.ROOT())))
      }

    def projectField(wb: WorkflowBuilder[F], name: String): PlannerError \/ WorkflowBuilder[F] =
      wb.unFix match {
        case ShapePreservingBuilderF(src, inputs, op) =>
          projectField(src, name).map(ShapePreservingBuilder(_, inputs, op))
        case ValueBuilderF(Bson.Doc(fields)) =>
          fields.get(name).fold[PlannerError \/ WorkflowBuilder[F]](
            -\/(UnsupportedFunction(structural.ObjectProject.name, Some("value does not contain a field ‘" + name + "’."))))(
            x => \/-(ValueBuilder(x)))
        case ValueBuilderF(_) =>
          -\/(UnsupportedFunction(structural.ObjectProject.name, Some("value is not a document.")))
        case GroupBuilderF(wb0, key, Expr(\/-($var(dv)))) =>
          projectField(wb0, name).map(GroupBuilder(_, key, Expr(\/-($var(dv)))))
        case GroupBuilderF(wb0, key, Doc(doc)) =>
          doc.get(BsonField.Name(name)).fold[PlannerError \/ WorkflowBuilder[F]](
            -\/(UnsupportedFunction(structural.ObjectProject.name, Some("group does not contain a field ‘" + name + "’."))))(
            x => \/-(GroupBuilder(wb0, key, Expr(x))))
        case DocBuilderF(wb, doc) =>
          doc.get(BsonField.Name(name)).fold[PlannerError \/ WorkflowBuilder[F]](
            -\/(UnsupportedFunction(structural.ObjectProject.name, Some("document does not contain a field ‘" + name + "’."))))(
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
              structural.ArrayProject.name,
              Some("value does not contain index ‘" + index + "’.")))
        case ArrayBuilderF(wb0, elems) =>
          if (index < elems.length) // UGH!
            \/-(ExprBuilder(wb0, elems(index)))
          else
            -\/(UnsupportedFunction(
              structural.ArrayProject.name,
              Some("array does not contain index ‘" + index + "’.")))
        case ValueBuilderF(_) =>
          -\/(UnsupportedFunction(
            structural.ArrayProject.name,
            Some("value is not an array.")))
        case DocBuilderF(_, _) =>
          -\/(UnsupportedFunction(
            structural.ArrayProject.name,
            Some("value is not an array.")))
        case _ =>
          jsExpr1(wb, JsFn(jsBase,
            jscore.Access(jscore.Ident(jsBase), jscore.Literal(Js.num(index.toLong))))).right
      }

    def deleteField(wb: WorkflowBuilder[F], name: String): PlannerError \/ WorkflowBuilder[F] =
      wb.unFix match {
        case ShapePreservingBuilderF(src, inputs, op) =>
          deleteField(src, name).map(ShapePreservingBuilder(_, inputs, op))
        case ValueBuilderF(Bson.Doc(fields)) =>
          \/-(ValueBuilder(Bson.Doc(fields - name)))
        case ValueBuilderF(_) =>
          -\/(UnsupportedFunction(
            structural.DeleteField.name,
            Some("value is not a document.")))
        case GroupBuilderF(wb0, key, Expr(\/-($var(DocVar.ROOT(None))))) =>
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

    def reduce(wb: WorkflowBuilder[F])(f: Expression => Accumulator): WorkflowBuilder[F] =
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
      (implicit ev2: Show[WorkflowBuilder[F]])
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
                    case Subset(fields) => fields.toList.map(k => k -> $first($var(DocField(k))).left[Expression]).right
                    case b => InternalError(s"Expected a Subset but found $b").left
                  },
                  List().right)
                .fold(fail, i => GroupBuilder(newSrc, keys, Doc(i.toListMap)).point[M])
            }
        })

    def distinctBy(src: WorkflowBuilder[F], keys: List[WorkflowBuilder[F]])
      (implicit ev2: Show[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] =
      findSort(src)(s => reduce(groupBy(s, keys))($first(_)).point[M])

    // TODO: handle concating value, expr, or collection with group (#439)
    def objectConcat(wb1: WorkflowBuilder[F], wb2: WorkflowBuilder[F])
      (implicit ev2: Show[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] = {
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
            GroupBuilderF(src1, keys1, Expr(\/-($var(DocVar.ROOT(_))))),
            GroupBuilderF(src2, keys2, Expr(\/-($var(DocVar.ROOT(_))))))
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
            structural.ObjectConcat.name,
            Some("unrecognized shapes:\n" + wb1.show + "\n" + wb2.show)))
        }
      }

      impl(wb1, wb2, unflipped)
    }

    def arrayConcat(left: WorkflowBuilder[F], right: WorkflowBuilder[F])
      (implicit ev2: Show[WorkflowBuilder[F]])
      : M[WorkflowBuilder[F]] = {
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
              structural.ArrayConcat.name,
              Some("values are not both arrays")))
        }
      }

      impl(left, right, unflipped)
    }
  }
  object Ops {
    implicit def apply[F[_]: Coalesce](implicit ev: WorkflowOpCoreF :<: F): Ops[F] =
      new Ops[F]
  }

  implicit def WorkflowBuilderRenderTree[F[_]: Coalesce]
    (implicit
      RG: RenderTree[Contents[GroupValue[Expression]]],
      RC: RenderTree[Contents[Expr]],
      RF: RenderTree[Fix[F]],
      ev: WorkflowOpCoreF :<: F
    ): RenderTree[Fix[WorkflowBuilderF[F, ?]]] =
    new RenderTree[Fix[WorkflowBuilderF[F, ?]]] {
      val nodeType = "WorkflowBuilder" :: Nil

      def render(v: WorkflowBuilder[F]) = v.unFix match {
        case CollectionBuilderF(graph, base, struct) =>
          NonTerminal("CollectionBuilder" :: nodeType, Some(base.toString),
            graph.render ::
              Terminal("Schema" :: "CollectionBuilder" :: nodeType, Some(struct.toString)) ::
              Nil)
        case spb @ ShapePreservingBuilderF(src, inputs, op) =>
          val nt = "ShapePreservingBuilder" :: nodeType
          NonTerminal(nt, None,
            render(src) ::
              (inputs.map(render) :+
                Terminal("Op" :: nt, Some(spb.dummyOp.toString))))
        case ValueBuilderF(value) =>
          Terminal("ValueBuilder" :: nodeType, Some(value.toString))
        case ExprBuilderF(src, expr) =>
          NonTerminal("ExprBuilder" :: nodeType, None,
            render(src) :: expr.render :: Nil)
        case DocBuilderF(src, shape) =>
          val nt = "DocBuilder" :: nodeType
          NonTerminal(nt, None,
            render(src) ::
              NonTerminal("Shape" :: nt, None,
                shape.toList.map {
                  case (name, expr) =>
                    NonTerminal("Name" :: nodeType, Some(name.value), List(expr.render))
                }) ::
              Nil)
        case ArrayBuilderF(src, shape) =>
          val nt = "ArrayBuilder" :: nodeType
          NonTerminal(nt, None,
            render(src) ::
              NonTerminal("Shape" :: nt, None, shape.map(_.render)) ::
              Nil)
        case GroupBuilderF(src, keys, content) =>
          val nt = "GroupBuilder" :: nodeType
          NonTerminal(nt, None,
            render(src) ::
              NonTerminal("By" :: nt, None, keys.map(render)) ::
              RG.render(content).copy(nodeType = "Content" :: nt) ::
              Nil)
        case FlatteningBuilderF(src, fields) =>
          val nt = "FlatteningBuilder" :: nodeType
          NonTerminal(nt, None,
            render(src) ::
              fields.toList.map(x => $var(x.field).render.copy(nodeType = (x match {
                case StructureType.Array(_) => "Array"
                case StructureType.Object(_) => "Object"
              }) :: nt)))
        case SpliceBuilderF(src, structure) =>
          NonTerminal("SpliceBuilder" :: nodeType, None,
            render(src) :: structure.map(RC.render))
        case ArraySpliceBuilderF(src, structure) =>
          NonTerminal("ArraySpliceBuilder" :: nodeType, None,
            render(src) :: structure.map(RC.render))
      }
    }
}
