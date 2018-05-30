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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar._
import quasar.RenderTree.ops._
import quasar.common.SortDir
import quasar.contrib.matryoshka._
import quasar.contrib.scalaz._
import quasar.fp._
import quasar.fs.Planner, Planner._
import quasar.javascript._
import quasar.jscore, jscore.JsFn
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.selector.Selector
import quasar.physical.mongodb.workflow._
import quasar.qscript.{IdStatus, ShiftType}
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
          case (ExprBuilderF(s1, e1), ExprBuilderF(s2, e2)) =>
            eq.equal(s1, s2) && e1 == e2
          case (DocBuilderF(s1, e1), DocBuilderF(s2, e2)) =>
            eq.equal(s1, s2) && e1 == e2
          case (GroupBuilderF(s1, k1, c1), GroupBuilderF(s2, k2, c2)) =>
            eq.equal(s1, s2) && k1 ≟ k2 && c1 == c2
          case (FlatteningBuilderF(s1, f1, r1), FlatteningBuilderF(s2, f2, r2)) =>
            eq.equal(s1, s2) && f1 == f2 && r1 ≟ r2
          case (UnionBuilderF(ls1, rs1), UnionBuilderF(ls2, rs2)) =>
            eq.equal(ls1, ls2) && eq.equal(rs1, rs2)
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
            f(src).map(ShapePreservingBuilderF(_, inputs, op))
          case ExprBuilderF(src, expr) => f(src).map(ExprBuilderF(_, expr))
          case DocBuilderF(src, shape) => f(src).map(DocBuilderF(_, shape))
          case GroupBuilderF(src, keys, contents) =>
            f(src).map(GroupBuilderF(_, keys, contents))
          case FlatteningBuilderF(src, fields, rest) =>
            f(src).map(FlatteningBuilderF(_, fields, rest))
          case UnionBuilderF(lSrc, rSrc) =>
            (f(lSrc) ⊛ f(rSrc))(UnionBuilderF(_, _))
        }
    }

  implicit def renderTree[F[_]: Coalesce: Functor](implicit
      RG: RenderTree[ListMap[BsonField.Name, AccumOp[Fix[ExprOp]]]],
      RF: RenderTree[Fix[F]],
      ev: WorkflowOpCoreF :<: F
    ): Delay[RenderTree, WorkflowBuilderF[F, ?]] =
    Delay.fromNT(λ[RenderTree ~> (RenderTree ∘ WorkflowBuilderF[F, ?])#λ](rt => {
      val nodeType = "WorkflowBuilder" :: Nil
      RenderTree.make {
        case CollectionBuilderF(graph, base, struct) =>
          NonTerminal("CollectionBuilder" :: nodeType, Some(base.shows),
            graph.render ::
              Terminal("Schema" :: "CollectionBuilder" :: nodeType, struct ∘ (_.shows)) ::
              Nil)
        case spb @ ShapePreservingBuilderF(src, inputs, op) =>
          val nt = "ShapePreservingBuilder" :: nodeType
          NonTerminal(nt, None,
            rt.render(src) :: (inputs.map(_.render) :+ spb.dummyOp.render))
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
        case GroupBuilderF(src, keys, content) =>
          val nt = "GroupBuilder" :: nodeType
          NonTerminal(nt, None,
            rt.render(src) ::
              NonTerminal("By" :: nt, None, keys.map(_.render)) ::
              RG.render(content).copy(nodeType = "Content" :: nt) ::
              Nil)
        case FlatteningBuilderF(src, fields, rest) =>
          val nt = "FlatteningBuilder" :: nodeType
          val flds = NonTerminal("Fields" :: nt, None,
            fields.toList.map {
              case StructureType.Array(field, id) =>
                NonTerminal("Array" :: nt, field.shows.some, List(id.render))
              case StructureType.Object(field, id) =>
                NonTerminal("Object" :: nt, field.shows.some, List(id.render))
            })
          val rst = Terminal.opt("Rest" :: nt, rest.map(_.shows))
          NonTerminal(nt, None, rt.render(src) :: flds :: rst.toList)
        case UnionBuilderF(lSrc, rSrc) =>
          val nt = "UnionBuilder" :: nodeType
          NonTerminal(nt, None, List(rt.render(lSrc), rt.render(rSrc)))
      }
    }))
}

object WorkflowBuilder {
  import fixExprOp._

  val fixExprOp344 = new ExprOp3_4_4F.fixpoint[Fix[ExprOp], ExprOp](_.embed)

  /** A partial description of a query that can be run on an instance of MongoDB */
  type WorkflowBuilder[F[_]] = Fix[WorkflowBuilderF[F, ?]]
  /** If we know what the shape is, represents the list of Fields. */
  type Schema = Option[NonEmptyList[BsonField.Name]]

  /** Either arbitrary javascript expression or Pipeline expression
    * An arbitrary javascript is more powerful but less performant because it
    * gets materialized into a Map/Reduce operation.
    */
  // TODO: We should be able to handle _every_ MapFunc as JS, so this should
  //       eventually be `AndMaybe[JsFn, Fix[ExprOp]]`
  type Expr = JsFn \&/ Fix[ExprOp]

  // FIXME: We should never need to convert to JS anymore, so get rid of this.
  private def exprToJs(expr: Expr): PlannerError \/ JsFn = expr match {
    case HasThis(js)        => js.right
    case \&/.That($var(dv)) => dv.toJs.right
    case _                  => UnsupportedJS(expr.render.shows).left
  }

  def docVarToExpr(dv: DocVar): Expr = \&/(dv.toJs, $var(dv))

  def semiAlignExpr[F[_]: Traverse](fa: F[Expr])
      : Option[F[Fix[ExprOp]]] =
    fa.traverse(HasThat.unapply)

  def alignExpr[F[_]: Traverse](fa: F[Expr])
      : Option[F[JsFn] \/ F[Fix[ExprOp]]] =
    Bitraverse[\/].leftTraverse.sequence(
      fa.traverse(HasThat.unapply) \/> fa.traverse(HasThis.unapply))

  /** This is a Leaf node which can be used to construct a more complicated
    * WorkflowBuilder. Takes a value resulting from a Workflow and wraps it in a
    * WorkflowBuilder. For example: If you want to read from MongoDB and then
    * project on a field, the read would be the CollectionBuilder.
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
    inputs: List[Expr],
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
      inputs: List[Expr],
      op: PartialFunction[List[BsonField], FixOp[F]])
      (implicit ev: WorkflowOpCoreF :<: F) =
      Fix[WorkflowBuilderF[F, ?]](new ShapePreservingBuilderF(src, inputs, op))
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

  sealed abstract class DocContents[A] extends Product with Serializable
  object DocContents {
    final case class Exp[A](contents: A) extends DocContents[A]
    final case class Doc[A](contents: ListMap[BsonField.Name, A]) extends DocContents[A]

    implicit def DocContentsRenderTree[A: RenderTree]: RenderTree[DocContents[A]] =
      new RenderTree[DocContents[A]] {
        val nodeType = "Contents" :: Nil

        def render(v: DocContents[A]) =
          v match {
            case Exp(a) => NonTerminal("Exp" :: nodeType, None, a.render :: Nil)
            case Doc(b) => NonTerminal("Doc" :: nodeType, None, b.render :: Nil)
          }
      }
  }
  import DocContents._

  def contentsToBuilder[F[_]]: DocContents[Expr] => WorkflowBuilder[F] => WorkflowBuilder[F] = {
    case Exp(expr) => ExprBuilder(_, expr)
    case Doc(doc)  => DocBuilder(_, doc)
  }

  type GroupContents = ListMap[BsonField.Name, AccumOp[Fix[ExprOp]]]

  final case class GroupBuilderF[F[_], A](
    src: A, keys: List[Expr], contents: GroupContents)
      extends WorkflowBuilderF[F, A]
  object GroupBuilder {
    def apply[F[_]](
      src: WorkflowBuilder[F],
      keys: List[Expr],
      contents: GroupContents) =
      Fix[WorkflowBuilderF[F, ?]](new GroupBuilderF(src, keys, contents))
  }

  sealed abstract class StructureType[A] {
    val field: A
  }
  object StructureType {
    final case class Array[A](field: A, includeIndex: IdStatus)
        extends StructureType[A]
    final case class Object[A](field: A, includeKey: IdStatus)
        extends StructureType[A]

    def mk[A](shiftType: ShiftType, field: A, idStatus: IdStatus)
        : StructureType[A] =
      shiftType match {
        case ShiftType.Array => Array(field, idStatus)
        case ShiftType.Map => Object(field, idStatus)
      }

    implicit val StructureTypeTraverse: Traverse[StructureType] =
      new Traverse[StructureType] {
        def traverseImpl[G[_], A, B](fa: StructureType[A])(f: A => G[B])(implicit G: Applicative[G]):
            G[StructureType[B]] =
          fa match {
            case Array(field, i) => f(field).map(Array(_, i))
            case Object(field, i) => f(field).map(Object(_, i))
          }
      }
  }

  /**
   * fields - the fields to flatten
   * rest - the other fields of the structure or `None` if unknown
   */
  final case class FlatteningBuilderF[F[_], A](src: A, fields: Set[StructureType[BsonField.Name]], rest: Option[List[BsonField.Name]])
      extends WorkflowBuilderF[F, A]
  object FlatteningBuilder {
    def apply[F[_]](src: WorkflowBuilder[F], fields: Set[StructureType[BsonField.Name]], rest: Option[List[BsonField.Name]]) =
      Fix[WorkflowBuilderF[F, ?]](new FlatteningBuilderF(src, fields, rest))
  }

  final case class UnionBuilderF[F[_], A](lSrc: A, rSrc: A) extends WorkflowBuilderF[F, A]
  object UnionBuilder {
    def apply[F[_]](lSrc: WorkflowBuilder[F], rSrc: WorkflowBuilder[F]) =
      Fix[WorkflowBuilderF[F, ?]](new UnionBuilderF(lSrc, rSrc))
  }

  private def rewriteDocPrefix(doc: ListMap[BsonField.Name, Expr], base: Base)
      (implicit exprOps: ExprOpOps.Uni[ExprOp]): ListMap[BsonField.Name, Expr] =
    doc ∘ (rewriteExprPrefix(_, base))

  private def rewriteExprPrefix(expr: Expr, base: Base)
      (implicit exprOps: ExprOpOps.Uni[ExprOp]): Expr =
    expr.bimap(
      base.toDocVar.toJs >>> _,
      _.cata(exprOps.rewriteRefs(prefixBase0(base))))

  private def rewriteExpr
    (expr: Expr, base: Expr)
    (implicit exprOps: ExprOpOps.Uni[ExprOp])
      : Option[Expr] =
    (expr, base) match {
      case (\&/.Both(js1, ex1), \&/.Both(js2, ex2)) =>
        ex1.transCataM(exprOps.rebase(ex2)) ∘ (\&/(js2 >>> js1, _))
      case (HasThat(ex1),       HasThat(ex2))       =>
        ex1.transCataM(exprOps.rebase(ex2)) ∘ \&/-
      case (HasThis(js1),       HasThis(js2))       => -\&/(js2 >>> js1).some
      case (_,                  _)                  => none
    }

  private def prefixBase0(base: Base): PartialFunction[DocVar, DocVar] =
    prefixBase(base.toDocVar)

  private val jsBase = jscore.Name("__val")

  // TODO: Determine if this is necessary. If so, flesh it out.
  def normalize[WF[_], T]
    (implicit T: Recursive.Aux[T, WorkflowBuilderF[WF, ?]],
      exprOps: ExprOpOps.Uni[ExprOp])
      : TransformM[Option, T, WorkflowBuilderF[WF, ?], WorkflowBuilderF[WF, ?]] = {
    case ExprBuilderF(Embed(ExprBuilderF(src, inner)), outer) =>
      rewriteExpr(outer, inner) ∘ (ExprBuilderF(src, _))
    case DocBuilderF(Embed(ExprBuilderF(src, inner)), doc) =>
      doc.traverse(rewriteExpr(_, inner)) >>=
        (d => (doc ≠ d).option(DocBuilderF(src, d)))
    case _ => none
  }

  def schema[WF[_]]: Algebra[WorkflowBuilderF[WF, ?], Schema] = {
    case CollectionBuilderF(_, _, schema)   => schema
    case ShapePreservingBuilderF(src, _, _) => src
    case ExprBuilderF(_, _)                 => none
    case DocBuilderF(_, shape)              => shape.keys.toList.toNel
    case GroupBuilderF(_, _, shape)         => shape.keys.toList.toNel
    case FlatteningBuilderF(src, _, _)      => src
    case UnionBuilderF(lSrc, rSrc)          => if (lSrc ≟ rSrc) lSrc else none

  }

  // TODO: See if we can extract `WB => Base` from this.
  // FIXME: There are a few recursive references to this function. We need to
  //        eliminate those.
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def toWorkflow[M[_]: Monad, WF[_]: Coalesce](queryModel: MongoQueryModel)
    (implicit M: MonadError_[M, PlannerError], ev0: WorkflowOpCoreF :<: WF, ev1: RenderTree[WorkflowBuilder[WF]], exprOps: ExprOpOps.Uni[ExprOp])
      : AlgebraM[M, WorkflowBuilderF[WF, ?], (Fix[WF], Base)] = {
    case CollectionBuilderF(graph, base, _) => (graph, base).point[M]
    case ShapePreservingBuilderF((g, b), inputs, op) =>
      inputs match {
        case Nil => (op(Nil)(g), b).point[M]
        case _ =>
          inputs.map(rewriteExprPrefix(_, b)).traverse {
            case HasThat($var(DocField(x))) => x.some
            case _                          => none
          }.fold {
            val pairs = inputs.zipWithIndex.map(_.map(i => BsonField.Name(i.shows)).swap)
            val srcName = BsonField.Name("src")
            op.lift(pairs.map(_._1)).fold(
              M.raiseError[(Fix[WF], Base)](UnsupportedFunction(set.Filter, Some("failed to build operation"))))(
              op => toWorkflow[M, WF](queryModel).apply(DocBuilderF((g, b), pairs.toListMap + (srcName -> docVarToExpr(DocVar.ROOT())))) ∘ {
                case (graph, _) => (chain(graph, op), Field(srcName))
              })
          } (
            op.lift(_).fold(
              M.raiseError[(Fix[WF], Base)](UnsupportedFunction(set.Filter, Some("failed to build operation"))))(
              op => (chain(g, op), b).point[M]))
      }
    case ExprBuilderF((g, b), HasThat($var(d))) =>
      (g, b \ fromDocVar(d)).point[M]
    case ExprBuilderF((g, b), expr) =>
      (rewriteExprPrefix(expr, b) match {
        case HasThat(op) =>
          (chain(g, $project[WF](Reshape(ListMap(BsonField.Name("0") -> \/-(op))))),
            Field(BsonField.Name("0")))
        case \&/.This(js) =>
          (chain(g, $simpleMap[WF](NonEmptyList(MapExpr(js)), ListMap())),
            Root())
      }).point[M]
    case DocBuilderF((wf, base), shape) =>
      alignExpr(rewriteDocPrefix(shape, base)).fold(
        M.raiseError[(Fix[WF], Base)](InternalError fromMsg "Could not align the expressions"))(
        s => shape.keys.toList.toNel.fold(
          M.raiseError[(Fix[WF], Base)](InternalError fromMsg "A shape with no fields does not make sense"))(
          fields => (
            chain(wf,
              s.fold(
                jsExprs => {
                  $simpleMap[WF](NonEmptyList(
                    MapExpr(JsFn(jsBase,
                      jscore.Obj(jsExprs.map {
                        case (name, expr) => jscore.Name(name.asText) -> expr(jscore.Ident(jsBase))
                      })))),
                    ListMap()) },
                exprOps => $project[WF](Reshape(exprOps ∘ \/.right)))),
            Root(): Base).point[M]))
    case GroupBuilderF((gʹ, bʹ), keysʹ, content) =>
      val keys = keysʹ ∘ (rewriteExprPrefix(_, bʹ))
      semiAlignExpr(keys).fold {
        val ks = keys.zipWithIndex.map(_.map(i => BsonField.Name(i.shows)).swap)

        toWorkflow[M, WF](queryModel).apply(DocBuilderF((gʹ, Root()), ks.toListMap + (BsonField.Name("content") -> docVarToExpr(bʹ.toDocVar)))) strengthR
          ((Field(BsonField.Name("content")): Base, ks.map(key => $var(DocField(key._1)))))
      }(
        ks => ((gʹ, bʹ), (bʹ, ks)).point[M]) >>= { case ((g, b), (c, keys)) =>

          val key: Reshape.Shape[ExprOp] =
            (keys.all {
              case $literal(_) => true
              case _           => false
            }).fold(
              \/-($literal(Bson.Null)),
              -\/(Reshape(keys.zipWithIndex.map {
                case (key, index) => BsonField.Name(index.toString) -> \/-(key)
              }.toListMap)))

          (chain(g, $group[WF](Grouped(content).rewriteRefs(prefixBase0(c)), key)),
            Root(): Base).point[M]
      }
    case FlatteningBuilderF((graph, base), fields, rest) =>
      (fields.foldRight(graph)(flattenField(queryModel, base, rest)), base).point[M]
    case UnionBuilderF((lGraph, lBase), (rGraph, rBase)) =>
      if (lBase == rBase)
        ($foldLeft(
          lGraph,
          chain(rGraph,
            $map($MapF.mapFresh, ListMap()),
            $reduce($ReduceF.reduceNOP, ListMap()))),
          lBase).point[M]
      else
        (toWorkflow[M, WF](queryModel).apply(DocBuilderF((lGraph, Root()), ListMap(
          BsonField.Name("0") -> docVarToExpr(lBase.toDocVar)))) ⊛
          toWorkflow[M, WF](queryModel).apply(DocBuilderF((rGraph, Root()), ListMap(
            BsonField.Name("0") -> docVarToExpr(rBase.toDocVar)))))((l, r) =>
          ($foldLeft(
            l._1,
            chain(
              r._1,
              $map($MapF.mapFresh, ListMap()),
              $reduce($ReduceF.reduceNOP, ListMap()))),
            Field(BsonField.Name("0"))))
  }

  // TODO: Organize this to put all the $maps and all the $unwinds adjacent.
  private def flattenField[WF[_]: Coalesce]
    (queryModel: MongoQueryModel, base: Base, rest: Option[List[BsonField.Name]])
    (implicit ev0: WorkflowOpCoreF :<: WF)
      : (StructureType[BsonField.Name], Fix[WF]) => Fix[WF] =
    (field, acc) => {
      (field, rest, queryModel) match {
        case (StructureType.Array(field, quasar.qscript.ExcludeId), _, _) =>
          $unwind[WF](base.toDocVar \ field, None, None).apply(acc)
        case (StructureType.Array(field, idStatus), Some(r), _) =>
          flattenFieldArrayUnwindProject(base.toDocVar, field, idStatus, r, acc)
        case (StructureType.Array(field, idStatus), _, _) =>
          flattenFieldMapReduce(base.toDocVar \ field, idStatus).apply(acc)
        case (StructureType.Object(field, idStatus), Some(r), m)
          if m gte MongoQueryModel.`3.4.4` =>
            flattenFieldObject344(base.toDocVar, field, idStatus, r, acc)
        case (StructureType.Object(field, idStatus), _, _) =>
          flattenFieldMapReduce(base.toDocVar \ field, idStatus).apply(acc)
      }
    }

  private def flattenFieldObject344[WF[_]: Coalesce]
    (docVar: DocVar, field: BsonField.Name, idStatus: IdStatus, rest: List[BsonField.Name], wf: Fix[WF])
    (implicit ev0: WorkflowOpCoreF :<: WF)
      : Fix[WF] = {
    val rst = ListMap(rest.map(f => f -> \/-($var(DocVar.ROOT(f)))) : _*)
    val oKey = $var(DocVar.ROOT(BsonField.Name("o")) \ BsonField.Name("k"))
    val oValue = $var(DocVar.ROOT(BsonField.Name("o")) \ BsonField.Name("v"))
    val prj = idStatus match {
      case quasar.qscript.ExcludeId => oValue
      case quasar.qscript.IdOnly => oKey
      case quasar.qscript.IncludeId => $arrayLit(List(oKey, oValue))
    }

    chain(wf,
      $project[WF](Reshape(rst ++ ListMap(
        BsonField.Name("o") -> \/-(fixExprOp344.$objectToArray($var(docVar \ field))))),
        ExcludeId),
      $unwind[WF](DocVar.ROOT(BsonField.Name("o")), None, None),
      $project[WF](Reshape(rst ++
        ListMap(field -> \/-(prj))),
        ExcludeId))
  }

  private def flattenFieldArrayUnwindProject[WF[_]: Coalesce]
    (docVar: DocVar, field: BsonField.Name, idStatus: IdStatus, rest: List[BsonField.Name], wf: Fix[WF])
    (implicit ev0: WorkflowOpCoreF :<: WF)
      : Fix[WF] = {
    val rst = ListMap(rest.map(f => f -> \/-($var(DocVar.ROOT(f)))) : _*)
    val prj = idStatus match {
      case quasar.qscript.ExcludeId => $var(docVar \ field)
      case quasar.qscript.IdOnly => $var(DocField(BsonField.Name("ix")))
      case quasar.qscript.IncludeId => $arrayLit(List(
        $var(DocField(BsonField.Name("ix"))),
        $var(docVar \ field)))
    }
    chain(wf,
      $unwind[WF](docVar \ field, Some(BsonField.Name("ix")), None),
      $project[WF](Reshape(rst ++
        // TODO: mongo >= 3.4 has $addFields - should be able to plan without `rest` being known
        // { $unwind: {path: "$f", includeArrayIndex: "ix"} } ,
        // { $addFields: { "f": ["$ix", "$f"] } },
        // { $project: { "ix": false } },
        ListMap(field -> \/-(prj))),
        ExcludeId))
  }

  private def flattenFieldMapReduce[WF[_]: Coalesce]
    (docVar: DocVar, idStatus: IdStatus)
    (implicit ev0: WorkflowOpCoreF :<: WF)
      : FixOp[WF] =
    $simpleMap[WF](
      idStatus match {
        case quasar.qscript.ExcludeId =>
          NonEmptyList(FlatExpr(docVar.toJs))
        case quasar.qscript.IncludeId =>
          NonEmptyList(
            SubExpr(
              docVar.toJs,
              JsFn(jsBase,
                jscore.Let(
                  jscore.Name("m"),
                  docVar.toJs(jscore.Ident(jsBase)),
                  jscore.Call(
                    jscore.Select(
                      jscore.Call(
                        jscore.Select(jscore.ident("Object"), "keys"),
                        List(jscore.ident("m"))),
                      "map"),
                    List(jscore.Fun(List(jscore.Name("k")), jscore.Arr(List(
                      jscore.ident("k"),
                      jscore.Access(jscore.ident("m"), jscore.ident("k")))))))))),
            FlatExpr(docVar.toJs))
        case quasar.qscript.IdOnly =>
          NonEmptyList(
            SubExpr(
              docVar.toJs,
              JsFn(jsBase,
                jscore.Call(
                  jscore.Select(jscore.ident("Object"), "keys"),
                  List(docVar.toJs(jscore.Ident(jsBase)))))),
            FlatExpr(docVar.toJs))
      },
      ListMap())

  def generateWorkflow[M[_]: Monad, F[_]: Coalesce](wb: WorkflowBuilder[F], queryModel: MongoQueryModel)
    (implicit M: MonadError_[M, PlannerError], ev0: WorkflowOpCoreF :<: F, ev1: RenderTree[WorkflowBuilder[F]], ev2: ExprOpOps.Uni[ExprOp])
      : M[(Fix[F], Base)] =
    (wb: Fix[WorkflowBuilderF[F, ?]]).cataM(toWorkflow[M, F](queryModel))

  def shift[F[_]: Coalesce](base: Base, struct: Schema, graph: Fix[F])
    (implicit ev: WorkflowOpCoreF :<: F)
    : (Fix[F], Base) = {
    (base, struct) match {
      case (Field(QuasarSigilName), None) =>
        (graph, Field(QuasarSigilName))

      case (_, None) =>
        (chain(graph,
          $project[F](Reshape(ListMap(QuasarSigilName -> \/-($var(base.toDocVar)))),
            ExcludeId)),
          Field(QuasarSigilName))

      case (_, Some(fields)) =>
        (chain(graph,
          $project[F](
            Reshape(fields.map(name =>
              name -> \/-($var((base \ name).toDocVar))).toList.toListMap),
            if (fields.element(IdName)) IncludeId else ExcludeId)),
          Root())
    }
  }

  def build[M[_]: Monad, F[_]: Coalesce](wb: WorkflowBuilder[F], queryModel: MongoQueryModel)
    (implicit M: MonadError_[M, PlannerError], ev0: WorkflowOpCoreF :<: F, ev1: RenderTree[WorkflowBuilder[F]], ev2: ExprOpOps.Uni[ExprOp])
      : M[Fix[F]] =
    (wb: Fix[WorkflowBuilderF[F, ?]]).cataM(AlgebraMZip[M, WorkflowBuilderF[F, ?]].zip(toWorkflow[M, F](queryModel), schema.generalizeM[M])) ∘ {
      case ((graph, Root()), _)      => graph
      case ((graph, base),   struct) => shift(base, struct, graph)._1
    }

  def asLiteral[F[_]](wb: WorkflowBuilder[F])(implicit ev0: WorkflowOpCoreF :<: F): Option[Bson] = wb.unFix match {
    case CollectionBuilderF(Fix(ev0($PureF(value))), _, _) => value.some
    case ExprBuilderF(_, HasThat($literal(value)))         => value.some
    case _                                                 => none
  }

  /** The location of the desired content relative to the current $$ROOT.
    *
    * Various transformations (merging, conversion to Workflow, etc.) combine
    * structures that we need to be able to extract later. This tells us how to
    * extract them.
    */
  sealed abstract class Base extends Product with Serializable {
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
    implicit val show: Show[Base] = new Show[Base] {
      override def shows(b: Base): String = b match {
        case Root() => "Root()"
        case Field(n) => s"Field(${n.show})"
        case Subset(fs) => s"Subset(${fs.show})"
      }
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
    case DocField(name)    => Field(name)
  }

  // TODO: Cases that match on `$$ROOT` should be generalized to look up the
  //       shape of any DocVar in the source.
  @tailrec def findKeys[F[_]](wb: WorkflowBuilder[F]): Option[Base] = {
    wb.unFix match {
      case CollectionBuilderF(_, _, s2)       => s2.map(s => Subset(s.toSet))
      case DocBuilderF(_, shape)              => Subset(shape.keySet).some
      case FlatteningBuilderF(src, _, _)      => findKeys(src)
      case GroupBuilderF(_, _, obj)           => Subset(obj.keySet).some
      case ShapePreservingBuilderF(src, _, _) => findKeys(src)
      case ExprBuilderF(_, _)                 => Root().some
      case _                                  => None
    }
  }

  final class Ops[F[_]: Coalesce](implicit ev0: WorkflowOpCoreF :<: F, ev1: ExprOpOps.Uni[ExprOp]) {
    def read(coll: Collection): WorkflowBuilder[F] =
      CollectionBuilder($read[F](coll), Root(), None)

    def limit(wb: WorkflowBuilder[F], count: Long): WorkflowBuilder[F] =
      ShapePreservingBuilder(wb, Nil, { case Nil => $limit[F](count) })

    def skip(wb: WorkflowBuilder[F], count: Long): WorkflowBuilder[F] =
      ShapePreservingBuilder(wb, Nil, { case Nil => $skip[F](count) })

    def filter
      (src: WorkflowBuilder[F],
        those: List[Expr],
        sel: PartialFunction[List[BsonField], Selector])
      : WorkflowBuilder[F] =
      ShapePreservingBuilder(src, those, PartialFunction(fields => $match[F](sel(fields))))

    def makeObject(wb: WorkflowBuilder[F], name: String): WorkflowBuilder[F] =
      wb.unFix match {
        case ExprBuilderF(src, expr) =>
          DocBuilder(src, ListMap(BsonField.Name(name) -> expr))
        case _ =>
          DocBuilder(wb, ListMap(BsonField.Name(name) -> \&/.Both(JsFn.identity, $$ROOT)))
      }

    private def deleteField(wb: WorkflowBuilder[F], name: String): WorkflowBuilder[F] =
      wb.unFix match {
        case DocBuilderF(wb0, doc) =>
          DocBuilder(wb0, doc - BsonField.Name(name))
        case GroupBuilderF(wb0, keys, doc) =>
          GroupBuilder(wb0, keys, doc - BsonField.Name(name))
        case ExprBuilderF(wb0, HasThis(js)) =>
          ExprBuilder(
            wb0,
            -\&/(JsFn(jsBase,
              jscore.Call(jscore.ident("remove"),
                List(js(jscore.Ident(jsBase)), jscore.Literal(Js.Str(name)))))))
        case _ =>
          ExprBuilder(
            wb,
            -\&/(JsFn(jsBase,
              jscore.Call(jscore.ident("remove"),
                List(jscore.Ident(jsBase), jscore.Literal(Js.Str(name)))))))
      }

    def groupBy(src: Fix[WorkflowBuilderF[F, ?]], keys: List[Expr], contents: GroupContents)
        : WorkflowBuilder[F] =
      keys match {
        case List(HasThat($$ROOT)) =>
          findKeys(src).fold(
            // TODO: Might not always want to delete `_id`?
            GroupBuilder(deleteField(src, "_id"), List(docVarToExpr(DocVar.ROOT())), contents)) {
            case Root()   => GroupBuilder(src, keys, contents)
            case Field(k) => GroupBuilder(src, List(docVarToExpr(DocField(k))), contents)
            case Subset(ks) =>
              GroupBuilder(
                src,
                ks.toList.map(k => docVarToExpr(DocField(k))),
                contents)
          }
        case _ => GroupBuilder(src, keys, contents)
      }

    def sortBy
      (src: WorkflowBuilder[F], keys: List[Expr], sortTypes: List[SortDir])
        : WorkflowBuilder[F] =
      ShapePreservingBuilder(
        src,
        keys,
        // FIXME: This pattern match is non total!
        // possible solution: make sortTypes and the argument to this partial function NonEmpty
        _.zip(sortTypes) match {
          case x :: xs => $sort[F](NonEmptyList.nel(x, IList.fromList(xs)))
        })
  }
  object Ops {
    implicit def apply[F[_]: Coalesce](implicit ev0: WorkflowOpCoreF :<: F, ev1: ExprOpOps.Uni[ExprOp]): Ops[F] =
      new Ops[F]
  }
}
