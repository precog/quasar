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
import quasar.{NonTerminal, RenderTree, RenderedTree}, RenderTree.ops._
import quasar.fp._
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.optimize.pipeline._
import quasar.physical.mongodb.workflowtask._

import matryoshka._, Recursive.ops._, FunctorT.ops._
import monocle.syntax.all._
import scalaz._, Scalaz._

/** A Workflow is a graph of atomic operations, with WorkflowOps for the
  * vertices. We crush them down into a WorkflowTask. This `crush` gives us a
  * location to optimize our workflow decisions. EG, A sequence of simple ops
  * may be combined into a single pipeline request, but if one of those
  * operations contains JS, we have to execute that outside of a pipeline,
  * possibly reordering the other operations to avoid having two pipelines with
  * a JS operation in the middle.
  *
  * We also implement the optimizations atomic
  * http://docs.mongodb.org/manual/core/aggregation-pipeline-optimization/ so
  * that we can build others potentially on top of them (including reordering
  * non-pipelines around pipelines, etc.).
  */
package object workflow {
  /** The type for workflows targeting MongoDB 2.6 specifically. */
  type Workflow2_6F[A] = WorkflowOpCoreF[A]

  /** The type for workflows targeting MongoDB 3.2 specifically. */
  type Workflow3_2F[A] = Coproduct[WorkflowOp3_2F, WorkflowOpCoreF, A]

  /** The type for workflows supporting the most advanced capabilities. */
  type WorkflowF[A] = Workflow3_2F[A]
  type Workflow = Fix[WorkflowF]

  type FixOp[F[_]] = Fix[F] => Fix[F]

  /** A "newtype" for ops that appear in pipelines, for use mostly after a
    * workflow is constructed, with fixed type that can represent any workflow.
    */
  final case class PipelineOp(op: WorkflowF[Unit], bson: Bson.Doc) {
    def rewrite[F[_]](f: F[Unit] => Option[PipelineF[F, Unit]])
      (implicit I: F :<: WorkflowF): PipelineOp =
      I.prj(op).flatMap(f).cata(PipelineOp(_), this)
  }
  object PipelineOp {
    def apply[F[_]](f: PipelineF[F, Unit])(implicit I: F :<: WorkflowF): PipelineOp =
      PipelineOp(I.inj(f.wf), f.bson)
  }
  /** Provides an extractor for core ops wrapped up in `PipelineOp`. */
  object PipelineOpCore {
    def unapply(p: PipelineOp): Option[WorkflowOpCoreF[Unit]] =
      Inject[WorkflowOpCoreF, WorkflowF].prj(p.op)
  }

  val ExprLabel  = "value"
  val ExprName   = BsonField.Name(ExprLabel)
  val ExprVar    = DocVar.ROOT(ExprName)

  val IdLabel  = "_id"
  val IdName   = BsonField.Name(IdLabel)
  val IdVar    = DocVar.ROOT(IdName)

  // NB: it's only safe to emit "core" expr ops here, but we always use the
  // largest type here, so they're immediately injected into ExprOp.
  private val exprFp: ExprOpCoreF.fixpoint[Fix, ExprOp] = ExprOpCoreF.fixpoint[Fix, ExprOp]
  import exprFp._

  def task[F[_]: Functor](fop: Crystallized[F])(implicit C: Crush[F]): WorkflowTask =
    (finish(_, _)).tupled(fop.op.para(C.crush[Fix]))._2.transAna(normalize)

  // NB: no need for a typeclass if implementing this way, but will be needed as
  //     soon as we need to coalesce anything _into_ a type that isn't 2.6.
  //     Furthermore, if this implementation is made implicit, then lots of
  //     functions that require it are able to resolve it from other evidence.
  //     Since that seems likely to be a short-lived phenomenon, instead for now
  //     implicits are defined below for just the specific types being used.
  def coalesceAll[F[_]: Functor](implicit I: WorkflowOpCoreF :<: F):
      Coalesce[F] = new Coalesce[F] {
    def coalesceƒ:
        F[Fix[F]] => Option[F[Fix[F]]] = {
      case $match(src, selector) => src.project match {
        case $sort(src0, value) =>
          I.inj($SortF(I.inj($MatchF(src0, selector)).embed, value)).some
        case $match(src0, sel0) =>
          I.inj($MatchF(src0, sel0 ⊹ selector)).some
        case _ => None
      }
      case WorkflowOpCoreF(p @ $ProjectF(src, shape, id)) => src.project match {
        case $project(src0, shape0, id0) =>
          inlineProject(p, List(shape0)).map(sh => I.inj($ProjectF(src0, sh, id0 |+| id)))
        // Would like to inline a $project into a preceding $simpleMap, but
        // This is not safe, because sometimes a $project is inserted after
        // $simpleMap specifically to pull fields out of `value`, and those
        // $project ops need to be preserved.
        // case $SimpleMapF(src0, js, flatten, scope) =>
        //   shape.toJs.fold(
        //     κ(op),
        //     jsShape => chain(src0,
        //       $simpleMap(
        //         JsMacro(base =>
        //           jscore.Let(
        //             ListMap("__tmp" -> js(base)),
        //             jsShape(jscore.Ident("__tmp")))),
        //         flatten, scope)))
        case $group(src, grouped, by) if id != ExcludeId =>
          inlineProjectGroup(shape, grouped).map(gr => I.inj($GroupF(src, gr, by)))
        case $unwind(Embed($group(src, grouped, by)), unwound)
            if id != ExcludeId =>
          inlineProjectUnwindGroup(shape, unwound, grouped).map { case (unwound, grouped) =>
            I.inj($UnwindF(I.inj($GroupF(src, grouped, by)).embed, unwound))
          }
        case _ => None
      }
      case $sort(Embed($sort(src, sort1)), sort2) =>
        I.inj($SortF(src, sort2 ⊹ sort1)).some
      case $limit(src, count) => src.project match {
        case $limit(src0, count0) =>
          I.inj($LimitF(src0, count0 min count)).some
        case $skip(src0, count0) =>
          I.inj($SkipF(I.inj($LimitF(src0, count0 + count)).embed, count0)).some
        case _ => None
      }
      case $skip(src, count) => src.project match {
        case $skip(src0, count0) => I.inj($SkipF(src0, count0 + count)).some
        case _                   => None
      }
      case $group(src, grouped, \/-($literal(bson))) if bson != Bson.Null =>
        I.inj($GroupF(src, grouped, \/-($literal(Bson.Null)))).some
      case WorkflowOpCoreF(op0 @ $GroupF(_, _, _)) =>
        inlineGroupProjects(op0).map { case (src, gr, by) => I.inj($GroupF(src, gr, by)) }
      case $geoNear(src, _, _, _, _, _, _, _, _, _) => src.project match {
        // FIXME: merge the params
        case $geoNear(_, _, _, _, _, _, _, _, _, _) => None
        case _                                      => None
      }
      case $map(src, fn, scope) => src.project match {
        case $map(src0, fn0, scope0) =>
          Reshape.mergeMaps(scope0, scope).map(sc =>
            I.inj($MapF(src0, $MapF.compose(fn, fn0), sc)))
        case $flatMap(src0, fn0, scope0) =>
          Reshape.mergeMaps(scope0, scope).map(sc =>
            I.inj($FlatMapF(src0, $FlatMapF.mapCompose(fn, fn0), sc)))
        case _                   => None
      }
      case $flatMap(src, fn, scope) => src.project match {
        case $map(src0, fn0, scope0)     =>
          Reshape.mergeMaps(scope0, scope).map(sc =>
            I.inj($FlatMapF(src0, $MapF.compose(fn, fn0), sc)))
        case $flatMap(src0, fn0, scope0) =>
          Reshape.mergeMaps(scope0, scope).map(sc =>
            I.inj($FlatMapF(src0, $FlatMapF.kleisliCompose(fn, fn0), sc)))
        case _                   => None
      }
      case WorkflowOpCoreF(sm @ $SimpleMapF(src, _, _)) => src.project match {
        case WorkflowOpCoreF(sm0 @ $SimpleMapF(_, _, _)) => I.inj(sm0 >>> sm).some
        case _                                      => None
      }
      case WorkflowOpCoreF($FoldLeftF(head, tail)) => head.project match {
        case WorkflowOpCoreF($FoldLeftF(head0, tail0)) =>
          I.inj($FoldLeftF(head0, tail0 ⊹ tail)).some
        case _                       => None
      }
      case $out(src, _) => src.project match {
        case $read(_) => src.project.some
        case _        => None
      }
      case _ => None
    }
  }

  def toPipelineOp[F[_]: Functor, A](op: PipelineF[F, A], base: DocVar)(implicit I: F :<: WorkflowF): PipelineOp = {
    val prefix = prefixBase(base)
    PipelineOp(I.inj(op.wf.void).run.fold(
      op => (op match {
        case op @ $LookupF(_, _, _, _, _) => rewriteRefs3_2(prefix).apply(op).pipeline
        case op @ $SampleF(_, _)          => rewriteRefs3_2(prefix).apply(op).shapePreserving
      }).fmap(ι, Inject[WorkflowOp3_2F, WorkflowF]),
      op => (op match {
        case op @ $MatchF(_, _)           => rewriteRefs2_6(prefix).apply(op).shapePreserving
        case op @ $ProjectF(_, _, _)      => rewriteRefs2_6(prefix).apply(op).pipeline
        case op @ $RedactF(_, _)          => rewriteRefs2_6(prefix).apply(op).pipeline
        case op @ $SkipF(_, _)            => rewriteRefs2_6(prefix).apply(op).shapePreserving
        case op @ $LimitF(_, _)           => rewriteRefs2_6(prefix).apply(op).shapePreserving
        case op @ $UnwindF(_, _)          => rewriteRefs2_6(prefix).apply(op).pipeline
        case op @ $GroupF(_, _, _)        => rewriteRefs2_6(prefix).apply(op).pipeline
        case op @ $SortF(_, _)            => rewriteRefs2_6(prefix).apply(op).shapePreserving
        case op @ $GeoNearF(_, _, _, _, _, _, _, _, _, _) => rewriteRefs2_6(prefix).apply(op).pipeline
        case op @ $OutF(_, _)             => rewriteRefs2_6(prefix).apply(op).shapePreserving
        case _ => scala.sys.error("never happens")
      }).fmap(ι, Inject[WorkflowOpCoreF, WorkflowF])))
  }

  // helper for rewriteRefs
  def prefixBase(base: DocVar): PartialFunction[DocVar, DocVar] =
    PartialFunction(base \\ _)

  abstract class RewriteRefs[F[_]](val applyVar0: PartialFunction[DocVar, DocVar]) {
    val applyVar = (f: DocVar) => applyVar0.lift(f).getOrElse(f)

    def applyFieldName(name: BsonField): BsonField = {
      applyVar(DocField(name)).deref.getOrElse(name) // TODO: Delete field if it's transformed away to nothing???
    }

    def applySelector(s: Selector): Selector = s.mapUpFields(PartialFunction(applyFieldName _))

    def applyMap[A](m: ListMap[BsonField, A]): ListMap[BsonField, A] = m.map(t => applyFieldName(t._1) -> t._2)

    def applyNel[A](m: NonEmptyList[(BsonField, A)]): NonEmptyList[(BsonField, A)] = m.map(t => applyFieldName(t._1) -> t._2)

    def apply[A <: F[_]](op: A): A
  }

  // NB: it's useful to be able to return the precise type here, so this is
  // explicitly implemented for each version's trait.
  // TODO: Make this a trait, and implement it for actual types, rather than all
  //       in here (already done for ExprOp and Reshape). (#438)
  private [workflow] def rewriteRefs2_6(f: PartialFunction[DocVar, DocVar])
      (implicit exprOps: ExprOpOps.Uni[ExprOp]) = new RewriteRefs[WorkflowOpCoreF](f) {
    def apply[A <: WorkflowOpCoreF[_]](op: A) = {
      (op match {
        case $ProjectF(src, shape, xId) =>
          $ProjectF(src, shape.rewriteRefs(applyVar0), xId)
        case $GroupF(src, grouped, by)  =>
          $GroupF(src,
            grouped.rewriteRefs(applyVar0),
            by.bimap(_.rewriteRefs(applyVar0), _.cata(exprOps.rewriteRefs(applyVar0))))
        case $MatchF(src, s)            => $MatchF(src, applySelector(s))
        case $RedactF(src, e)           => $RedactF(src, e.cata(exprOps.rewriteRefs(applyVar0)))
        case $UnwindF(src, f)           => $UnwindF(src, applyVar(f))
        case $SortF(src, l)             => $SortF(src, applyNel(l))
        case g: $GeoNearF[_]            =>
          g.copy(
            distanceField = applyFieldName(g.distanceField),
            query = g.query.map(applySelector))
        case _                          => op
      }).asInstanceOf[A]
    }
  }

  private [workflow] def rewriteRefs3_2(f: PartialFunction[DocVar, DocVar]) = new RewriteRefs[WorkflowOp3_2F](f) {
    def apply[A <: WorkflowOp3_2F[_]](op: A) = {
      (op match {
        case $LookupF(src, from, lf, ff, as) =>
          // NB: rewrite only the source reference; the foreignField is not part
          // of the workflow at this point
          $LookupF(src, from, applyFieldName(lf), ff, applyFieldName(as))
        case _ => op
      }).asInstanceOf[A]
    }
  }

  def simpleShape[F[_]](op: Fix[F])(implicit I: F :<: Workflow3_2F): Option[List[BsonField.Name]] = {
    I.inj(op.unFix).run.fold[Option[List[BsonField.Name]]](
      {
        case $LookupF(_, _, _, _, _) => ???
        case $SampleF(_, _)          => ???
      },
      {
        case $PureF(Bson.Doc(value))          =>
          value.keys.toList.map(BsonField.Name(_)).some
        case $ProjectF(_, Reshape(value), id) =>
          (if (id == IncludeId) IdName :: value.keys.toList
          else value.keys.toList).some
        case sm @ $SimpleMapF(_, _, _)        =>
          def loop(expr: JsCore): Option[List[jscore.Name]] =
            expr.simplify match {
              case jscore.Obj(value)      => value.keys.toList.some
              case jscore.Let(_, _, body) => loop(body)
              case _ => None
            }
          loop(sm.simpleExpr.expr).map(_.map(n => BsonField.Name(n.value)))
        case $GroupF(_, Grouped(value), _)    => (IdName :: value.keys.toList).some
        case $UnwindF(src, _)                 => simpleShape(src)
        case IsShapePreserving(sp)            => simpleShape(sp.src)
        case _                                => None
      })
  }

  /** Newtype for source ops (that is, ops that are themselves sources). */
  // TODO: prevent construction of invalid instances
  final case class SourceF[F[_], A](wf: F[A]) {
    def op(implicit ev: Functor[F]): F[Unit] = wf.void

    def fmap[G[_], B](f: F[A] => G[B]): SourceF[G, B] =
      SourceF(f(wf))
  }
  object IsSource {
    def unapply[F[_], A](op: F[A])(implicit F: Classify[F]): Option[SourceF[F, A]] =
      F.source(op)
  }

  /** Newtype for ops which have a single source op. */
  // TODO: prevent construction of invalid instances
  abstract class SingleSourceF[F[_], A] { self =>
    def wf: F[A]
    def src: A
    def reparent[B](newSrc: B): SingleSourceF[F, B]

    /** Reparenting that handles coalescing (but is more restrictive as a
      * result).
      */
    // TODO: this doesn't seem to actually handle coalescing, so what was the
    // comment referring to?
    def reparentW[T[_[_]]: Corecursive](newSrc: T[F])(implicit F: Functor[F]):
        T[F] =
      reparent(newSrc).wf.embed

    def fmap[G[_], B](f: A => B, g: F ~> G): SingleSourceF[G, B] =
      new SingleSourceF[G, B] {
        val src = f(self.src)
        val wf = g(self.reparent(src).wf)
        def reparent[C](newSrc: C) = self.reparent(newSrc).fmap(ι, g)
      }

    // NB: needed because making A covariant breaks pattern-matching ("GADT skolem" errors)
    def widen[B >: A]: SingleSourceF[F, B] = reparent(src)
  }
  object IsSingleSource {
    def unapply[F[_], A](op: F[A])(implicit F: Classify[F]): Option[SingleSourceF[F, A]] =
      F.singleSource(op)
  }

  /** Newtype for ops which can appear in aggregation pipeline. */
  // TODO: prevent construction of invalid instances
  abstract class PipelineF[F[_], A] extends SingleSourceF[F, A] { self =>
    // NB: narrows the result type
    def reparent[B](newSrc: B): PipelineF[F, B]

    def op: String
    def rhs: Bson
    def bson: Bson.Doc = Bson.Doc(ListMap(op -> rhs))

    // NB: narrows the result type
    override def fmap[G[_], B](f: A => B, g: F ~> G): PipelineF[G, B] =
      new PipelineF[G, B] {
        val src = f(self.src)
        val wf = g(self.reparent(src).wf)
        def reparent[C](newSrc: C) = self.reparent(newSrc).fmap(ι, g)

        def op = self.op
        def rhs = self.rhs
      }

    // NB: needed because making A covariant breaks pattern-matching ("GADT skolem" errors)
    override def widen[B >: A]: PipelineF[F, B] = reparent(src)
  }

  object IsPipeline {
    def unapply[F[_], A](op: F[A])(implicit F: Classify[F]): Option[PipelineF[F, A]] =
      F.pipeline(op)
  }

  /** Newtype for ops which preserve the shape of the input. */
  // TODO: prevent construction of invalid instances
  abstract class ShapePreservingF[F[_], A] extends PipelineF[F, A] { self =>
    // NB: narrows the result type
    def reparent[B](newSrc: B): ShapePreservingF[F, B]

    // NB: narrows the result type
    override def fmap[G[_], B](f: A => B, g: F ~> G): ShapePreservingF[G, B] =
      new ShapePreservingF[G, B] {
        val src = f(self.src)
        val wf = g(self.reparent(src).wf)
        def reparent[C](newSrc: C) = self.reparent(newSrc).fmap(ι, g)

        def op = self.op
        def rhs = self.rhs
      }

    // NB: needed because making A covariant breaks pattern-matching ("GADT skolem" errors)
    override def widen[B >: A]: ShapePreservingF[F, B] = reparent(src)
  }
  object IsShapePreserving {
    def unapply[F[_], A](op: F[A])(implicit F: Classify[F]): Option[ShapePreservingF[F, A]] =
      F.shapePreserving(op)
  }

  /**
   * Flattens the sequence of operations like so:
   * {{{
   * chain(
   *   \$read(Path.fileAbs("foo")),
   *   \$match(Selector.Where(Js.Bool(true))),
   *   \$limit(7))
   * }}}
   * {{{
   * val read = \$read(Path.fileAbs("foo"))
   * val match = \$match(Selector.Where(Js.Bool(true))(read)
   * \$limit(7)(match)
   * }}}
   */
  def chain[A](src: A, op1: A => A, ops: (A => A)*): A =
    ops.foldLeft(op1(src))((s, o) => o(s))

  implicit def workflowFCrush(implicit I: WorkflowOpCoreF :<: WorkflowF):
      Crush[WorkflowF] =
    new Crush[WorkflowF] {
      def crush[T[_[_]]: Recursive: FunctorT](
        op: WorkflowF[(T[WorkflowF], (DocVar, WorkflowTask))]) = op match {
        case $pure(value) => (DocVar.ROOT(), PureTask(value))
        case $read(coll)  => (DocVar.ROOT(), ReadTask(coll))
        case WorkflowOpCoreF(op @ $MatchF((src, rez), selector)) =>
          // TODO: If we ever allow explicit request of cursors (instead of
          //       collections), we could generate a FindQuery here.
          lazy val nonPipeline = {
            val (base, crushed) = (finish(_, _)).tupled(rez)
            (ExprVar,
              MapReduceTask(
                crushed,
                MapReduce(
                  $MapF.mapFn(base match {
                    case DocVar(DocVar.ROOT, None) => $MapF.mapNOP
                    case _                         => $MapF.mapProject(base)
                  }),
                  $ReduceF.reduceNOP,
                  // TODO: Get rid of this asInstanceOf!
                  selection = Some(rewriteRefs2_6(prefixBase(base)).apply(Functor[WorkflowOpCoreF].void(op).asInstanceOf[$MatchF[T[WorkflowOpCoreF]]]).selector)),
                None))
          }
          pipeline($MatchF[T[WorkflowF]](src, selector).shapePreserving.fmap(ι, I)) match {
            case Some((base, up, mine)) => (base, PipelineTask(up, mine))
            case None                   => nonPipeline
          }
        case IsPipeline(p) =>
          alwaysPipePipe(p.reparent(p.src._1)) match {
            case (base, up, pipe) => (base, PipelineTask(up, pipe))
          }

        case WorkflowOpCoreF(op @ $MapF(
            (_, (base, src1 @ MapReduceTask(src0, mr @ MapReduce(m, r, sel, sort, limit, None, scope0, _, _), oa))),
            fn, scope))
            if m == $MapF.mapNOP && r == $ReduceF.reduceNOP =>
          Reshape.mergeMaps(scope0, scope).fold(
            op.newMR(base, src1, sel, sort, limit))(
            s => base -> MapReduceTask(
              src0,
              mr applyLens MapReduce._map set fn
                applyLens MapReduce._scope set s,
              oa))

        // A "simple" map op that doesn't do any flattening is "inlined" into
        // the finalizer of a previous map-reduce.
        // TODO: handle more than one MapExpr.
        case WorkflowOpCoreF(op @ $SimpleMapF(
            (_, (base, src1 @ MapReduceTask(src0, mr @ MapReduce(_, _, _, _, _, None, scope0, _, _), oa))),
            NonEmptyList(MapExpr(expr), INil()),
            scope)) =>
          Reshape.mergeMaps(scope0, scope).fold(
            op.newMR(base, src1, None, None, None))(
              s => base -> MapReduceTask(
                src0,
                mr applyLens MapReduce._finalizer set Some($MapF.finalizerFn(expr))
                  applyLens MapReduce._scope set s,
                oa))

        case WorkflowOpCoreF(op @ $SimpleMapF(_, _, _)) => crush(I.inj(op.raw))

        case WorkflowOpCoreF(op @ $ReduceF((_, (base, src1 @ MapReduceTask(src0, mr @ MapReduce(_, reduceNOP, _, _, _, None, scope0, _, _), oa))), fn, scope)) =>
          Reshape.mergeMaps(scope0, scope).fold(
            op.newMR(base, src1, None, None, None))(
            s => base -> MapReduceTask(
              src0,
              mr applyLens MapReduce._reduce set fn
                applyLens MapReduce._scope set s,
              oa))

        case WorkflowOpCoreF(op: MapReduceF[_]) =>
          op.singleSource.src match {
            case (_, (base, PipelineTask(src0, List(PipelineOpCore($MatchF(_, sel)))))) =>
              op.newMR(base, src0, Some(sel), None, None)
            case (_, (base, PipelineTask(src0, List(PipelineOpCore($SortF(_, sort)))))) =>
              op.newMR(base, src0, None, Some(sort), None)
            case (_, (base, PipelineTask(src0, List(PipelineOpCore($LimitF(_, count)))))) =>
              op.newMR(base, src0, None, None, Some(count))
            case (_, (base, PipelineTask(src0, List(PipelineOpCore($MatchF(_, sel)), PipelineOpCore($SortF(_, sort)))))) =>
              op.newMR(base, src0, Some(sel), Some(sort), None)
            case (_, (base, PipelineTask(src0, List(PipelineOpCore($MatchF(_, sel)), PipelineOpCore($LimitF(_, count)))))) =>
              op.newMR(base, src0, Some(sel), None, Some(count))
            case (_, (base, PipelineTask(src0, List(PipelineOpCore($SortF(_, sort)), PipelineOpCore($LimitF(_, count)))))) =>
              op.newMR(base, src0, None, Some(sort), Some(count))
            case (_, (base, PipelineTask(src0, List(PipelineOpCore($MatchF(_, sel)), PipelineOpCore($SortF(_, sort)), PipelineOpCore($LimitF(_, count)))))) =>
              op.newMR(base, src0, Some(sel), Some(sort), Some(count))
            case (_, (base, srcTask)) =>
              val (nb, task) = finish(base, srcTask)
              op.newMR(nb, task, None, None, None)
          }

        case WorkflowOpCoreF($FoldLeftF(head, tail)) =>
          (ExprVar,
            FoldLeftTask(
              (finish(_, _)).tupled(head._2)._2,
              tail.map(_._2._2 match {
                case MapReduceTask(src, mr, _) =>
                  // FIXME: $FoldLeftF currently always reduces, but in future we’ll
                  //        want to have more control.
                  MapReduceTask(src, mr, Some(MapReduce.Action.Reduce(Some(true))))
                // NB: `finalize` should ensure that the final op is always a
                //     $ReduceF.
                case src => scala.sys.error("not a mapReduce: " + src)
              })))
      }

      def pipeline[T[_[_]]: Recursive: FunctorT](
        op: PipelineF[WorkflowF, T[WorkflowF]]):
          Option[(DocVar, WorkflowTask, List[PipelineOp])] =
        op.wf match {
          case $match(src, selector) =>
            def pipelinable(sel: Selector): Boolean = sel match {
              case Selector.Where(_) => false
              case comp: Selector.CompoundSelector =>
                pipelinable(comp.left) && pipelinable(comp.right)
              case _ => true
            }

            if (pipelinable(selector)) {
              lazy val (base, crushed) = src.para(Crush[WorkflowF].crush[T])
              src.project match {
                case IsPipeline(p) => pipeline(p).cata(
                  { case (base, up, prev) => Some((base, up, prev :+ toPipelineOp(op, base))) },
                  Some((base, crushed, List(toPipelineOp(op, base)))))
                case _ => Some((base, crushed, List(toPipelineOp(op, base))))
              }
            }
            else None
          // TODO: Not all $GroupFs can be pipelined. Need to determine when we may
          //       need the group command or a map/reduce.
          case _ => Some(alwaysPipePipe(op))
        }

      def alwaysPipePipe[T[_[_]]: Recursive: FunctorT](
        op: PipelineF[WorkflowF, T[WorkflowF]]):
          (DocVar, WorkflowTask, List[PipelineOp]) = {
        lazy val (base, crushed) = (finish(_, _)).tupled(op.src.para(crush[T]))
        // TODO: this is duplicated in `WorkflowBuilder.rewrite`
        def repairBase(base: DocVar) = I.prj(op.wf) match {
          case Some($GroupF(_, _, _))   => DocVar.ROOT()
          case Some($ProjectF(_, _, _)) => DocVar.ROOT()
          case _                       => base
        }
          (op.src.project match {
            case IsPipeline(p) => pipeline(p)
            case _             => None
          }).cata(
            {
              case (base, up, prev) =>
                val (nb, task) = finish(base, up)
                (repairBase(nb),
                  task,
                  prev :+ toPipelineOp(op, nb))
            },
            (repairBase(base),
              crushed,
              List(toPipelineOp(op, base))))
      }
    }

  // TODO[matryoshka]: Add this there
  def transHylo[T[_[_]]: FunctorT, F[_]: Functor, G[_]: Functor, H[_]: Functor](
    t: T[F])(
    φ: G[T[H]] => H[T[H]], ψ: F[T[F]] => G[T[F]]):
      T[H] =
    FunctorT[T].map(t)(ft => φ(ψ(ft).map(transHylo(_)(φ, ψ))))

  // NB: no need for a typeclass if implementing this way, but it will be needed
  // as soon as we need to match on anything here that isn't in core.
  implicit def crystallizeWorkflowF[F[_]: Functor: Classify: Coalesce: Refs](
    implicit I: WorkflowOpCoreF :<: F, ev1: F :<: WorkflowF, ev2: ExprOpOps.Uni[ExprOp]):
      Crystallize[F] =
    new Crystallize[F] {
      // probable conversions
      // to $MapF:          $ProjectF
      // to $FlatMapF:      $MatchF, $LimitF (using scope), $SkipF (using scope), $UnwindF, $GeoNearF
      // to $MapF/$ReduceF:  $GroupF
      // ???:              $RedactF
      // none:             $SortF
      // NB: We don’t convert a $ProjectF after a map/reduce op because it could
      //     affect the final shape unnecessarily.
      def crystallize(op: Fix[F]) = {
        def unwindSrc(uw: $UnwindF[Fix[F]]): F[Fix[F]] =
          uw.src.project match {
            case WorkflowOpCoreF(uw1 @ $UnwindF(_, _)) => unwindSrc(uw1)
            case src => src
          }

        val uncleanƒ: F[Fix[F]] => Fix[F] = {
          case WorkflowOpCoreF(x @ $SimpleMapF(_, _, _)) => I.inj(x.raw).embed
          case x                                         => x.embed
        }

        val crystallizeƒ: F[Fix[F]] => F[Fix[F]] = {
          case WorkflowOpCoreF(mr: MapReduceF[Fix[F]]) => mr.singleSource.src.project match {
            case $project(src, shape, _)  =>
              shape.toJs.fold(
                κ(I.inj(mr)),
                x => {
                  val base = jscore.Name("__rez")
                  mr.singleSource.fmap(ι, I).reparentW(
                    chain(src,
                      $simpleMap[F](
                        NonEmptyList(MapExpr(JsFn(base, x(jscore.Ident(base))))),
                        ListMap()))).project
                })
            case WorkflowOpCoreF(uw @ $UnwindF(_, _)) if IsPipeline.unapply(unwindSrc(uw)).isEmpty =>
              mr.singleSource.fmap(ι, I).reparentW(I.inj(uw.flatmapop).embed).project
            case _                        => I.inj(mr)
          }
          case WorkflowOpCoreF($FoldLeftF(head, tail)) =>
            I.inj($FoldLeftF[Fix[F]](
              chain(head,
                $project[F](
                  Reshape(ListMap(ExprName -> \/-($$ROOT))),
                  IncludeId)),
              tail.map(x => x.project match {
                case $reduce(_, _, _) => x
                case _ => chain(x, $reduce[F]($ReduceF.reduceFoldLeft, ListMap()))
              })))

          case op => op
        }

        val finished =
          deleteUnusedFields(reorderOps(op.transCata(orOriginal(simplifyGroupƒ[F]))))

        def fixShape(wf: Fix[F]) =
          simpleShape(wf).fold(
            finished)(
            n => $project[F](Reshape(n.map(_ -> \/-($include())).toListMap), IgnoreId).apply(finished))

        def promoteKnownShape(wf: Fix[F]): Fix[F] = wf.project match {
          case $simpleMap(_, _, _)   => fixShape(wf)
          case IsShapePreserving(sp) => promoteKnownShape(sp.src)
          case _                     => finished
        }

        Crystallized(
          transHylo(promoteKnownShape(finished))(Coalesce[F].coalesce, crystallizeƒ)
            // TODO: this can coalesce more cases, but hasn’t been done thus far and
            //       requires rewriting many tests in a much less readable way.
            // .cata[Workflow](x => coalesce(uncleanƒ(x).project))
        )
      }
    }

  implicit def workflowRenderTree[T[_[_]]: Recursive, F[_]: Traverse: Classify]
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: RenderTree[F[Unit]])
      : RenderTree[T[F]] =
    new RenderTree[T[F]] {
      val wfType = "Workflow" :: Nil

      def chain(op: T[F]): List[RenderedTree] = op.project match {
        case IsSingleSource(ss) =>
          chain(ss.src) :+ Traverse[F].void(ss.wf).render
        case _ => List(render(op))
      }

      def render(v: T[F]) = v.project match {
        case IsSource(s)       => s.op.render
        case IsSingleSource(_) =>
          NonTerminal("Chain" :: wfType, None, chain(v))
        case $foldLeft(_, _) =>
          NonTerminal("$FoldLeftF" :: wfType, None, v.children.map(render(_)))
      }
    }
}
