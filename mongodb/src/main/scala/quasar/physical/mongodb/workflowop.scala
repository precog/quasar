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
import quasar.{RenderTree, RenderedTree, Terminal, NonTerminal}, RenderTree.ops._
import quasar.fp._
import optimize.pipeline._
import quasar.javascript._, Js._
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.physical.mongodb.workflowtask._
import quasar.qscript._

import matryoshka._, Recursive.ops._, FunctorT.ops._
import monocle.syntax.all._
import scalaz._, Scalaz._
import shapeless.contrib.scalaz._
import simulacrum.typeclass

sealed trait IdHandling
object IdHandling {
  final case object ExcludeId extends IdHandling
  final case object IncludeId extends IdHandling
  final case object IgnoreId extends IdHandling

  implicit val IdHandlingMonoid: Monoid[IdHandling] = new Monoid[IdHandling] {
    // this is the `coalesce` function
    def append(f1: IdHandling, f2: => IdHandling) = (f1, f2) match {
      case (_, IgnoreId) => f1
      case (_, _)        => f2
    }

    def zero = IgnoreId
  }
}

/*
  A Workflow is a graph of atomic operations, with WorkflowOps for the vertices.
  We crush them down into a WorkflowTask. This `crush` gives us a location to
  optimize our workflow decisions. EG, A sequence of simple ops may be combined
  into a single pipeline request, but if one of those operations contains JS, we
  have to execute that outside of a pipeline, possibly reordering the other
  operations to avoid having two pipelines with a JS operation in the middle.

  We also implement the optimizations at
  http://docs.mongodb.org/manual/core/aggregation-pipeline-optimization/ so that
  we can build others potentially on top of them (including reordering
  non-pipelines around pipelines, etc.).
  */

/** Ops that are provided by all supported MongoDB versions (since 2.6), or are
  * internal to quasar and supported everywhere. */
sealed trait WorkflowOpCoreF[+A]
object WorkflowOpCoreF {
  // NB: this extractor has to be used instead of the simpler ones provided for
  // each op if you need to bind the op itself, and not just its fields.
  // For example: `case $project(src, shape, id) => ` vs.
  // `case WorkflowOpCoreF(p @ $ProjectF(_, _, _)) =>`
  def unapply[F[_], A](fa: F[A])(implicit I: WorkflowOpCoreF :<: F): Option[WorkflowOpCoreF[A]] =
    I.prj(fa)
}
/** Ops that are provided by MongoDB since 3.2. */
sealed trait WorkflowOp3_2F[+A]
object WorkflowOp3_2F {
  // NB: this extractor has to be used instead of the simpler ones provided for
  // each op if you need to bind the op itself, and not just its fields.
  // For example: `case $lookup(src, from, lf, ff, as) => ` vs.
  // `case WorkflowOp3_2F(l @ $LookupF(_, _, _, _, _)) =>`
  def unapply[F[_], A](fa: F[A])(implicit I: WorkflowOp3_2F :<: F): Option[WorkflowOp3_2F[A]] =
    I.prj(fa)
}

object Workflow {
  import quasar.physical.mongodb.accumulator._
  import quasar.physical.mongodb.expression._
  import IdHandling._
  import MapReduce._

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

  sealed trait CardinalExpr[A]
  final case class MapExpr[A](fn: A) extends CardinalExpr[A]
  final case class FlatExpr[A](fn: A) extends CardinalExpr[A]

  implicit val TraverseCardinalExpr: Traverse[CardinalExpr] =
    new Traverse[CardinalExpr] {
      def traverseImpl[G[_]: Applicative, A, B](
        fa: CardinalExpr[A])(f: A => G[B]):
          G[CardinalExpr[B]] =
        fa match {
          case MapExpr(e)  => f(e).map(MapExpr(_))
          case FlatExpr(e) => f(e).map(FlatExpr(_))
        }
    }

  implicit val CardinalExprComonad: Comonad[CardinalExpr] =
    new Comonad[CardinalExpr] {
      def map[A, B](fa: CardinalExpr[A])(f: A => B): CardinalExpr[B] =
        fa match {
          case MapExpr(e)  => MapExpr(f(e))
          case FlatExpr(e) => FlatExpr(f(e))
        }

      def cobind[A, B](fa: CardinalExpr[A])(f: CardinalExpr[A] => B):
          CardinalExpr[B] = fa match {
        case MapExpr(_)  => MapExpr(f(fa))
        case FlatExpr(_) => FlatExpr(f(fa))
      }

      def copoint[A](p: CardinalExpr[A]) = p match {
        case MapExpr(e)  => e
        case FlatExpr(e) => e
      }
    }

  // implicit val PipelineFTraverse: Traverse[PipelineF] =
  //   new Traverse[PipelineF] {
  //     def traverseImpl[G[_], A, B](fa: PipelineF[A])(f: A => G[B])
  //       (implicit G: Applicative[G]):
  //         G[PipelineF[B]] = fa match {
  //       case $MatchF(src, sel)         => G.apply(f(src))($MatchF(_, sel))
  //       case $ProjectF(src, shape, id) => G.apply(f(src))($ProjectF(_, shape, id))
  //       case $RedactF(src, value)      => G.apply(f(src))($RedactF(_, value))
  //       case $LimitF(src, count)       => G.apply(f(src))($LimitF(_, count))
  //       case $SkipF(src, count)        => G.apply(f(src))($SkipF(_, count))
  //       case $UnwindF(src, field)      => G.apply(f(src))($UnwindF(_, field))
  //       case $GroupF(src, grouped, by) => G.apply(f(src))($GroupF(_, grouped, by))
  //       case $SortF(src, value)        => G.apply(f(src))($SortF(_, value))
  //       case $GeoNearF(src, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs) =>
  //         G.apply(f(src))($GeoNearF(_, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs))
  //       case $OutF(src, col)           => G.apply(f(src))($OutF(_, col))
  //     }
  //   }

  implicit val WorkflowOpCoreFTraverse: Traverse[WorkflowOpCoreF] =
    new Traverse[WorkflowOpCoreF] {
      def traverseImpl[G[_], A, B](fa: WorkflowOpCoreF[A])(f: A => G[B])
        (implicit G: Applicative[G]):
          G[WorkflowOpCoreF[B]] = fa match {
        case x @ $PureF(_)             => G.point(x)
        case x @ $ReadF(_)             => G.point(x)
        case $MapF(src, fn, scope)     => G.apply(f(src))($MapF(_, fn, scope))
        case $FlatMapF(src, fn, scope) => G.apply(f(src))($FlatMapF(_, fn, scope))
        case $SimpleMapF(src, exprs, scope) =>
          G.apply(f(src))($SimpleMapF(_, exprs, scope))
        case $ReduceF(src, fn, scope)  => G.apply(f(src))($ReduceF(_, fn, scope))
        case $FoldLeftF(head, tail)    =>
          G.apply2(f(head), tail.traverse(f))($FoldLeftF(_, _))
        // NB: Would be nice to replace the rest of this impl with the following
        //     line, but the invariant definition of Traverse doesn’t allow it.
        // case p: PipelineF[_]          => PipelineFTraverse.traverseImpl(p)(f)
        case $MatchF(src, sel)         => G.apply(f(src))($MatchF(_, sel))
        case $ProjectF(src, shape, id) => G.apply(f(src))($ProjectF(_, shape, id))
        case $RedactF(src, value)      => G.apply(f(src))($RedactF(_, value))
        case $LimitF(src, count)       => G.apply(f(src))($LimitF(_, count))
        case $SkipF(src, count)        => G.apply(f(src))($SkipF(_, count))
        case $UnwindF(src, field)      => G.apply(f(src))($UnwindF(_, field))
        case $GroupF(src, grouped, by) => G.apply(f(src))($GroupF(_, grouped, by))
        case $SortF(src, value)        => G.apply(f(src))($SortF(_, value))
        case $GeoNearF(src, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs) =>
          G.apply(f(src))($GeoNearF(_, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs))
        case $OutF(src, col)           => G.apply(f(src))($OutF(_, col))
      }
    }

  implicit val WorkflowOp3_2FTraverse: Traverse[WorkflowOp3_2F] =
    new Traverse[WorkflowOp3_2F] {
      def traverseImpl[G[_], A, B](fa: WorkflowOp3_2F[A])(f: A => G[B])
        (implicit G: Applicative[G]):
          G[WorkflowOp3_2F[B]] = fa match {
        case $LookupF(src, from, localField, foreignField, as) =>
          G.apply(f(src))($LookupF(_, from, localField, foreignField, as))
        case $SampleF(src, size)       => G.apply(f(src))($SampleF(_, size))
      }
    }

  def task[F[_]: Functor](fop: Crystallized[F])(implicit C: Crush[F]): WorkflowTask =
    (finish(_, _)).tupled(fop.op.para(C.crush))._2.transAna(normalize)

  @typeclass sealed trait Coalesce[F[_]] {
    def coalesceƒ: F[Fix[F]] => Option[F[Fix[F]]]

    def coalesce: F[Fix[F]] => F[Fix[F]] = repeatedly(coalesceƒ)
  }

  // NB: no need for a typeclass if implementing this way, but will be needed as
  // soon as we need to coalesce anything _into_ a type that isn't 2.6.
  // Furthermore, if this implementation is made implicit, then lots of functions
  // that require it are able to resolve it from other evidence. Since that seems
  // likely to be a short-lived phenomenon, instead for now implicits are defined
  // below for just the specific types being used.
  def coalesceAll[F[_]: Functor](implicit I: WorkflowOpCoreF :<: F): Coalesce[F] = new Coalesce[F] {
    def coalesceƒ: F[Fix[F]] => Option[F[Fix[F]]] = {
      case $match(src, selector) => src.unFix match {
        case $sort(src0, value) =>
          I.inj($SortF(Fix(I.inj($MatchF(src0, selector))), value)).some
        case $match(src0, sel0) =>
          I.inj($MatchF(src0, sel0 ⊹ selector)).some
        case _ => None
      }
      case WorkflowOpCoreF(p @ $ProjectF(src, shape, id)) => src.unFix match {
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
        case $unwind(Fix($group(src, grouped, by)), unwound)
            if id != ExcludeId =>
          inlineProjectUnwindGroup(shape, unwound, grouped).map { case (unwound, grouped) =>
            I.inj($UnwindF(Fix(I.inj($GroupF(src, grouped, by))), unwound))
          }
        case _ => None
      }
      case $sort(Fix($sort(src, sort1)), sort2) =>
        I.inj($SortF(src, sort2 ⊹ sort1)).some
      case $limit(src, count) => src.unFix match {
        case $limit(src0, count0) =>
          I.inj($LimitF(src0, count0 min count)).some
        case $skip(src0, count0) =>
          I.inj($SkipF(Fix(I.inj($LimitF(src0, count0 + count))), count0)).some
        case _ => None
      }
      case $skip(src, count) => src.unFix match {
        case $skip(src0, count0) => I.inj($SkipF(src0, count0 + count)).some
        case _                   => None
      }
      case $group(src, grouped, \/-($literal(bson))) if bson != Bson.Null =>
        I.inj($GroupF(src, grouped, \/-($literal(Bson.Null)))).some
      case WorkflowOpCoreF(op0 @ $GroupF(_, _, _)) =>
        inlineGroupProjects(op0).map { case (src, gr, by) => I.inj($GroupF(src, gr, by)) }
      case $geoNear(src, _, _, _, _, _, _, _, _, _) => src.unFix match {
        // FIXME: merge the params
        case $geoNear(_, _, _, _, _, _, _, _, _, _) => None
        case _                                      => None
      }
      case $map(src, fn, scope) => src.unFix match {
        case $map(src0, fn0, scope0) =>
          Reshape.mergeMaps(scope0, scope).map(sc =>
            I.inj($MapF(src0, $MapF.compose(fn, fn0), sc)))
        case $flatMap(src0, fn0, scope0) =>
          Reshape.mergeMaps(scope0, scope).map(sc =>
            I.inj($FlatMapF(src0, $FlatMapF.mapCompose(fn, fn0), sc)))
        case _                   => None
      }
      case $flatMap(src, fn, scope) => src.unFix match {
        case $map(src0, fn0, scope0)     =>
          Reshape.mergeMaps(scope0, scope).map(sc =>
            I.inj($FlatMapF(src0, $MapF.compose(fn, fn0), sc)))
        case $flatMap(src0, fn0, scope0) =>
          Reshape.mergeMaps(scope0, scope).map(sc =>
            I.inj($FlatMapF(src0, $FlatMapF.kleisliCompose(fn, fn0), sc)))
        case _                   => None
      }
      case WorkflowOpCoreF(sm @ $SimpleMapF(src, _, _)) => src.unFix match {
        case WorkflowOpCoreF(sm0 @ $SimpleMapF(_, _, _)) => I.inj(sm0 >>> sm).some
        case _                                      => None
      }
      case WorkflowOpCoreF($FoldLeftF(head, tail)) => head.unFix match {
        case WorkflowOpCoreF($FoldLeftF(head0, tail0)) =>
          I.inj($FoldLeftF(head0, tail0 ⊹ tail)).some
        case _                       => None
      }
      case $out(src, _) => src.unFix match {
        case $read(_) => src.unFix.some
        case _        => None
      }
      case _ => None
    }
  }
  implicit val coalesce2_6: Coalesce[WorkflowOpCoreF] = coalesceAll[WorkflowOpCoreF]
  implicit val coalesce3_2: Coalesce[Workflow3_2F] = coalesceAll[Workflow3_2F]

  def toPipelineOp[F[_]: Functor, A](op: PipelineF[F, A], base: DocVar)(implicit I: F :<: WorkflowF): PipelineOp = {
    val prefix = prefixBase(base)
    PipelineOp(I.inj(op.wf.void).run.fold(
      op => (op match {
        case op @ $LookupF(_, _, _, _, _) => rewriteRefs3_2(op, prefix).pipeline
        case op @ $SampleF(_, _)          => rewriteRefs3_2(op, prefix).pipeline
      }).fmap(ι, Inject[WorkflowOp3_2F, WorkflowF]),
      op => (op match {
        case op @ $MatchF(_, _)           => rewriteRefs2_6(op, prefix).shapePreserving
        case op @ $ProjectF(_, _, _)      => rewriteRefs2_6(op, prefix).pipeline
        case op @ $RedactF(_, _)          => rewriteRefs2_6(op, prefix).pipeline
        case op @ $SkipF(_, _)            => rewriteRefs2_6(op, prefix).shapePreserving
        case op @ $LimitF(_, _)           => rewriteRefs2_6(op, prefix).shapePreserving
        case op @ $UnwindF(_, _)          => rewriteRefs2_6(op, prefix).pipeline
        case op @ $GroupF(_, _, _)        => rewriteRefs2_6(op, prefix).pipeline
        case op @ $SortF(_, _)            => rewriteRefs2_6(op, prefix).shapePreserving
        case op @ $GeoNearF(_, _, _, _, _, _, _, _, _, _) => rewriteRefs2_6(op, prefix).pipeline
        case op @ $OutF(_, _)             => rewriteRefs2_6(op, prefix).shapePreserving
        case _ => scala.sys.error("never happens")
      }).fmap(ι, Inject[WorkflowOpCoreF, WorkflowF])))
  }

  /** Operations that are applied to a completed workflow to produce an
    * executable WorkflowTask. NB: when this is applied, information about the
    * the type of plan (i.e. the required MongoDB version) is discarded.
    */
  @typeclass sealed trait Crush[F[_]] {
    /**
      Returns both the final WorkflowTask as well as a DocVar indicating the base
      of the collection.
      */
    def crush(op: F[(Fix[F], (DocVar, WorkflowTask))]): (DocVar, WorkflowTask)
  }
  object Crush {
    implicit def crushInjected[F[_]: Functor, G[_]: Functor](implicit I: F :<: G, CG: Crush[G]): Crush[F] =
      new Crush[F] {
        def crush(op: F[(Fix[F], (DocVar, WorkflowTask))]): (DocVar, WorkflowTask) =
          CG.crush(I.inj(op.map { case (f, t) => (f.transCata(I.inj(_)), t) }))
      }
  }

  implicit def crushWorkflowF(implicit I: WorkflowOpCoreF :<: WorkflowF): Crush[WorkflowF] = new Crush[WorkflowF] {
    def crush(op: WorkflowF[(Fix[WorkflowF], (DocVar, WorkflowTask))]): (DocVar, WorkflowTask) = op match {
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
                selection = Some(rewriteRefs2_6(Functor[WorkflowOpCoreF].void(op).asInstanceOf[$MatchF[Fix[WorkflowOpCoreF]]], prefixBase(base)).selector)),
              None))
        }
        pipeline($MatchF[Fix[WorkflowF]](src, selector).shapePreserving.fmap(ι, I)) match {
          case Some((base, up, mine)) => (base, PipelineTask(up, mine))
          case None                   => nonPipeline
        }
      case IsPipeline(p) =>
        alwaysPipePipe(p.reparent(p.src._1)) match {
          case (base, up, pipe) => (base, PipelineTask(up, pipe))
        }

      case WorkflowOpCoreF(op @ $MapF((_, (base, src1 @ MapReduceTask(src0, mr @ MapReduce(m, r, sel, sort, limit, None, scope0, _, _), oa))), fn, scope)) if m == $MapF.mapNOP && r == $ReduceF.reduceNOP =>
        Reshape.mergeMaps(scope0, scope).fold(
          op.newMR(base, src1, sel, sort, limit))(
          s => base -> MapReduceTask(
            src0,
            mr applyLens MapReduce._map set fn
               applyLens MapReduce._scope set s,
            oa))

      case WorkflowOpCoreF(op @ $MapF((_, (base, src1 @ MapReduceTask(src0, mr @ MapReduce(_, _, _, _, _, None, scope0, _, _), oa))), fn, scope)) =>
        Reshape.mergeMaps(scope0, scope).fold(
          op.newMR(base, src1, None, None, None))(
          s => base -> MapReduceTask(
            src0,
            mr applyLens MapReduce._finalizer set Some($MapF.finalizerFn(fn))
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

    def pipeline(op: PipelineF[WorkflowF, Fix[WorkflowF]]):
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
            lazy val (base, crushed) = src.para(Crush[WorkflowF].crush)
            src.unFix match {
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

    def alwaysPipePipe(op: PipelineF[WorkflowF, Fix[WorkflowF]])
      : (DocVar, WorkflowTask, List[PipelineOp]) = {
      lazy val (base, crushed) = (finish(_, _)).tupled(op.src.para(crush))
      // TODO: this is duplicated in `WorkflowBuilder.rewrite`
      def repairBase(base: DocVar) = I.prj(op.wf) match {
        case Some($GroupF(_, _, _))   => DocVar.ROOT()
        case Some($ProjectF(_, _, _)) => DocVar.ROOT()
        case _                       => base
      }
      (op.src.unFix match {
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

  implicit val crush2_6: Crush[WorkflowOpCoreF] = Crush.crushInjected[WorkflowOpCoreF, WorkflowF]

  def collectShapes[F[_]](implicit I: WorkflowOpCoreF :<: F)
    : F[(Fix[F], (List[Reshape], Fix[F]))] => (List[Reshape], Fix[F]) =
    I.prj(_) match {
      case Some($ProjectF(src, shape, _)) =>
        ((x: List[Reshape]) => shape :: x).first(src._2)
      case Some(x)                       => (Nil, Fix(I.inj(x.map(_._1))))
      case None                          => ???  // TODO: implement for 3.2 ops
    }

  // helper for rewriteRefs
  def prefixBase(base: DocVar): PartialFunction[DocVar, DocVar] =
    PartialFunction(base \\ _)


  @typeclass sealed trait Refs[F[_]] {
    def refs[A](op: F[A]): List[DocVar]
  }
  object Refs {
    def fromRewrite[F[_]](rewrite: (F[_], PartialFunction[DocVar, DocVar]) => F[_]) = new Refs[F] {
      def refs[A](op: F[A]): List[DocVar] = {
        // FIXME: Sorry world
        val vf = new scala.collection.mutable.ListBuffer[DocVar]
        ignore(rewrite(op, { case v => ignore(vf += v); v }))
        vf.toList
      }
    }

    implicit def coproductRefs[F[_], G[_]](implicit RF: Refs[F], RG: Refs[G]): Refs[Coproduct[F, G, ?]] =
      new Refs[Coproduct[F, G, ?]] {
        def refs[A](op: Coproduct[F, G, A]): List[DocVar] =
          op.run.fold(RF.refs, RG.refs)
      }
  }

  implicit val refs2_6: Refs[WorkflowOpCoreF] = Refs.fromRewrite[WorkflowOpCoreF](rewriteRefs2_6)
  implicit val refs3_2: Refs[WorkflowOp3_2F] = Refs.fromRewrite[WorkflowOp3_2F](rewriteRefs3_2)


  // NB: it's useful to be able to return the precise type here, so this is
  // explicitly implemented for each version's trait.
  // TODO: Make this a trait, and implement it for actual types, rather than all
  //       in here (already done for ExprOp and Reshape). (#438)
  private def rewriteRefs2_6[A <: WorkflowOpCoreF[_]](
    op: A, applyVar0: PartialFunction[DocVar, DocVar]):
      A = {
    val applyVar = (f: DocVar) => applyVar0.lift(f).getOrElse(f)

    def applyFieldName(name: BsonField): BsonField = {
      applyVar(DocField(name)).deref.getOrElse(name) // TODO: Delete field if it's transformed away to nothing???
    }

    def applySelector(s: Selector): Selector = s.mapUpFields(PartialFunction(applyFieldName _))

    def applyMap[A](m: ListMap[BsonField, A]): ListMap[BsonField, A] = m.map(t => applyFieldName(t._1) -> t._2)

    def applyNel[A](m: NonEmptyList[(BsonField, A)]): NonEmptyList[(BsonField, A)] = m.map(t => applyFieldName(t._1) -> t._2)

    (op match {
      case $ProjectF(src, shape, xId) =>
        $ProjectF(src, shape.rewriteRefs(applyVar0), xId)
      case $GroupF(src, grouped, by)  =>
        $GroupF(src,
          grouped.rewriteRefs(applyVar0),
          by.bimap(_.rewriteRefs(applyVar0), rewriteExprRefs(_)(applyVar0)))
      case $MatchF(src, s)            => $MatchF(src, applySelector(s))
      case $RedactF(src, e)           => $RedactF(src, rewriteExprRefs(e)(applyVar0))
      case $UnwindF(src, f)           => $UnwindF(src, applyVar(f))
      case $SortF(src, l)             => $SortF(src, applyNel(l))
      case g: $GeoNearF[_]            =>
        g.copy(
          distanceField = applyFieldName(g.distanceField),
          query = g.query.map(applySelector))
      case _                          => op
    }).asInstanceOf[A]
  }

  def rewriteRefs3_2[A <: WorkflowOp3_2F[_]]
      (op: A, applyVar0: PartialFunction[DocVar, DocVar]): A =
      // TODO: implement for 3.2 ops
      op

  // implicit def coproductRewrite[F[_], G[_]](implicit RF: Rewrite[F], RG: Rewrite[G]): Rewrite[Coproduct[F, G, ?]] = new Rewrite[Coproduct[F, G, ?]] {
  //   def rewriteRefsx[A](op: Coproduct[F, G, A], applyVar0: PartialFunction[DocVar, DocVar]) =
  //     op.run.fold(
  //       fa => Coproduct.leftc(RF.rewriteRefsx(fa, applyVar0)),
  //       ga => Coproduct.rightc(RG.rewriteRefsx(ga, applyVar0)))
  //
  //   // def rewriteBase[A <: Coproduct[F, G, _]](op: A, base: DocVar) =
  //   //   base
  // }

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

  @typeclass sealed trait Classify[F[_]] {
    def source[A](op: F[A]):          Option[SourceF[F, A]]

    def singleSource[A](op: F[A]):    Option[SingleSourceF[F, A]]
    def pipeline[A](op: F[A]):        Option[PipelineF[F, A]]
    def shapePreserving[A](op: F[A]): Option[ShapePreservingF[F, A]]
  }
  object Classify {
    implicit def coproductClassify[F[_]: Functor, G[_]: Functor, A]
      (implicit CF: Classify[F], CG: Classify[G])
      : Classify[Coproduct[F, G, ?]] = new Classify[Coproduct[F, G, ?]] {
      def source[A](v: Coproduct[F, G, A]) =
        v.run.fold(
          CF.source(_).map(_.fmap(Coproduct.leftc[F, G, A](_))),
          CG.source(_).map(_.fmap(Coproduct.rightc[F, G, A](_))))

      def singleSource[A](v: Coproduct[F, G, A]) =
        v.run.fold(
          CF.singleSource(_).map(_.fmap(ι, Inject[F, Coproduct[F, G, ?]])),
          CG.singleSource(_).map(_.fmap(ι, Inject[G, Coproduct[F, G, ?]])))

      def pipeline[A](v: Coproduct[F, G, A]) =
        v.run.fold(
          CF.pipeline(_).map(_.fmap(ι, Inject[F, Coproduct[F, G, ?]])),
          CG.pipeline(_).map(_.fmap(ι, Inject[G, Coproduct[F, G, ?]])))

      def shapePreserving[A](v: Coproduct[F, G, A]) =
        v.run.fold(
          CF.shapePreserving(_).map(_.fmap(ι, Inject[F, Coproduct[F, G, ?]])),
          CG.shapePreserving(_).map(_.fmap(ι, Inject[G, Coproduct[F, G, ?]])))
    }
  }

  implicit val classify2_6: Classify[WorkflowOpCoreF] = new Classify[WorkflowOpCoreF] {
    override def source[A](op: WorkflowOpCoreF[A]) = op match {
      case $ReadF(_) | $PureF(_) => SourceF(op).some
      case _ => None
    }

    override def singleSource[A](op: WorkflowOpCoreF[A]) = op match {
      case op @ $MatchF(_, _)        => op.shapePreserving.widen[A].some
      case op @ $ProjectF(_, _, _)   => op.pipeline.widen[A].some
      case op @ $RedactF(_, _)       => op.pipeline.widen[A].some
      case op @ $SkipF(_, _)         => op.shapePreserving.widen[A].some
      case op @ $LimitF(_, _)        => op.shapePreserving.widen[A].some
      case op @ $UnwindF(_, _)       => op.pipeline.widen[A].some
      case op @ $GroupF(_, _, _)     => op.pipeline.widen[A].some
      case op @ $SortF(_, _)         => op.shapePreserving.widen[A].some
      case op @ $GeoNearF(_, _, _, _, _, _, _, _, _, _) => op.pipeline.widen[A].some
      case op @ $OutF(_, _)          => op.shapePreserving.widen[A].some
      case op @ $MapF(_, _, _)       => op.singleSource.widen[A].some
      case op @ $SimpleMapF(_, _, _) => op.singleSource.widen[A].some
      case op @ $FlatMapF(_, _, _)   => op.singleSource.widen[A].some
      case op @ $ReduceF(_, _, _)    => op.singleSource.widen[A].some
      case _ => None
    }

    override def pipeline[A](op: WorkflowOpCoreF[A]) = op match {
      case op @ $MatchF(_, _)      => op.shapePreserving.widen[A].some
      case op @ $ProjectF(_, _, _) => op.pipeline.widen[A].some
      case op @ $RedactF(_, _)     => op.pipeline.widen[A].some
      case op @ $SkipF(_, _)       => op.shapePreserving.widen[A].some
      case op @ $LimitF(_, _)      => op.shapePreserving.widen[A].some
      case op @ $UnwindF(_, _)     => op.pipeline.widen[A].some
      case op @ $GroupF(_, _, _)   => op.pipeline.widen[A].some
      case op @ $SortF(_, _)       => op.shapePreserving.widen[A].some
      case op @ $GeoNearF(_, _, _, _, _, _, _, _, _, _) => op.pipeline.widen[A].some
      case op @ $OutF(_, _)        => op.shapePreserving.widen[A].some
      case _ => None
    }

    override def shapePreserving[A](op: WorkflowOpCoreF[A]) = op match {
      case op @ $MatchF(_, _) => op.shapePreserving.widen[A].some
      case op @ $SkipF(_, _)  => op.shapePreserving.widen[A].some
      case op @ $LimitF(_, _) => op.shapePreserving.widen[A].some
      case op @ $SortF(_, _)  => op.shapePreserving.widen[A].some
      case op @ $OutF(_, _)   => op.shapePreserving.widen[A].some
      case _ => None
    }
  }

  implicit val classify3_2: Classify[WorkflowOp3_2F] = new Classify[WorkflowOp3_2F] {
    override def source[A](op: WorkflowOp3_2F[A]) =
      None

    override def singleSource[A](op: WorkflowOp3_2F[A]) = op match {
      case op @ $LookupF(_, _, _, _, _) => op.pipeline.widen[A].some
      case op @ $SampleF(_, _)          => op.pipeline.widen[A].some
    }

    override def pipeline[A](op: WorkflowOp3_2F[A]) = op match {
      case op @ $LookupF(_, _, _, _, _) => op.pipeline.widen[A].some
      case op @ $SampleF(_, _)          => op.pipeline.widen[A].some
    }

    override def shapePreserving[A](op: WorkflowOp3_2F[A]) =
      None
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
  sealed trait SingleSourceF[F[_], A] { self =>
    def wf: F[A]
    def src: A
    def reparent[B](newSrc: B): SingleSourceF[F, B]

    /** Reparenting that handles coalescing (but is more restrictive as a
      * result).
      */
    // TODO: this doesn't seem to actually handle coalescing, so what was the
    // comment referring to?
    def reparentW(newSrc: Fix[F]): Fix[F] = Fix(reparent(newSrc).wf)

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
  sealed trait PipelineF[F[_], A] extends SingleSourceF[F, A] { self =>
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
  sealed trait ShapePreservingF[F[_], A] extends PipelineF[F, A] { self =>
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

  /** A type for a `Workflow` which has had `crystallize` applied to it. */
  final case class Crystallized[F[_]](op: Fix[F]) {
    def inject[G[_]: Functor](implicit F: Functor[F], I: F :<: G): Crystallized[G] =
      copy(op = op.transCata(I.inj(_)))
  }

  @typeclass sealed trait Crystallize[F[_]] {
    /**
      Performs some irreversible conversions, meant to be used once, after the
      entire workflow has been generated.
      */
    def crystallize(op: Fix[F]): Crystallized[F]
  }

  // NB: no need for a typeclass if implementing this way, but it will be needed
  // as soon as we need to match on anything here that isn't in core.
  implicit def crystallizeWorkflowF[F[_]: Functor: Classify: Coalesce: Refs](implicit I: WorkflowOpCoreF :<: F, ev1: F :<: WorkflowF): Crystallize[F] = new Crystallize[F] {
    // probable conversions
    // to $MapF:          $ProjectF
    // to $FlatMapF:      $MatchF, $LimitF (using scope), $SkipF (using scope), $UnwindF, $GeoNearF
    // to $MapF/$ReduceF:  $GroupF
    // ???:              $RedactF
    // none:             $SortF
    // NB: We don’t convert a $ProjectF after a map/reduce op because it could
    //     affect the final shape unnecessarily.
    def crystallize(op: Fix[F]): Crystallized[F] = {
      def unwindSrc(uw: $UnwindF[Fix[F]]): F[Fix[F]] =
        uw.src.unFix match {
          case WorkflowOpCoreF(uw1 @ $UnwindF(_, _)) => unwindSrc(uw1)
          case src => src
        }

      val uncleanƒ: F[Fix[F]] => Fix[F] = {
        case WorkflowOpCoreF(x @ $SimpleMapF(_, _, _)) => Fix(I.inj(x.raw))
        case x                                     => Fix(x)
      }

      val crystallizeƒ: F[Fix[F]] => F[Fix[F]] = {
        case WorkflowOpCoreF(mr: MapReduceF[Fix[F]]) => mr.singleSource.src.unFix match {
          case $project(src, shape, _)  =>
            shape.toJs.fold(
              κ(I.inj(mr)),
              x => {
                val base = jscore.Name("__rez")
                mr.singleSource.fmap(ι, I).reparentW(
                  chain(src,
                    $simpleMap[F](
                      NonEmptyList(MapExpr(JsFn(base, x(jscore.Ident(base))))),
                      ListMap()))).unFix
              })
          case WorkflowOpCoreF(uw @ $UnwindF(_, _)) if IsPipeline.unapply(unwindSrc(uw)).isEmpty =>
            mr.singleSource.fmap(ι, I).reparentW(Fix(I.inj(uw.flatmapop))).unFix
          case _                        => I.inj(mr)
        }
        case WorkflowOpCoreF($FoldLeftF(head, tail)) =>
          I.inj($FoldLeftF[Fix[F]](
            chain(head,
              $project[F](
                Reshape(ListMap(ExprName -> \/-($$ROOT))),
                IncludeId)),
            tail.map(x => x.unFix match {
              case $reduce(_, _, _) => x
              case _ => chain(x, $reduce[F]($ReduceF.reduceFoldLeft, ListMap()))
            })))

        case op => op
      }

      val finished =
        deleteUnusedFields(reorderOps(op.transCata(orOriginal(simplifyGroupƒ[F]))))

      def fixShape(wf: Fix[F]) =
        Workflow.simpleShape(wf).fold(
          finished)(
          n => $project[F](Reshape(n.map(_ -> \/-($include())).toListMap), IgnoreId).apply(finished))

      def promoteKnownShape(wf: Fix[F]): Fix[F] = wf.unFix match {
        case $simpleMap(_, _, _)   => fixShape(wf)
        case IsShapePreserving(sp) => promoteKnownShape(sp.src)
        case _                     => finished
      }

      Crystallized(
        promoteKnownShape(finished).transAna(crystallizeƒ).transCata[F](Coalesce[F].coalesce)
          // TODO: this can coalesce more cases, but hasn’t been done thus far and
          //       requires rewriting many tests in a much less readable way.
          // .cata[Workflow](x => coalesce(uncleanƒ(x).unFix))
      )
    }
  }

  final case class $PureF(value: Bson) extends WorkflowOpCoreF[Nothing]
  object $pure {
    def apply[F[_]: Coalesce](value: Bson)(implicit I: WorkflowOpCoreF :<: F) =
      Fix(Coalesce[F].coalesce(I.inj($PureF(value))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[Bson] =
      I.prj(op) collect {
        case $PureF(value) => (value)
      }
  }

  final case class $ReadF(coll: Collection) extends WorkflowOpCoreF[Nothing]
  object $read {
    def apply[F[_]: Coalesce](coll: Collection)(implicit I: WorkflowOpCoreF :<: F) =
      Fix(Coalesce[F].coalesce(I.inj($ReadF(coll))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[Collection] =
      I.prj(op) collect {
        case $ReadF(coll) => (coll)
      }
  }

  final case class $MatchF[A](src: A, selector: Selector)
    extends WorkflowOpCoreF[A]  { self =>
    def shapePreserving: ShapePreservingF[WorkflowOpCoreF, A] =
      new ShapePreservingF[WorkflowOpCoreF, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = self.copy(src = newSrc).shapePreserving

        def op = "$match"
        def rhs = selector.bson
      }
  }
  object $match {
    def apply[F[_]: Coalesce](selector: Selector)
      (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($MatchF(src, selector))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[(A, Selector)] =
      I.prj(op) collect {
        case $MatchF(src, sel) => (src, sel)
      }
  }

  final case class $ProjectF[A](src: A, shape: Reshape, idExclusion: IdHandling)
      extends WorkflowOpCoreF[A] { self =>
    def pipeline: PipelineF[WorkflowOpCoreF, A] =
      new PipelineF[WorkflowOpCoreF, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = self.copy(src = newSrc).pipeline

        def op = "$project"
        def rhs = self.pipelineRhs
      }
    // NB: this is exposed separately so that the type can be narrower
    def pipelineRhs: Bson.Doc = idExclusion match {
      case IdHandling.ExcludeId =>
        Bson.Doc(shape.bson.value + (Workflow.IdLabel -> Bson.Bool(false)))
      case _         => shape.bson
    }
    def empty: $ProjectF[A] = $ProjectF.EmptyDoc(src)

    def set(field: BsonField, value: Reshape.Shape): $ProjectF[A] =
      $ProjectF(src,
        shape.set(field, value),
        if (field == IdName) IncludeId else idExclusion)

    def get(ref: DocVar): Option[Reshape.Shape] = ref match {
      case DocVar(_, Some(field)) => shape.get(field)
      case _                      => Some(-\/(shape))
    }

    def getAll: List[(BsonField, Expression)] = {
      val all = Reshape.getAll(shape)
      idExclusion match {
        case IncludeId => all.collectFirst {
          case (IdName, _) => all
        }.getOrElse((IdName, $include()) :: all)
        case _         => all
      }
    }

    def setAll(fvs: Iterable[(BsonField, Reshape.Shape)]): $ProjectF[A] =
      $ProjectF(
        src,
        Reshape.setAll(shape, fvs),
        if (fvs.exists(_._1 == IdName)) IncludeId else idExclusion)

    def deleteAll(fields: List[BsonField]): $ProjectF[A] =
      $ProjectF(src,
        Reshape.setAll(Reshape.EmptyDoc,
          Reshape.getAll(this.shape)
            .filterNot(t => fields.exists(t._1.startsWith(_)))
            .map(t => t._1 -> \/-(t._2))),
        if (fields.contains(IdName)) ExcludeId else idExclusion)

    def id: $ProjectF[A] = {
      def loop(prefix: Option[BsonField], p: $ProjectF[A]): $ProjectF[A] = {
        def nest(child: BsonField): BsonField =
          prefix.map(_ \ child).getOrElse(child)

        $ProjectF(
          p.src,
          Reshape(
            p.shape.value.transform {
              case (k, v) =>
                v.fold(
                  r => -\/(loop(Some(nest(k)), $ProjectF(p.src, r, p.idExclusion)).shape),
                  κ(\/-($var(DocVar.ROOT(nest(k))))))
            }),
          p.idExclusion)
      }

      loop(None, this)
    }
  }
  object $ProjectF {
    def EmptyDoc[A](src: A) = $ProjectF(src, Reshape.EmptyDoc, ExcludeId)
  }
  object $project {
    def apply[F[_]: Coalesce](shape: Reshape, id: IdHandling)
      (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($ProjectF(src, shape, id))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[(A, Reshape, IdHandling)] =
      I.prj(op) collect {
        case $ProjectF(src, shape, id) => (src, shape, id)
      }
  }

  final case class $RedactF[A](src: A, value: Expression)
    extends WorkflowOpCoreF[A] { self =>
    def pipeline: PipelineF[WorkflowOpCoreF, A] =
      new PipelineF[WorkflowOpCoreF, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = self.copy(src = newSrc).pipeline

        def op = "$redact"
        def rhs = value.cata(bsonƒ)
      }
  }
  object $RedactF {
    // def make(value: Expression)(src: Workflow): Workflow =
    //   Fix(coalesce($RedactF(src, value)))

    val DESCEND = DocVar(DocVar.Name("DESCEND"),  None)
    val PRUNE   = DocVar(DocVar.Name("PRUNE"),    None)
    val KEEP    = DocVar(DocVar.Name("KEEP"),     None)
  }
  object $redact {
    def apply[F[_]: Coalesce](value: Expression)
      (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($RedactF(src, value))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[(A, Expression)] =
      I.prj(op) collect {
        case $RedactF(src, value) => (src, value)
      }
  }

  final case class $LimitF[A](src: A, count: Long)
      extends WorkflowOpCoreF[A] { self =>
    def shapePreserving: ShapePreservingF[WorkflowOpCoreF, A] =
      new ShapePreservingF[WorkflowOpCoreF, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = self.copy(src = newSrc).shapePreserving

        def op = "$limit"
        def rhs = Bson.Int64(count)
      }
  }
  object $limit {
    def apply[F[_]: Coalesce](count: Long)
      (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($LimitF(src, count))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F): Option[(A, Long)] =
      I.prj(op).collect {
        case $LimitF(src, count) => (src, count)
      }
  }

  final case class $SkipF[A](src: A, count: Long)
      extends WorkflowOpCoreF[A] { self =>
    def shapePreserving: ShapePreservingF[WorkflowOpCoreF, A] =
      new ShapePreservingF[WorkflowOpCoreF, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = self.copy(src = newSrc).shapePreserving

        def op = "$skip"
        def rhs = Bson.Int64(count)
      }
  }
  object $skip {
    def apply[F[_]: Coalesce](count: Long)
      (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($SkipF(src, count))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F): Option[(A, Long)] =
      I.prj(op).collect {
        case $SkipF(src, count) => (src, count)
      }
  }

  final case class $UnwindF[A](src: A, field: DocVar)
      extends WorkflowOpCoreF[A] { self =>
    def pipeline: PipelineF[WorkflowOpCoreF, A] =
      new PipelineF[WorkflowOpCoreF, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = self.copy(src = newSrc).pipeline

        def op = "$unwind"
        def rhs = field.bson
      }
    lazy val flatmapop = $SimpleMapF(src, NonEmptyList(FlatExpr(field.toJs)), ListMap())
  }
  object $unwind {
    def apply[F[_]: Coalesce](field: DocVar)
      (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($UnwindF(src, field))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[(A, DocVar)] =
      I.prj(op) collect {
        case $UnwindF(src, field) => (src, field)
      }
  }

  final case class $GroupF[A](src: A, grouped: Grouped, by: Reshape.Shape)
      extends WorkflowOpCoreF[A] { self =>

    def pipeline: PipelineF[WorkflowOpCoreF, A] =
      new PipelineF[WorkflowOpCoreF, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = self.copy(src = newSrc).pipeline

        def op = "$group"
        def rhs = {
          val Bson.Doc(m) = grouped.bson
          Bson.Doc(m + (Workflow.IdLabel -> by.fold(_.bson, _.cata(bsonƒ))))
        }
      }

    def empty = copy(grouped = Grouped(ListMap()))

    def getAll: List[(BsonField.Name, Accumulator)] =
      grouped.value.toList

    def deleteAll(fields: List[BsonField.Name]): Workflow.$GroupF[A] = {
      empty.setAll(getAll.filterNot(t => fields.contains(t._1)))
    }

    def setAll(vs: Seq[(BsonField.Name, Accumulator)]) = copy(grouped = Grouped(ListMap(vs: _*)))
  }
  object $group {
    def apply[F[_]: Coalesce](grouped: Grouped, by: Reshape.Shape)
      (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($GroupF(src, grouped, by))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[(A, Grouped, Reshape.Shape)] =
      I.prj(op) collect {
        case $GroupF(src, grouped, shape) => (src, grouped, shape)
      }
  }

  final case class $SortF[A](src: A, value: NonEmptyList[(BsonField, SortDir)])
      extends WorkflowOpCoreF[A] { self =>
    def shapePreserving: ShapePreservingF[WorkflowOpCoreF, A] =
      new ShapePreservingF[WorkflowOpCoreF, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = self.copy(src = newSrc).shapePreserving

        def op = "$sort"
        // Note: ListMap preserves the order of entries.
        def rhs: Bson.Doc = $SortF.keyBson(value)
      }
  }
  object $SortF {
    def keyBson(value: NonEmptyList[(BsonField, SortDir)]) =
      Bson.Doc(ListMap((value.map { case (k, t) => k.asText -> sortDirToBson(t) }).list.toList: _*))
  }
  object $sort {
    def apply[F[_]: Coalesce](value: NonEmptyList[(BsonField, SortDir)])
      (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($SortF(src, value))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[(A, NonEmptyList[(BsonField, SortDir)])] =
      I.prj(op) collect {
        case $SortF(src, value) => (src, value)
      }
  }

  /**
   * TODO: If an \$OutF has anything after it, we need to either do
   * {{{\$seq(\$out(src, dst), after(\$read(dst), ...))}}}
   * or
   * {{{\$Fork(src, List(\$out(_, dst), after(_, ...)))}}}
   * The latter seems preferable, but currently the forking semantics are not
   * clear.
   */
  final case class $OutF[A](src: A, collection: Collection)
      extends WorkflowOpCoreF[A] { self =>
    def shapePreserving: ShapePreservingF[WorkflowOpCoreF, A] =
      new ShapePreservingF[WorkflowOpCoreF, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) =
          self.copy(src = newSrc).shapePreserving

        def op = "$out"
        def rhs = Bson.Text(collection.collectionName)
      }
  }
  object $out {
    def apply[F[_]: Coalesce](collection: Collection)
      (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($OutF(src, collection))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[(A, Collection)] =
      I.prj(op) collect {
        case $OutF(src, collection) => (src, collection)
      }
  }

  final case class $GeoNearF[A](
    src: A,
    near: (Double, Double), distanceField: BsonField,
    limit: Option[Int], maxDistance: Option[Double],
    query: Option[Selector], spherical: Option[Boolean],
    distanceMultiplier: Option[Double], includeLocs: Option[BsonField],
    uniqueDocs: Option[Boolean])
      extends WorkflowOpCoreF[A] { self =>
    def pipeline: PipelineF[WorkflowOpCoreF, A] =
      new PipelineF[WorkflowOpCoreF, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = self.copy(src = newSrc).pipeline

        def op = "$geoNear"
        def rhs = Bson.Doc(List(
          List("near"           -> Bson.Arr(Bson.Dec(near._1) :: Bson.Dec(near._2) :: Nil)),
          List("distanceField"  -> distanceField.bson),
          limit.toList.map(limit => "limit" -> Bson.Int32(limit)),
          maxDistance.toList.map(maxDistance => "maxDistance" -> Bson.Dec(maxDistance)),
          query.toList.map(query => "query" -> query.bson),
          spherical.toList.map(spherical => "spherical" -> Bson.Bool(spherical)),
          distanceMultiplier.toList.map(distanceMultiplier => "distanceMultiplier" -> Bson.Dec(distanceMultiplier)),
          includeLocs.toList.map(includeLocs => "includeLocs" -> includeLocs.bson),
          uniqueDocs.toList.map(uniqueDocs => "uniqueDocs" -> Bson.Bool(uniqueDocs))
        ).flatten.toListMap)
      }
  }
  object $geoNear {
    def apply[F[_]: Coalesce]
      (near: (Double, Double), distanceField: BsonField,
        limit: Option[Int], maxDistance: Option[Double],
        query: Option[Selector], spherical: Option[Boolean],
        distanceMultiplier: Option[Double], includeLocs: Option[BsonField],
        uniqueDocs: Option[Boolean])
      (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
        src => Fix(Coalesce[F].coalesce(I.inj($GeoNearF(
          src, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[(A, (Double, Double), BsonField, Option[Int], Option[Double],
        Option[Selector], Option[Boolean], Option[Double], Option[BsonField],
        Option[Boolean])] =
      I.prj(op) collect {
        case $GeoNearF(src, n, df, l, md, q, s, dm, il, ud) =>
          (src, n, df, l, md, q, s, dm, il, ud)
      }
  }

  sealed trait MapReduceF[A] extends WorkflowOpCoreF[A] {
    def singleSource: SingleSourceF[WorkflowOpCoreF, A]

    def newMR[F[_]](
      base: DocVar,
      src: WorkflowTask,
      sel: Option[Selector],
      sort: Option[NonEmptyList[(BsonField, SortDir)]],
      count: Option[Long])
      : (DocVar, WorkflowTask)
  }

  /**
    Takes a function of two parameters. The first is the current key (which
    defaults to `this._id`, but may have been overridden by previous
    [Flat]\$MapFs) and the second is the document itself. The function must
    return a 2-element array containing the new key and new value.
    */
  final case class $MapF[A](src: A, fn: Js.AnonFunDecl, scope: Scope) extends MapReduceF[A] { self =>
    def singleSource: SingleSourceF[WorkflowOpCoreF, A] =
      new SingleSourceF[WorkflowOpCoreF, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = self.copy(src = newSrc).singleSource
      }

    import $MapF._

    def newMR[F[_]](base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortDir)]], count: Option[Long]) =
      (ExprVar,
        MapReduceTask(
          src,
          MapReduce(
            mapFn(base match {
              case DocVar(DocVar.ROOT, None) => this.fn
              case _ => compose(this.fn, mapProject(base))
            }),
            $ReduceF.reduceNOP,
            selection = sel, inputSort = sort, limit = count, scope = scope),
          None))
  }
  object $MapF {
    import jscore._

    def compose(g: Js.AnonFunDecl, f: Js.AnonFunDecl): Js.AnonFunDecl =
      Js.AnonFunDecl(List("key", "value"), List(
        Js.Return(Js.Call(Js.Select(g, "apply"),
          List(Js.Null, Js.Call(f, List(Js.Ident("key"), Js.Ident("value"))))))))

    def mapProject(base: DocVar) =
      Js.AnonFunDecl(List("key", "value"), List(
        Js.Return(Js.AnonElem(List(Js.Ident("key"), base.toJs(jscore.ident("value")).toJs)))))


    def mapKeyVal(idents: (String, String), key: Js.Expr, value: Js.Expr) =
      Js.AnonFunDecl(List(idents._1, idents._2),
        List(Js.Return(Js.AnonElem(List(key, value)))))
    def mapMap(ident: String, transform: Js.Expr) =
      mapKeyVal(("key", ident), Js.Ident("key"), transform)
    val mapNOP = mapMap("value", Js.Ident("value"))

    def finalizerFn(fn: Js.Expr) =
      Js.AnonFunDecl(List("key", "value"),
        List(Js.Return(Js.Access(
          Js.Call(fn, List(Js.Ident("key"), Js.Ident("value"))),
          Js.Num(1, false)))))

    def mapFn(fn: Js.Expr) =
      Js.AnonFunDecl(Nil,
        List(Js.Call(Js.Select(Js.Ident("emit"), "apply"),
          List(
            Js.Null,
            Js.Call(fn, List(Js.Select(Js.This, IdLabel), Js.This))))))
  }
  object $map {
    def apply[F[_]: Coalesce](fn: Js.AnonFunDecl, scope: Scope)
      (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($MapF(src, fn, scope))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[(A, Js.AnonFunDecl, Scope)] =
      I.prj(op) collect {
        case $MapF(src, fn, scope) => (src, fn, scope)
      }
  }

  // FIXME: this one should become $MapF, with the other one being replaced by
  // a new op that combines a map and reduce operation?
  final case class $SimpleMapF[A](src: A, exprs: NonEmptyList[CardinalExpr[JsFn]], scope: Scope)
      extends MapReduceF[A] { self =>
    def singleSource: SingleSourceF[WorkflowOpCoreF, A] =
      new SingleSourceF[WorkflowOpCoreF, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = self.copy(src = newSrc).singleSource
      }
    def getAll: Option[List[BsonField]] = {
      def loop(x: JsCore): Option[List[BsonField]] = x match {
        case jscore.Obj(values) => Some(values.toList.flatMap { case (k, v) =>
          val n = BsonField.Name(k.value)
          loop(v).map(_.map(n \ _)).getOrElse(List(n))
        })
        case _ => None
      }
      // Note: this is not safe if `expr` inspects the argument to decide what
      // JS to construct, but all we need here is names of fields that we may
      // be able to optimize away.
      loop(simpleExpr(jscore.ident("?")))
    }

    def deleteAll(fields: List[BsonField]): $SimpleMapF[A] = {
      def loop(x: JsCore, fields: List[List[BsonField.Name]]): Option[JsCore] = x match {
        case jscore.Obj(values) => Some(jscore.Obj(
          values.collect(Function.unlift[(jscore.Name, JsCore), (jscore.Name, JsCore)] { t =>
            val (k, v) = t
            if (fields contains List(BsonField.Name(k.value))) None
            else {
              val v1 = loop(v, fields.collect {
                case BsonField.Name(k.value) :: tail => tail
              }).getOrElse(v)
              v1 match {
                case jscore.Obj(values) if values.isEmpty => None
                case _ => Some(k -> v1)
              }
            }
          })))
        case _ => Some(x)
      }

      exprs match {
        case NonEmptyList(MapExpr(expr), INil()) =>
          $SimpleMapF(src,
            NonEmptyList(
              MapExpr(JsFn(jscore.Name("base"), loop(expr(jscore.ident("base")), fields.map(_.flatten.toList)).getOrElse(jscore.Literal(Js.Null))))),
            scope)
        case _ => this
      }
    }

    private def fn: Js.AnonFunDecl = {
      import jscore._

      def body(fs: List[(CardinalExpr[JsFn], String)]) =
        Js.AnonFunDecl(List("key", "value"),
          List(
            Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
            fs.foldRight[JsCore => Js.Stmt](b =>
              Js.Call(Js.Select(Js.Ident("rez"), "push"),
                List(
                  Js.AnonElem(List(
                    Js.Call(Js.Ident("ObjectId"), Nil),
                    b.toJs))))){
              case ((MapExpr(m), n), inner) => b =>
                Js.Block(List(
                  Js.VarDef(List(n -> m(b).toJs)),
                  inner(ident(n))))
              case ((FlatExpr(m), n), inner) => b =>
                Js.ForIn(Js.Ident("elem"), m(b).toJs,
                  Js.Block(List(
                    Js.VarDef(List(n -> Js.Call(Js.Ident("clone"), List(b.toJs)))),
                    unsafeAssign(m(ident(n)), Access(m(b), ident("elem"))),
                    inner(ident(n)))))
            }(ident("value")),
            Js.Return(Js.Ident("rez"))))

      body(exprs.toList.zipWithIndex.map(("each" + _).second))
    }

    def >>>(that: $SimpleMapF[A]) = {
      $SimpleMapF(
        this.src,
        (this.exprs.last, that.exprs.head) match {
          case (MapExpr(l), MapExpr(r)) =>
            this.exprs.init <::: NonEmptyList.nel(MapExpr(l >>> r), that.exprs.tail)
          case _ => this.exprs <+> that.exprs
        },
        this.scope <+> that.scope
      )
    }

    def raw = {
      import jscore._

      val funcs = (exprs).map(_.copoint(ident("_")).para(findFunctionsƒ)).foldLeft(Set[String]())(_ ++ _)

      exprs match {
        case NonEmptyList(MapExpr(expr), INil()) =>
          $MapF(src,
            Js.AnonFunDecl(List("key", "value"), List(
              Js.Return(Arr(List(
                ident("key"),
                expr(ident("value")))).toJs))),
            scope <+> $SimpleMapF.implicitScope(funcs)
          )
        case _ =>
          $FlatMapF(src, fn, $SimpleMapF.implicitScope(funcs + "clone") ++ scope)
      }
    }

    def newMR[F[_]](base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortDir)]], count: Option[Long]) =
      raw.newMR(base, src, sel, sort, count)

    def simpleExpr = exprs.foldRight(JsFn.identity) {
      case (MapExpr(expr), acc) => expr >>> acc
      case (_,             acc) => acc
    }
  }
  object $SimpleMapF {
    def implicitScope(fs: Set[String]) =
      $SimpleMapF.jsLibrary.filter(x => fs.contains(x._1))

    val jsLibrary = ListMap(
      "remove" -> Bson.JavaScript(
        Js.AnonFunDecl(List("obj", "field"), List(
          Js.VarDef(List("dest" -> Js.AnonObjDecl(Nil))),
          Js.ForIn(Js.Ident("i"), Js.Ident("obj"),
            Js.If(Js.BinOp("!=", Js.Ident("i"), Js.Ident("field")),
              Js.BinOp("=",
                Js.Access(Js.Ident("dest"), Js.Ident("i")),
                Js.Access(Js.Ident("obj"), Js.Ident("i"))),
              None)),
          Js.Return(Js.Ident("dest"))))),
      "clone" -> Bson.JavaScript(
        Js.AnonFunDecl(List("src"), List(
          Js.If(
            Js.BinOp("||",
              Js.BinOp("!=", Js.UnOp("typeof", Js.Ident("src")), Js.Str("object")),
              Js.BinOp("==", Js.Ident("src"), Js.Null)),
            Js.Return(Js.Ident("src")),
            None),
          Js.VarDef(List("dest" -> Js.New(Js.Select(Js.Ident("src"), "constructor")))),
          Js.ForIn(Js.Ident("i"), Js.Ident("src"),
            Js.BinOp ("=",
              Js.Access(Js.Ident("dest"), Js.Ident("i")),
              Js.Call(Js.Ident("clone"), List(
                Js.Access(Js.Ident("src"), Js.Ident("i")))))),
          Js.Return(Js.Ident("dest"))))))
  }
  object $simpleMap {
    def apply[F[_]: Coalesce](exprs: NonEmptyList[CardinalExpr[JsFn]], scope: Scope)
      (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($SimpleMapF(src, exprs, scope))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[(A, NonEmptyList[CardinalExpr[JsFn]], Scope)] =
      I.prj(op) collect {
        case $SimpleMapF(src, exprs, scope) => (src, exprs, scope)
      }
  }

  /**
    Takes a function of two parameters. The first is the current key (which
    defaults to `this._id`, but may have been overridden by previous
    [Flat]\$MapFs) and the second is the document itself. The function must
    return an array of 2-element arrays, each containing a new key and a new
    value.
    */
  final case class $FlatMapF[A](src: A, fn: Js.AnonFunDecl, scope: Scope)
      extends MapReduceF[A] { self =>
    def singleSource: SingleSourceF[WorkflowOpCoreF, A] =
      new SingleSourceF[WorkflowOpCoreF, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = self.copy(src = newSrc).singleSource
      }

    import $FlatMapF._

    def newMR[F[_]](base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortDir)]], count: Option[Long]) =
      (ExprVar,
        MapReduceTask(
          src,
          MapReduce(
            mapFn(base match {
              case DocVar(DocVar.ROOT, None) => this.fn
              case _ => $MapF.compose(this.fn, $MapF.mapProject(base))
            }),
            $ReduceF.reduceNOP,
            selection = sel, inputSort = sort, limit = count, scope = scope),
          None))
  }
  object $FlatMapF {
    import Js._

    private def composition(g: Js.AnonFunDecl, f: Js.AnonFunDecl) =
      Call(
        Select(Call(f, List(Ident("key"), Ident("value"))), "map"),
        List(AnonFunDecl(List("args"), List(
          Return(Call(Select(g, "apply"), List(Null, Ident("args"))))))))

    def kleisliCompose(g: Js.AnonFunDecl, f: Js.AnonFunDecl) =
      AnonFunDecl(List("key", "value"), List(
        Return(
          Call(
            Select(Select(AnonElem(Nil), "concat"), "apply"),
            List(AnonElem(Nil), composition(g, f))))))

    def mapCompose(g: Js.AnonFunDecl, f: Js.AnonFunDecl) =
      AnonFunDecl(List("key", "value"), List(Return(composition(g, f))))

    def mapFn(fn: Js.Expr) =
      AnonFunDecl(Nil,
        List(
          Call(
            Select(
              Call(fn, List(Select(This, IdLabel), This)),
              "map"),
            List(AnonFunDecl(List("__rez"),
              List(Call(Select(Ident("emit"), "apply"),
                List(Null, Ident("__rez")))))))))
  }
  object $flatMap {
    def apply[F[_]: Coalesce](fn: Js.AnonFunDecl, scope: Scope)
      (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($FlatMapF(src, fn, scope))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[(A, Js.AnonFunDecl, Scope)] =
      I.prj(op) collect {
        case $FlatMapF(src, fn, scope) => (src, fn, scope)
      }
  }

  /**
    Takes a function of two parameters – a key and an array of values. The
    function must return a single value.
    */
  final case class $ReduceF[A](src: A, fn: Js.AnonFunDecl, scope: Scope)
      extends MapReduceF[A] { self =>
    def singleSource: SingleSourceF[WorkflowOpCoreF, A] =
      new SingleSourceF[WorkflowOpCoreF, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = self.copy(src = newSrc).singleSource
      }

    def newMR[F[_]](base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortDir)]], count: Option[Long]) =
      (ExprVar,
        MapReduceTask(
          src,
          MapReduce(
            $MapF.mapFn(base match {
              case DocVar(DocVar.ROOT, None) => $MapF.mapNOP
              case _                         => $MapF.mapProject(base)
            }),
            this.fn,
            selection = sel, inputSort = sort, limit = count, scope = scope),
          None))
  }
  object $ReduceF {
    import jscore._

    val reduceNOP =
      Js.AnonFunDecl(List("key", "values"), List(
        Js.Return(Access(ident("values"), Literal(Js.Num(0, false))).toJs)))

    val reduceFoldLeft =
      Js.AnonFunDecl(List("key", "values"), List(
        Js.VarDef(List("rez" -> Js.AnonObjDecl(Nil))),
        Js.Call(Select(ident("values"), "forEach").toJs,
          List(Js.AnonFunDecl(List("value"),
            List(copyAllFields(ident("value"), Name("rez")))))),
        Js.Return(Js.Ident("rez"))))
  }
  object $reduce {
    def apply[F[_]: Coalesce](fn: Js.AnonFunDecl, scope: Scope)
      (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($ReduceF(src, fn, scope))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[(A, Js.AnonFunDecl, Scope)] =
      I.prj(op) collect {
        case $ReduceF(src, fn, scope) => (src, fn, scope)
      }
  }

  final case class $LookupF[A](
    src: A,
    from: Collection,
    localField: BsonField.Name,
    foreignField: BsonField.Name,
    as: BsonField.Name)
    extends WorkflowOp3_2F[A] { self =>
    def pipeline: PipelineF[WorkflowOp3_2F, A] =
      new PipelineF[WorkflowOp3_2F, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = self.copy(src = newSrc).pipeline

        def op = "$lookup"
        def rhs = Bson.Doc(ListMap(
          "from" -> Bson.Text(from.collectionName),
          "localField" -> localField.bson,
          "foreignField" -> foreignField.bson,
          "as" -> as.bson
        ))
      }
  }
  object $lookup {
    def $loookup[F[_]: Coalesce](
      from: Collection,
      localField: BsonField.Name,
      foreignField: BsonField.Name,
      as: BsonField.Name)
      (implicit I: WorkflowOp3_2F :<: F): FixOp[F] =
        src => Fix(Coalesce[F].coalesce(I.inj($LookupF(src, from, localField, foreignField, as))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOp3_2F :<: F)
      : Option[(A, Collection, BsonField.Name, BsonField.Name, BsonField.Name)] =
      I.prj(op) collect {
        case $LookupF(src, from, lf, ff, as) => (src, from, lf, ff, as)
      }
  }

  final case class $SampleF[A](src: A, size: Int)
    extends WorkflowOp3_2F[A] { self =>
    def pipeline: PipelineF[WorkflowOp3_2F, A] =
      new PipelineF[WorkflowOp3_2F, A] {
        def wf = self
        def src = self.src
        def reparent[B](newSrc: B) = copy(src = newSrc).pipeline

        def op = "$sample"
        def rhs = Bson.Int32(size)
      }
  }
  object $sample {
    def apply[F[_]: Coalesce](size: Int)(implicit I: WorkflowOp3_2F :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($SampleF(src, size))))

    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOp3_2F :<: F)
      : Option[(A, Int)] =
      I.prj(op) collect {
        case $SampleF(src, size) => (src, size)
      }
    }

  /**
    Performs a sequence of operations, sequentially, merging their results.
    */
  final case class $FoldLeftF[A](head: A, tail: NonEmptyList[A])
      extends WorkflowOpCoreF[A]
  object $foldLeft {
    def apply[F[_]: Coalesce](first: Fix[F], second: Fix[F], rest: Fix[F]*)
      (implicit I: WorkflowOpCoreF :<: F): Fix[F] =
      Fix(Coalesce[F].coalesce(I.inj($FoldLeftF(first, NonEmptyList.nel(second, IList.fromList(rest.toList))))))

    // FIXME: the result should be in NonEmptyList, but it gives a compile error
    // saying "this is a GADT skolem"
    def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
      : Option[(A, List[A])] =
      I.prj(op) collect {
        case $FoldLeftF(head, tail) => (head, tail.toList)
      }
  }

  implicit def WorkflowOpCoreFRenderTree: RenderTree[WorkflowOpCoreF[Unit]] = new RenderTree[WorkflowOpCoreF[Unit]] {
    val wfType = "Workflow" :: Nil

    def render(v: WorkflowOpCoreF[Unit]) = v match {
      case $PureF(value)       => Terminal("$PureF" :: wfType, Some(value.toString))
      case $ReadF(coll)        => coll.render.copy(nodeType = "$ReadF" :: wfType)
      case $MatchF(_, sel)     =>
        NonTerminal("$MatchF" :: wfType, None, sel.render :: Nil)
      case $ProjectF(_, shape, xId) =>
        NonTerminal("$ProjectF" :: wfType, None,
          Reshape.renderReshape(shape) :+
            Terminal(xId.toString :: "$ProjectF" :: wfType, None))
      case $RedactF(_, value) => NonTerminal("$RedactF" :: wfType, None,
        value.render ::
          Nil)
      case $LimitF(_, count)  => Terminal("$LimitF" :: wfType, Some(count.shows))
      case $SkipF(_, count)   => Terminal("$SkipF" :: wfType, Some(count.shows))
      case $UnwindF(_, field) => Terminal("$UnwindF" :: wfType, Some(field.toString))
      case $GroupF(_, grouped, -\/(by)) =>
        val nt = "$GroupF" :: wfType
        NonTerminal(nt, None,
          grouped.render ::
            NonTerminal("By" :: nt, None, Reshape.renderReshape(by)) ::
            Nil)
      case $GroupF(_, grouped, \/-(expr)) =>
        val nt = "$GroupF" :: wfType
        NonTerminal(nt, None,
          grouped.render ::
            Terminal("By" :: nt, Some(expr.toString)) ::
            Nil)
      case $SortF(_, value)   =>
        val nt = "$SortF" :: wfType
        NonTerminal(nt, None,
          value.map { case (field, st) => Terminal("SortKey" :: nt, Some(field.asText + " -> " + st)) }.toList)
      case $GeoNearF(_, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs) =>
        val nt = "$GeoNearF" :: wfType
        NonTerminal(nt, None,
          Terminal("Near" :: nt, Some(near.shows)) ::
            Terminal("DistanceField" :: nt, Some(distanceField.toString)) ::
            Terminal("Limit" :: nt, Some(limit.shows)) ::
            Terminal("MaxDistance" :: nt, Some(maxDistance.shows)) ::
            Terminal("Query" :: nt, Some(query.toString)) ::
            Terminal("Spherical" :: nt, Some(spherical.shows)) ::
            Terminal("DistanceMultiplier" :: nt, Some(distanceMultiplier.toString)) ::
            Terminal("IncludeLocs" :: nt, Some(includeLocs.toString)) ::
            Terminal("UniqueDocs" :: nt, Some(uniqueDocs.shows)) ::
            Nil)

      case $MapF(_, fn, scope) =>
        val nt = "$MapF" :: wfType
        NonTerminal(nt, None,
          JSRenderTree.render(fn) ::
            Terminal("Scope" :: nt, Some((scope ∘ (_.toJs.pprint(2))).toString)) ::
            Nil)
      case $FlatMapF(_, fn, scope) =>
        val nt = "$FlatMapF" :: wfType
        NonTerminal(nt, None,
          JSRenderTree.render(fn) ::
            Terminal("Scope" :: nt, Some((scope ∘ (_.toJs.pprint(2))).toString)) ::
            Nil)
      case $SimpleMapF(_, exprs, scope) =>
        val nt = "$SimpleMapF" :: wfType
        NonTerminal(nt, None,
          exprs.toList.map {
            case MapExpr(e)  => NonTerminal("Map" :: nt, None, List(e.render))
	    case FlatExpr(e) => NonTerminal("Flatten" :: nt, None, List(e.render))
          } :+
            Terminal("Scope" :: nt, Some((scope ∘ (_.toJs.pprint(2))).toString)))
      case $ReduceF(_, fn, scope) =>
        val nt = "$ReduceF" :: wfType
        NonTerminal(nt, None,
          JSRenderTree.render(fn) ::
            Terminal("Scope" :: nt, Some((scope ∘ (_.toJs.pprint(2))).toString)) ::
            Nil)
      case $OutF(_, coll) => coll.render.copy(nodeType = "$OutF" :: wfType)
      case $FoldLeftF(_, _) => Terminal("$FoldLeftF" :: wfType, None)
    }
  }

  implicit def WorkflowOp3_2FRenderTree: RenderTree[WorkflowOp3_2F[Unit]] = new RenderTree[WorkflowOp3_2F[Unit]] {
    val wfType = "Workflow3.2" :: Nil

    def render(v: WorkflowOp3_2F[Unit]) = v match {
      case $LookupF(_, from, localField, foreignField, as) =>
        Terminal("$LookupF" :: wfType, Some(s"$from with $foreignField = $localField as $as"))
      case $SampleF(_, size) =>
        Terminal("$SampleF" :: wfType, Some(size.toString))
    }
  }

  implicit def WorkflowRenderTree[F[_]: Traverse: Classify]
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: RenderTree[F[Unit]])
    : RenderTree[Fix[F]] =
    new RenderTree[Fix[F]] {
      val wfType = "Workflow" :: Nil

      def chain(op: Fix[F]): List[RenderedTree] = op.unFix match {
        case IsSingleSource(ss) =>
          chain(ss.src) :+ Traverse[F].void(ss.wf).render
        case ms => List(render(Fix(ms)))
      }

      def render(v: Fix[F]) = v.unFix match {
        case IsSource(s)       => s.op.render
        case IsSingleSource(_) =>
          NonTerminal("Chain" :: wfType, None, chain(v))
        case $foldLeft(_, _) =>
          NonTerminal("$FoldLeftF" :: wfType, None, v.children.map(render(_)))
      }
    }

  implicit def CrystallizedRenderTree[F[_]](implicit R: RenderTree[Fix[F]]): RenderTree[Crystallized[F]] =
    new RenderTree[Crystallized[F]] {
      def render(v: Crystallized[F]) = v.op.render
    }
}
