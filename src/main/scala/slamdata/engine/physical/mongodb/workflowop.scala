package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import slamdata.engine.fs.Path
import slamdata.engine.{Error}
import slamdata.engine.fp._
import optimize.pipeline._
import WorkflowTask._
import MapReduce._

import slamdata.engine.analysis.fixplate._

import scalaz._
import Scalaz._
import monocle.Macro._
import monocle.syntax._

/**
 * A WorkflowOp is basically an atomic operation, with references to its inputs.
 * After generating a tree of these (actually, a graph, but we’ll get to that),
 * we crush them down into a Workflow. This `crush` gives us a location to
 * optimize our workflow decisions. EG, A sequence of simple ops may be combined
 * into a single pipeline request, but if one of those operations contains JS,
 * we have to execute that outside of a pipeline, possibly reordering the other
 * operations to avoid having two pipelines with a JS operation in the middle.
 *
 * We should also try to implement the optimizations at
 * http://docs.mongodb.org/manual/core/aggregation-pipeline-optimization/ so
 * that we can build others potentially on top of them (including reordering
 * non-pipelines around pipelines, etc.).
 * 
 * Also, this doesn’t yet go far enough – EG, if we have a ProjectOp before a
 * MapReduceOp, it might make sense to collapse the ProjectOp into the mapping
 * function, but we no longer have a handle on the JS to make that happen.
  */

sealed trait WorkflowOp[+A] {
  def srcs: List[A]

  import ExprOp.{GroupOp => _, _}
  import PipelineOp._
  import WorkflowOp._

  def rewriteRefs(applyVar0: PartialFunction[DocVar, DocVar]): this.type = {
    val applyVar = (f: DocVar) => applyVar0.lift(f).getOrElse(f)

    def applyExprOp(e: ExprOp): ExprOp = e.mapUp {
      case f : DocVar => applyVar(f)
    }

    def applyFieldName(name: BsonField): BsonField = {
      applyVar(DocField(name)).deref.getOrElse(name) // TODO: Delete field if it's transformed away to nothing???
    }

    def applySelector(s: Selector): Selector = s.mapUpFields(PartialFunction(applyFieldName _))

    def applyReshape(shape: Reshape): Reshape = shape match {
      case Reshape.Doc(value) => Reshape.Doc(value.transform {
        case (k, -\/(e)) => -\/(applyExprOp(e))
        case (k, \/-(r)) => \/-(applyReshape(r))
      })

      case Reshape.Arr(value) => Reshape.Arr(value.transform {
        case (k, -\/(e)) => -\/(applyExprOp(e))
        case (k, \/-(r)) => \/-(applyReshape(r))
      })
    }

    def applyGrouped(grouped: Grouped): Grouped = Grouped(grouped.value.transform {
      case (k, groupOp) => applyExprOp(groupOp) match {
        case groupOp : ExprOp.GroupOp => groupOp
        case _ => sys.error("Transformation changed the type -- error!")
      }
    })

    def applyMap[A](m: ListMap[BsonField, A]): ListMap[BsonField, A] = m.map(t => applyFieldName(t._1) -> t._2)

    def applyNel[A](m: NonEmptyList[(BsonField, A)]): NonEmptyList[(BsonField, A)] = m.map(t => applyFieldName(t._1) -> t._2)

    def applyFindQuery(q: FindQuery): FindQuery = {
      q.copy(
        query   = applySelector(q.query),
        max     = q.max.map(applyMap _),
        min     = q.min.map(applyMap _),
        orderby = q.orderby.map(applyNel _)
      )
    }

    (this match {
      case ProjectOp(src, shape)     => ProjectOp(src, applyReshape(shape))
      case GroupOp(src, grouped, by) =>
        GroupOp(src,
          applyGrouped(grouped), by.bimap(applyExprOp _, applyReshape _))
      case MatchOp(src, s)           => MatchOp(src, applySelector(s))
      case RedactOp(src, e)          => RedactOp(src, applyExprOp(e))
      case v @ LimitOp(src, _)       => v
      case v @ SkipOp(src, _)        => v
      case v @ UnwindOp(src, f)      => UnwindOp(src, applyVar(f))
      case v @ SortOp(src, l)        => SortOp(src, applyNel(l))
      // case v @ OutOp(src, _)         => v
      case g @ GeoNearOp(_, _, _, _, _, _, _, _, _, _) =>
        g.copy(
          distanceField = applyFieldName(g.distanceField),
          query = g.query.map(applyFindQuery _))
      case v                       => v
    }).asInstanceOf[this.type]
  }

  final def refs: List[DocVar] = {
    // FIXME: Sorry world
    val vf = new scala.collection.mutable.ListBuffer[DocVar]

    rewriteRefs {
      case v => vf += v; v
    }

    vf.toList
  }
}

object WorkflowOp {
  implicit val WorkflowOpTraverse = new Traverse[WorkflowOp] {
    def traverseImpl[G[_], A, B](fa: WorkflowOp[A])(f: A => G[B])
      (implicit G: Applicative[G]):
        G[WorkflowOp[B]] = fa match {
      case x @ DummyOp => G.point(x)
      case x @ PureOp(_) => G.point(x)
      case x @ ReadOp(_) => G.point(x)
      case MatchOp(src, sel) => G.apply(f(src))(MatchOp(_, sel))
      case ProjectOp(src, shape) => G.apply(f(src))(ProjectOp(_, shape))
      case RedactOp(src, value) => G.apply(f(src))(RedactOp(_, value))
      case LimitOp(src, count) => G.apply(f(src))(LimitOp(_, count))
      case SkipOp(src, count) => G.apply(f(src))(SkipOp(_, count))
      case UnwindOp(src, field) => G.apply(f(src))(UnwindOp(_, field))
      case GroupOp(src, grouped, by) => G.apply(f(src))(GroupOp(_, grouped, by))
      case SortOp(src, value) => G.apply(f(src))(SortOp(_, value))
      case GeoNearOp(src, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs) =>
        G.apply(f(src))(GeoNearOp(_, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs))
      case MapReduceOp(src, mr) => G.apply(f(src))(MapReduceOp(_, mr))
      case FoldLeftOp(srcs) =>
        G.map(Traverse[NonEmptyList].sequence(srcs.map(f)))(FoldLeftOp(_))
      case JoinOp(srcs) =>
        G.map(Traverse[List].sequence(srcs.map(f).toList))(x => JoinOp(x.toSet))
      // case OutOp(src, col) => G.apply(f(src))(OutOp(_, col))
    }
  }

  // TODO: Automatically call `coalesce` when an op is created, rather than here
  //       and recursively in every overriden coalesce.
  def finish(op: Term[WorkflowOp]): Workflow = {
    Workflow(crush(deleteUnusedFields(coalesce(op), Set.empty)))
  }


  def coalesce(op: Term[WorkflowOp]): Term[WorkflowOp] =
    op.cata[Term[WorkflowOp]] { x => Term(x match {
      case x @ MatchOp(src, selector) => src.unFix match {
        case SortOp(src0, value) =>
          SortOp(Term(MatchOp(src0, selector)), value)
        case MatchOp(src0, sel0) =>
          MatchOp(src0, Semigroup[Selector].append(sel0, selector))
        case _ => x
      }
      case x @ ProjectOp(src, _) => src.unFix match {
        case ProjectOp(_, _) =>
          val (rs, src) = collectShapes(Term(x))
          inlineProject(rs.head, rs.tail).fold(x)(ProjectOp(src, _))
        case _ => x
      }
      case x @ LimitOp(src, count) => src.unFix match {
        case LimitOp(src0, count0) =>
          LimitOp(src0, Math.min(count0, count))
        case MapReduceOp(src0, mr) =>
          MapReduceOp(
            src0,
            mr applyLens _limit modify (x => Some(x match {
              case None        => count
              case Some(count0) => Math.min(count, count0)})))
        case SkipOp(src0, count0) =>
          SkipOp(Term(LimitOp(src0, count0 + count)), count)
        case _ => x
      }
      case x @ SkipOp(src, count) => src.unFix match {
        case SkipOp(src0, count0) => SkipOp(src0, count0 + count)
        case _                    => x
      }
      case x @ GroupOp(_, _, _) => inlineGroupProjects(x).getOrElse(x)
      case x @ GeoNearOp(src, _, _, _, _, _, _, _, _, _) => src.unFix match {
        case _ @ GeoNearOp(_, _, _, _, _, _, _, _, _, _) => x
        case p: WPipelineOp[_] =>
          val cast: WPipelineOp[Term[WorkflowOp]] = p
          cast.reparent(Term(x.copy(src = cast.src)))
        case _              => x
      }
      case x @ MapReduceOp(src, mr) => src.unFix match {
        case MatchOp(src0, sel0) =>
          MapReduceOp(src0,
            mr applyLens _selection modify (x =>
              Some(x.fold(sel0)(sel => Semigroup[Selector].append(sel0, sel)))))
        case SortOp(src0, keys0) =>
          MapReduceOp(src0,
            mr applyLens _inputSort modify (x =>
              Some(x.fold(keys0)(keys => keys append keys0))))
        // case OutOp(src, _) => src.unFix match {
        //   case read @ ReadOp(_) => read
        //   case _                => x
        // }
        case _ => x
      }
      case x => x
    })
    }

  def pipeline(op: Term[WorkflowOp]): Option[(WorkflowTask, List[PipelineOp])] =
    op.unFix match {
      case MatchOp(src, selector) =>
        def pipelinable(sel: Selector): Boolean = sel match {
          case Selector.Where(_) => false
          case comp: Selector.CompoundSelector =>
            pipelinable(comp.left) && pipelinable(comp.right)
          case _ => true
        }
        if (pipelinable(selector)) {
          val op = PipelineOp.Match(selector)
          src.unFix match {
            case _: WPipelineOp[_] => pipeline(src).cata(
              { case (up, prev) => Some((up, prev :+ op)) },
              Some((crush(src), List(op))))
            case _ => Some((crush(src), List(op)))
          }
        }
        else None
      case p: WPipelineOp[_] => Some(alwaysPipePipe(p.src, p.pipeop))
      case _ => None
    }
    

  def crush(op: Term[WorkflowOp]): WorkflowTask =
    op.para[WorkflowTask] { _ match {
      case DummyOp => sys.error("Should not have any DummyOps at this point.")
      case PureOp(value) => PureTask(value)
      case ReadOp(coll) => ReadTask(coll)
      case x @ MatchOp(src, selector) => {
        // TODO: If we ever allow explicit request of cursors (instead of
        //       collections), we could generate a FindQuery here.
        lazy val nonPipeline =
          MapReduceTask(
            src._2,
            MapReduce(
              Js.AnonFunDecl(Nil,
                List(Js.Call(Js.Ident("emit"),
                  List(Js.Select(Js.Ident("this"), "_id"), Js.Ident("this"))))),
              Js.AnonFunDecl(List("key", "values"),
                List(Js.Return(Js.Access(Js.Ident("values"), Js.Num(0, false))))),
              selection = Some(selector)))
        pipeline(Term(MatchOp(src._1, selector)))
          .fold[WorkflowTask](nonPipeline)(x =>
          PipelineTask(x._1, Pipeline(x._2)))
      }
      case op: WPipelineOp[_] =>
        alwaysCrushPipe(op.src._1, op.pipeop)
      case MapReduceOp((_, src), mr) => MapReduceTask(src, mr)
      case FoldLeftOp(srcs) => FoldLeftTask(srcs.map(_._2))
      case JoinOp(srcs) => JoinTask(srcs.map(_._2))
    }
    }

  def collectShapes(op: Term[WorkflowOp]):
      (List[PipelineOp.Reshape], Term[WorkflowOp]) =
    op.para2[(List[PipelineOp.Reshape], Term[WorkflowOp])] { (rec, rez) =>
      rez match {
        case ProjectOp(src, shape) =>
          Arrow[Function1].first((x: List[PipelineOp.Reshape]) => shape :: x)(src)
        case _                     => (Nil, rec)
      }
    }

  /**
    * Operations without an input.
    */
  sealed trait SourceOp extends WorkflowOp[Nothing] {
    def srcs = Nil // Set.empty
  }

  /**
    * This should be renamed once the other PipelineOp goes away, but it is the
    * subset of operations that can ever be pipelined.
    */
  sealed trait WPipelineOp[+A] extends WorkflowOp[A] {
    def src: A
    def pipeop: PipelineOp
    def reparent[B](newSrc: B): WPipelineOp[B]

    def srcs = List(src) // Set(src)
  }
  sealed trait ShapePreservingOp[+A] extends WPipelineOp[A]

  /**
    * Flattens the sequence of operations like so:
    * 
    *   chain(
    *     ReadOp(Path.fileAbs("foo")),
    *     MatchOp(_, Selector.Where(Js.Bool(true))),
    *     LimitOp(_, 7))
    * ==
    *   LimitOp(
    *     MatchOp(
    *       ReadOp(Path.fileAbs("foo")),
    *       Selector.Where(Js.Bool(true))),
    *     7)
    */
  def chain(
    src: WorkflowOp[Term[WorkflowOp]],
    ops: (Term[WorkflowOp] => WorkflowOp[Term[WorkflowOp]])*):
      Term[WorkflowOp] =
    Term(ops.foldLeft(src)((s, o) => o(Term(s))))

  /**
    * A dummy op does nothing except complete a WorkflowOp so it can be merged
    * as the tail of some other op.
    */
  // FIXME: get rid of this
  case object DummyOp extends SourceOp

  case class PureOp(value: Bson) extends SourceOp

  case class ReadOp(coll: Collection) extends SourceOp

  case class MatchOp[A](src: A, selector: Selector)
      extends ShapePreservingOp[A] {
    def pipeop = PipelineOp.Match(selector)
    def reparent[B](newSrc: B) = copy(src = newSrc)
  }

  private def alwaysPipePipe(src: Term[WorkflowOp], op: PipelineOp) = {
    src.unFix match {
      case _: WPipelineOp[_] => pipeline(src).cata(
        { case (up, prev) => (up, prev :+ op) },
        (crush(src), List(op)))
      case _ => (crush(src), List(op))
    }
  }

  private def alwaysCrushPipe(src: Term[WorkflowOp], op: PipelineOp) =
    alwaysPipePipe(src, op) match {
      case (up, pipe) => PipelineTask(up, Pipeline(pipe))
    }

  case class ProjectOp[A](src: A, shape: PipelineOp.Reshape)
      extends WPipelineOp[A] {

    import PipelineOp._

    def pipeop = PipelineOp.Project(shape)
    def reparent[B](newSrc: B) = copy(src = newSrc)

    def empty: ProjectOp[A] = shape match {
      case Reshape.Doc(_) => ProjectOp.EmptyDoc(src)
      case Reshape.Arr(_) => ProjectOp.EmptyArr(src)
    }

    def set(field: BsonField, value: ExprOp \/ PipelineOp.Reshape):
        ProjectOp[A] =
      ProjectOp(src, shape.set(field, value))

    def getAll: List[(BsonField, ExprOp)] = {
      def fromReshape(r: Reshape): List[(BsonField, ExprOp)] = r match {
        case Reshape.Arr(m) => m.toList.map { case (f, e) => getAll0(f, e) }.flatten
        case Reshape.Doc(m) => m.toList.map { case (f, e) => getAll0(f, e) }.flatten
      }

      def getAll0(f0: BsonField, e: ExprOp \/ Reshape): List[(BsonField, ExprOp)] = e.fold(
        e => (f0 -> e) :: Nil,
        r => fromReshape(r).map { case (f, e) => (f0 \ f) -> e }
      )

      fromReshape(shape)
    }

    def setAll(fvs: Iterable[(BsonField, ExprOp \/ PipelineOp.Reshape)]):
        ProjectOp[A] =
      fvs.foldLeft(this) {
        case (project, (field, value)) => project.set(field, value)
      }

    def deleteAll(fields: List[BsonField]): ProjectOp[A] = {
      empty.setAll(getAll.filterNot(t => fields.exists(t._1.startsWith(_))).map(t => t._1 -> -\/ (t._2)))
    }
  }
  object ProjectOp {
    import PipelineOp._

    def EmptyDoc[A](src: A) = ProjectOp(src, Reshape.EmptyDoc)
    def EmptyArr[A](src: A) = ProjectOp(src, Reshape.EmptyArr)
  }

  case class RedactOp[A](src: A, value: ExprOp)
      extends WPipelineOp[A] {
    def pipeop = PipelineOp.Redact(value)
    def reparent[B](newSrc: B) = copy(src = newSrc)
  }

  case class LimitOp[A](src: A, count: Long) extends ShapePreservingOp[A] {
    import MapReduce._

    def pipeop = PipelineOp.Limit(count)
    // TODO: If the preceding is a MatchOp, and it or its source isn’t
    //       pipelineable, then return a FindQuery combining the match and this
    //       limit
    def reparent[B](newSrc: B) = copy(src = newSrc)
  }
  case class SkipOp[A](src: A, count: Long) extends ShapePreservingOp[A] {
    def pipeop = PipelineOp.Skip(count)
    // TODO: If the preceding is a MatchOp (or a limit preceded by a MatchOp),
    //       and it or its source isn’t pipelineable, then return a FindQuery
    //       combining the match and this skip
    def reparent[B](newSrc: B) = copy(src = newSrc)
  }
  case class UnwindOp[A](src: A, field: ExprOp.DocVar) extends WPipelineOp[A] {
    def pipeop = PipelineOp.Unwind(field)
    def reparent[B](newSrc: B) = copy(src = newSrc)
  }
  case class GroupOp[A] (
    src: A,
    grouped: PipelineOp.Grouped,
    by: ExprOp \/ PipelineOp.Reshape)
      extends WPipelineOp[A] {

    import PipelineOp._

    // TODO: Not all GroupOps can be pipelined. Need to determine when we may
    //       need the group command or a map/reduce.
    def pipeop = PipelineOp.Group(grouped, by)
    def reparent[B](newSrc: B) = copy(src = newSrc)

    def toProject: ProjectOp[A] =
      grouped.value.foldLeft(ProjectOp(src, PipelineOp.Reshape.EmptyArr)) {
        case (p, (f, v)) => p.set(f, -\/ (v))
      }

    def empty = copy(grouped = Grouped(ListMap()))

    def getAll: List[(BsonField.Leaf, ExprOp.GroupOp)] =
      grouped.value.toList

    def deleteAll(fields: List[BsonField.Leaf]): WorkflowOp.GroupOp[A] = {
      empty.setAll(getAll.filterNot(t => fields.exists(t._1 == _)))
    }

    def setAll(vs: Seq[(BsonField.Leaf, ExprOp.GroupOp)]) = copy(grouped = Grouped(ListMap(vs: _*)))
  }

  case class SortOp[A](src: A, value: NonEmptyList[(BsonField, SortType)])
      extends WPipelineOp[A] {
    def pipeop = PipelineOp.Sort(value)
    def reparent[B](newSrc: B) = copy(src = newSrc)
  }

  /**
    * TODO: If an OutOp has anything after it, we need to either do
    *   SeqOp(OutOp(src, dst), after(ReadOp(dst), ...))
    * or
    *   ForkOp(src, List(OutOp(_, dst), after(_, ...)))
    * The latter seems preferable, but currently the forking semantics are not
    * clear.
    */
  // case class OutOp[A](src: A, collection: Collection)
  //     extends ShapePreservingOp[A] {
  //   def pipeline = Some(alwaysPipePipe(src, PipelineOp.Out(field)))
  // }

  case class GeoNearOp[A](
    src: A,
    near: (Double, Double), distanceField: BsonField,
    limit: Option[Int], maxDistance: Option[Double],
    query: Option[FindQuery], spherical: Option[Boolean],
    distanceMultiplier: Option[Double], includeLocs: Option[BsonField],
    uniqueDocs: Option[Boolean])
      extends WPipelineOp[A] {
    def pipeop = PipelineOp.GeoNear(near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs)
    def reparent[B](newSrc: B) = copy(src = newSrc)
  }

  case class MapReduceOp[A](src: A, mr: MapReduce) extends WorkflowOp[A] {
    import MapReduce._

    def srcs = List(src) // Set(src)
  }

  case class FoldLeftOp[A](lsrcs: NonEmptyList[A]) extends WorkflowOp[A] {
    def srcs = lsrcs.toList
  }

  case class JoinOp[A](ssrcs: Set[A]) extends WorkflowOp[A] {
    def srcs = ssrcs.toList
  }
}
