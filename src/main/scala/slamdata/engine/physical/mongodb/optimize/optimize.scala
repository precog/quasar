package slamdata.engine.physical.mongodb

import slamdata.engine.fp._
import slamdata.engine.analysis.fixplate._
import scala.collection.immutable.ListMap

import scalaz._
import Scalaz._
import Liskov._

package object optimize {
  object pipeline {
    import ExprOp._
    import WorkflowOp._
    import PipelineOp._

    def deleteUnusedFields(op: Term[WorkflowOp], usedRefs: Set[DocVar]):
        Term[WorkflowOp] = {
      def getRefs[A](op: WorkflowOp[A]): Set[DocVar] = (op match {
        // Don't count unwinds (if the var isn't referenced elsewhere, it's effectively unused)
        case UnwindOp(_, _) => Nil
        case _ => op.refs
      }).toSet

      def unused(defs: Set[DocVar], refs: Set[DocVar]): Set[DocVar] = {
        defs.filterNot(d => refs.exists(ref => d.startsWith(ref) || ref.startsWith(d)))
      }

      def getDefs[A](op: WorkflowOp[A]): Set[DocVar] = (op match {
        case p @ ProjectOp(_, _) => p.getAll.map(_._1)
        case g @ WorkflowOp.GroupOp(_, _, _) => g.getAll.map(_._1)
        case _ => Nil
      }).map(DocVar.ROOT(_)).toSet

      val pruned =
        if (!usedRefs.isEmpty) {
          val unusedRefs =
            unused(getDefs(op.unFix), usedRefs).toList.flatMap(_.deref.toList)
          op.unFix match {
            case p @ ProjectOp(_, _) => p.deleteAll(unusedRefs)
            case g @ WorkflowOp.GroupOp(_, _, _) => g.deleteAll(unusedRefs.map(_.flatten.head))
            case _ => op.unFix
          }
        }
        else op.unFix
      Term(pruned.map(deleteUnusedFields(_, usedRefs ++ getRefs(pruned))))
    }

    def get0(leaves: List[BsonField.Leaf], rs: List[Reshape]): Option[ExprOp \/ Reshape] = {
      (leaves, rs) match {
        case (_, Nil) => Some(-\/ (BsonField(leaves).map(DocVar.ROOT(_)).getOrElse(DocVar.ROOT())))

        case (Nil, r :: rs) => inlineProject(r, rs).map(\/- apply)

        case (l :: ls, r :: rs) => r.get(l).flatMap {
          case -\/ (d @ DocVar(_, _)) => get0(d.path ++ ls, rs)

          case -\/ (e) => 
            if (ls.isEmpty) fixExpr(rs, e).map(-\/ apply) else None

          case \/- (r) => get0(ls, r :: rs)
        }
      }
    }

    private def fixExpr(rs: List[Reshape], e: ExprOp): Option[ExprOp] = {
      type OptionTramp[X] = OptionT[Free.Trampoline, X]

      def lift[A](o: Option[A]): OptionTramp[A] = OptionT(o.point[Free.Trampoline])

      (e.mapUpM[OptionTramp] {
        case ref @ DocVar(_, _) => 
          lift {
            get0(ref.path, rs).flatMap(_.fold(Some.apply, _ => None))
          }
      }).run.run
    }

    def inlineProject(r: Reshape, rs: List[Reshape]): Option[Reshape] = {
      type MapField[X] = Map[BsonField, X]

      val p = Project(r)

      val map = Traverse[MapField].sequence(p.getAll.toMap.mapValues {
        case d @ DocVar(_, _) => get0(d.path, rs)
        case e => fixExpr(rs, e).map(-\/ apply)
      })

      map.map(vs => p.empty.setAll(vs).shape)
    }

    def inlineGroupProjects(g: WorkflowOp.GroupOp[Term[WorkflowOp]]):
        Option[WorkflowOp.GroupOp[Term[WorkflowOp]]] = {
      import ExprOp._

      val (rs, src) = collectShapes(g.src)

      type MapField[X] = ListMap[BsonField.Leaf, X]

      val grouped = Traverse[MapField].sequence(ListMap(g.getAll: _*).map { t =>
        val (k, v) = t

        k -> (v match {
          case AddToSet(e)  =>
            fixExpr(rs, e) flatMap {
              case d @ DocVar(_, _) => Some(AddToSet(d))
              case _ => None
            }
          case Push(e)      =>
            fixExpr(rs, e) flatMap {
              case d @ DocVar(_, _) => Some(Push(d))
              case _ => None
            }
          case First(e)     => fixExpr(rs, e).map(First(_))
          case Last(e)      => fixExpr(rs, e).map(Last(_))
          case Max(e)       => fixExpr(rs, e).map(Max(_))
          case Min(e)       => fixExpr(rs, e).map(Min(_))
          case Avg(e)       => fixExpr(rs, e).map(Avg(_))
          case Sum(e)       => fixExpr(rs, e).map(Sum(_))
        })
      })

      val by = g.by.fold(e => fixExpr(rs, e).map(-\/ apply), r => inlineProject(r, rs).map(\/- apply))

      (grouped |@| by)((grouped, by) =>
        WorkflowOp.GroupOp(src, Grouped(grouped), by))
    }
  }
}
