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

package quasar.mimir

import scalaz._, Scalaz._

import scala.collection.mutable

trait Memoizer extends DAG {
  import dag._

  val MemoThreshold = 1

  def memoize(graph: DepGraph): DepGraph = {
    val refs = findForcingRefs(graph, OpSide.Center(graph))
    applyMemoizations(graph, refs)
  }

  def scaleMemoPriority(count: Int): Int = count // TODO exponential function?

  private def applyMemoizations(target: DepGraph, refs: Map[DepGraph, Set[OpSide]]): DepGraph = {
    // todo investigate why if we use a `DepGraphWrapper` here, tests fail in MiscStackSpecs
    val memotable = mutable.Map[DepGraph, DepGraph]()

    def numRefs(node: DepGraph) = refs get node map { _.size } getOrElse 0

    def memoized(node: DepGraph): DepGraph = {
      def inner(target: DepGraph): DepGraph = target match {
        // not using extractors due to bug
        case s: dag.SplitParam =>
          dag.SplitParam(s.id, s.parentId)(s.loc)

        // not using extractors due to bug
        case s: dag.SplitGroup =>
          dag.SplitGroup(s.id, s.identities, s.parentId)(s.loc)

        case dag.Const(_) => target

        case dag.Undefined() => target

        case target @ dag.New(parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.New(memoized(parent))(target.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.New(memoized(parent))(target.loc)
        }

        case node @ dag.Morph1(m, parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Morph1(m, memoized(parent))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Morph1(m, memoized(parent))(node.loc)
        }

        case node @ dag.Morph2(m, left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Morph2(m, memoized(left), memoized(right))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Morph2(m, memoized(left), memoized(right))(node.loc)
        }

        case node @ dag.Distinct(parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Distinct(memoized(parent))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Distinct(memoized(parent))(node.loc)
        }

        case target @ dag.AbsoluteLoad(parent, jtpe) =>
          dag.AbsoluteLoad(memoized(parent), jtpe)(target.loc)

        case target @ dag.RelativeLoad(parent, jtpe) =>
          dag.RelativeLoad(memoized(parent), jtpe)(target.loc)

        case target @ dag.Operate(op, parent) =>
          dag.Operate(op, memoized(parent))(target.loc)

        case node @ dag.Reduce(red, parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Reduce(red, memoized(parent))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Reduce(red, memoized(parent))(node.loc)
        }

        case node @ dag.MegaReduce(reds, parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.MegaReduce(reds, memoized(parent)), scaleMemoPriority(numRefs(node)))
          else
            dag.MegaReduce(reds, memoized(parent))
        }

        case s @ dag.Split(spec, child, id) => {
          val spec2             = memoizedSpec(spec)
          val child2            = memoized(child)
          val result: dag.Split = dag.Split(spec2, child2, id)(s.loc)

          if (numRefs(s) > MemoThreshold)
            Memoize(result, scaleMemoPriority(numRefs(s)))
          else
            result
        }

        case node @ dag.Assert(pred, child) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Assert(memoized(pred), memoized(child))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Assert(memoized(pred), memoized(child))(node.loc)
        }

        case node @ dag.Cond(pred, left, leftJoin, right, rightJoin) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Cond(memoized(pred), memoized(left), leftJoin, memoized(right), rightJoin)(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Cond(memoized(pred), memoized(left), leftJoin, memoized(right), rightJoin)(node.loc)
        }

        case node @ dag.Observe(data, samples) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Observe(memoized(data), memoized(samples))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Observe(memoized(data), memoized(samples))(node.loc)
        }

        case node @ dag.IUI(union, left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.IUI(union, memoized(left), memoized(right))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.IUI(union, memoized(left), memoized(right))(node.loc)
        }

        case node @ dag.Diff(left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Diff(memoized(left), memoized(right))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Diff(memoized(left), memoized(right))(node.loc)
        }

        case node @ dag.Join(op, joinSort, left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Join(op, joinSort, memoized(left), memoized(right))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Join(op, joinSort, memoized(left), memoized(right))(node.loc)
        }

        case node @ dag.Filter(joinSort, left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Filter(joinSort, memoized(left), memoized(right))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Filter(joinSort, memoized(left), memoized(right))(node.loc)
        }

        case dag.AddSortKey(parent, sortField, valueField, id) =>
          dag.AddSortKey(memoized(parent), sortField, valueField, id)

        case dag.Memoize(parent, priority) => dag.Memoize(memoized(parent), priority)
      }

      def memoizedSpec(spec: dag.BucketSpec): dag.BucketSpec = spec match { //TODO generalize?
        case dag.UnionBucketSpec(left, right) =>
          dag.UnionBucketSpec(memoizedSpec(left), memoizedSpec(right))

        case dag.IntersectBucketSpec(left, right) =>
          dag.IntersectBucketSpec(memoizedSpec(left), memoizedSpec(right))

        case dag.Group(id, target, child) =>
          dag.Group(id, memoized(target), memoizedSpec(child))

        case dag.UnfixedSolution(id, target) =>
          dag.UnfixedSolution(id, memoized(target))

        case dag.Extra(target) =>
          dag.Extra(memoized(target))
      }

      memotable.get(node) getOrElse {
        val result = inner(node)
        memotable += (node -> result)
        result
      }
    }

    memoized(target)
  }

  private def findForcingRefsInSpec(spec: BucketSpec, split: Split): Map[DepGraph, Set[OpSide]] = spec match {
    case UnionBucketSpec(left, right) =>
      findForcingRefsInSpec(left, split) |+| findForcingRefsInSpec(right, split)

    case IntersectBucketSpec(left, right) =>
      findForcingRefsInSpec(left, split) |+| findForcingRefsInSpec(right, split)

    case Group(id, target, forest) =>
      findForcingRefs(target, OpSide.Center(split)) |+| findForcingRefsInSpec(forest, split)

    case UnfixedSolution(_, solution) =>
      findForcingRefs(solution, OpSide.Center(split))

    case Extra(expr) =>
      findForcingRefs(expr, OpSide.Center(split))
  }

  private def findForcingRefs(graph: DepGraph, force: OpSide): Map[DepGraph, Set[OpSide]] = graph match {
    case _: SplitParam | _: SplitGroup | Const(_) | Undefined() => Map()

    case New(parent) =>
      updateMap(findForcingRefs(parent, force), graph, force)

    case Morph1(_, parent) =>
      updateMap(findForcingRefs(parent, OpSide.Center(graph)), graph, force)

    case Morph2(_, left, right) => {
      val merged = findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }

    case Distinct(parent) =>
      updateMap(findForcingRefs(parent, OpSide.Center(graph)), graph, force)

    case AbsoluteLoad(parent, _) =>
      findForcingRefs(parent, OpSide.Center(graph)) // load is a forcing point, but not a memo candidate

    case RelativeLoad(parent, _) =>
      findForcingRefs(parent, OpSide.Center(graph)) // load is a forcing point, but not a memo candidate

    case Operate(_, parent) =>
      findForcingRefs(parent, force)

    case Reduce(_, parent) =>
      findForcingRefs(parent, OpSide.Center(graph)) // reduce is a forcing point, but not a memo candidate

    case MegaReduce(_, parent) =>
      updateMap(findForcingRefs(parent, OpSide.Center(graph)), graph, force)

    case graph @ Split(spec, child, _) => {
      val childRefs = findForcingRefs(child, OpSide.Center(graph)) // TODO is this right?
      val specRefs  = findForcingRefsInSpec(spec, graph)

      updateMap(childRefs |+| specRefs, graph, force)
    }

    case Assert(pred, child) => {
      val merged = findForcingRefs(pred, OpSide.Left(graph)) |+| findForcingRefs(child, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }

    case Cond(pred, left, Cross(_), right, _) if !pred.isInstanceOf[Root] && !left.isInstanceOf[Root] => {
      // no, the sides here are *not* typos; don't change them
      val merged = findForcingRefs(pred, OpSide.Right(graph)) |+|
          findForcingRefs(left, OpSide.Left(graph)) |+|
          findForcingRefs(right, OpSide.Left(graph))

      updateMap(merged, graph, force)
    }

    case Cond(pred, left, _, right, Cross(_)) if !pred.isInstanceOf[Root] && !right.isInstanceOf[Root] => {
      // no, the sides here are *not* typos; don't change them
      val merged = findForcingRefs(pred, OpSide.Right(graph)) |+|
          findForcingRefs(left, OpSide.Left(graph)) |+|
          findForcingRefs(right, OpSide.Left(graph))

      updateMap(merged, graph, force)
    }

    case Cond(pred, left, IdentitySort | ValueSort(_), right, _) if pred.identities != left.identities => {
      // no, the sides here are *not* typos; don't change them
      val merged = findForcingRefs(pred, OpSide.Right(graph)) |+|
          findForcingRefs(left, OpSide.Left(graph)) |+|
          findForcingRefs(right, OpSide.Left(graph))

      updateMap(merged, graph, force)
    }

    case Cond(pred, left, _, right, IdentitySort | ValueSort(_)) if pred.identities != right.identities => {
      // no, the sides here are *not* typos; don't change them
      val merged = findForcingRefs(pred, OpSide.Right(graph)) |+|
          findForcingRefs(left, OpSide.Left(graph)) |+|
          findForcingRefs(right, OpSide.Left(graph))

      updateMap(merged, graph, force)
    }

    case Cond(pred, left, leftJoin, right, rightJoin) => {
      // no, the sides here are *not* typos; don't change them
      findForcingRefs(pred, OpSide.Right(graph)) |+|
        findForcingRefs(left, OpSide.Left(graph)) |+|
        findForcingRefs(right, OpSide.Left(graph))
    }

    case Observe(data, samples) => {
      val merged = findForcingRefs(data, OpSide.Left(graph)) |+| findForcingRefs(samples, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }

    case IUI(_, left, right) => {
      val merged = findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }

    case Diff(left, right) => {
      val merged = findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }

    case Join(_, Cross(_), left, right) if left.isInstanceOf[Root] || right.isInstanceOf[Root] =>
      findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))

    // an approximation of table heritage that *should* be accurate
    case Join(_, _, left, right) /*if left.identities != right.identities*/ => {
      val merged = findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }

    //case Join(_, _, left, right) =>
    //  findForcingRefs(left, force) |+| findForcingRefs(right, force)

    case Filter(Cross(_), target, boolean) if target.isInstanceOf[Root] || boolean.isInstanceOf[Root] => {
      findForcingRefs(target, OpSide.Left(graph)) |+| findForcingRefs(boolean, OpSide.Right(graph))
    }

    // an approximation of table heritage that *should* be accurate
    case Filter(_, target, boolean) /*if target.identities != boolean.identities*/ => {
      val merged = findForcingRefs(target, OpSide.Left(graph)) |+| findForcingRefs(boolean, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }

    //case Filter(_, target, boolean) =>
    //  findForcingRefs(target, force) |+| findForcingRefs(boolean, force)

    case AddSortKey(parent, _, _, _) =>
      findForcingRefs(parent, OpSide.Center(graph))

    case Memoize(parent, _) =>
      findForcingRefs(parent, OpSide.Center(graph)) // memoize is a forcing point, but not a memo candidate
  }

  private def updateMap(refs: Map[DepGraph, Set[OpSide]], graph: DepGraph, force: OpSide): Map[DepGraph, Set[OpSide]] = {
    val set = refs get graph getOrElse Set()
    refs + (graph -> (set + force))
  }

  private sealed trait OpSide

  private object OpSide {
    case class Left(graph: DepGraph)   extends OpSide
    case class Right(graph: DepGraph)  extends OpSide
    case class Center(graph: DepGraph) extends OpSide
  }
}
