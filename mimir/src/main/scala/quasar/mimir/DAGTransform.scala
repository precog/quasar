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

import quasar.blueeyes._

trait DAGTransform extends DAG {
  import dag._
  // import instructions.{ DerefObject, Eq, JoinObject, Line, PushString, WrapObject }

  def transformBottomUp(graph: DepGraph)(f: DepGraph => DepGraph): DepGraph = {

    val memotable = scmMap[DepGraphWrapper, DepGraph]()

    def transformSpec(spec: BucketSpec): BucketSpec = spec match {
      case UnionBucketSpec(left, right) =>
        UnionBucketSpec(transformSpec(left), transformSpec(right))

      case IntersectBucketSpec(left, right) =>
        IntersectBucketSpec(transformSpec(left), transformSpec(right))

      case Group(id, target, child) =>
        Group(id, transformAux(target), transformSpec(child))

      case UnfixedSolution(id, target) =>
        UnfixedSolution(id, transformAux(target))

      case Extra(target) =>
        Extra(transformAux(target))
    }

    def transformAux(graph: DepGraph): DepGraph = {
      def inner(graph: DepGraph): DepGraph = graph match {
        case r: Root => f(r)

        case graph @ New(parent) => f(New(transformAux(parent))(graph.loc))

        case graph @ AbsoluteLoad(parent, jtpe) => f(AbsoluteLoad(transformAux(parent), jtpe)(graph.loc))

        case graph @ RelativeLoad(parent, jtpe) => f(RelativeLoad(transformAux(parent), jtpe)(graph.loc))

        case graph @ Operate(op, parent) => f(Operate(op, transformAux(parent))(graph.loc))

        case graph @ Reduce(red, parent) => f(Reduce(red, transformAux(parent))(graph.loc))

        case MegaReduce(reds, parent) => f(MegaReduce(reds, transformAux(parent)))

        case graph @ Morph1(m, parent) => f(Morph1(m, transformAux(parent))(graph.loc))

        case graph @ Morph2(m, left, right) => f(Morph2(m, transformAux(left), transformAux(right))(graph.loc))

        case graph @ Join(op, joinSort, left, right) => f(Join(op, joinSort, transformAux(left), transformAux(right))(graph.loc))

        case graph @ Assert(pred, child) => f(Assert(transformAux(pred), transformAux(child))(graph.loc))

        case graph @ Cond(pred, left, leftJoin, right, rightJoin) =>
          f(Cond(transformAux(pred), transformAux(left), leftJoin, transformAux(right), rightJoin)(graph.loc))

        case graph @ Observe(data, samples) => f(Observe(transformAux(data), transformAux(samples))(graph.loc))

        case graph @ IUI(union, left, right) => f(IUI(union, transformAux(left), transformAux(right))(graph.loc))

        case graph @ Diff(left, right) => f(Diff(transformAux(left), transformAux(right))(graph.loc))

        case graph @ Filter(cross, target, boolean) =>
          f(Filter(cross, transformAux(target), transformAux(boolean))(graph.loc))

        case AddSortKey(parent, sortField, valueField, id) => f(AddSortKey(transformAux(parent), sortField, valueField, id))

        case Memoize(parent, priority) => f(Memoize(transformAux(parent), priority))

        case graph @ Distinct(parent) => f(Distinct(transformAux(parent))(graph.loc))

        case s @ Split(spec, child, id) => {
          val spec2  = transformSpec(spec)
          val child2 = transformAux(child)
          f(Split(spec2, child2, id)(s.loc))
        }

        // not using extractors due to bug
        case s: SplitGroup =>
          f(SplitGroup(s.id, s.identities, s.parentId)(s.loc))

        // not using extractors due to bug
        case s: SplitParam =>
          f(SplitParam(s.id, s.parentId)(s.loc))
      }

      memotable.get(new DepGraphWrapper(graph)) getOrElse {
        val result = inner(graph)
        memotable += (new DepGraphWrapper(graph) -> result)
        result
      }
    }

    transformAux(graph)
  }
}
