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

import quasar.yggdrasil.execution.EvaluationContext

trait PredicatePullupsModule[M[+ _]] extends TransSpecableModule[M] {
  trait PredicatePullups extends TransSpecable {
    import dag._
    import instructions.And

    case class GroupEdit(group: BucketSpec, graphEdit: (DepGraph, DepGraph), specEdit: (dag.BucketSpec, dag.BucketSpec))

    def predicatePullups(graph: DepGraph, ctx: EvaluationContext): DepGraph = {
      val edits = graph.foldDown(true) {
        case s @ Split(g @ Group(id, target, gchild), schild, _) =>
          def extractFilter(spec: dag.BucketSpec): Option[(List[DepGraph], List[dag.BucketSpec])] = spec match {
            case dag.IntersectBucketSpec(left, right) =>
              for {
                fl <- extractFilter(left)
                fr <- extractFilter(right)
              } yield fl |+| fr
            case dag.Extra(target)                                                  => Some((List(target), Nil))
            case u @ dag.UnfixedSolution(id, expr) if isTransSpecable(expr, target) => Some((Nil, List(u)))
            case other                                                              => None
          }

          extractFilter(gchild) match {
            case Some((booleans @ (_ :: _), newChildren @ (_ :: _))) => {
              val newChild  = newChildren.reduceLeft(dag.IntersectBucketSpec(_, _))
              val boolean   = booleans.reduceLeft(Join(And, IdentitySort, _, _)(s.loc))
              val newTarget = Filter(IdentitySort, target, boolean)(target.loc)
              List(GroupEdit(g, target -> newTarget, gchild -> newChild))
            }
            case _ => Nil
          }

        case other => Nil
      }

      edits.foldLeft(graph) {
        case (graph, ge @ GroupEdit(group0, graphEdit, specEdit)) =>
          val (n0, group1) = graph.substituteDown(group0, specEdit)
          val (n1, _)      = n0.substituteDown(group1, graphEdit)
          n1
      }
    }
  }
}
