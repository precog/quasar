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
import quasar.precog.util.IdGen
import quasar.precog.common._
import quasar.yggdrasil.TableModule
import quasar.yggdrasil.execution.EvaluationContext

trait JoinOptimizerModule[M[+ _]] extends DAGTransform with TransSpecableModule[M] {

  trait JoinOptimizer extends TransSpecable {
    import dag._
    import instructions._
    import TableModule.CrossOrder
    import CrossOrder._

    def optimizeJoins(graph: DepGraph, ctx: EvaluationContext, idGen: IdGen = IdGen): DepGraph = {

      def compareAncestor(lhs: DepGraph, rhs: DepGraph): Boolean =
        findAncestor(lhs, ctx) == findAncestor(rhs, ctx)

      def liftRewrite(graph: DepGraph, eq: DepGraph, lifted: DepGraph): DepGraph =
        transformBottomUp(graph) { g =>
          if (g == eq) lifted else g
        }

      def rewrite(filter: dag.Filter, body: DepGraph, eqA: DepGraph, eqB: DepGraph): DepGraph = {
        val sortId = idGen.nextInt()

        def lift(keyGraph: DepGraph, valueGraph: DepGraph): DepGraph = {
          val key   = "key"
          val value = "value"

          AddSortKey(
            Join(
              JoinObject,
              IdentitySort,
              Join(WrapObject, Cross(Some(CrossRight)), Const(CString(key))(filter.loc), keyGraph)(filter.loc),
              Join(WrapObject, Cross(Some(CrossRight)), Const(CString(value))(filter.loc), valueGraph)(filter.loc))(filter.loc),
            key,
            value,
            sortId)
        }

        val rewritten = transformBottomUp(body) {
          case j @ Join(_, _, Const(_), _) => j

          case j @ Join(_, _, _, Const(_)) => j

          case j @ Join(op, Cross(_), lhs, rhs)
              if (compareAncestor(lhs, eqA) && compareAncestor(rhs, eqB)) ||
                (compareAncestor(lhs, eqB) && compareAncestor(rhs, eqA)) => {

            val (eqLHS, eqRHS) = {
              if (compareAncestor(lhs, eqA))
                (eqA, eqB)
              else
                (eqB, eqA)
            }

            val ancestorLHS = findOrderAncestor(lhs, ctx) getOrElse lhs
            val ancestorRHS = findOrderAncestor(rhs, ctx) getOrElse rhs

            val liftedLHS = lift(eqLHS, ancestorLHS)
            val liftedRHS = lift(eqRHS, ancestorRHS)

            Join(op, ValueSort(sortId), liftRewrite(lhs, ancestorLHS, liftedLHS), liftRewrite(rhs, ancestorRHS, liftedRHS))(j.loc)
          }

          case other => other
        }

        if (rewritten == body) filter else rewritten
      }

      transformBottomUp(graph) {
        case filter @ Filter(IdentitySort, body @ Join(BuiltInFunction2Op(op2), _, _, _), Join(Eq, Cross(_), eqA, eqB)) if op2.rowLevel =>
          rewrite(filter, body, eqA, eqB)

        case filter @ Filter(IdentitySort, body @ Join(BuiltInMorphism2(morph2), _, _, _), Join(Eq, Cross(_), eqA, eqB)) if morph2.rowLevel =>
          rewrite(filter, body, eqA, eqB)

        case filter @ Filter(IdentitySort, body @ Join(_, _, _, _), Join(Eq, Cross(_), eqA, eqB)) => rewrite(filter, body, eqA, eqB)

        case filter @ Filter(IdentitySort, body @ Operate(BuiltInFunction1Op(op1), _), Join(Eq, Cross(_), eqA, eqB)) if op1.rowLevel =>
          rewrite(filter, body, eqA, eqB)

        case other => other
      }
    }
  }
}
