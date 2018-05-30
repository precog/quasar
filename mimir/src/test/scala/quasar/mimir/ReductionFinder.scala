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

package quasar.mimir

import scalaz._

import quasar.precog.common._
import quasar.yggdrasil._
import quasar.yggdrasil.execution.EvaluationContext

import scala.collection.mutable

trait ReductionFinderModule extends DAG with EvaluatorMethodsModule with TransSpecableModule {
  type TS1 = trans.TransSpec1
  import library._
  import trans._
  import TableModule.CrossOrder
  import CrossOrder._

  trait ReductionFinder extends EvaluatorMethods with TransSpecable {
    import dag._
    import instructions._

    case class ReduceInfo(reduce: dag.Reduce, spec: TransSpec1, ancestor: DepGraph)

    def buildReduceInfo(reduce: dag.Reduce, ctx: EvaluationContext): ReduceInfo = {
      val (spec, ancestor) = findTransSpecAndAncestor(reduce.parent, ctx).getOrElse((Leaf(Source), reduce.parent))
      ReduceInfo(reduce, spec, ancestor)
    }

    def findReductions(node: DepGraph, ctx: EvaluationContext): MegaReduceState = {
      implicit val m = new Monoid[List[dag.Reduce]] {
        def zero: List[dag.Reduce]                              = Nil
        def append(x: List[dag.Reduce], y: => List[dag.Reduce]) = x ::: y
      }

      val reduces = node
        .foldDown[List[dag.Reduce]](true) {
          case r: dag.Reduce => List(r)
      } distinct

      val info: List[ReduceInfo] = reduces map { buildReduceInfo(_: dag.Reduce, ctx) }

      // for each reduce node, associate it with its ancestor
      val (ancestorByReduce, specByParent) = info.foldLeft((Map[dag.Reduce, DepGraph](), Map[DepGraph, TransSpec1]())) {
        case ((ancestorByReduce, specByParent), ReduceInfo(reduce, spec, ancestor)) =>
          (ancestorByReduce + (reduce -> ancestor), specByParent + (reduce.parent -> spec))
      }

      // for each ancestor, assemble a list of the parents it created
      val parentsByAncestor = (info groupBy { _.ancestor }).foldLeft(Map[DepGraph, List[DepGraph]]()) {
        case (parentsByAncestor, (ancestor, lst)) =>
          parentsByAncestor + (ancestor -> (lst map { _.reduce.parent } distinct))
      }

      // for each parent, assemble a list of the reduces it created
      val reducesByParent = (info groupBy { _.reduce.parent }).foldLeft(Map[DepGraph, List[dag.Reduce]]()) {
        case (reducesByParent, (parent, lst)) =>
          reducesByParent + (parent -> (lst map { _.reduce }))
      }

      MegaReduceState(ancestorByReduce, parentsByAncestor, reducesByParent, specByParent)
    }

    case class MegaReduceState(ancestorByReduce: Map[dag.Reduce, DepGraph],
                               parentsByAncestor: Map[DepGraph, List[DepGraph]],
                               reducesByParent: Map[DepGraph, List[dag.Reduce]],
                               specByParent: Map[DepGraph, TransSpec1]) {

      def buildMembers(ancestor: DepGraph): List[(TransSpec1, List[Reduction])] = {
        parentsByAncestor(ancestor) map { p =>
          (specByParent(p), reducesByParent(p) map { _.red })
        }
      }
    }

    def megaReduce(node: DepGraph, st: MegaReduceState): DepGraph = {
      val reduceTable = mutable.Map[DepGraph, dag.MegaReduce]()

      node mapDown { recurse =>
        {
          case graph @ dag.Reduce(red, parent) if st.ancestorByReduce contains graph => {
            val ancestor = st.ancestorByReduce(graph)
            val members  = st.buildMembers(ancestor)

            val left = reduceTable get ancestor getOrElse {
              val result = dag.MegaReduce(members, recurse(ancestor))
              reduceTable(ancestor) = result
              result
            }

            val firstIndex  = st.parentsByAncestor(ancestor).reverse indexOf parent
            val secondIndex = st.reducesByParent(parent).reverse indexOf graph

            dag.Join(
              DerefArray,
              Cross(Some(CrossLeft)),
              dag.Join(DerefArray, Cross(Some(CrossLeft)), left, Const(CLong(firstIndex))),
              Const(CLong(secondIndex)))
          }
        }
      }
    }
  }
}
