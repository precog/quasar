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
import quasar.precog.common._
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil._
import quasar.yggdrasil.TableModule._
import quasar.yggdrasil.execution.EvaluationContext
import quasar.yggdrasil.vfs._
import quasar.precog.util._

import org.slf4j.LoggerFactory

import scalaz._, Scalaz._, StateT._

import scala.annotation.tailrec
import scala.collection.immutable.Queue

trait EvaluatorModule[M[+ _]]
    extends CrossOrdering
    with Memoizer
    with TypeInferencer
    with CondRewriter
    with JoinOptimizerModule[M]
    with OpFinderModule[M]
    with StaticInlinerModule[M]
    with ReductionFinderModule[M]
    with TransSpecableModule[M]
    with PredicatePullupsModule[M]
    with TableModule[M] // Remove this explicit dep!
    with TableLibModule[M] {

  import dag._
  import instructions._

  type GroupId = Int

  type Evaluator[N[+ _]] <: EvaluatorLike[N]

  abstract class EvaluatorLike[N[+ _]](N0: Monad[N])(implicit mn: M ~> N, nm: N ~> M)
      extends OpFinder
      with ReductionFinder
      with StaticInliner
      with JoinOptimizer
      with PredicatePullups {

    import library._
    import trans._
    import constants._

    private implicit val N = N0
    private val evalLogger = LoggerFactory.getLogger("quasar.mimir.Evaluator")
    private val transState = StateMonadTrans[EvaluatorState]
    private val monadState = stateTMonadState[EvaluatorState, N]

    def report: QueryLogger[N, instructions.Line]

    def freshIdScanner: Scanner

    def Forall: Reduction { type Result = Option[Boolean] }
    def Exists: Reduction { type Result = Option[Boolean] }
    def concatString: F2
    def coerceToDouble: F1

    def composeOptimizations(optimize: Boolean, funcs: List[DepGraph => DepGraph]): DepGraph => DepGraph =
      if (optimize) funcs.reverse.map(Endo[DepGraph]).suml.run else identity

    // Have to be idempotent on subgraphs
    def stagedRewriteDAG(optimize: Boolean, ctx: EvaluationContext): DepGraph => DepGraph = {
      // rewrites are written in `andThen` order
      // we reverse above because our semigroup uses `compose`
      composeOptimizations(
        optimize,
        List(
          inlineStatics(_, ctx),
          optimizeJoins(_, ctx),
          rewriteConditionals(_)
        ))
    }

    def fullRewriteDAG(optimize: Boolean, ctx: EvaluationContext): DepGraph => DepGraph = {
      stagedRewriteDAG(optimize, ctx) andThen
        (orderCrosses _) andThen
        composeOptimizations(
          optimize,
          List[DepGraph => DepGraph](
            // TODO: Predicate pullups break a SnapEngage query (see PLATFORM-951)
            //predicatePullups(_, ctx),
            inferTypes(JType.JUniverseT), { g =>
              megaReduce(g, findReductions(g, ctx))
            },
            memoize
          ))
    }

    /**
      * The entry point to the evaluator.  The main implementation of the evaluator
      * is comprised by the inner functions, `fullEval` (the main evaluator function)
      * and `prepareEval` (which has the primary eval loop).
      */
    def eval(graph: DepGraph, ctx: EvaluationContext, optimize: Boolean): N[Table] = {
      // evalLogger.debug("Eval for {} = {}", ctx.account.apiKey.toString, graph)

      val rewrittenDAG = fullRewriteDAG(optimize, ctx)(graph)

      def resolveTopLevelGroup(spec: BucketSpec, splits: Map[Identifier, Int => N[Table]]): StateT[N, EvaluatorState, N[GroupingSpec]] = spec match {
        case UnionBucketSpec(left, right) =>
          for {
            leftSpec <- resolveTopLevelGroup(left, splits)
            rightSpec <- resolveTopLevelGroup(right, splits)

            val commonIds = findCommonIds(left, right)
            val keySpec = buildKeySpec(commonIds)
          } yield {
            for {
              leftRes <- leftSpec
              rightRes <- rightSpec
            } yield GroupingAlignment(keySpec, keySpec, leftRes, rightRes, GroupingSpec.Union)
          }

        case IntersectBucketSpec(left, right) =>
          for {
            leftSpec <- resolveTopLevelGroup(left, splits)
            rightSpec <- resolveTopLevelGroup(right, splits)

            val commonIds = findCommonIds(left, right)
            val keySpec = buildKeySpec(commonIds)
          } yield {
            for {
              leftRes <- leftSpec
              rightRes <- rightSpec
            } yield GroupingAlignment(keySpec, keySpec, leftRes, rightRes, GroupingSpec.Intersection)
          }

        case dag.Group(id, target, forest) =>
          val common = findCommonality(enumerateGraphs(forest) + target)

          common match {
            case Some(reducedTarget) =>
              for {
                pendingTarget <- prepareEval(reducedTarget, splits)
                resultTargetTable = pendingTarget.table.transform { liftToValues(pendingTarget.trans) }
                _ <- monadState.gets(identity)
                subSpec <- resolveLowLevelGroup(resultTargetTable, reducedTarget, forest, splits)
              } yield {
                // TODO FIXME if the target has forcing points, targetTrans is insufficient
                val Some(trans) = mkTransSpec(target, reducedTarget, ctx)
                N.point(GroupingSource(resultTargetTable, SourceKey.Single, Some(liftToValues(trans)), id, subSpec))
              }

            case None =>
              sys.error("Attempted to evaluate bucket spec without transspecable commonality.")
          }

        case UnfixedSolution(_, _) | dag.Extra(_) =>
          sys.error("assertion error")
      }

      //** only used in resolveTopLevelGroup **/
      def resolveLowLevelGroup(commonTable: Table,
                               commonGraph: DepGraph,
                               forest: BucketSpec,
                               splits: Map[Identifier, Int => N[Table]]): StateT[N, EvaluatorState, GroupKeySpec] = forest match {
        case UnionBucketSpec(left, right) =>
          for {
            leftRes <- resolveLowLevelGroup(commonTable, commonGraph, left, splits)
            rightRes <- resolveLowLevelGroup(commonTable, commonGraph, right, splits)
          } yield GroupKeySpecOr(leftRes, rightRes)

        case IntersectBucketSpec(left, right) =>
          for {
            leftRes <- resolveLowLevelGroup(commonTable, commonGraph, left, splits)
            rightRes <- resolveLowLevelGroup(commonTable, commonGraph, right, splits)
          } yield GroupKeySpecAnd(leftRes, rightRes)

        case UnfixedSolution(id, solution) =>
          val Some(spec) = mkTransSpec(solution, commonGraph, ctx)
          for {
            _ <- monadState.gets(identity)
            liftedTrans = TransSpec.deepMap(spec) {
              case Leaf(_) => DerefObjectStatic(Leaf(Source), paths.Value)
            }
          } yield GroupKeySpecSource(CPathField(id.toString), liftedTrans)

        case dag.Extra(graph) =>
          val Some(spec) = mkTransSpec(graph, commonGraph, ctx)
          for {
            state <- monadState.gets(identity)
            extraId = state.extraCount
            _ <- monadState.modify { _.copy(extraCount = extraId + 1) }

            liftedTrans = TransSpec.deepMap(spec) {
              case Leaf(_) => DerefObjectStatic(Leaf(Source), paths.Value)
            }
          } yield GroupKeySpecSource(CPathField("extra" + extraId), trans.Filter(liftedTrans, liftedTrans))

        case dag.Group(_, _, _) =>
          sys.error("assertion error")
      }

      def prepareEval(graph: DepGraph, splits: Map[Identifier, Int => N[Table]]): StateT[N, EvaluatorState, PendingTable] = {
        evalLogger.trace("Loop on %s".format(graph))

        val startTime = System.nanoTime

        def assumptionCheck(graph: DepGraph): StateT[N, EvaluatorState, Option[(Table, TableOrder)]] =
          for (state <- monadState.gets(identity)) yield state.assume.get(graph)

        def memoized(graph: DepGraph, f: DepGraph => StateT[N, EvaluatorState, PendingTable]) = {
          def memoizedResult = graph match {
            case graph: StagingPoint => {
              for {
                pending <- f(graph)
                _ <- monadState.modify { state =>
                      state.copy(assume = state.assume + (graph -> (pending.table, pending.sort)))
                    }
              } yield pending
            }

            case _ => f(graph)
          }

          assumptionCheck(graph) flatMap { assumedResult: Option[(Table, TableOrder)] =>
            val liftedAssumption = assumedResult map {
              case (table, sort) =>
                monadState point PendingTable(table, graph, TransSpec1.Id, sort)
            }

            liftedAssumption getOrElse memoizedResult
          }
        }

        def get0(pt: PendingTable): (TransSpec1, DepGraph) = (pt.trans, pt.graph)

        def set0(pt: PendingTable, tg: (TransSpec1, DepGraph)): StateT[N, EvaluatorState, PendingTable] = {
          for {
            _ <- monadState.modify { state =>
                  state.copy(assume = state.assume + (tg._2 -> (pt.table, pt.sort)))
                }
          } yield pt.copy(trans = tg._1, graph = tg._2)
        }

        def init0(tg: (TransSpec1, DepGraph)): StateT[N, EvaluatorState, PendingTable] =
          memoized(tg._2, evalNotTransSpecable)

        /**
          * Crosses the result of the left and right DepGraphs together.
          */
        def cross(graph: DepGraph, left: DepGraph, right: DepGraph, hint: Option[CrossOrder])(
            spec: (TransSpec2, TransSpec2) => TransSpec2): StateT[N, EvaluatorState, PendingTable] = {
          import CrossOrder._
          def valueSpec = DerefObjectStatic(Leaf(Source), paths.Value)
          (prepareEval(left, splits) |@| prepareEval(right, splits)).tupled flatMap {
            case (ptLeft, ptRight) =>
              val lTable    = ptLeft.table.transform(liftToValues(ptLeft.trans))
              val rTable    = ptRight.table.transform(liftToValues(ptRight.trans))
              val crossSpec = buildWrappedCrossSpec(spec)
              val resultM   = Table.cross(lTable.compact(valueSpec), rTable.compact(valueSpec), hint)(crossSpec)

              transState liftM mn(resultM map {
                case (joinOrder, table) =>
                  val sort = joinOrder match {
                    case CrossLeft =>
                      ptLeft.sort match {
                        case IdentityOrder(ids) =>
                          val rIds = ptRight.sort match {
                            case IdentityOrder(rIds) if graph.uniqueIdentities => rIds
                            case _                                             => Vector.empty
                          }
                          IdentityOrder(ids ++ rIds.map(_ + left.identities.length))

                        case otherSort => otherSort
                      }

                    case CrossRight =>
                      ptRight.sort match {
                        case IdentityOrder(ids) =>
                          val lIds = ptLeft.sort match {
                            case IdentityOrder(lIds) if graph.uniqueIdentities => lIds
                            case _                                             => Vector.empty
                          }
                          IdentityOrder(ids.map(_ + left.identities.length) ++ lIds)

                        case valueOrder => valueOrder
                      }

                    case CrossLeftRight => // Not actually hit yet. Soon!
                      (ptLeft.sort, ptRight.sort) match {
                        case (IdentityOrder(lIds), IdentityOrder(rIds)) =>
                          IdentityOrder(lIds ++ rIds.map(_ + left.identities.length))
                        case (otherSort, _) => otherSort
                      }

                    case CrossRightLeft => // Not actually hit yet. Soon!
                      (ptLeft.sort, ptRight.sort) match {
                        case (IdentityOrder(lIds), IdentityOrder(rIds)) =>
                          IdentityOrder(rIds.map(_ + left.identities.length) ++ lIds)
                        case (otherSort, _) => otherSort
                      }
                  }

                  PendingTable(table, graph, TransSpec1.Id, sort)
              })
          }
        }

        def join(graph: DepGraph, left: DepGraph, right: DepGraph, joinSort: JoinSort)(
            spec: (TransSpec2, TransSpec2) => TransSpec2): StateT[N, EvaluatorState, PendingTable] = {

          import JoinOrder._

          sealed trait JoinKey
          case class IdentityJoin(ids: Vector[(Int, Int)]) extends JoinKey
          case class ValueJoin(id: Int)                    extends JoinKey

          val idMatch = IdentityMatch(left, right)

          def joinSortToJoinKey(sort: JoinSort): JoinKey = sort match {
            case IdentitySort  => IdentityJoin(idMatch.sharedIndices)
            case ValueSort(id) => ValueJoin(id)
            case x             => sys.error(s"Unexpected arg $x")
          }

          def identityJoinSpec(ids: Vector[Int]): TransSpec1 = {
            if (ids.isEmpty) {
              trans.ConstLiteral(CEmptyArray, SourceKey.Single) // join with undefined, probably
            } else {
              val components = for (i <- ids)
                yield trans.WrapArray(DerefArrayStatic(SourceKey.Single, CPathIndex(i))): TransSpec1
              components reduceLeft { trans.InnerArrayConcat(_, _) }
            }
          }

          val joinKey = joinSortToJoinKey(joinSort)
          val (leftKeySpec, rightKeySpec) = joinKey match {
            case IdentityJoin(ids) =>
              val (lIds, rIds) = ids.unzip
              (identityJoinSpec(lIds), identityJoinSpec(rIds))
            case ValueJoin(id) =>
              val valueKeySpec = trans.DerefObjectStatic(Leaf(Source), CPathField("sort-" + id))
              (valueKeySpec, valueKeySpec)
          }

          def isSorted(sort: TableOrder): Boolean = (joinKey, sort) match {
            case (IdentityJoin(keys), IdentityOrder(ids))            =>
              ids.zipWithIndex take keys.length forall { case (i, j) => i == j }
            case (ValueJoin(id0), ValueOrder(id1))                   => id0 == id1
            case _                                                   => false
          }

          def join0(pendingTableLeft: PendingTable, pendingTableRight: PendingTable): M[PendingTable] = {
            val leftResult  = pendingTableLeft.table.transform(liftToValues(pendingTableLeft.trans))
            val rightResult = pendingTableRight.table.transform(liftToValues(pendingTableRight.trans))

            def adjustTableOrder(order: TableOrder)(f: Int => Int) = order match {
              case IdentityOrder(ids) => IdentityOrder(ids map f)
              case valueOrder         => valueOrder
            }

            val leftSort  = adjustTableOrder(pendingTableLeft.sort)(idMatch.mapLeftIndex)
            val rightSort = adjustTableOrder(pendingTableRight.sort)(idMatch.mapRightIndex)

            val joinSpec = buildWrappedJoinSpec(idMatch)(spec)
            val resultM = (isSorted(leftSort), isSorted(rightSort)) match {
              case (true, true) =>
                M point (KeyOrder -> simpleJoin(leftResult, rightResult)(leftKeySpec, rightKeySpec, joinSpec))
              case (lSorted, rSorted) =>
                val hint = Some(if (lSorted) LeftOrder else if (rSorted) RightOrder else KeyOrder)
                Table.join(leftResult, rightResult, hint)(leftKeySpec, rightKeySpec, joinSpec)
            }

            resultM map {
              case (joinOrder, result) =>
                val sort = (joinKey, joinOrder) match {
                  case (ValueJoin(id), KeyOrder)       => ValueOrder(id)
                  case (ValueJoin(_), LeftOrder)       => leftSort
                  case (ValueJoin(_), RightOrder)      => rightSort
                  case (IdentityJoin(ids), KeyOrder)   => IdentityOrder(Vector.range(0, ids.size))
                  case (IdentityJoin(ids), LeftOrder)  => leftSort
                  case (IdentityJoin(ids), RightOrder) => rightSort
                }

                PendingTable(result, graph, TransSpec1.Id, sort)
            }
          }

          (prepareEval(left, splits) |@| prepareEval(right, splits)).tupled flatMap {
            case (pendingTableLeft, pendingTableRight) =>
              transState liftM mn(join0(pendingTableLeft, pendingTableRight))
          }
        }

        type TSM[T] = StateT[N, EvaluatorState, T]
        def evalTransSpecable(to: DepGraph): StateT[N, EvaluatorState, PendingTable] = {
          mkTransSpecWithState[TSM, PendingTable](to, None, ctx, get0, set0, init0)
        }

        def evalNotTransSpecable(graph: DepGraph): StateT[N, EvaluatorState, PendingTable] = graph match {
          case dag.Observe(data, samples) =>
            for {
              pendingTableData <- prepareEval(data, splits)
              pendingTableSamples <- prepareEval(samples, splits)

              tableData = pendingTableData.table
              tableSamples = pendingTableSamples.table

              result <- {
                //idea: make `samples` table an infinite sample table with replacement
                //then don't need to kick out non-infinite sample sets in the compiler
                //also can make solution more general by accepting discrete random variable
                val keySpec   = trans.WrapObject(trans.DerefObjectStatic(TransSpec1.Id, paths.Key), paths.Key.name)
                val valueSpec = trans.WrapObject(trans.DerefObjectStatic(TransSpec1.Id, paths.Value), paths.Value.name)

                val keyTable   = tableData.transform(keySpec).canonicalize(yggConfig.maxSliceSize)
                val valueTable = tableSamples.transform(valueSpec).canonicalize(yggConfig.maxSliceSize)

                transState liftM mn(keyTable.zip(valueTable))
              }
            } yield {
              val sort = pendingTableData.sort match {
                case ValueOrder(_) => IdentityOrder.empty
                case identityOrder => identityOrder
              }
              PendingTable(result, graph, TransSpec1.Id, sort)
            }

          case Join(op, joinSort @ (IdentitySort | ValueSort(_)), left, right) =>
            join(graph, left, right, joinSort)(transFromBinOp(op))

          case dag.Filter(joinSort @ (IdentitySort | ValueSort(_)), target, boolean) =>
            join(graph, target, boolean, joinSort)(trans.Filter(_, _))

          case s: SplitParam =>
            sys.error("Inlining of SplitParam failed")

          // not using extractors due to bug
          case s: SplitGroup =>
            val f = splits(s.parentId)
            transState liftM f(s.id) map { PendingTable(_, graph, TransSpec1.Id, IdentityOrder(s)) }

          case Const(value) =>
            val table = value match {
              case CString(str) => Table.constString(Set(str))

              case CLong(ln)  => Table.constLong(Set(ln))
              case CDouble(d) => Table.constDouble(Set(d))
              case CNum(n)    => Table.constDecimal(Set(n))

              case CBoolean(b) => Table.constBoolean(Set(b))

              case CNull => Table.constNull

              case CDate(d) => Table.constDate(Set(d))

              case RObject.empty => Table.constEmptyObject
              case RArray.empty  => Table.constEmptyArray
              case CEmptyObject  => Table.constEmptyObject
              case CEmptyArray   => Table.constEmptyArray

              case CUndefined => Table.empty

              case rv => Table.fromRValues(Stream(rv))
            }

            val spec = buildConstantWrapSpec(Leaf(Source))
            monadState point PendingTable(table.transform(spec), graph, TransSpec1.Id, IdentityOrder.empty)

          case Undefined() =>
            monadState point PendingTable(Table.empty, graph, TransSpec1.Id, IdentityOrder.empty)

          // TODO technically, we can do this without forcing by pre-lifting PendingTable#trans
          case dag.New(parent) =>
            for {
              pendingTable <- prepareEval(parent, splits)
              idSpec = makeTableTrans(Map(paths.Key -> trans.WrapArray(Scan(Leaf(Source), freshIdScanner))))
              tableM2 = pendingTable.table.transform(liftToValues(pendingTable.trans)).transform(idSpec)
            } yield PendingTable(tableM2, graph, TransSpec1.Id, IdentityOrder(graph))

          // TODO abstract over absolute/relative load
          case dag.AbsoluteLoad(parent, jtpe) =>
            for {
              pendingTable <- prepareEval(parent, splits)
              Path(prefixStr) = ctx.basePath
              f1 = concatString.applyl(CString(prefixStr.replaceAll("([^/])$", "$1/")))
              trans2 = trans.Map1(trans.DerefObjectStatic(pendingTable.trans, paths.Value), f1)
              loaded = pendingTable.table
                .transform(trans2)
                .load(ctx.apiKey, jtpe)
                .fold(
                  {
                    case ResourceError.NotFound(message)         => report.warn(graph.loc, message) >> Table.empty.point[N]
                    case ResourceError.PermissionsError(message) => report.warn(graph.loc, message) >> Table.empty.point[N]
                    case fatal                                   => report.error(graph.loc, "Fatal error while loading dataset") >> report.die() >> Table.empty.point[N]
                  },
                  table => table.point[N]
                )
              back <- transState liftM mn(loaded).join
            } yield PendingTable(back, graph, TransSpec1.Id, IdentityOrder(graph))

          case dag.RelativeLoad(parent, jtpe) =>
            for {
              pendingTable <- prepareEval(parent, splits)

              Path(prefixStr) = ctx.basePath
              Path(midStr) = ctx.scriptPath
              fullPrefix = prefixStr.replaceAll("([^/])$", "$1/") + midStr.replaceAll("([^/])$", "$1/")

              f1 = concatString.applyl(CString(fullPrefix))
              trans2 = trans.Map1(trans.DerefObjectStatic(pendingTable.trans, paths.Value), f1)
              loaded = pendingTable.table
                .transform(trans2)
                .load(ctx.apiKey, jtpe)
                .fold(
                  {
                    case ResourceError.NotFound(message)         => report.warn(graph.loc, message) >> Table.empty.point[N]
                    case ResourceError.PermissionsError(message) => report.warn(graph.loc, message) >> Table.empty.point[N]
                    case fatal                                   => report.error(graph.loc, "Fatal error while loading dataset") >> report.die() >> Table.empty.point[N]
                  },
                  table => table.point[N]
                )
              back <- transState liftM mn(loaded).join
            } yield PendingTable(back, graph, TransSpec1.Id, IdentityOrder(graph))

          case dag.Morph1(mor, parent) =>
            for {
              pendingTable <- prepareEval(parent, splits)
              back <- transState liftM mn(mor(pendingTable.table.transform(liftToValues(pendingTable.trans))))
            } yield {
              PendingTable(back, graph, TransSpec1.Id, findMorphOrder(mor.idPolicy, pendingTable.sort))
            }

          // TODO: There are many thigns wrong. Morph2 needs to get join info from compiler, which
          // isn't possible currently. We special case "Match" when they don't match to deal with it,
          // but that is weird.
          case dag.Morph2(mor, left, right) =>
            val spec: (TransSpec2, TransSpec2) => TransSpec2 = { (srcLeft, srcRight) =>
              trans.InnerArrayConcat(trans.WrapArray(srcLeft), trans.WrapArray(srcRight))
            }

            val joined: StateT[N, EvaluatorState, (Morph1Apply, PendingTable)] = mor.alignment match {
              case MorphismAlignment.Cross(morph1) =>
                ((transState liftM mn(morph1)) |@| cross(graph, left, right, None)(spec)).tupled

              case MorphismAlignment.Match(morph1) if areJoinable(left, right) =>
                ((transState liftM mn(morph1)) |@| join(graph, left, right, IdentitySort)(spec)).tupled

              // TODO: Remove and see if things break. Also,
              case MorphismAlignment.Match(morph1) =>
                val hint =
                  if (left.isSingleton || !right.isSingleton) CrossOrder.CrossRight
                  else CrossOrder.CrossLeft
                ((transState liftM mn(morph1)) |@| cross(graph, left, right, Some(hint))(spec)).tupled

              case MorphismAlignment.Custom(idPolicy, f) =>
                val pair = (prepareEval(left, splits) |@| prepareEval(right, splits)).tupled
                pair flatMap {
                  case (ptLeft, ptRight) =>
                    val leftTable  = ptLeft.table.transform(liftToValues(ptLeft.trans))
                    val rightTable = ptRight.table.transform(liftToValues(ptRight.trans))
                    def sort(policy: IdentityPolicy): TableOrder = policy match {
                      case IdentityPolicy.Retain.Left            => ptLeft.sort
                      case IdentityPolicy.Retain.Right           => ptRight.sort
                      case IdentityPolicy.Retain.Merge           => IdentityOrder.empty
                      case IdentityPolicy.Retain.Cross           => ptLeft.sort
                      case IdentityPolicy.Synthesize             => IdentityOrder.single
                      case IdentityPolicy.Strip                  => IdentityOrder.empty
                      case IdentityPolicy.Product(leftPolicy, _) => sort(leftPolicy)
                    }

                    transState liftM mn(f(leftTable, rightTable) map {
                      case (table, morph1) =>
                        (morph1, PendingTable(table, graph, TransSpec1.Id, sort(idPolicy)))
                    })
                }
            }

            joined flatMap {
              case (morph1, PendingTable(joinedTable, _, _, sort)) =>
                transState liftM mn(morph1(joinedTable)) map { table =>
                  PendingTable(table, graph, TransSpec1.Id, findMorphOrder(mor.idPolicy, sort))
                }
            }

          case dag.Distinct(parent) =>
            val idSpec = makeTableTrans(Map(paths.Key -> trans.WrapArray(Scan(Leaf(Source), freshIdScanner))))

            for {
              pending <- prepareEval(parent, splits)

              valueSpec = DerefObjectStatic(Leaf(Source), paths.Value)
              table = pending.table.transform(liftToValues(pending.trans))
              distinct = table.distinct(valueSpec)
              result = distinct.transform(idSpec)
            } yield {
              PendingTable(result, graph, TransSpec1.Id, IdentityOrder(graph))
            }

          /**
          returns an array (to be dereferenced later) containing the result of each reduction
            */
          case m @ MegaReduce(reds, parent) =>
            val firstCoalesce = reds.map {
              case (_, reductions) => coalesce(reductions.map((_, None)))
            }

            def makeJArray(idx: Int)(tpe: JType): JType = JArrayFixedT(Map(idx -> tpe))

            def derefArray(idx: Int)(ref: ColumnRef): Option[ColumnRef] =
              ref.selector.dropPrefix(CPath.Identity \ idx).map(ColumnRef(_, ref.ctype))

            val original = firstCoalesce.zipWithIndex map {
              case (red, idx) =>
                (red, Some((makeJArray(idx) _, derefArray(idx) _)))
            }
            val reduction = coalesce(original)

            val spec = combineTransSpecs(reds.map(_._1))

            for {
              pendingTable <- prepareEval(parent, splits)
              liftedTrans = liftToValues(pendingTable.trans)

              result = mn(
                pendingTable.table
                  .transform(liftedTrans)
                  .transform(DerefObjectStatic(Leaf(Source), paths.Value))
                  .transform(spec)
                  .reduce(reduction.reducer)(reduction.monoid))

              table = result.map(reduction.extract)

              keyWrapped = trans.WrapObject(trans.ConstLiteral(CEmptyArray, trans.DerefArrayStatic(Leaf(Source), CPathIndex(0))), paths.Key.name)

              valueWrapped = trans.InnerObjectConcat(keyWrapped, trans.WrapObject(Leaf(Source), paths.Value.name))

              wrapped <- transState liftM table map { _.transform(valueWrapped) }
              rvalue <- transState liftM result.map(reduction.extractValue)

              _ <- monadState.modify { state =>
                    state.copy(
                      assume = state.assume + (m         -> (wrapped, IdentityOrder.empty)),
                      reductions = state.reductions + (m -> rvalue)
                    )
                  }
            } yield {
              PendingTable(wrapped, graph, TransSpec1.Id, IdentityOrder(graph))
            }

          case r @ dag.Reduce(red, parent) =>
            for {
              pendingTable <- prepareEval(parent, splits)
              liftedTrans = liftToValues(pendingTable.trans)
              result <- transState liftM mn(red(pendingTable.table.transform(DerefObjectStatic(liftedTrans, paths.Value))))
              wrapped = result transform buildConstantWrapSpec(Leaf(Source))
            } yield PendingTable(wrapped, graph, TransSpec1.Id, IdentityOrder(graph))

          case s @ dag.Split(spec, child, id) =>
            val idSpec = makeTableTrans(Map(paths.Key -> trans.WrapArray(Scan(Leaf(Source), freshIdScanner))))

            val params = child.foldDown(true) {
              case param: dag.SplitParam if param.parentId == id => Set(param)
            }

            val table = for {
              grouping <- resolveTopLevelGroup(spec, splits)
              state <- monadState.gets(identity)
              grouping2 <- transState liftM grouping
              result <- transState liftM mn(Table.merge(grouping2) { (key, map) =>
                         val splits2 = splits + (id -> (map andThen mn))
                         val rewritten = params.foldLeft(child) {
                           case (child, param) =>
                             val subKey = key \ param.id.toString
                             replaceNode(child, param, Const(subKey)(param.loc))
                         }

                         val back = fullEval(rewritten, splits2, id :: splits.keys.toList)
                         back.eval(state)
                       })
            } yield {
              result.transform(idSpec)
            }

            table map { PendingTable(_, graph, TransSpec1.Id, IdentityOrder(graph)) }

          case dag.Assert(pred, child) =>
            for {
              predPending <- prepareEval(pred, splits)
              childPending <- prepareEval(child, splits)

              liftedTrans = liftToValues(predPending.trans)
              predTable = predPending.table transform DerefObjectStatic(liftedTrans, paths.Value)

              truthiness <- transState liftM mn(predTable.reduce(Forall.reducer)(Forall.monoid))

              assertion = if (truthiness getOrElse false) {
                N.point(())
              } else {
                for {
                  _ <- report.error(graph.loc, "Assertion failed")
                  _ <- report.die() // Arrrrrrrgggghhhhhhhhhhhhhh........ *gurgle*
                } yield ()
              }
              _ <- transState liftM assertion

              result = childPending.table transform liftToValues(childPending.trans)
            } yield PendingTable(result, graph, TransSpec1.Id, childPending.sort)

          case c @ dag.Cond(pred, left, _, right, _) =>
            evalNotTransSpecable(c.peer)

          case IUI(union, left, right) =>
            // TODO: Get rid of ValueSorts.
            for {
              pair <- zip(prepareEval(left, splits), prepareEval(right, splits))
              (leftPending, rightPending) = pair

              keyValueSpec = TransSpec1.PruneToKeyValue

              leftTable = leftPending.table.transform(liftToValues(leftPending.trans))
              rightTable = rightPending.table.transform(liftToValues(rightPending.trans))

              leftSortedM = transState liftM mn(leftTable.sort(keyValueSpec, SortAscending))
              rightSortedM = transState liftM mn(rightTable.sort(keyValueSpec, SortAscending))

              pair <- zip(leftSortedM, rightSortedM)
              (leftSorted, rightSorted) = pair

              result = if (union) {
                leftSorted.cogroup(keyValueSpec, keyValueSpec, rightSorted)(Leaf(Source), Leaf(Source), Leaf(SourceLeft))
              } else {
                leftSorted.cogroup(keyValueSpec, keyValueSpec, rightSorted)(TransSpec1.DeleteKeyValue, TransSpec1.DeleteKeyValue, TransSpec2.LeftId)
              }
            } yield {
              PendingTable(result, graph, TransSpec1.Id, IdentityOrder(graph))
            }

          // TODO unify with IUI
          case Diff(left, right) =>
            for {
              pair <- zip(prepareEval(left, splits), prepareEval(right, splits))
              (leftPending, rightPending) = pair

              leftTable = leftPending.table.transform(liftToValues(leftPending.trans))
              rightTable = rightPending.table.transform(liftToValues(rightPending.trans))

              // this transspec prunes everything that is not a key or a value.
              keyValueSpec = TransSpec1.PruneToKeyValue

              leftSortedM = transState liftM mn(leftTable.sort(keyValueSpec, SortAscending))
              rightSortedM = transState liftM mn(rightTable.sort(keyValueSpec, SortAscending))

              pair <- zip(leftSortedM, rightSortedM)
              (leftSorted, rightSorted) = pair

              result = leftSorted.cogroup(keyValueSpec, keyValueSpec, rightSorted)(TransSpec1.Id, TransSpec1.DeleteKeyValue, TransSpec2.DeleteKeyValueLeft)
            } yield {
              PendingTable(result, graph, TransSpec1.Id, IdentityOrder(graph))
            }

          case j @ Join(op, Cross(hint), left, right) =>
            cross(graph, left, right, hint)(transFromBinOp(op))

          case f @ dag.Filter(Cross(hint), target, boolean) =>
            cross(graph, target, boolean, hint)(trans.Filter(_, _))

          case s @ AddSortKey(parent, sortField, valueField, id) =>
            val sortSpec  = DerefObjectStatic(DerefObjectStatic(Leaf(Source), paths.Value), CPathField(sortField))
            val valueSpec = DerefObjectStatic(DerefObjectStatic(Leaf(Source), paths.Value), CPathField(valueField))
            if (parent.valueKeys contains id) {
              prepareEval(parent, splits)
            } else {
              for {
                pending <- prepareEval(parent, splits)
                table = pending.table.transform(liftToValues(pending.trans))
                result = {
                  val wrappedSort  = trans.WrapObject(sortSpec, "sort-" + id)
                  val wrappedValue = trans.WrapObject(valueSpec, paths.Value.name)
                  val oldSortFields = parent.valueKeys map { id0 =>
                    CPathField("sort-" + id0)
                  }
                  val spec = InnerObjectConcat(
                    InnerObjectConcat(ObjectDelete(Leaf(Source), Set(CPathField("sort-" + id), paths.Value) ++ oldSortFields), wrappedSort),
                    wrappedValue)

                  table.transform(spec)
                }
              } yield {
                PendingTable(result, graph, TransSpec1.Id, pending.sort)
              }
            }

          case Memoize(parent, _) =>
            for {
              pending <- prepareEval(parent, splits)
              table = pending.table.transform(liftToValues(pending.trans))

              result <- transState liftM mn(table.force)
            } yield {
              PendingTable(result, graph, TransSpec1.Id, pending.sort)
            }
        }

        val back    = memoized(graph, evalTransSpecable)
        val endTime = System.nanoTime

        val timingM = transState liftM report.timing(graph.loc, endTime - startTime)

        timingM >> back
      }

      /**
        * The base eval function.  Takes a (rewritten) graph and evaluates the forcing
        * points at the current Split level in topological order.  The endpoint of the
        * graph is considered to be a special forcing point, but as it is the endpoint,
        * it will perforce be evaluated last.
        */
      def fullEval(graph: DepGraph, splits: Map[Identifier, Int => N[Table]], parentSplits: List[Identifier]): StateT[N, EvaluatorState, Table] = {
        type EvaluatorStateT[A] = StateT[N, EvaluatorState, A]

        def stage(toEval: List[StagingPoint], graph: DepGraph): EvaluatorStateT[DepGraph] = {
          for {
            state <- monadState gets identity
            optPoint = toEval find { g =>
              !(state.assume contains g)
            }

            optBack = optPoint map { point =>
              for {
                _ <- prepareEval(point, splits)
                rewritten <- stagedOptimizations(graph, ctx, optimize)

                toEval = listStagingPoints(Queue(rewritten)) filter referencesOnlySplit(parentSplits)
                result <- stage(toEval, rewritten)
              } yield result
            }

            back <- optBack getOrElse (monadState point graph)
          } yield back
        }

        // find the topologically-sorted forcing points (excluding the endpoint)
        // at the current split level
        val toEval = listStagingPoints(Queue(graph)) filter referencesOnlySplit(parentSplits)

        for {
          rewrittenGraph <- stage(toEval, graph)
          pendingTable <- prepareEval(rewrittenGraph, splits)
          table = pendingTable.table transform liftToValues(pendingTable.trans)
        } yield table
      }

      val resultState: StateT[N, EvaluatorState, Table] = fullEval(rewrittenDAG, Map(), Nil)

      val resultTable: N[Table] = resultState.eval(EvaluatorState())
      resultTable map { _ paged yggConfig.maxSliceSize compact DerefObjectStatic(Leaf(Source), paths.Value) }
    }

    private[this] def stagedOptimizations(graph: DepGraph, ctx: EvaluationContext, optimize: Boolean) =
      for {
        reductions <- monadState.gets(_.reductions)
      } yield
        stagedRewriteDAG(optimize, ctx)(reductions.toList.foldLeft(graph) {
          case (graph, (from, Some(result))) if optimize => inlineNodeValue(graph, from, result)
          case (graph, _)                                => graph
        })

    /**
      * Takes a graph, a node and a value. Replaces the node (and
      * possibly its parents) with the value into the graph.
      */
    def inlineNodeValue(graph: DepGraph, from: DepGraph, result: RValue) = {
      val replacements = graph.foldDown(true) {
        case join @ Join(DerefArray, Cross(_), Join(DerefArray, Cross(_), `from`, Const(CLong(index1))), Const(CLong(index2))) =>
          List((join, Const(result)(from.loc)))
      }

      replacements.foldLeft(graph) {
        case (graph, (from, to)) => replaceNode(graph, from, to)
      }
    }

    private[this] def replaceNode(graph: DepGraph, from: DepGraph, to: DepGraph) = {
      graph mapDown { recurse =>
        {
          case `from` => to
        }
      }
    }

    /**
      * Returns all forcing points in the graph, ordered topologically.
      */
    @tailrec
    private[this] def listStagingPoints(queue: Queue[DepGraph], acc: List[dag.StagingPoint] = Nil): List[dag.StagingPoint] = {
      def listParents(spec: BucketSpec): Set[DepGraph] = spec match {
        case UnionBucketSpec(left, right)     => listParents(left) ++ listParents(right)
        case IntersectBucketSpec(left, right) => listParents(left) ++ listParents(right)

        case dag.Group(_, target, forest) => listParents(forest) + target

        case dag.UnfixedSolution(_, solution) => Set(solution)
        case dag.Extra(expr)                  => Set(expr)
      }
      if (queue.isEmpty) {
        acc
      } else {
        val (graph, queue2) = queue.dequeue

        val (queue3, addend) = {
          val queue3 = graph match {
            case _: dag.SplitParam => queue2
            case _: dag.SplitGroup => queue2
            case _: dag.Root       => queue2

            case dag.New(parent) => queue2 enqueue parent

            case dag.Morph1(_, parent)      => queue2 enqueue parent
            case dag.Morph2(_, left, right) => queue2 enqueue left enqueue right

            case dag.Distinct(parent) => queue2 enqueue parent

            case dag.AbsoluteLoad(parent, _) => queue2 enqueue parent
            case dag.RelativeLoad(parent, _) => queue2 enqueue parent

            case dag.Operate(_, parent) => queue2 enqueue parent

            case dag.Reduce(_, parent)     => queue2 enqueue parent
            case dag.MegaReduce(_, parent) => queue2 enqueue parent

            case dag.Split(specs, child, _) => queue2 enqueue child enqueue listParents(specs)

            case dag.Assert(pred, child)           => queue2 enqueue pred enqueue child
            case dag.Cond(pred, left, _, right, _) => queue2 enqueue pred enqueue left enqueue right

            case dag.Observe(data, samples) => queue2 enqueue data enqueue samples

            case dag.IUI(_, left, right) => queue2 enqueue left enqueue right
            case dag.Diff(left, right)   => queue2 enqueue left enqueue right

            case dag.Join(_, _, left, right) => queue2 enqueue left enqueue right
            case dag.Filter(_, left, right)  => queue2 enqueue left enqueue right

            case dag.AddSortKey(parent, _, _, _) => queue2 enqueue parent

            case dag.Memoize(parent, _) => queue2 enqueue parent
          }

          val addend = Some(graph) collect {
            case fp: StagingPoint => fp
          }

          (queue3, addend)
        }

        listStagingPoints(queue3, addend map { _ :: acc } getOrElse acc)
      }
    }

    // Takes a list of Splits, head is the current Split, which must be referenced.
    // The rest of the referenced Splits must be in the the list.
    private def referencesOnlySplit(parentSplits: List[Identifier])(graph: DepGraph): Boolean = {
      val referencedSplits = graph.foldDown(false) {
        case s: dag.SplitParam => Set(s.parentId)
        case s: dag.SplitGroup => Set(s.parentId)
      }

      val currentIsReferenced = parentSplits.headOption.map(referencedSplits.contains(_)).getOrElse(true)

      currentIsReferenced && (referencedSplits -- parentSplits).isEmpty
    }

    private def findCommonality(nodes: Set[DepGraph]): Option[DepGraph] = {
      case class Kernel(nodes: Set[DepGraph], seen: Set[DepGraph])

      @tailrec
      def bfs(kernels: Set[Kernel]): Set[DepGraph] = {
        // check for convergence
        val results = kernels flatMap { k =>
          val results = kernels.foldLeft(k.nodes) { _ & _.seen }

          // TODO if the below isEmpty, then can drop results from all kernels
          nodes.foldLeft(results) { (results, node) =>
            results filter { isTransSpecable(node, _) }
          }
        }

        // iterate
        if (results.isEmpty) {
          val kernels2 = kernels map { k =>
            val nodes2 = k.nodes flatMap enumerateParents
            val nodes3 = nodes2 &~ k.seen

            Kernel(nodes3, k.seen ++ nodes3)
          }

          if (kernels2 forall { _.nodes.isEmpty }) {
            Set()
          } else {
            bfs(kernels2)
          }
        } else {
          results
        }
      }

      if (nodes.size == 1) {
        nodes.headOption
      } else {
        val kernels = nodes map { n =>
          Kernel(Set(n), Set(n))
        }
        val results = bfs(kernels)

        if (results.size == 1)
          results.headOption
        else
          None
      }
    }

    private def enumerateParents(node: DepGraph): Set[DepGraph] = node match {
      case _: SplitParam | _: SplitGroup | _: Root => Set()
      case dag.New(parent)                         => Set(parent)
      case dag.Morph1(_, parent)                   => Set(parent)
      case dag.Morph2(_, left, right)              => Set(left, right)
      case dag.Assert(pred, child)                 => Set(pred, child)
      case dag.Cond(pred, left, _, right, _)       => Set(pred, left, right)
      case dag.Distinct(parent)                    => Set(parent)
      case dag.AbsoluteLoad(parent, _)             => Set(parent)
      case dag.RelativeLoad(parent, _)             => Set(parent)
      case Operate(_, parent)                      => Set(parent)
      case dag.Reduce(_, parent)                   => Set(parent)
      case MegaReduce(_, parent)                   => Set(parent)
      case dag.Split(spec, _, _)                   => enumerateSpecParents(spec).toSet
      case IUI(_, left, right)                     => Set(left, right)
      case Diff(left, right)                       => Set(left, right)
      case Join(_, _, left, right)                 => Set(left, right)
      case dag.Filter(_, target, boolean)          => Set(target, boolean)
      case AddSortKey(parent, _, _, _)             => Set(parent)
      case Memoize(parent, _)                      => Set(parent)
      case x                                       => sys.error(s"Unexpected arg $x")
    }

    private def enumerateSpecParents(spec: BucketSpec): Set[DepGraph] = spec match {
      case UnionBucketSpec(left, right)     => enumerateSpecParents(left) ++ enumerateSpecParents(right)
      case IntersectBucketSpec(left, right) => enumerateSpecParents(left) ++ enumerateSpecParents(right)

      case dag.Group(_, target, child) => enumerateSpecParents(child) + target

      case UnfixedSolution(_, target) => Set(target)
      case dag.Extra(target)          => Set(target)
    }

    private def findCommonIds(left: BucketSpec, right: BucketSpec): Set[Int] =
      enumerateSolutionIds(left) & enumerateSolutionIds(right)

    private def enumerateSolutionIds(spec: BucketSpec): Set[Int] = spec match {
      case UnionBucketSpec(left, right) =>
        enumerateSolutionIds(left) ++ enumerateSolutionIds(right)

      case IntersectBucketSpec(left, right) =>
        enumerateSolutionIds(left) ++ enumerateSolutionIds(right)

      case dag.Group(_, _, forest) => enumerateSolutionIds(forest)

      case UnfixedSolution(id, _) => Set(id)
      case dag.Extra(_)           => Set()
    }

    private def buildKeySpec(commonIds: Set[Int]): TransSpec1 = {
      val parts: Set[TransSpec1] = commonIds map { id =>
        trans.WrapObject(DerefObjectStatic(Leaf(Source), CPathField(id.toString)), id.toString)
      }

      parts reduceOption { (left, right) =>
        trans.InnerObjectConcat(left, right)
      } getOrElse ConstLiteral(CEmptyArray, Leaf(Source))
    }

    private def disjunctiveEquals(specs: (IdentitySpec, IdentitySpec)): Boolean = specs match {
      case (CoproductIds(left, right), b) => disjunctiveEquals(b, left) || disjunctiveEquals(b, right)
      case (a, CoproductIds(left, right)) => disjunctiveEquals(a, left) || disjunctiveEquals(a, right)
      case (a, b)                         => a == b
    }

    private def areJoinable(left: DepGraph, right: DepGraph): Boolean =
      IdentityMatch(left, right).sharedIndices.size > 0

    private def enumerateGraphs(forest: BucketSpec): Set[DepGraph] = forest match {
      case UnionBucketSpec(left, right)     => enumerateGraphs(left) ++ enumerateGraphs(right)
      case IntersectBucketSpec(left, right) => enumerateGraphs(left) ++ enumerateGraphs(right)

      case dag.Group(_, target, subForest) =>
        enumerateGraphs(subForest) + target

      case UnfixedSolution(_, graph) => Set(graph)
      case dag.Extra(graph)          => Set(graph)
    }

    private def simpleJoin(left: Table, right: Table)(leftKey: TransSpec1, rightKey: TransSpec1, spec: TransSpec2): Table = {
      val emptySpec = trans.ConstLiteral(CEmptyArray, Leaf(Source))
      val result    = left.cogroup(leftKey, rightKey, right)(emptySpec, emptySpec, trans.WrapArray(spec))

      result.transform(trans.DerefArrayStatic(Leaf(Source), CPathIndex(0)))
    }

    private def findMorphOrder(policy: IdentityPolicy, order: TableOrder): TableOrder = {
      import IdentityPolicy._
      def rec(policy: IdentityPolicy): Vector[Int] = policy match {
        case Product(leftPolicy, rightPolicy) =>
          rec(leftPolicy) ++ rec(rightPolicy)
        case Synthesize => IdentityOrder.single.ids
        case Strip      => IdentityOrder.empty.ids
        case (_: Retain) =>
          order match {
            case IdentityOrder(ids) => ids
            case _                  => IdentityOrder.empty.ids
          }
      }
      IdentityOrder(rec(policy))
    }

    private def flip[A, B, C](f: (A, B) => C)(b: B, a: A): C = f(a, b) // is this in scalaz?

    private def zip[A](table1: StateT[N, EvaluatorState, A], table2: StateT[N, EvaluatorState, A]): StateT[N, EvaluatorState, (A, A)] =
      monadState.apply2(table1, table2) { (_, _) }

    private case class EvaluatorState(
        assume: Map[DepGraph, (Table, TableOrder)] = Map.empty,
        reductions: Map[DepGraph, Option[RValue]] = Map.empty,
        extraCount: Int = 0
    )

    private sealed trait TableOrder {
      def ids: Vector[Int]
    }
    private case class ValueOrder(id: Int) extends TableOrder {
      def ids: Vector[Int] = Vector.empty
    }
    private case class IdentityOrder(ids: Vector[Int]) extends TableOrder
    private object IdentityOrder {
      def apply(node: DepGraph): IdentityOrder = IdentityOrder(Vector.range(0, node.identities.length))
      def empty: IdentityOrder                 = IdentityOrder(Vector.empty)
      def single: IdentityOrder                = IdentityOrder(Vector(0))
    }

    private case class PendingTable(table: Table, graph: DepGraph, trans: TransSpec1, sort: TableOrder)
  }
}
