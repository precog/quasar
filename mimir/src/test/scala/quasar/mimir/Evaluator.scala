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

import quasar.precog.common._
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil._
import quasar.yggdrasil.TableModule._
import quasar.yggdrasil.execution.EvaluationContext
import quasar.yggdrasil.vfs._
import quasar.precog.util._

import org.slf4j.LoggerFactory

import cats.effect.IO
import scalaz._, Scalaz._, StateT._
import shims._

import scala.annotation.tailrec
import scala.collection.immutable.Queue

trait EvaluatorModule
    extends OpFinderModule
    with ReductionFinderModule
    with TransSpecableModule
    with TableModule // Remove this explicit dep!
    with TableLibModule {

  import dag._
  import instructions._

  type GroupId = Int

  type Evaluator <: EvaluatorLike

  abstract class EvaluatorLike
      extends OpFinder
      with ReductionFinder {

    import library._
    import trans._
    import constants._

    private val evalLogger = LoggerFactory.getLogger("quasar.mimir.Evaluator")
    private val transState = StateMonadTrans[EvaluatorState]
    private val monadState = stateTMonadState[EvaluatorState, IO]

    def report: QueryLogger[Unit]

    def Forall: Reduction { type Result = Option[Boolean] }
    def Exists: Reduction { type Result = Option[Boolean] }
    def concatString: F2
    def coerceToDouble: F1

    def composeOptimizations(optimize: Boolean, funcs: List[DepGraph => DepGraph]): DepGraph => DepGraph =
      if (optimize) funcs.reverse.map(Endo[DepGraph]).suml.run else identity

    // Have to be idempotent on subgraphs
    def stagedRewriteDAG(optimize: Boolean, ctx: EvaluationContext): DepGraph => DepGraph =
      identity

    def fullRewriteDAG(optimize: Boolean, ctx: EvaluationContext): DepGraph => DepGraph = {
      stagedRewriteDAG(optimize, ctx) andThen
        composeOptimizations(
          optimize,
          List[DepGraph => DepGraph](
            { g =>
              megaReduce(g, findReductions(g, ctx))
            }))
    }

    /**
      * The entry point to the evaluator.  The main implementation of the evaluator
      * is comprised by the inner functions, `fullEval` (the main evaluator function)
      * and `prepareEval` (which has the primary eval loop).
      */
    def eval(graph: DepGraph, ctx: EvaluationContext, optimize: Boolean): IO[Table] = {
      // evalLogger.debug("Eval for {} = {}", ctx.account.apiKey.toString, graph)

      val rewrittenDAG = fullRewriteDAG(optimize, ctx)(graph)

      def prepareEval(graph: DepGraph, splits: Map[Identifier, Int => IO[Table]]): StateT[IO, EvaluatorState, PendingTable] = {
        evalLogger.trace("Loop on %s".format(graph))

        val startTime = System.nanoTime

        def assumptionCheck(graph: DepGraph): StateT[IO, EvaluatorState, Option[(Table, TableOrder)]] =
          for (state <- monadState.gets(identity)) yield state.assume.get(graph)

        def memoized(graph: DepGraph, f: DepGraph => StateT[IO, EvaluatorState, PendingTable]) = {
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

        def set0(pt: PendingTable, tg: (TransSpec1, DepGraph)): StateT[IO, EvaluatorState, PendingTable] = {
          for {
            _ <- monadState.modify { state =>
                  state.copy(assume = state.assume + (tg._2 -> (pt.table, pt.sort)))
                }
          } yield pt.copy(trans = tg._1, graph = tg._2)
        }

        def init0(tg: (TransSpec1, DepGraph)): StateT[IO, EvaluatorState, PendingTable] =
          memoized(tg._2, evalNotTransSpecable)

        /**
          * Crosses the result of the left and right DepGraphs together.
          */
        def cross(graph: DepGraph, left: DepGraph, right: DepGraph, hint: Option[CrossOrder])(
            spec: (TransSpec2, TransSpec2) => TransSpec2): StateT[IO, EvaluatorState, PendingTable] = {
          import CrossOrder._
          def valueSpec = DerefObjectStatic(Leaf(Source), paths.Value)
          (prepareEval(left, splits) |@| prepareEval(right, splits)).tupled flatMap {
            case (ptLeft, ptRight) =>
              val lTable    = ptLeft.table.transform(liftToValues(ptLeft.trans))
              val rTable    = ptRight.table.transform(liftToValues(ptRight.trans))
              val crossSpec = buildWrappedCrossSpec(spec)
              val resultM   = Table.cross(lTable.compact(valueSpec), rTable.compact(valueSpec), hint)(crossSpec)

              transState liftM (resultM map {
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
            spec: (TransSpec2, TransSpec2) => TransSpec2): StateT[IO, EvaluatorState, PendingTable] = {

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

          def join0(pendingTableLeft: PendingTable, pendingTableRight: PendingTable): IO[PendingTable] = {
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
                IO(KeyOrder -> simpleJoin(leftResult, rightResult)(leftKeySpec, rightKeySpec, joinSpec))
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
              transState liftM (join0(pendingTableLeft, pendingTableRight))
          }
        }

        type TSM[T] = StateT[IO, EvaluatorState, T]
        def evalTransSpecable(to: DepGraph): StateT[IO, EvaluatorState, PendingTable] = {
          mkTransSpecWithState[TSM, PendingTable](to, None, ctx, get0, set0, init0)
        }

        def evalNotTransSpecable(graph: DepGraph): StateT[IO, EvaluatorState, PendingTable] = graph match {
          case Join(op, joinSort @ (IdentitySort | ValueSort(_)), left, right) =>
            join(graph, left, right, joinSort)(transFromBinOp(op))

          case Const(value) =>
            val table = value match {
              case CString(str) => Table.constString(Set(str))

              case CLong(ln)  => Table.constLong(Set(ln))
              case CDouble(d) => Table.constDouble(Set(d))
              case CNum(n)    => Table.constDecimal(Set(n))

              case CBoolean(b) => Table.constBoolean(Set(b))

              case CNull => Table.constNull

              case COffsetDateTime(d) => Table.constOffsetDateTime(Set(d))
              case COffsetTime(d)     => Table.constOffsetTime(Set(d))
              case COffsetDate(d)     => Table.constOffsetDate(Set(d))
              case CLocalDateTime(d)  => Table.constLocalDateTime(Set(d))
              case CLocalTime(d)      => Table.constLocalTime(Set(d))
              case CLocalDate(d)      => Table.constLocalDate(Set(d))

              case RObject.empty => Table.constEmptyObject
              case RArray.empty  => Table.constEmptyArray
              case CEmptyObject  => Table.constEmptyObject
              case CEmptyArray   => Table.constEmptyArray

              case CUndefined => Table.empty

              case rv => Table.fromRValues(Vector(rv))
            }

            val spec = buildConstantWrapSpec(Leaf(Source))
            monadState point PendingTable(table.transform(spec), graph, TransSpec1.Id, IdentityOrder.empty)

          case Undefined() =>
            monadState point PendingTable(Table.empty, graph, TransSpec1.Id, IdentityOrder.empty)

          // TODO abstract over absolute/relative load
          case dag.AbsoluteLoad(parent, jtpe) =>
            for {
              pendingTable <- prepareEval(parent, splits)
              Path(prefixStr) = ctx.basePath
              f1 = concatString.applyl(CString(prefixStr.replaceAll("([^/])$", "$1/")))
              trans2 = trans.Map1(trans.DerefObjectStatic(pendingTable.trans, paths.Value), f1)
              loaded = pendingTable.table
                .transform(trans2)
                .load(jtpe)
                .fold(
                  {
                    case ResourceError.NotFound(message)         => report.warn((), message) >> Table.empty.point[IO]
                    case ResourceError.PermissionsError(message) => report.warn((), message) >> Table.empty.point[IO]
                    case fatal                                   => report.error((), "Fatal error while loading dataset") >> report.die() >> Table.empty.point[IO]
                  },
                  table => table.point[IO]
                )
              back <- transState liftM (loaded).join
            } yield PendingTable(back, graph, TransSpec1.Id, IdentityOrder(graph))

          case dag.Morph1(mor, parent) =>
            for {
              pendingTable <- prepareEval(parent, splits)
              back <- transState liftM (mor(pendingTable.table.transform(liftToValues(pendingTable.trans))))
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

            val joined: StateT[IO, EvaluatorState, (Morph1Apply, PendingTable)] = mor.alignment match {
              case MorphismAlignment.Cross(morph1) =>
                ((transState liftM (morph1)) |@| cross(graph, left, right, None)(spec)).tupled

              case MorphismAlignment.Match(morph1) if areJoinable(left, right) =>
                ((transState liftM (morph1)) |@| join(graph, left, right, IdentitySort)(spec)).tupled

              // TODO: Remove and see if things break. Also,
              case MorphismAlignment.Match(morph1) =>
                val hint =
                  if (left.isSingleton || !right.isSingleton) CrossOrder.CrossRight
                  else CrossOrder.CrossLeft
                ((transState liftM (morph1)) |@| cross(graph, left, right, Some(hint))(spec)).tupled

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

                    transState liftM (f(leftTable, rightTable) map {
                      case (table, morph1) =>
                        (morph1, PendingTable(table, graph, TransSpec1.Id, sort(idPolicy)))
                    })
                }
            }

            joined flatMap {
              case (morph1, PendingTable(joinedTable, _, _, sort)) =>
                transState liftM (morph1(joinedTable)) map { table =>
                  PendingTable(table, graph, TransSpec1.Id, findMorphOrder(mor.idPolicy, sort))
                }
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

              result = (
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
              result <- transState liftM (red(pendingTable.table.transform(DerefObjectStatic(liftedTrans, paths.Value))))
              wrapped = result transform buildConstantWrapSpec(Leaf(Source))
            } yield PendingTable(wrapped, graph, TransSpec1.Id, IdentityOrder(graph))

          case IUI(union, left, right) =>
            // TODO: Get rid of ValueSorts.
            for {
              pair <- zip(prepareEval(left, splits), prepareEval(right, splits))
              (leftPending, rightPending) = pair

              keyValueSpec = TransSpec1.PruneToKeyValue

              leftTable = leftPending.table.transform(liftToValues(leftPending.trans))
              rightTable = rightPending.table.transform(liftToValues(rightPending.trans))

              leftSortedM = transState liftM (leftTable.sort(keyValueSpec, SortAscending))
              rightSortedM = transState liftM (rightTable.sort(keyValueSpec, SortAscending))

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

          case j @ Join(op, Cross(hint), left, right) =>
            cross(graph, left, right, hint)(transFromBinOp(op))

          case _ => sys.error("not implemented for testing")
        }

        val back    = memoized(graph, evalTransSpecable)
        val endTime = System.nanoTime

        val timingM = transState liftM report.timing((), endTime - startTime)

        timingM >> back
      }

      /**
        * The base eval function.  Takes a (rewritten) graph and evaluates the forcing
        * points at the current Split level in topological order.  The endpoint of the
        * graph is considered to be a special forcing point, but as it is the endpoint,
        * it will perforce be evaluated last.
        */
      def fullEval(graph: DepGraph, splits: Map[Identifier, Int => IO[Table]], parentSplits: List[Identifier]): StateT[IO, EvaluatorState, Table] = {
        type EvaluatorStateT[A] = StateT[IO, EvaluatorState, A]

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

      val resultState: StateT[IO, EvaluatorState, Table] = fullEval(rewrittenDAG, Map(), Nil)

      val resultTable: IO[Table] = resultState.eval(EvaluatorState())
      resultTable map { _ paged Config.maxSliceSize compact DerefObjectStatic(Leaf(Source), paths.Value) }
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
          List((join, Const(result)))
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

    private def areJoinable(left: DepGraph, right: DepGraph): Boolean =
      IdentityMatch(left, right).sharedIndices.size > 0

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

    private def zip[A](table1: StateT[IO, EvaluatorState, A], table2: StateT[IO, EvaluatorState, A]): StateT[IO, EvaluatorState, (A, A)] =
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
