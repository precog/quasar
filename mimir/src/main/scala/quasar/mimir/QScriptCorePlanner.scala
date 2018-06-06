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

import slamdata.Predef._

import quasar._
import quasar.common._
import quasar.contrib.scalaz._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.numeric._
import quasar.fp.ski.κ
import quasar.precog.common.{CNumericValue, ColumnRef, CPath, CPathField, CPathIndex}
import quasar.mimir.MimirCake._
import quasar.qscript._, MapFuncCore._
import quasar.yggdrasil.TableModule
import quasar.yggdrasil.bytecode.{JArrayFixedT, JType}

import cats.effect.IO
// import fs2.interop.scalaz._
import matryoshka.{Hole => _, _}
import matryoshka.implicits._
import matryoshka.data._
import matryoshka.patterns._
import scalaz._, Scalaz._
import shims._

import scala.collection.immutable.{Map => ScalaMap}

final class QScriptCorePlanner[T[_[_]]: BirecursiveT: EqualT: ShowT, F[_]: Monad](
    liftF: IO ~> F, liftFCake: CakeM ~> F) {

  def mapFuncPlanner[G[_]: Monad] = MapFuncPlanner[T, G, MapFunc[T, ?]]

  def plan(planQST: AlgebraM[F, QScriptTotal[T, ?], MimirRepr])
      : AlgebraM[F, QScriptCore[T, ?], MimirRepr] = {

    case qscript.Map(src, f) =>
      import src.P.trans._
      for {
        trans <- interpretMapFunc[T, F](src.P, mapFuncPlanner[F])(f.linearize)
        newSort = for {
          lastSort <- src.lastSort
          newBucket = lastSort.bucket.flatMap(TransSpec.rephrase(_, Source, trans))
          newOrderings <- lastSort.orderings.traverse { ord =>
            val rephrasedSortKeys = ord.sortKeys.flatMap(TransSpec.rephrase(_, Source, trans))
            // can't guarantee uniqueness is preserved by all functions,
            // but maybe it's worth keeping track of that
            if (rephrasedSortKeys.isEmpty) None
            else Some(SortOrdering(rephrasedSortKeys, ord.sortOrder, unique = false))
          }
        } yield SortState(newBucket, newOrderings)
      } yield MimirRepr.withSort(src.P)(src.table.transform(trans))(newSort)

    // special-case for distinct (TODO this should be a new node in qscript)
    case qscript.Reduce(src, bucket :: Nil, ReduceFuncs.Arbitrary(arb) :: Nil, repair) if bucket === arb =>
      import src.P.trans._

      for {
        bucketTrans <- interpretMapFunc[T, F](src.P, mapFuncPlanner[F])(bucket)

        distinctedUnforced <- liftF.apply(sortT[src.P.type](MimirRepr.single[src.P](src))(
          src.table,
          bucketTrans,
          unique = true))

        distincted = src.unsafeMerge(distinctedUnforced)

        repairTrans <- repair.cataM[F, TransSpec1](
          interpretM[F, MapFunc[T, ?], ReduceIndex, TransSpec1](
            {
              case ReduceIndex(-\/(0) | \/-(0)) => bucketTrans.point[F]
              case _ => sys.error("should be impossible")
            },
            mapFuncPlanner[F].plan(src.P)[Source1](TransSpec1.Id)))

        repaired = distincted.table.transform(repairTrans)
      } yield MimirRepr(src.P)(repaired)

    case qscript.Reduce(src, buckets, reducers, repair) =>
      import src.P.trans._
      import src.P.Library

      def extractReduction(red: ReduceFunc[FreeMap[T]]): (Library.Reduction, FreeMap[T]) = red match {
        case ReduceFuncs.Count(f) => (Library.Count, f)
        case ReduceFuncs.Sum(f) => (Library.Sum, f)
        case ReduceFuncs.Min(f) => (Library.Min, f)
        case ReduceFuncs.Max(f) => (Library.Max, f)
        case ReduceFuncs.Avg(f) => (Library.Mean, f)
        case ReduceFuncs.Arbitrary(f) => (Library.First, f)   // first is the most efficient for Table
        case ReduceFuncs.First(f) => (Library.First, f)
        case ReduceFuncs.Last(f) => (Library.Last, f)
        case ReduceFuncs.UnshiftArray(f) => (Library.UnshiftArray, f)
        case ReduceFuncs.UnshiftMap(f1, f2) => ???
      }

      val pairs: List[(Library.Reduction, FreeMap[T])] =
        reducers.map(extractReduction)

      // we add First so we can pull out the group key
      def reductions(bucketed: Boolean): List[Library.Reduction] = {
        if (bucketed)
          Library.First :: pairs.map(_._1)
        else
          pairs.map(_._1)
      }

      // note that this means that funcs will NOT align with reductions!
      val funcs: List[FreeMap[T]] = pairs.map(_._2)

      def makeJArray(idx: Int)(tpe: JType): JType =
        JArrayFixedT(ScalaMap[Int, JType]((idx, tpe)))

      def derefArray(idx: Int)(ref: ColumnRef): Option[ColumnRef] =
        ref.selector.dropPrefix(CPath.Identity \ idx).map(ColumnRef(_, ref.ctype))

      def megaReduction(bucketed: Boolean): Library.Reduction = {
        Library.coalesce(reductions(bucketed).zipWithIndex map {
          case (r, i) => (r, Some((makeJArray(i)(_), derefArray(i)(_))))
        })
      }

      // mimir reverses the order of the returned results
      def remapIndex(bucketed: Boolean): ScalaMap[Int, Int] =
        (0 until (reducers.length + (if (bucketed) 1 else 0))).reverse.zipWithIndex.toMap

      for {
        specs <- funcs.traverse(interpretMapFunc[T, F](src.P, mapFuncPlanner[F])(_))

        adjustedSpecs = { bucketed: Boolean =>
          if (bucketed) {
            specs map { spec =>
              TransSpec.deepMap(spec) {
                case Leaf(source) =>
                  DerefObjectStatic(Leaf(source), CPathField("1"))
              }
            }
          } else {
            specs
          }
        }

        // add back in the group key reduction (corresponds with the First we add above)
        megaSpec = { bucketed: Boolean =>
          combineTransSpecs(src.P)(DerefObjectStatic(Leaf(Source), CPathField("0")) :: adjustedSpecs(bucketed))
        }

        table <- {
          def reduceAll(bucketed: Boolean)(table: src.P.Table): IO[src.P.Table] = {
            for {
              // ok this isn't working right now because we throw away the key when we reduce; need to fold in a First reducer to carry along the key
              red <- megaReduction(bucketed)(table.transform(megaSpec(bucketed)))

              trans <- repair.cataM[IO, TransSpec1](
                // note that .0 is the partition key
                // and .1 is the value (if bucketed)
                // if we aren't bucketed, everything is unwrapped
                // these are effectively implementation details of partitionMerge
                interpretM[IO, MapFunc[T, ?], ReduceIndex, TransSpec1](
                  {
                    case ReduceIndex(\/-(idx)) =>
                      // we don't add First if we aren't bucketed
                      remapIndex(bucketed).get(idx + (if (bucketed) 1 else 0)) match {
                        case Some(i) =>
                          (DerefArrayStatic(
                            Leaf(Source),
                            CPathIndex(i)): TransSpec1).point[IO]

                        case None => ???
                      }

                    case ReduceIndex(-\/(idx)) =>
                      // note that an undefined bucket will still retain indexing as long as we don't compact the slice
                      IO(scala.Predef.assert(bucketed, s"bucketed was false in a ReduceIndex(-\\/($idx))")) >>
                        (DerefArrayStatic(
                          DerefArrayStatic(
                            Leaf(Source),
                            CPathIndex(remapIndex(bucketed)(0))),
                          CPathIndex(idx)): TransSpec1).point[IO]
                  },
                  mapFuncPlanner[IO].plan(src.P)[Source1](TransSpec1.Id)))
            } yield red.transform(trans)
          }

          if (buckets.isEmpty) {
            liftF(reduceAll(false)(src.table))
          } else {
            for {
              bucketTranses <- buckets.traverse(interpretMapFunc[T, F](src.P, mapFuncPlanner[F]))
              bucketTrans = combineTransSpecs(src.P)(bucketTranses)

              prepared <- liftF(sortT[src.P.type](MimirRepr.single[src.P](src))(src.table, bucketTrans))
                .map(r => src.unsafeMergeTable(r.table))

              table <- liftF(prepared.partitionMerge(bucketTrans, keepKey = true)(reduceAll(true)))
            } yield table
          }
        }
      } yield MimirRepr(src.P)(table)

    case qscript.LeftShift(src, struct, idStatus, shiftType, onUndef, repair) =>
      import src.P.trans._

      val source = "source"
      val focus = "focus"

      for {

        structTrans <- interpretMapFunc[T, F](src.P, mapFuncPlanner[F])(struct.linearize)
        wrappedStructTrans =
          OuterObjectConcat(WrapObject(TransSpec1.Id, source), WrapObject(structTrans, focus))

        repairTrans <- repair.cataM[F, TransSpec1](
          interpretM[F, MapFunc[T, ?], JoinSide, TransSpec1](
            {
              case qscript.LeftSide =>
                (DerefObjectStatic(TransSpec1.Id, CPathField(source)): TransSpec1).point[F]

              case qscript.RightSide =>
                val target = DerefObjectStatic(TransSpec1.Id, CPathField(focus))

                val back: TransSpec1 = idStatus match {
                  case IdOnly => DerefArrayStatic(target, CPathIndex(0))
                  case IncludeId => target
                  case ExcludeId => DerefArrayStatic(target, CPathIndex(1))
                }

                back.point[F]
            },
            mapFuncPlanner[F].plan(src.P)[Source1](TransSpec1.Id)))

        emit = onUndef === OnUndefined.Emit
        shifted = src.table.transform(wrappedStructTrans).leftShift(CPath.Identity \ focus, emit)
        repaired = shifted.transform(repairTrans)
      } yield MimirRepr(src.P)(repaired)

    case qscript.Sort(src, buckets, orders) =>
      import src.P.trans._
      import TableModule.DesiredSortOrder

      for {
        transDirs <- orders.toList traverse {
          case (fm, dir) =>
            val order = dir match {
              case SortDir.Ascending => TableModule.SortAscending
              case SortDir.Descending => TableModule.SortDescending
            }

            interpretMapFunc[T, F](src.P, mapFuncPlanner[F])(fm).map(ts => (ts, order))
        }

        pair = transDirs.foldLeft((Vector.empty[(Vector[TransSpec1], DesiredSortOrder)], None: Option[DesiredSortOrder])) {
          case ((acc, None), (ts, order)) =>
            (acc :+ ((Vector(ts), order)), Some(order))

          case ((acc, Some(ord1)), (ts, ord2)) if ord1 == ord2 =>
            val idx = acc.length - 1
            (acc.updated(idx, (acc(idx)._1 :+ ts, ord1)), Some(ord1))

          case ((acc, Some(ord1)), (ts, ord2)) =>
            (acc :+ ((Vector(ts), ord2)), Some(ord2))
        }

        (sorts, _) = pair

        tableAndSort <- {
          val sortOrderings = sorts.foldRight(List.empty[SortOrdering[TransSpec1]]) {
            case ((transes, sortOrder), a) =>
              val sortKey = OuterArrayConcat(transes.map(WrapArray(_)): _*)
              SortOrdering[TransSpec1](Set(sortKey), sortOrder, unique = false) :: a
          }

          def sortAll(table: src.P.Table): IO[src.P.Table] = {
            sortOrderings.foldRightM(table) {
              case (ordering, table) =>
                ordering.sort(src.P)(table)
            }
          }

          for {
            bucketTranses <-
              buckets.traverse(interpretMapFunc[T, F](src.P, mapFuncPlanner[F]))

            bucketTrans = combineTransSpecs(src.P)(bucketTranses)

            newSort = SortState(
              Some(bucketTrans).filterNot(_ => buckets.isEmpty),
              sortOrderings)

            sortNeeded = src.lastSort.fold(true)(last => needToSort(MimirRepr.single[src.P](src).P)(last, newSort))

            sortedTable <- {
              if (sortNeeded) {
                if (buckets.isEmpty) {
                  liftF(sortAll(src.table))
                } else {
                  for {
                    prepared <- liftF(sortT[src.P.type](MimirRepr.single[src.P](src))(src.table, bucketTrans))
                      .map(r => src.unsafeMergeTable(r.table))
                    table <- liftF(prepared.partitionMerge(bucketTrans)(sortAll))
                  } yield table
                }
              } else {
                src.table.point[F]
              }
            }
          } yield (sortedTable, newSort)
        }
        (table, sort) = tableAndSort
      } yield MimirRepr.withSort(src.P)(table)(Some(sort))

    case qscript.Filter(src, f) =>
      import src.P.trans._

      for {
        trans <- interpretMapFunc[T, F](src.P, mapFuncPlanner[F])(f.linearize)
      } yield MimirRepr.withSort(src.P)(src.table.transform(Filter(TransSpec1.Id, trans)))(src.lastSort)

    case qscript.Union(src, lBranch, rBranch) =>
      for {
       leftRepr <- interpretBranch(planQST)(src, lBranch)
       rightRepr <- interpretBranch(planQST)(src, rBranch)

       rightCoerced = leftRepr.unsafeMerge(rightRepr)
      } yield MimirRepr(leftRepr.P)(leftRepr.table.concat(rightCoerced.table))

    case qscript.Subset(src, from, op, count) =>
      for {
        fromRepr <- interpretBranch(planQST)(src, from)
        countRepr <- interpretBranch(planQST)(src, count)

        back <- {
          def result = for {
            vals <- countRepr.table.toJson
            nums = vals collect { case n: CNumericValue[_] => n.toCNum.value.toInt } // TODO error if we get something strange
            number = nums.head
            compacted = fromRepr.table.compact(fromRepr.P.trans.TransSpec1.Id)
            retainsOrder = op != Sample
            back <- op match {
              case Take =>
                IO.pure(compacted.take(number))

              case Drop =>
                IO.pure(compacted.drop(number))

              case Sample =>
                compacted.sample(number, List(fromRepr.P.trans.TransSpec1.Id)).map(_.head) // the number of Reprs returned equals the number of transspecs
            }
          } yield if (retainsOrder) {
            MimirRepr.withSort(fromRepr.P)(back)(fromRepr.lastSort)
          } else {
            MimirRepr(fromRepr.P)(back)
          }

          liftF.apply(result)
        }
      } yield back

    // FIXME look for Map(Unreferenced, Constant) and return constant table
    case qscript.Unreferenced() =>
      liftFCake(MimirRepr.meld[CakeM, LightweightFileSystem](
        new DepFn1[Cake, λ[`P <: Cake` => CakeM[P#Table]]] {
          def apply(P: Cake): CakeM[P.Table] =
            P.Table.constLong(Set(0)).point[CakeM]
        }))
  }

  ////////

  private def interpretBranch(
    planQST: AlgebraM[F, QScriptTotal[T, ?], MimirRepr])(
    src: MimirRepr, qs: FreeQS[T])
      : F[MimirRepr] =
    qs.cataM[F, MimirRepr](
      interpretM[F, QScriptTotal[T, ?], Hole, MimirRepr](κ(src.point[F]), planQST))
}
