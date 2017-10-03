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

import slamdata.Predef._

import quasar._
import quasar.common._
import quasar.connector._
import quasar.contrib.pathy._
import quasar.contrib.scalaz._, eitherT._
import quasar.fp._
import quasar.fp.numeric._
import quasar.fp.ski.κ
import quasar.fs._
import quasar.fs.mount._
import quasar.qscript._

import quasar.blueeyes.json.{JNum, JValue}
import quasar.precog.common.{ColumnRef, CPath, CPathField, CPathIndex, Path}
import quasar.yggdrasil.TableModule
import quasar.yggdrasil.bytecode.{JArrayFixedT, JType}

import fs2.{async, Stream}
import fs2.async.mutable.{Queue, Signal}
import fs2.interop.scalaz._

import matryoshka._
import matryoshka.implicits._
import matryoshka.data._
import matryoshka.patterns._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

import org.slf4s.Logging

import pathy.Path._

import delorean._

import scala.Predef.implicitly
import scala.collection.immutable.{Map => ScalaMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import quasar.yggdrasil.TableModule.{DesiredSortOrder, SortAscending}

import scalaz.Leibniz.===

object Mimir extends BackendModule with Logging {
  import FileSystemError._
  import PathError._
  import Precog.startTask

  // pessimistically equal to couchbase's
  type QS[T[_[_]]] =
    QScriptCore[T, ?] :\:
    EquiJoin[T, ?] :/:
    Const[ShiftedRead[AFile], ?]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])

  private type Cake = Precog with Singleton

  // EquiJoin results are sorted by both keys at the same time, so we need to keep track of both
  final case class SortOrdering[TS1](sortKeys: Set[TS1], sortOrder: DesiredSortOrder, unique: Boolean) {
    def sort(src: Cake)(table: src.Table)(implicit ev: TS1 === src.trans.TransSpec1): Future[src.Table] =
      if (sortKeys.isEmpty) {
        Future.successful(table)
      } else {
        table.sort(ev(sortKeys.head), sortOrder, unique)
      }
  }

  final case class SortState[TS1](bucket: Option[TS1], orderings: List[SortOrdering[TS1]])

  trait Repr {
    type P <: Cake

    // Scala is afraid to infer this but it must be valid,
    // because Cake <: Singleton
    val sing: Leibniz[⊥, Cake, P, P.type]

    val P: P
    val table: P.Table
    val lastSort: Option[SortState[P.trans.TransSpec1]]

    // these casts can't crash, because `Precog` is final.
    // however they're unsafe because they could lead to sharing cakes
    // between state threads.
    def unsafeMerge(other: Repr): Repr.Aux[Repr.this.P.type] =
      other.asInstanceOf[Repr.Aux[Repr.this.P.type]]

    def unsafeMergeTable(other: Repr#P#Table): Repr.this.P.Table =
      other.asInstanceOf[Repr.this.P.Table]

    // this is totally safe because `Precog` is final *and* `TransSpec` can't reference cake state.
    def mergeTS1(other: or.P.trans.TransSpec1 forSome { val or: Repr }): Repr.this.P.trans.TransSpec1 =
      other.asInstanceOf[Repr.this.P.trans.TransSpec1]

    def map(f: P.Table => P.Table): Repr.Aux[Repr.this.P.type] =
      Repr(P)(f(table))
  }

  object Repr {
    type Aux[P0 <: Cake] = Repr { type P = P0 }

    def apply(P0: Precog)(table0: P0.Table): Repr.Aux[P0.type] =
      new Repr {
        type P = P0.type

        val sing: Leibniz[P, P, P, P] = Leibniz.refl[P]

        val P: P0.type = P0
        val table: P.Table = table0
        val lastSort: Option[SortState[P0.trans.TransSpec1]] = None
      }

    def withSort(P0: Precog)(table0: P0.Table)(lastSort0: Option[SortState[P0.trans.TransSpec1]]): Repr.Aux[P0.type] =
      new Repr {
        type P = P0.type

        val sing: Leibniz[P, P, P, P] = Leibniz.refl[P]

        val P: P0.type = P0
        val table: P.Table = table0
        val lastSort: Option[SortState[P0.trans.TransSpec1]] = lastSort0
      }

    def meld[F[_]: Monad](fn: DepFn1[Cake, λ[`P <: Cake` => F[P#Table]]])(
      implicit
        F: MonadReader_[F, Cake]): F[Repr] =
      F.ask.flatMap(cake => fn(cake).map(table => Repr(cake)(table)))

    // witness that all cakes have a singleton type for P
    def single[P0 <: Cake](src: Repr.Aux[P0]): Repr.Aux[src.P.type] =
      src.sing.subst[Repr.Aux](src)
  }

  private type MT[F[_], A] = Kleisli[F, Cake, A]
  type M[A] = MT[Task, A]

  def cake[F[_]](implicit F: MonadReader_[F, Cake]): F[Cake] = F.ask

  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM = Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  final case class Config(dataDir: java.io.File)

  def optimize[T[_[_]]: BirecursiveT: EqualT: ShowT]
      : QSM[T, T[QSM[T, ?]]] => QSM[T, T[QSM[T, ?]]] = {
    val O = new Optimize[T]
    O.optimize(reflNT[QSM[T, ?]])
  }

  def parseConfig(uri: ConnectionUri): BackendDef.DefErrT[Task, Config] =
    Config(new java.io.File(uri.value)).point[BackendDef.DefErrT[Task, ?]]

  def compile(cfg: Config): BackendDef.DefErrT[Task, (M ~> Task, Task[Unit])] = {
    val t = for {
      cake <- Precog(cfg.dataDir)
    } yield (λ[M ~> Task](_.run(cake)), cake.shutdown.toTask)

    t.liftM[BackendDef.DefErrT]
  }

  def needToSort(p: Precog)(oldSort: SortState[p.trans.TransSpec1], newSort: SortState[p.trans.TransSpec1]): Boolean = {
    (oldSort.orderings.length != newSort.orderings.length || oldSort.bucket != newSort.bucket) || {
      def requiresSort(oldOrdering: SortOrdering[p.trans.TransSpec1], newOrdering: SortOrdering[p.trans.TransSpec1]) = {
        (oldOrdering.sortKeys & newOrdering.sortKeys).isEmpty ||
          (oldOrdering.sortOrder != newOrdering.sortOrder) ||
          (oldOrdering.unique && !newOrdering.unique)
      }

      (oldSort.bucket != newSort.bucket) || {
        val allOrderings = oldSort.orderings.zip(newSort.orderings)
        allOrderings.exists { case (o, n) => requiresSort(o, n) }
      }
    }
  }

  // sort by one dimension
  def sortT[P0 <: Cake](c: Repr.Aux[P0])(
      table: c.P.Table,
      sortKey: c.P.trans.TransSpec1,
      sortOrder: DesiredSortOrder = SortAscending,
      unique: Boolean = false): Task[Repr.Aux[c.P.type]] = {

    val newRepr =
      SortState[c.P.trans.TransSpec1](
        bucket = None,
        orderings = SortOrdering(Set(sortKey), sortOrder, unique) :: Nil)

    if (c.lastSort.fold(true)(needToSort(c.P)(_, newSort = newRepr))) {
      table.sort(sortKey, sortOrder, unique).toTask map { sorted =>
        Repr.withSort(c.P)(sorted)(Some(newRepr))
      }
    } else {
      Task.now(Repr.withSort(c.P)(table)(Some(newRepr)))
    }
  }

  val Type = FileSystemType("mimir")

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      cp: T[QSM[T, ?]]): Backend[Repr] = {

// M = Backend
// F[_] = MapFuncCore[T, ?]
// B = Repr
// A = SrcHole
// AlgebraM[M, CoEnv[A, F, ?], B] = AlgebraM[Backend, CoEnv[Hole, MapFuncCore[T, ?], ?], Repr]
//def interpretM[M[_], F[_], A, B](f: A => M[B], φ: AlgebraM[M, F, B]): AlgebraM[M, CoEnv[A, F, ?], B]
// f.cataM(interpretM)

    def mapFuncPlanner[F[_]: Monad] = MapFuncPlanner[T, F, MapFunc[T, ?]]

    lazy val planQST: AlgebraM[Backend, QScriptTotal[T, ?], Repr] =
      _.run.fold(
        planQScriptCore,
        _.run.fold(
          _ => ???,   // ProjectBucket
          _.run.fold(
            _ => ???,   // ThetaJoin
            _.run.fold(
              planEquiJoin,
              _.run.fold(
                _ => ???,    // ShiftedRead[ADir]
                _.run.fold(
                  planShiftedRead,
                  _.run.fold(
                    _ => ???,   // Read[ADir]
                    _.run.fold(
                      _ => ???,   // Read[AFile]
                      _ => ???))))))))    // DeadEnd

    def interpretMapFunc[F[_]: Monad](P: Precog)(fm: FreeMap[T]): F[P.trans.TransSpec1] =
      fm.cataM[F, P.trans.TransSpec1](
        interpretM(
          κ(P.trans.TransSpec1.Id.point[F]),
          mapFuncPlanner[F].plan(P)[P.trans.Source1](P.trans.TransSpec1.Id)))

    def combineTransSpecs(P: Precog)(specs: Seq[P.trans.TransSpec1]): P.trans.TransSpec1 = {
      import P.trans._

      if (specs.isEmpty)
        TransSpec1.Id
      else if (specs.lengthCompare(1) == 0)
        specs.head
      else
        OuterArrayConcat(specs.map(WrapArray(_): TransSpec1) : _*)
    }

    lazy val planQScriptCore: AlgebraM[Backend, QScriptCore[T, ?], Repr] = {
      case qscript.Map(src, f) =>
        import src.P.trans._
        for {
          trans <- interpretMapFunc[Backend](src.P)(f)
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
        } yield Repr.withSort(src.P)(src.table.transform(trans))(newSort)

      // special-case for distinct (TODO this should be a new node in qscript)
      case qscript.Reduce(src, bucket :: Nil, ReduceFuncs.Arbitrary(arb) :: Nil, repair) if bucket === arb =>
        import src.P.trans._

        for {
          bucketTrans <- interpretMapFunc[Backend](src.P)(bucket)

          distinctedUnforced <- sortT[src.P.type](Repr.single[src.P](src))(
            src.table,
            bucketTrans,
            unique = true).liftM[MT].liftB

          distincted = src.unsafeMerge(distinctedUnforced)

          repairTrans <- repair.cataM[Backend, TransSpec1](
            interpretM(
              {
                case ReduceIndex(-\/(0) | \/-(0)) => bucketTrans.point[Backend]
                case _ => sys.error("should be impossible")
              },
              mapFuncPlanner[Backend].plan(src.P)[Source1](TransSpec1.Id)))

          repaired = distincted.table.transform(repairTrans)
        } yield Repr(src.P)(repaired)

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
          JArrayFixedT(ScalaMap(idx -> tpe))

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
          specs <- funcs.traverse(interpretMapFunc[Backend](src.P)(_))

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
            def reduceAll(bucketed: Boolean)(table: src.P.Table): Future[src.P.Table] = {
              for {
                // ok this isn't working right now because we throw away the key when we reduce; need to fold in a First reducer to carry along the key
                red <- megaReduction(bucketed)(table.transform(megaSpec(bucketed)))
                trans <- repair.cataM[Future, TransSpec1](
                  // note that .0 is the partition key
                  // and .1 is the value (if bucketed)
                  // if we aren't bucketed, everything is unwrapped
                  // these are effectively implementation details of partitionMerge
                  interpretM(
                    {
                      case ReduceIndex(\/-(idx)) =>
                        // we don't add First if we aren't bucketed
                        remapIndex(bucketed).get(idx + (if (bucketed) 1 else 0)) match {
                          case Some(i) =>
                            (DerefArrayStatic(
                              Leaf(Source),
                              CPathIndex(i)): TransSpec1).point[Future]

                          case None => ???
                        }

                      case ReduceIndex(-\/(idx)) =>
                        // note that an undefined bucket will still retain indexing as long as we don't compact the slice
                        Future(scala.Predef.assert(bucketed, s"bucketed was false in a ReduceIndex(-\\/($idx))")) >>
                          (DerefArrayStatic(
                            DerefArrayStatic(
                              Leaf(Source),
                              CPathIndex(remapIndex(bucketed)(0))),
                            CPathIndex(idx)): TransSpec1).point[Future]
                    },
                    mapFuncPlanner[Future].plan(src.P)[Source1](TransSpec1.Id)))
              } yield red.transform(trans)
            }

            if (buckets.isEmpty) {
              reduceAll(false)(src.table).toTask.liftM[MT].liftB
            } else {
              for {
                bucketTranses <- buckets.traverse(interpretMapFunc[Backend](src.P))
                bucketTrans = combineTransSpecs(src.P)(bucketTranses)

                prepared <- sortT[src.P.type](Repr.single[src.P](src))(src.table, bucketTrans)
                  .liftM[MT].liftB.map(r => src.unsafeMergeTable(r.table))

                table <- prepared.partitionMerge(bucketTrans, keepKey = true)(reduceAll(true)).toTask.liftM[MT].liftB
              } yield table
            }
          }
        } yield Repr(src.P)(table)

      case qscript.LeftShift(src, struct, idStatus, repair) =>
        import src.P.trans._

        for {
          structTrans <- interpretMapFunc[Backend](src.P)(struct)
          wrappedStructTrans = InnerArrayConcat(WrapArray(TransSpec1.Id), WrapArray(structTrans))

          repairTrans <- repair.cataM[Backend, TransSpec1](
            interpretM(
              {
                case qscript.LeftSide =>
                  (DerefArrayStatic(TransSpec1.Id, CPathIndex(0)): TransSpec1).point[Backend]

                case qscript.RightSide =>
                  val target = DerefArrayStatic(TransSpec1.Id, CPathIndex(1))

                  val back: TransSpec1 = idStatus match {
                    case IdOnly => DerefArrayStatic(target, CPathIndex(0))
                    case IncludeId => target
                    case ExcludeId => DerefArrayStatic(target, CPathIndex(1))
                  }

                  back.point[Backend]
              },
              mapFuncPlanner[Backend].plan(src.P)[Source1](TransSpec1.Id)))

          shifted = src.table.transform(wrappedStructTrans).leftShift(CPath.Identity \ 1)
          repaired = shifted.transform(repairTrans)
        } yield Repr(src.P)(repaired)

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

              interpretMapFunc[Backend](src.P)(fm).map(ts => (ts, order))
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

            def sortAll(table: src.P.Table): Future[src.P.Table] = {
              sortOrderings.foldRightM(table) {
                case (ordering, table) =>
                  ordering.sort(src.P)(table)
              }
            }

            for {
              bucketTranses <-
                buckets.traverse(interpretMapFunc[Backend](src.P))

              bucketTrans = combineTransSpecs(src.P)(bucketTranses)

              newSort = SortState(
                Some(bucketTrans).filterNot(_ => buckets.isEmpty),
                sortOrderings)

              sortNeeded = src.lastSort.fold(true)(last => needToSort(Repr.single[src.P](src).P)(last, newSort))

              sortedTable <- {
                if (sortNeeded) {
                  if (buckets.isEmpty) {
                    sortAll(src.table).toTask.liftM[MT].liftB
                  } else {
                    for {
                      prepared <- sortT[src.P.type](Repr.single[src.P](src))(src.table, bucketTrans)
                        .liftM[MT].liftB.map(r => src.unsafeMergeTable(r.table))
                      table <- prepared.partitionMerge(bucketTrans)(sortAll).toTask.liftM[MT].liftB
                    } yield table
                  }
                } else {
                  src.table.point[Backend]
                }
              }
            } yield (sortedTable, newSort)
          }
          (table, sort) = tableAndSort
        } yield Repr.withSort(src.P)(table)(Some(sort))

      case qscript.Filter(src, f) =>
        import src.P.trans._

        for {
          trans <- interpretMapFunc[Backend](src.P)(f)
        } yield Repr.withSort(src.P)(src.table.transform(Filter(TransSpec1.Id, trans)))(src.lastSort)

      case qscript.Union(src, lBranch, rBranch) =>
        for {
         leftRepr <- lBranch.cataM(interpretM(κ(src.point[Backend]), planQST))
         rightRepr <- rBranch.cataM(interpretM(κ(src.point[Backend]), planQST))
         rightCoerced = leftRepr.unsafeMerge(rightRepr)
        } yield Repr(leftRepr.P)(leftRepr.table.concat(rightCoerced.table))

      case qscript.Subset(src, from, op, count) =>
        for {
          fromRepr <- from.cataM(interpretM(κ(src.point[Backend]), planQST))
          countRepr <- count.cataM(interpretM(κ(src.point[Backend]), planQST))
          back <- {
            def result = for {
              vals <- countRepr.table.toJson
              nums = vals collect { case n: JNum => n.toLong.toInt } // TODO error if we get something strange
              number = nums.head
              compacted = fromRepr.table.compact(fromRepr.P.trans.TransSpec1.Id)
              retainsOrder = op != Sample
              back <- op match {
                case Take =>
                  Future.successful(compacted.take(number))

                case Drop =>
                  Future.successful(compacted.drop(number))

                case Sample =>
                  compacted.sample(number, List(fromRepr.P.trans.TransSpec1.Id)).map(_.head) // the number of Reprs returned equals the number of transspecs
              }
            } yield if (retainsOrder) {
              Repr.withSort(fromRepr.P)(back)(fromRepr.lastSort)
            } else {
              Repr(fromRepr.P)(back)
            }

            result.toTask.liftM[MT].liftB
          }
        } yield back

      // FIXME look for Map(Unreferenced, Constant) and return constant table
      case qscript.Unreferenced() =>
        Repr.meld[M](new DepFn1[Cake, λ[`P <: Cake` => M[P#Table]]] {
          def apply(P: Cake): M[P.Table] =
            P.Table.constLong(Set(0)).point[M]
        }).liftB
    }

    lazy val planEquiJoin: AlgebraM[Backend, EquiJoin[T, ?], Repr] = {
      case qscript.EquiJoin(src, lbranch, rbranch, keys, tpe, combine) =>
        import src.P.trans._, scalaz.syntax.std.option._

        def rephrase2(projection: TransSpec2, rootL: TransSpec1, rootR: TransSpec1): Option[SortOrdering[TransSpec1]] = {
          val leftRephrase = TransSpec.rephrase(projection, SourceLeft, rootL).fold(Set.empty[TransSpec1])(Set(_))
          val rightRephrase = TransSpec.rephrase(projection, SourceRight, rootR).fold(Set.empty[TransSpec1])(Set(_))
          val bothRephrased = leftRephrase ++ rightRephrase

          if (bothRephrased.isEmpty)
            None
          else
            SortOrdering(bothRephrased, SortAscending, unique = false).some
        }

        val (lkeys, rkeys) = keys.unfzip

        for {
          leftRepr <- lbranch.cataM(interpretM(κ(src.point[Backend]), planQST))
          rightRepr <- rbranch.cataM(interpretM(κ(src.point[Backend]), planQST))

          lmerged = src.unsafeMerge(leftRepr)
          ltable = lmerged.table

          rmerged = src.unsafeMerge(rightRepr)
          rtable = rmerged.table

          transLKeys <- lkeys traverse interpretMapFunc[Backend](src.P)
          transLKey = combineTransSpecs(src.P)(transLKeys)
          transRKeys <- rkeys traverse interpretMapFunc[Backend](src.P)
          transRKey = combineTransSpecs(src.P)(transRKeys)

          transMiddle <- combine.cataM[Backend, TransSpec2](
            interpretM(
              {
                case qscript.LeftSide => TransSpec2.LeftId.point[Backend]
                case qscript.RightSide => TransSpec2.RightId.point[Backend]
              },
              mapFuncPlanner[Backend].plan(src.P)[Source2](TransSpec2.LeftId)
            )
          ) // TODO weirdly left-biases things like constants

          // identify full-cross and avoid cogroup
          resultAndSort <- {
            if (keys.isEmpty) {
              log.trace("EQUIJOIN: full-cross detected!")

              (ltable.cross(rtable)(transMiddle), src.unsafeMerge(leftRepr).lastSort).point[Backend]
            } else {
              log.trace("EQUIJOIN: not a full-cross; sorting and cogrouping")

              for {
                lsorted <- sortT[leftRepr.P.type](Repr.single[leftRepr.P](leftRepr))(leftRepr.table, leftRepr.mergeTS1(transLKey))
                  .liftM[MT].liftB.map(r => src.unsafeMergeTable(r.table))
                rsorted <- sortT[rightRepr.P.type](Repr.single[rightRepr.P](rightRepr))(rightRepr.table, rightRepr.mergeTS1(transRKey))
                  .liftM[MT].liftB.map(r => src.unsafeMergeTable(r.table))

                transLeft <- tpe match {
                  case JoinType.LeftOuter | JoinType.FullOuter =>
                    combine.cataM[Backend, TransSpec1](
                      interpretM(
                        {
                          case qscript.LeftSide => TransSpec1.Id.point[Backend]
                          case qscript.RightSide => TransSpec1.Undef.point[Backend]
                        },
                        mapFuncPlanner[Backend].plan(src.P)[Source1](TransSpec1.Id)
                      )
                    )

                  case JoinType.Inner | JoinType.RightOuter =>
                    TransSpec1.Undef.point[Backend]
                }

                transRight <- tpe match {
                  case JoinType.RightOuter | JoinType.FullOuter =>
                    combine.cataM[Backend, TransSpec1](
                      interpretM(
                        {
                          case qscript.LeftSide => TransSpec1.Undef.point[Backend]
                          case qscript.RightSide => TransSpec1.Id.point[Backend]
                        },
                        mapFuncPlanner[Backend].plan(src.P)[Source1](TransSpec1.Id)
                      )
                    )

                  case JoinType.Inner | JoinType.LeftOuter =>
                    TransSpec1.Undef.point[Backend]
                }
                newSortOrder = rephrase2(transMiddle, transLKey, transRKey)
              } yield (lsorted.cogroup(transLKey, transRKey, rsorted)(transLeft, transRight, transMiddle),
                  newSortOrder.map(order => SortState(None, order :: Nil)))
            }
          }

          (result, newSort) = resultAndSort
        } yield Repr.withSort(src.P)(result)(newSort)
    }

    lazy val planShiftedRead: AlgebraM[Backend, Const[ShiftedRead[AFile], ?], Repr] = {
      case Const(ShiftedRead(path, status)) => {
        val pathStr: String = pathy.Path.posixCodec.printPath(path)

        val loaded: EitherT[M, FileSystemError, Repr] =
          for {
            precog <- cake[EitherT[M, FileSystemError, ?]]
            apiKey <- precog.RootAPIKey.toTask.liftM[MT].liftM[EitherT[?[_], FileSystemError, ?]]

            repr <-
              Repr.meld[EitherT[M, FileSystemError, ?]](
                new DepFn1[Cake, λ[`P <: Cake` => EitherT[M, FileSystemError, P#Table]]] {
                  def apply(P: Cake): EitherT[M, FileSystemError, P.Table] = {
                    val et =
                      P.Table.constString(Set(pathStr)).load(apiKey, JType.JUniverseT).mapT(_.toTask)

                    et.mapT(_.liftM[MT]) leftMap { err =>
                      val msg = err.messages.toList.reduce(_ + ";" + _)
                      readFailed(posixCodec.printPath(path), msg)
                    }
                  }
                })
          } yield {
            import repr.P.trans._

            status match {
              case IdOnly =>
                repr.map(_.transform(constants.SourceKey.Single))

              case IncludeId =>
                val ids = constants.SourceKey.Single
                val values = constants.SourceValue.Single

                // note that ids are already an array
                repr.map(_.transform(InnerArrayConcat(ids, WrapArray(values))))

              case ExcludeId =>
                repr.map(_.transform(constants.SourceValue.Single))
            }
          }

        val result: FileSystemErrT[M, ?] ~> Backend =
          Hoist[FileSystemErrT].hoist[M, PhaseResultT[Configured, ?]](
            λ[Configured ~> PhaseResultT[Configured, ?]](_.liftM[PhaseResultT])
              compose λ[M ~> Configured](_.liftM[ConfiguredT]))

        result(loaded)
      }
    }

    def planQSM(in: QSM[T, Repr]): Backend[Repr] =
      in.run.fold(planQScriptCore, _.run.fold(planEquiJoin, planShiftedRead))

    cp.cataM(planQSM _)
  }

  private def dirToPath(dir: ADir): Path = Path(pathy.Path.posixCodec.printPath(dir))
  private def fileToPath(file: AFile): Path = Path(pathy.Path.posixCodec.printPath(file))

  object QueryFileModule extends QueryFileModule {
    import QueryFile._

    private val map = new ConcurrentHashMap[ResultHandle, Precog#TablePager]
    private val cur = new AtomicLong(0L)

    def executePlan(repr: Repr, out: AFile): Backend[Unit] = {
      val path = fileToPath(out)

      // TODO it's kind of ugly that we have to page through JValue to get back into NIHDB
      val driver = for {
        q <- async.boundedQueue[Task, Vector[JValue]](1)

        populator = repr.table.slices.trans(λ[Future ~> Task](_.toTask)) foreachRec { slice =>
          if (!slice.isEmpty) {
            val json = slice.toJsonElements
            if (!json.isEmpty)
              q.enqueue1(json)
            else
              Task.now(())
          } else {
            Task.now(())
          }
        }

        populatorWithTermination = populator >> q.enqueue1(Vector.empty)

        ingestor = repr.P.ingest(path, q.dequeue.takeWhile(_.nonEmpty).flatMap(Stream.emits)).run

        // generally this function is bad news (TODO provide a way to ingest as a Stream)
        _ <- Task.gatherUnordered(Seq(populatorWithTermination, ingestor))
      } yield ()

      driver.liftM[MT].liftB
    }

    def evaluatePlan(repr: Repr): Backend[ResultHandle] = {
      val t = for {
        handle <- Task.delay(ResultHandle(cur.getAndIncrement()))
        pager <- repr.P.TablePager(repr.table)
        _ <- Task.delay(map.put(handle, pager))
      } yield handle

      t.liftM[MT].liftB
    }

    def more(h: ResultHandle): Backend[Vector[Data]] = {
      val t = for {
        pager <- Task.delay(Option(map.get(h)).get)
        chunk <- pager.more
      } yield chunk

      t.liftM[MT].liftB
    }

    def close(h: ResultHandle): Configured[Unit] = {
      val t = for {
        pager <- Task.delay(Option(map.get(h)).get)
        check <- Task.delay(map.remove(h, pager))
        _ <- if (check) pager.close else Task.now(())
      } yield ()

      t.liftM[MT].liftM[ConfiguredT]
    }

    def explain(repr: Repr): Backend[String] = "🤹".point[Backend]

    def listContents(dir: ADir): Backend[Set[PathSegment]] = {
      for {
        precog <- cake[Backend]

        exists <- precog.fs.exists(dir).liftM[MT].liftB

        _ <- if (exists)
          ().point[Backend]
        else
          MonadError_[Backend, FileSystemError].raiseError(pathErr(pathNotFound(dir)))

        back <- precog.fs.listContents(dir).liftM[MT].liftB
      } yield back
    }

    def fileExists(file: AFile): Configured[Boolean] =
      cake[M].flatMap(_.fs.exists(file).liftM[MT]).liftM[ConfiguredT]
  }

  object ReadFileModule extends ReadFileModule {
    import ReadFile._

    private val map = new ConcurrentHashMap[ReadHandle, Precog#TablePager]
    private val cur = new AtomicLong(0L)

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] = {
      for {
        precog <- cake[Backend]
        handle <- Task.delay(ReadHandle(file, cur.getAndIncrement())).liftM[MT].liftB

        target = precog.Table.constString(Set(posixCodec.printPath(file)))

        // apparently read on a non-existent file is equivalent to reading the empty file??!!
        eitherTable <- precog.Table.load(target, JType.JUniverseT).mapT(_.toTask).run.liftM[MT].liftB
        table = eitherTable.fold(_ => precog.Table.empty, table => table)

        limited = if (offset.value === 0L && !limit.isDefined)
          table
        else
          table.takeRange(offset.value, limit.fold(slamdata.Predef.Int.MaxValue.toLong)(_.value))

        projected = limited.transform(precog.trans.constants.SourceValue.Single)

        pager <- precog.TablePager(projected).liftM[MT].liftB
        _ <- Task.delay(map.put(handle, pager)).liftM[MT].liftB
      } yield handle
    }

    def read(h: ReadHandle): Backend[Vector[Data]] = {
      for {
        maybePager <- Task.delay(Option(map.get(h))).liftM[MT].liftB

        pager <- maybePager match {
          case Some(pager) =>
            pager.point[Backend]

          case None =>
            MonadError_[Backend, FileSystemError].raiseError(unknownReadHandle(h))
        }

        chunk <- pager.more.liftM[MT].liftB
      } yield chunk
    }

    def close(h: ReadHandle): Configured[Unit] = {
      val t = for {
        pager <- Task.delay(Option(map.get(h)).get)
        check <- Task.delay(map.remove(h, pager))
        _ <- if (check) pager.close else Task.now(())
      } yield ()

      t.liftM[MT].liftM[ConfiguredT]
    }
  }

  object WriteFileModule extends WriteFileModule {
    import WriteFile._

    // we set this to 1 because we don't want the table evaluation "running ahead" of
    // quasar's paging logic.  See also: TablePager.apply
    private val QueueLimit = 1

    private val map: ConcurrentHashMap[WriteHandle, (Queue[Task, Vector[Data]], Signal[Task, Boolean])] =
      new ConcurrentHashMap

    private val cur = new AtomicLong(0L)

    def open(file: AFile): Backend[WriteHandle] = {
      val run: Task[M[WriteHandle]] = Task delay {
        log.debug(s"open file $file")

        val id = cur.getAndIncrement()
        val handle = WriteHandle(file, id)

        for {
          queue <- Queue.bounded[Task, Vector[Data]](QueueLimit).liftM[MT]
          signal <- fs2.async.signalOf[Task, Boolean](false).liftM[MT]

          path = fileToPath(file)
          jvs = queue.dequeue.takeWhile(_.nonEmpty).flatMap(Stream.emits).map(JValue.fromData)

          precog <- cake[M]

          ingestion = for {
            _ <- precog.ingest(path, jvs).run   // TODO log resource errors?
            _ <- signal.set(true)
          } yield ()

          // run asynchronously forever
          _ <- startTask(ingestion, ()).liftM[MT]
          _ <- Task.delay(log.debug(s"Started ingest.")).liftM[MT]

          _ <- Task.delay(map.put(handle, (queue, signal))).liftM[MT]
        } yield handle
      }

      run.liftM[MT].join.liftB
    }

    def write(h: WriteHandle, chunk: Vector[Data]): Configured[Vector[FileSystemError]] = {
      log.debug(s"write to $h and $chunk")

      val t = for {
        maybePair <- Task.delay(Option(map.get(h)))

        back <- maybePair match {
          case Some(pair) =>
            if (chunk.isEmpty) {
              Task.now(Vector.empty[FileSystemError])
            } else {
              val (queue, _) = pair
              queue.enqueue1(chunk).map(_ => Vector.empty[FileSystemError])
            }

          case _ =>
            Task.now(Vector(unknownWriteHandle(h)))
        }
      } yield back

      t.liftM[MT].liftM[ConfiguredT]
    }

    def close(h: WriteHandle): Configured[Unit] = {
      val t = for {
        // yolo we crash because quasar
        pair <- Task.delay(Option(map.get(h)).get).liftM[MT]
        (queue, signal) = pair

        _ <- Task.delay(map.remove(h)).liftM[MT]
        _ <- Task.delay(log.debug(s"close $h")).liftM[MT]
        // ask queue to stop
        _ <- queue.enqueue1(Vector.empty).liftM[MT]
        // wait until queue actually stops; task async completes when signal completes
        _ <- signal.discrete.takeWhile(!_).run.liftM[MT]
      } yield ()

      t.liftM[ConfiguredT]
    }
  }

  object ManageFileModule extends ManageFileModule {
    import ManageFile._

    // TODO directory moving and varying semantics
    def move(scenario: MoveScenario, semantics: MoveSemantics): Backend[Unit] = {
      scenario.fold(
        d2d = { (from, to) =>
          for {
            precog <- cake[Backend]

            exists <- precog.fs.exists(from).liftM[MT].liftB

            _ <- if (exists)
              ().point[Backend]
            else
              MonadError_[Backend, FileSystemError].raiseError(pathErr(pathNotFound(from)))

            result <- precog.fs.moveDir(from, to, semantics).liftM[MT].liftB

            _ <- if (result) {
              ().point[Backend]
            } else {
              val error = semantics match {
                case MoveSemantics.FailIfMissing => pathNotFound(to)
                case _ => pathExists(to)
              }

              MonadError_[Backend, FileSystemError].raiseError(pathErr(error))
            }
          } yield ()
        },
        f2f = { (from, to) =>
          for {
            precog <- cake[Backend]

            exists <- precog.fs.exists(from).liftM[MT].liftB

            _ <- if (exists)
              ().point[Backend]
            else
              MonadError_[Backend, FileSystemError].raiseError(pathErr(pathNotFound(from)))

            result <- precog.fs.moveFile(from, to, semantics).liftM[MT].liftB

            _ <- if (result) {
              ().point[Backend]
            } else {
              val error = semantics match {
                case MoveSemantics.FailIfMissing => pathNotFound(to)
                case _ => pathExists(to)
              }

              MonadError_[Backend, FileSystemError].raiseError(pathErr(error))
            }
          } yield ()
        })
    }

    def delete(path: APath): Backend[Unit] = {
      for {
        precog <- cake[Backend]

        exists <- precog.fs.exists(path).liftM[MT].liftB

        _ <- if (exists)
          ().point[Backend]
        else
          MonadError_[Backend, FileSystemError].raiseError(pathErr(pathNotFound(path)))

        _ <- precog.fs.delete(path).liftM[MT].liftB
      } yield ()
    }

    def tempFile(near: APath): Backend[AFile] = {
      for {
        seed <- Task.delay(UUID.randomUUID().toString).liftM[MT].liftB
      } yield refineType(near).fold(p => p, fileParent) </> file(seed)
    }
  }
}
