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

import quasar.contrib.scalaz._
import quasar.contrib.iota.copkTraverse
import quasar.fp.ski.κ
import quasar.qscript._
import quasar.yggdrasil.TableModule.{DesiredSortOrder, SortAscending}

import cats.effect.IO

import matryoshka.{Hole => _, _}
import matryoshka.implicits._
import matryoshka.data._
import matryoshka.patterns._
import scalaz._, Scalaz._
import scalaz.Leibniz.===
import scalaz.concurrent.Task

object MimirCake {

  type Cake = Precog with Singleton

  type CakeMT[F[_], A] = Kleisli[F, (Cake, LightweightFileSystem), A]
  type CakeM[A] = CakeMT[Task, A]

  def cake[F[_]](implicit F: MonadReader_[F, (Cake, LightweightFileSystem)])
      : F[(Cake, LightweightFileSystem)] =
    F.ask

  // EquiJoin results are sorted by both keys at the same time, so we need to keep track of both
  final case class SortOrdering[TS1](sortKeys: Set[TS1], sortOrder: DesiredSortOrder, unique: Boolean) {
    def sort(src: Cake)(table: src.Table)(implicit ev: TS1 === src.trans.TransSpec1): IO[src.Table] =
      if (sortKeys.isEmpty) {
        IO.pure(table)
      } else {
        table.sort(ev(sortKeys.head), sortOrder, unique)
      }
  }

  final case class SortState[TS1](bucket: Option[TS1], orderings: List[SortOrdering[TS1]])

  def interpretMapFunc[T[_[_]], F[_]: Monad]
    (P: Precog, planner: MapFuncPlanner[T, F, MapFunc[T, ?]])
    (fm: FreeMap[T])
      : F[P.trans.TransSpec1] =
    fm.cataM[F, P.trans.TransSpec1](
      interpretM[F, MapFunc[T, ?], Hole, P.trans.TransSpec1](
        κ(P.trans.TransSpec1.Id.point[F]),
        planner.plan(P)[P.trans.Source1](P.trans.TransSpec1.Id)))

  def combineTransSpecs(P: Precog)(specs: Seq[P.trans.TransSpec1]): P.trans.TransSpec1 = {
    import P.trans._

    if (specs.isEmpty)
      TransSpec1.Id
    else if (specs.lengthCompare(1) == 0)
      WrapArray(specs.head)
    else
      OuterArrayConcat(specs.map(WrapArray(_): TransSpec1) : _*)
  }

  // sort by one dimension
  def sortT[P0 <: Cake](c: MimirRepr.Aux[P0])(
      table: c.P.Table,
      sortKey: c.P.trans.TransSpec1,
      sortOrder: DesiredSortOrder = SortAscending,
      unique: Boolean = false): IO[MimirRepr.Aux[c.P.type]] = {

    val newRepr =
      SortState[c.P.trans.TransSpec1](
        bucket = None,
        orderings = SortOrdering(Set(sortKey), sortOrder, unique) :: Nil)

    if (c.lastSort.fold(true)(needToSort(c.P)(_, newSort = newRepr))) {
      table.sort(sortKey, sortOrder, unique) map { sorted =>
        MimirRepr.withSort(c.P)(sorted)(Some(newRepr))
      }
    } else {
      IO.pure(MimirRepr.withSort(c.P)(table)(Some(newRepr)))
    }
  }

  def needToSort(p: Precog)(oldSort: SortState[p.trans.TransSpec1], newSort: SortState[p.trans.TransSpec1])
      : Boolean = {
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

}
