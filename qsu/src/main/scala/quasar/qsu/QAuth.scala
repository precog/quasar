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

package quasar.qsu

import slamdata.Predef._
import quasar.ejson.implicits._
import quasar.contrib.iota.{copkEqual, copkShow, copkTraverse}
import quasar.fp.{symbolOrder, symbolShow}
import quasar.qscript.{FreeMap, MonadPlannerErr, PlannerError}, PlannerError.InternalError

import matryoshka._
import matryoshka.data.free._

import monocle.macros.Lenses

import scalaz.{Applicative, Equal, Monoid, Show}
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.map._
import scalaz.std.tuple._
import scalaz.syntax.equal._
import scalaz.syntax.std.option._

@Lenses
final case class QAuth[T[_[_]], P](
    dims: Map[Symbol, P],
    groupKeys: Map[(Symbol, Int), FreeMap[T]]) {

  def ++ (other: QAuth[T, P]): QAuth[T, P] =
    QAuth(dims ++ other.dims, groupKeys ++ other.groupKeys)

  def addDims(vertex: Symbol, qdims: P): QAuth[T, P] =
    copy(dims = dims + (vertex -> qdims))

  def addGroupKey(vertex: Symbol, idx: Int, key: FreeMap[T]): QAuth[T, P] =
    copy(groupKeys = groupKeys + ((vertex, idx) -> key))

  /** Duplicates all group keys for `src` to `target`. */
  def duplicateGroupKeys(src: Symbol, target: Symbol): QAuth[T, P] = {
    val srcKeys = groupKeys filterKeys { case (s, _) => s === src }
    val tgtKeys = srcKeys map { case ((_, i), v) => ((target, i), v) }

    copy(groupKeys = groupKeys ++ tgtKeys)
  }

  def filterVertices(p: Symbol => Boolean): QAuth[T, P] =
    QAuth(dims.filterKeys(p), groupKeys.filterKeys { case (s, _) => p(s) })

  def lookupDims(vertex: Symbol): Option[P] =
    dims get vertex

  def lookupDimsE[F[_]: Applicative: MonadPlannerErr](vertex: Symbol): F[P] =
    lookupDims(vertex) getOrElseF MonadPlannerErr[F].raiseError[P] {
      InternalError(s"Dimensions for $vertex not found.", None)
    }

  def lookupGroupKey(vertex: Symbol, idx: Int): Option[FreeMap[T]] =
    groupKeys get ((vertex, idx))

  def lookupGroupKeyE[F[_]: Applicative: MonadPlannerErr](vertex: Symbol, idx: Int): F[FreeMap[T]] =
    lookupGroupKey(vertex, idx) getOrElseF MonadPlannerErr[F].raiseError[FreeMap[T]] {
      InternalError(s"GroupKey[$idx] for $vertex not found.", None)
    }
}

object QAuth extends QAuthInstances {
  def empty[T[_[_]], P]: QAuth[T, P] =
    QAuth(Map(), Map())
}

sealed abstract class QAuthInstances {
  implicit def monoid[T[_[_]], P]: Monoid[QAuth[T, P]] =
    Monoid.instance(_ ++ _, QAuth.empty[T, P])

  implicit def equal[T[_[_]]: BirecursiveT: EqualT, P: Equal]: Equal[QAuth[T, P]] =
    Equal.equalBy(qa => (qa.dims, qa.groupKeys))

  implicit def show[T[_[_]]: ShowT, P: Show]: Show[QAuth[T, P]] =
    Show.shows { case QAuth(dims, keys) =>
      "QAuth\n" +
      "=====\n" +
      "Dimensions[\n" + printMultiline(dims.toList) + "\n]\n\n" +
      "GroupKeys[\n" + printMultiline(keys.toList) + "\n]\n" +
      "====="
    }
}
