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
import quasar.fs.Planner.{InternalError, PlannerErrorME}
import quasar.ejson.implicits._
import quasar.contrib.iota.{copkEqual, copkShow, copkTraverse}
import quasar.fp.{coproductEqual, coproductShow, symbolOrder, symbolShow}
import quasar.qscript.FreeMap

import matryoshka._
import matryoshka.data.free._
import scalaz.{Applicative, Equal, Monoid, Show}
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.map._
import scalaz.std.tuple._
import scalaz.syntax.equal._
import scalaz.syntax.std.option._

final case class QAuth[T[_[_]]](
    dims: Map[Symbol, QDims[T]],
    groupKeys: Map[(Symbol, Int), FreeMap[T]]) {

  def ++ (other: QAuth[T]): QAuth[T] =
    QAuth(dims ++ other.dims, groupKeys ++ other.groupKeys)

  def addDims(vertex: Symbol, qdims: QDims[T]): QAuth[T] =
    copy(dims = dims + (vertex -> qdims))

  def addGroupKey(vertex: Symbol, idx: Int, key: FreeMap[T]): QAuth[T] =
    copy(groupKeys = groupKeys + ((vertex, idx) -> key))

  /** Duplicates all group keys for `src` to `target`. */
  def duplicateGroupKeys(src: Symbol, target: Symbol): QAuth[T] = {
    val srcKeys = groupKeys filterKeys { case (s, _) => s === src }
    val tgtKeys = srcKeys map { case ((_, i), v) => ((target, i), v) }

    copy(groupKeys = groupKeys ++ tgtKeys)
  }

  def lookupDims(vertex: Symbol): Option[QDims[T]] =
    dims get vertex

  def lookupDimsE[F[_]: Applicative: PlannerErrorME](vertex: Symbol): F[QDims[T]] =
    lookupDims(vertex) getOrElseF PlannerErrorME[F].raiseError[QDims[T]] {
      InternalError(s"Dimensions for $vertex not found.", None)
    }

  def lookupGroupKey(vertex: Symbol, idx: Int): Option[FreeMap[T]] =
    groupKeys get ((vertex, idx))

  def lookupGroupKeyE[F[_]: Applicative: PlannerErrorME](vertex: Symbol, idx: Int): F[FreeMap[T]] =
    lookupGroupKey(vertex, idx) getOrElseF PlannerErrorME[F].raiseError[FreeMap[T]] {
      InternalError(s"GroupKey[$idx] for $vertex not found.", None)
    }

  /** Supplants `target` with `replacement`, removing the former and replacing
    * any references to it with the latter.
    */
  def supplant
      (target: Symbol, replacement: Symbol)
      (implicit T0: BirecursiveT[T], T1: EqualT[T])
      : QAuth[T] = {

    val qp = QProv[T]
    val supDims = (dims - target).mapValues(qp.rename(target, replacement, _))
    val supKeys = groupKeys filterKeys { case (s, _) => s =/= target }

    QAuth(supDims, supKeys)
  }

  def renameRefs
      (target: Symbol, replacement: Symbol)
      (implicit T0: BirecursiveT[T], T1: EqualT[T])
      : QAuth[T] = {

    val qp = QProv[T]
    val supDims = dims.mapValues(qp.rename(target, replacement, _))

    QAuth(supDims, groupKeys)
  }
}

object QAuth extends QAuthInstances {
  def empty[T[_[_]]]: QAuth[T] =
    QAuth(Map(), Map())
}

sealed abstract class QAuthInstances {
  implicit def monoid[T[_[_]]]: Monoid[QAuth[T]] =
    Monoid.instance(_ ++ _, QAuth.empty[T])

  implicit def equal[T[_[_]]: BirecursiveT: EqualT]: Equal[QAuth[T]] = {
    implicit val eqP: Equal[QProv.P[T]] = QProv[T].prov.provenanceEqual
    Equal.equalBy(qa => (qa.dims, qa.groupKeys))
  }

  implicit def show[T[_[_]]: ShowT]: Show[QAuth[T]] =
    Show.shows { case QAuth(dims, keys) =>
      "QAuth\n" +
      "=====\n" +
      "Dimensions[\n" + printMultiline(dims.toList) + "\n]\n\n" +
      "GroupKeys[\n" + printMultiline(keys.toList) + "\n]\n" +
      "====="
    }
}
