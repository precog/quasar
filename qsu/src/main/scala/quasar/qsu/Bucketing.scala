/*
 * Copyright 2014–2019 SlamData Inc.
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

import slamdata.Predef.{Eq => _, Int => SInt, _}

import quasar.contrib.scalaz.MonadState_
import quasar.ejson.implicits._

import cats.instances.symbol._
import cats.instances.tuple._
import cats.syntax.eq._
import cats.syntax.bifunctor._

import matryoshka.{BirecursiveT, EqualT}

import scalaz.{@@, ==>>, Id, IMap, State, Monad}
import scalaz.Tags.MaxVal
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

import shims._

final class Bucketing[T[_[_]]: BirecursiveT: EqualT, P] private (
    qprov: QProvAux[T, P])
    extends QSUTTypes[T] {

  import Bucketing.BucketsState

  /** Converts identity access in `p` to bucket access of `src`. */
  def bucketAccess(src: Symbol, p: P): P =
    bucketedP(src, p)._2

  /** Reifies the identities in the given provenance. */
  def buckets(p: P): List[IdAccess] =
    bucketedP(Symbol(""), p)._1
      .toList
      .sortBy(_._2)
      .map(_._1)

  /** The greatest index of group keys for `of` or None if none exist. */
  def maxGroupKeyIndex(of: Symbol, p: P): Option[SInt] =
    MaxVal.unsubst(qprov.foldMapVectorIds[Option[SInt @@ MaxVal]]((i, t) =>
      IdAccess.groupKey.getOption(i) collect {
        case (s, i) if s === of => MaxVal(i)
      }
    )(p))

  /** Returns new provenance where all identities have been modified by `f`. */
  def modifyIdentities(p: P)(f: IdAccess => IdAccess): P =
    qprov.traverseVectorIds[Id.Id]((i, t) => (f(i), t))(p)

  /** The index of the next group key for `of`. */
  def nextGroupKeyIndex(of: Symbol, p: P): SInt =
    maxGroupKeyIndex(of, p).fold(0)(_ + 1)

  /** Renames `from` to `to` in the given dimensions. */
  def rename(from: Symbol, to: Symbol, p: P): P = {
    def rename0(sym: Symbol): Symbol =
      if (sym === from) to else sym

    modifyIdentities(p)(IdAccess.symbols.modify(rename0))
  }

  ////

  private type BucketsM[F[_]] = MonadState_[F, BucketsState[IdAccess]]
  private def BucketsM[F[_]](implicit ev: BucketsM[F]): BucketsM[F] = ev

  private def bucketedIds[M[_]: Monad: BucketsM](src: Symbol, p: P): M[P] =
    qprov.traverseVectorIds((i, t) =>
      for {
        s <- BucketsM[M].get

        idx <- s.buckets.lookup(i) getOrElseF {
          BucketsM[M].put(BucketsState(
            s.nextIdx + 1,
            s.buckets + (i -> s.nextIdx)
          )) as s.nextIdx
        }

      } yield (IdAccess.bucket(src, idx), t)
    )(p)

  private def bucketedP(src: Symbol, p: P): (IdAccess ==>> SInt, P) =
    bucketedIds[State[BucketsState[IdAccess], ?]](src, p)
      .run(BucketsState(0, IMap.empty))
      .leftMap(_.buckets)
}

object Bucketing {
  final case class BucketsState[I](nextIdx: SInt, buckets: I ==>> SInt)

  def apply[T[_[_]]: BirecursiveT: EqualT](qp: QProv[T]): Bucketing[T, qp.P] =
    new Bucketing(qp)
}
