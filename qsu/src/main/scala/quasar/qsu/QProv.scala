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

import slamdata.Predef.{Eq => _, Int => SInt, _}
import quasar.contrib.scalaz.MonadState_
import quasar.ejson._
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.provenance._

import matryoshka.{Hole => _, _}
import matryoshka.implicits._
import matryoshka.data._
import scalaz.{Lens => _, _}, Scalaz._, Tags.MaxVal

final class QProv[T[_[_]]: BirecursiveT: EqualT]
    extends Dimension[T[EJson], IdAccess[T[EJson]], QProv.P[T]]
    with QSUTTypes[T] {

  import QProv.BucketsState

  type D     = T[EJson]
  type I     = IdAccess[D]
  type PF[A] = QProv.PF[T, A]
  type P     = QProv.P[T]

  val prov: Prov[D, I, P] =
    Prov[D, I, P](IdAccess.static(_))

  type BucketsM[F[_]] = MonadState_[F, BucketsState[I]]
  def BucketsM[F[_]](implicit ev: BucketsM[F]): BucketsM[F] = ev

  /** Returns provenance where the identities have been replaced with the
    * appropriate bucket references.
    */
  def bucketedIds[M[_]: Monad: BucketsM](src: Symbol, p: P): M[P] =
    p.cataM[M, P](bucketedIdsƒ[M](src))

  def bucketedIdsƒ[M[_]: Monad](src: Symbol)(implicit M: BucketsM[M]): AlgebraM[M, PF, P] = {
    case ProvF.Value(i) =>
      for {
        s <- M.get

        idx <- s.buckets.lookup(i) getOrElseF {
          M.put(BucketsState(
            s.nextIdx + 1,
            s.buckets + (i -> s.nextIdx)
          )) as s.nextIdx
        }

        b = IdAccess.bucket[D](src, idx)
      } yield prov.value(b)

    case other =>
      other.embed.point[M]
  }

  /** Converts identity access in `dims` to bucket access of `src`. */
  def bucketAccess(src: Symbol, dims: Dimensions[P]): Dimensions[P] =
    bucketedDims(src, dims)._2

  /** Reifies the identities in the given `Dimensions`. */
  def buckets(dims: Dimensions[P]): List[I] =
    bucketedDims(Symbol(""), dims)._1
      .toList
      .sortBy(_._2)
      .map(_._1)

  /** The greatest index of group keys for `of` or None if none exist. */
  def maxGroupKeyIndex(of: Symbol, dims: Dimensions[P]): Option[SInt] = {
    def maxIndex(p: P): Option[SInt @@ MaxVal] =
      p.foldMap(p0 => groupKey.getOption(p0) collect {
        case (s, i) if s === of => MaxVal(i)
      })

    MaxVal.unsubst(dims foldMap maxIndex)
  }

  /** Returns new dimensions where all identities have been modified by `f`. */
  def modifyIdentities(dims: Dimensions[P])(f: I => I): Dimensions[P] =
    canonicalize(dims map (_.transCata[P](pfo.value modify f)))

  /** The index of the next group key for `of`. */
  def nextGroupKeyIndex(of: Symbol, dims: Dimensions[P]): SInt =
    maxGroupKeyIndex(of, dims).fold(0)(_ + 1)

  /** Renames `from` to `to` in the given dimensions. */
  def rename(from: Symbol, to: Symbol, dims: Dimensions[P]): Dimensions[P] = {
    def rename0(sym: Symbol): Symbol =
      (sym === from) ? to | sym

    modifyIdentities(dims)(IdAccess.symbols[D].modify(rename0))
  }

  ////

  private val pfo = ProvF.Optics[D, I]
  private val groupKey = prov.value composePrism IdAccess.groupKey[D]

  private def bucketedDims(src: Symbol, dims: Dimensions[P]): (I ==>> SInt, Dimensions[P]) =
    dims.reverse
      .traverse(bucketedIds[State[BucketsState[I], ?]](src, _))
      .map(_.reverse)
      .run(BucketsState(0, IMap.empty))
      .leftMap(_.buckets)
}

object QProv {
  type PF[T[_[_]], A] = ProvF[T[EJson], IdAccess[T[EJson]], A]
  type P[T[_[_]]]     = T[PF[T, ?]]

  final case class BucketsState[I](nextIdx: SInt, buckets: I ==>> SInt)

  def apply[T[_[_]]: BirecursiveT: EqualT]: QProv[T] =
    new QProv[T]
}
