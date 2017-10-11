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

package quasar.qscript

import slamdata.Predef._
import quasar.fp.{ ExternallyManaged => EM, _ }
import quasar.qscript.analysis.DeepShape

import matryoshka._
import matryoshka.patterns.CoEnv
import simulacrum.typeclass
import scalaz._, Scalaz._

@typeclass trait Mergeable[F[_]] {
  type IT[F[_]]

  /**
   * Merges `left.source` and `right.source` into a common source, providing source
   * through that common source to the unknown targets X and Y.
   *
   * In general, if `left.source` and `right.source` are equal, we cannot simply return
   * `SrcMerge(a1, fm1, fm2)` because the `Hole` in `fm1` and `fm2` reference
   * some unknown source and do not reference `left.source`. In this case, our source
   * merging has converged (at least for this iteration), but `fm1` and `fm2` still
   * reference the old common source (which is not known to us here).
   */
  def mergeSrcs(
    left: Mergeable.MergeSide[IT, F],
    right: Mergeable.MergeSide[IT, F])
      : Option[SrcMerge[F[EM], FreeMap[IT]]]
}

object Mergeable {
  type Aux[T[_[_]], F[_]] = Mergeable[F] { type IT[F[_]] = T[F] }

  /**
   * @param access provides access to some unknown target
   * @param source the source to be merged
   * @param shape the shape of the child of the `source`
   */
  final case class MergeSide[T[_[_]], F[_]](
    access: FreeMap[T],
    source: F[EM],
    shape: DeepShape.FreeShape[T])

  implicit def const[T[_[_]]: EqualT, A: Equal]: Mergeable.Aux[T, Const[A, ?]] =
    new Mergeable[Const[A, ?]] {
      type IT[F[_]] = T[F]

      // NB: I think it is true that the buckets on `left.source` and `right.source` _must_ be equal
      //     for us to even get to this point, so we can always pick one
      //     arbitrarily for the result.
      //
      //     Also, note that we can optimize the `(left.source ≟ right.source)` case in the `Const` case.
      def mergeSrcs(
        left: Mergeable.MergeSide[IT, Const[A, ?]],
        right: Mergeable.MergeSide[IT, Const[A, ?]]) =
        (left.source ≟ right.source).option(
          SrcMerge[Const[A, EM], FreeMap[IT]](left.source, HoleF, HoleF))
    }

  implicit def coproduct[T[_[_]], F[_], G[_]](
    implicit F: Mergeable.Aux[T, F],
             G: Mergeable.Aux[T, G],
             FC: F :<: Coproduct[F, G, ?],
             GC: G :<: Coproduct[F, G, ?]):
      Mergeable.Aux[T, Coproduct[F, G, ?]] =
    new Mergeable[Coproduct[F, G, ?]] {
      type IT[F[_]] = T[F]

      def mergeSrcs(
        left: Mergeable.MergeSide[IT, Coproduct[F, G, ?]],
        right: Mergeable.MergeSide[IT, Coproduct[F, G, ?]]) =
        (left.source.run, right.source.run) match {
          case (-\/(left1), -\/(left2)) =>
            F.mergeSrcs(left.copy(source=left1), right.copy(source=left2)).map {
              case SrcMerge(src, left, right) =>
                SrcMerge(FC.inj(src), left, right)
            }
          case (\/-(right1), \/-(right2)) =>
            G.mergeSrcs(left.copy(source=right1), right.copy(source=right2)).map {
              case SrcMerge(src, left, right) =>
                SrcMerge(GC.inj(src), left, right)
            }
          case (_, _) => None
        }
    }

  implicit def coenv[T[_[_]], F[_]](
    implicit F: Mergeable.Aux[T, F]):
      Mergeable.Aux[T, CoEnv[Hole, F, ?]] =
    new Mergeable[CoEnv[Hole, F, ?]] {
      type IT[F[_]] = T[F]

      def mergeSrcs(
        left: Mergeable.MergeSide[IT, CoEnv[Hole, F, ?]],
        right: Mergeable.MergeSide[IT, CoEnv[Hole, F, ?]]) =
        (left.source.run, right.source.run) match {
          case (-\/(hole), -\/(_)) =>
            SrcMerge(CoEnv(hole.left[F[EM]]), left.access, right.access).some
          case (\/-(left1), \/-(right1)) =>
            F.mergeSrcs(left.copy(source=left1), right.copy(source=right1)).map {
              case SrcMerge(s, l, r) =>
                SrcMerge(CoEnv(s.right[Hole]), l, r)
            }
          case (_, _) => None
        }
    }
}
