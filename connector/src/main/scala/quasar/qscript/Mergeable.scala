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
import quasar.fp._

import matryoshka._
import matryoshka.patterns.CoEnv
import simulacrum.typeclass
import scalaz._, Scalaz._

@typeclass trait Mergeable[F[_]] {
  type IT[F[_]]

  /**
   * Merges `a1` and `a2` into a common source, providing access
   * through that common source to X and Y.
   *
   * In general, if `a1` and `a2` are equal, we cannot simply return
   * `SrcMerge(a1, fm1, fm2)` because the `Hole` in `fm1` and `fm2` reference
   * some unknown source and do not reference `a1`. In this case, our source
   * merging has converged (at least for this iteration), but `fm1` and `fm2` still
   * reference the old common source (which is not known to us here).
   *
   * @param fm1 provides access to some unknown target X.
   * @param fm2 provides access to some unknown target Y.
   * @param a1 the left source to be merged
   * @param a2 the right source to be merged
   */
  def mergeSrcs(
    fm1: FreeMap[IT],
    fm2: FreeMap[IT],
    a1: F[ExternallyManaged],
    a2: F[ExternallyManaged])
      : Option[SrcMerge[F[ExternallyManaged], FreeMap[IT]]]
}

object Mergeable {
  type Aux[T[_[_]], F[_]] = Mergeable[F] { type IT[F[_]] = T[F] }

  implicit def const[T[_[_]]: EqualT, A: Equal]: Mergeable.Aux[T, Const[A, ?]] =
    new Mergeable[Const[A, ?]] {
      type IT[F[_]] = T[F]

      // NB: I think it is true that the buckets on p1 and p2 _must_ be equal
      //     for us to even get to this point, so we can always pick one
      //     arbitrarily for the result.
      //
      //     Also, note that we can optimize the `(p1 ≟ p2)` case in the `Const` case.
      def mergeSrcs(
        left: FreeMap[T],
        right: FreeMap[T],
        p1: Const[A, ExternallyManaged],
        p2: Const[A, ExternallyManaged]) =
        (p1 ≟ p2).option(SrcMerge[Const[A, ExternallyManaged], FreeMap[IT]](p1, HoleF, HoleF))
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
        left: FreeMap[IT],
        right: FreeMap[IT],
        cp1: Coproduct[F, G, ExternallyManaged],
        cp2: Coproduct[F, G, ExternallyManaged]) =
        (cp1.run, cp2.run) match {
          case (-\/(left1), -\/(left2)) =>
            F.mergeSrcs(left, right, left1, left2).map {
              case SrcMerge(src, left, right) =>
                SrcMerge(FC.inj(src), left, right)
            }
          case (\/-(right1), \/-(right2)) =>
            G.mergeSrcs(left, right, right1, right2).map {
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
        left: FreeMap[IT],
        right: FreeMap[IT],
        cp1: CoEnv[Hole, F, ExternallyManaged],
        cp2: CoEnv[Hole, F, ExternallyManaged]) =
        (cp1.run, cp2.run) match {
          case (-\/(hole), -\/(_)) =>
            SrcMerge(CoEnv(hole.left[F[ExternallyManaged]]), left, right).some
          case (\/-(left1), \/-(right1)) =>
            F.mergeSrcs(left, right, left1, right1).map {
              case SrcMerge(s, l, r) =>
                SrcMerge(CoEnv(s.right[Hole]), l, r)
            }
          case (_, _) => None
        }
    }
}
