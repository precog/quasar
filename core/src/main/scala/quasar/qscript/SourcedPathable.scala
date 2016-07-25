/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.fp._

import matryoshka._
import matryoshka.patterns._
import monocle.macros.Lenses
import scalaz._, Scalaz._

sealed abstract class SourcedPathable[T[_[_]], A] {
  def src: A
}

/** Flattens nested structure, converting each value into a data set, which are
  * then unioned.
  *
  * `struct` is an expression that evaluates to an array or object, which is
  * then “exploded” into multiple values. `repair` is applied across the new
  * set, integrating the exploded values into the original set.
  */
@Lenses final case class LeftShift[T[_[_]], A](
  src: A,
  struct: FreeMap[T],
  repair: JoinFunc[T])
    extends SourcedPathable[T, A]

/** Creates a new dataset, |a|+|b|, containing all of the entries from each of
  * the input sets, without any indication of which set they came from
  *
  * This could be handled as another join type, the anti-join
  * (T[EJson] \/ T[EJson] => T[EJson], specifically as `_.merge`), with the
  * condition being `κ(true)`,
  */
@Lenses final case class Union[T[_[_]], A](
  src: A,
  lBranch: FreeQS[T],
  rBranch: FreeQS[T])
    extends SourcedPathable[T, A]

object SourcedPathable {
  implicit def equal[T[_[_]]: EqualT]: Delay[Equal, SourcedPathable[T, ?]] =
    new Delay[Equal, SourcedPathable[T, ?]] {
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (LeftShift(a1, s1, r1), LeftShift(a2, s2, r2)) =>
            eq.equal(a1, a2) && s1 ≟ s2 && r1 ≟ r2
          case (Union(a1, l1, r1), Union(a2, l2, r2)) =>
            eq.equal(a1, a2) && l1 ≟ l2 && r1 ≟ r2
          case (_, _) => false
        }
    }

  implicit def traverse[T[_[_]]]: Traverse[SourcedPathable[T, ?]] =
    new Traverse[SourcedPathable[T, ?]] {
      def traverseImpl[G[_], A, B](
        fa: SourcedPathable[T, A])(
        f: A => G[B])(
        implicit G: Applicative[G]):
          G[SourcedPathable[T, B]] =
        fa match {
          case LeftShift(a, s, r) => f(a) ∘ (LeftShift(_, s, r))
          case Union(a, l, r)     => f(a) ∘ (Union(_, l, r))
        }
    }

  implicit def show[T[_[_]]: ShowT]: Delay[Show, SourcedPathable[T, ?]] =
    new Delay[Show, SourcedPathable[T, ?]] {
      def apply[A](s: Show[A]): Show[SourcedPathable[T, A]] =
        Show.show(_ match {
          case LeftShift(src, struct, repair) => Cord("LeftShift(") ++
            s.show(src) ++ Cord(",") ++
            struct.show ++ Cord(",") ++
            repair.show ++ Cord(")")
          case Union(src, l, r) => Cord("Union(") ++
            s.show(src) ++ Cord(",") ++
            l.show ++ Cord(",") ++
            r.show ++ Cord(")")
        })
    }

  implicit def mergeable[T[_[_]]: EqualT]:
      Mergeable.Aux[T, SourcedPathable[T, ?]] =
    new Mergeable[SourcedPathable[T, ?]] {
      type IT[F[_]] = T[F]

      def mergeSrcs(
        left: FreeMap[IT],
        right: FreeMap[IT],
        p1: EnvT[Ann[T], SourcedPathable[IT, ?], Hole],
        p2: EnvT[Ann[T], SourcedPathable[IT, ?], Hole]) =
        // TODO: Merge two LeftShifts with different repair functions
        (p1 ≟ p2).option(SrcMerge(p1, left, right))
    }

  implicit def normalizable[T[_[_]]: Recursive: Corecursive: EqualT: ShowT]:
      Normalizable[SourcedPathable[T, ?]] =
    new Normalizable[SourcedPathable[T, ?]] {
      val opt = new Optimize[T]

      def normalize = new (SourcedPathable[T, ?] ~> SourcedPathable[T, ?]) {
        def apply[A](sp: SourcedPathable[T, A]) = sp match {
          case LeftShift(src, s, r) =>
            LeftShift(src, normalizeMapFunc(s), normalizeMapFunc(r))
          case Union(src, l, r) =>
            Union(
              src,
              freeTransCata(l)(liftCo(opt.applyToFreeQS[QScriptProject[T, ?], Hole])),
              freeTransCata(r)(liftCo(opt.applyToFreeQS[QScriptProject[T, ?], Hole])))
        }
      }
    }
}
