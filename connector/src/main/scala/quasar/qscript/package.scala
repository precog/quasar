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

package quasar

import quasar.Predef._
import quasar.fp._

import matryoshka._
import matryoshka.patterns._
import monocle.macros.Lenses
import scalaz.{NonEmptyList => NEL, _}, Scalaz._

/** The various representations of an arbitrary query, as seen by the filesystem
  * connectors, along with the operations for dealing with them.
  * 
  * There are a few patterns that are worth noting:
  * - `(src: A, ..., lBranch: FreeQS[T], rBranch: FreeQS[T], ...)` – used in
  *   operations that combine multiple data sources (notably joins and unions).
  *   This holds the divergent parts of the data sources in the branches, with
  *   [[SrcHole]] indicating a reference back to the common `src` of the two
  *   branches. There is not required to be a [[SrcHole]].
  * - `Free[F, A]` – we use this structure as a restricted form of variable
  *   binding, where `F` is some pattern functor, and `A` is some enumeration
  *   that has a specific referent. E.g., [[FreeMap]] is a recursive structure
  *   of [[MapFunc]] that has a single “variable”, [[SrcHole]], which (usually)
  *   refers to the `src` parameter of that operation. [[JoinFunc]], [[FreeQS]],
  *   and the `repair` parameter to [[Reduce]] behave similarly.
  */
// NB: Here we no longer care about provenance. Backends can’t do anything with
//     it, so we simply represent joins and crosses directly. This also means
//     that we don’t need to model certain things – project_d is just a
//     data-level function, nest_d & swap_d only modify provenance and so are
//     irrelevant here, and autojoin_d has been replaced with a lower-level join
//     operation that doesn’t include the cross portion.
package object qscript {
  private type CommonPathable[T[_[_]], A] =
    Coproduct[Const[DeadEnd, ?], QScriptCore[T, ?], A]

  /** Statically known path components potentially converted to Read.
    */
  type Pathable[T[_[_]], A] =
    Coproduct[ProjectBucket[T, ?], CommonPathable[T, ?], A]

  private type QScriptTotal0[T[_[_]], A] =
    Coproduct[ThetaJoin[T, ?], Pathable[T, ?], A]
  private type QScriptTotal1[T[_[_]], A] =
    Coproduct[EquiJoin[T, ?], QScriptTotal0[T, ?], A]
  private type QScriptTotal2[T[_[_]], A] =
    Coproduct[Const[Read, ?], QScriptTotal1[T, ?], A]
  /** This type is used for join branch-like structures. It’s an unfortunate
    * consequence of not having mutually-recursive data structures. Once we do,
    * this can go away.
    */
  type QScriptTotal[T[_[_]], A] =
    Coproduct[Const[ShiftedRead, ?], QScriptTotal2[T, ?], A]

  type FreeMap[T[_[_]]]  = Free[MapFunc[T, ?], Hole]
  type FreeQS[T[_[_]]]   = Free[QScriptTotal[T, ?], Hole]
  type JoinFunc[T[_[_]]] = Free[MapFunc[T, ?], JoinSide]

  @Lenses final case class Ann[T[_[_]]](provenance: List[FreeMap[T]], values: FreeMap[T])

  object Ann {
    implicit def equal[T[_[_]]: EqualT]: Equal[Ann[T]] =
      Equal.equal((a, b) => a.provenance ≟ b.provenance && a.values ≟ b.values)

    implicit def show[T[_[_]]: ShowT]: Show[Ann[T]] =
      Show.show(ann => Cord("Ann(") ++ ann.provenance.show ++ Cord(", ") ++ ann.values.show ++ Cord(")"))
  }

  def HoleF[T[_[_]]]: FreeMap[T] = Free.point[MapFunc[T, ?], Hole](SrcHole)
  def EmptyAnn[T[_[_]]]: Ann[T] = Ann[T](Nil, HoleF[T])

  final case class SrcMerge[A, B](src: A, left: B, right: B)

  def rebase[M[_]: Bind, A](in: M[A], field: M[A]): M[A] = in >> field

  import MapFunc._
  import MapFuncs._

  def EquiJF[T[_[_]]]: JoinFunc[T] =
    Free.roll(Eq(Free.point(LeftSide), Free.point(RightSide)))

  def concatBuckets[T[_[_]]: Recursive: Corecursive](buckets: List[FreeMap[T]]):
      Option[(FreeMap[T], NEL[FreeMap[T]])] =
    buckets match {
      case Nil => None
      case head :: tail =>
        (ConcatArraysN(buckets.map(b => Free.roll(MakeArray[T, FreeMap[T]](b)))),
          NEL(head, tail).zipWithIndex.map(p =>
            Free.roll(ProjectIndex[T, FreeMap[T]](
              HoleF[T],
              IntLit[T, Hole](p._2))))).some
    }

  def concat[T[_[_]]: Corecursive, A](
    l: Free[MapFunc[T, ?], A], r: Free[MapFunc[T, ?], A]):
      (Free[MapFunc[T, ?], A], FreeMap[T], FreeMap[T]) =
    (Free.roll(ConcatArrays(Free.roll(MakeArray(l)), Free.roll(MakeArray(r)))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](1))))

  def concat3[T[_[_]]: Corecursive, A](
    l: Free[MapFunc[T, ?], A], c: Free[MapFunc[T, ?], A], r: Free[MapFunc[T, ?], A]):
      (Free[MapFunc[T, ?], A], FreeMap[T], FreeMap[T], FreeMap[T]) =
    (Free.roll(ConcatArrays(Free.roll(ConcatArrays(Free.roll(MakeArray(l)), Free.roll(MakeArray(c)))), Free.roll(MakeArray(r)))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](1))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](2))))

  // TODO: move to matryoshka

  implicit def envtEqual[E: Equal, F[_]](implicit F: Delay[Equal, F]):
      Delay[Equal, EnvT[E, F, ?]] =
    new Delay[Equal, EnvT[E, F, ?]] {
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (env1, env2) =>
            env1.ask ≟ env2.ask && F(eq).equal(env1.lower, env2.lower)
        }
    }

  implicit def envtShow[E: Show, F[_]](implicit F: Delay[Show, F]):
      Delay[Show, EnvT[E, F, ?]] =
    new Delay[Show, EnvT[E, F, ?]] {
      def apply[A](sh: Show[A]) =
        Show.show {
          envt => Cord("EnvT(") ++ envt.ask.show ++ Cord(", ") ++ F(sh).show(envt.lower) ++ Cord(")")
        }
    }

  def envtHmap[F[_], G[_], E, A](f: F ~> G): EnvT[E, F, ?] ~> EnvT[E, G, ?] =
    new (EnvT[E, F, ?] ~> EnvT[E, G, ?]) {
      def apply[A](env: EnvT[E, F, A]) = EnvT((env.ask, f(env.lower)))
    }

  def envtLowerNT[F[_], E]: EnvT[E, F, ?] ~> F = new (EnvT[E, F, ?] ~> F) {
    def apply[A](fa: EnvT[E, F, A]): F[A] = fa.lower
  }
}
