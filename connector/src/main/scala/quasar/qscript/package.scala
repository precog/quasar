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

package quasar

import slamdata.Predef._
import quasar.contrib.pathy.{ADir, AFile}
import quasar.fp._
import quasar.qscript.MapFuncCore._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

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
  * - We use the type parameter `QS[_]` to indicate QScript, as well as the type
  *   parameters `IN[_]` and `OUT[_]` to indicate the input and output
  *   coproducts in transformations where they can be different.
  */
// NB: Here we no longer care about provenance. Backends can’t do anything with
//     it, so we simply represent joins and crosses directly. This also means
//     that we don’t need to model certain things – project_d is just a
//     data-level function, nest_d & swap_d only modify provenance and so are
//     irrelevant here, and autojoin_d has been replaced with a lower-level join
//     operation that doesn’t include the cross portion.
package object qscript {

  /** This type is _only_ used for join branch-like structures. It’s an
    * unfortunate consequence of not having mutually-recursive data structures.
    * Once we do, this can go away. It should _not_ be used in other situations.
    *
    * NB: We're using the "alias" method of building the coproduct here as it
    *     provides a modest reduction in compilation time (~15%) for this module.
    */
  type QScriptTotal[T[_[_]], A]  = Coproduct[QScriptCore[T, ?]           , QScriptTotal0[T, ?], A]
  type QScriptTotal0[T[_[_]], A] = Coproduct[ProjectBucket[T, ?]         , QScriptTotal1[T, ?], A]
  type QScriptTotal1[T[_[_]], A] = Coproduct[ThetaJoin[T, ?]             , QScriptTotal2[T, ?], A]
  type QScriptTotal2[T[_[_]], A] = Coproduct[EquiJoin[T, ?]              , QScriptTotal3[T, ?], A]
  type QScriptTotal3[T[_[_]], A] = Coproduct[Const[ShiftedRead[ADir], ?] , QScriptTotal4[T, ?], A]
  type QScriptTotal4[T[_[_]], A] = Coproduct[Const[ShiftedRead[AFile], ?], QScriptTotal5[T, ?], A]
  type QScriptTotal5[T[_[_]], A] = Coproduct[Const[Read[ADir], ?]        , QScriptTotal6[T, ?], A]
  type QScriptTotal6[T[_[_]], A] = Coproduct[Const[Read[AFile], ?]       , Const[DeadEnd, ?]  , A]

  object QCT {
    def apply[T[_[_]], A](qc: QScriptCore[T, A]): QScriptTotal[T, A] =
      Inject[QScriptCore[T, ?], QScriptTotal[T, ?]].inj(qc)

    def unapply[T[_[_]], A](qt: QScriptTotal[T, A]): Option[QScriptCore[T, A]] =
      Inject[QScriptCore[T, ?], QScriptTotal[T, ?]].prj(qt)
  }

  /** Initial QScript. */
  // FIXME should not include `Read[ADir]`
  type QScriptEducated[T[_[_]], A] =
    (QScriptCore[T, ?] :\: ThetaJoin[T, ?] :\: Const[Read[ADir], ?] :/: Const[Read[AFile], ?])#M[A]

  def educatedToTotal[T[_[_]]]
      : Injectable.Aux[QScriptEducated[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::\::[ThetaJoin[T, ?]](::/::[T, Const[Read[ADir], ?], Const[Read[AFile], ?]]))

  object QCE {
    def apply[T[_[_]], A](qc: QScriptCore[T, A]): QScriptEducated[T, A] =
      Inject[QScriptCore[T, ?], QScriptEducated[T, ?]].inj(qc)

    def unapply[T[_[_]], A](qt: QScriptEducated[T, A]): Option[QScriptCore[T, A]] =
      Inject[QScriptCore[T, ?], QScriptEducated[T, ?]].prj(qt)
  }

  /** QScript that has not gone through Read conversion. */
  type QScript[T[_[_]], A] =
    (QScriptCore[T, ?] :\: ThetaJoin[T, ?] :/: Const[DeadEnd, ?])#M[A]

  implicit def qScriptToQscriptTotal[T[_[_]]]
      : Injectable.Aux[QScript[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, ThetaJoin[T, ?], Const[DeadEnd, ?]])

  /** QScript that has gone through Read conversion.
    *
    * NB: Once QScriptTotal goes away, this could become parametric in the path type.
    */
  type QScriptRead[T[_[_]], A] =
    (QScriptCore[T, ?] :\: ThetaJoin[T, ?] :\: Const[Read[ADir], ?] :/: Const[Read[AFile], ?])#M[A]

  implicit def qScriptReadToQscriptTotal[T[_[_]]]: Injectable.Aux[QScriptRead[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::\::[ThetaJoin[T, ?]](::/::[T, Const[Read[ADir], ?], Const[Read[AFile], ?]]))

  /** QScript that has gone through Read conversion and shifted conversion.
    *
    * NB: Once QScriptTotal goes away, this could become parametric in the path type.
    */
  type QScriptShiftRead[T[_[_]], A] =
    (QScriptCore[T, ?] :\: ThetaJoin[T, ?] :\: Const[ShiftedRead[ADir], ?] :/: Const[ShiftedRead[AFile], ?])#M[A]

  implicit def qScriptShiftReadToQScriptTotal[T[_[_]]]: Injectable.Aux[QScriptShiftRead[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::\::[ThetaJoin[T, ?]](::/::[T, Const[ShiftedRead[ADir], ?], Const[ShiftedRead[AFile], ?]]))

  type MapFunc[T[_[_]], A] = (MapFuncCore[T, ?] :/: MapFuncDerived[T, ?])#M[A]

  object MFC {
    def apply[T[_[_]], A](mfc: MapFuncCore[T, A]): MapFunc[T, A] =
      Inject[MapFuncCore[T, ?], MapFunc[T, ?]].inj(mfc)

    def unapply[T[_[_]], A](mf: MapFunc[T, A]): Option[MapFuncCore[T, A]] =
      Inject[MapFuncCore[T, ?], MapFunc[T, ?]].prj(mf)
  }

  object MFD {
    def apply[T[_[_]], A](mfc: MapFuncDerived[T, A]): MapFunc[T, A] =
      Inject[MapFuncDerived[T, ?], MapFunc[T, ?]].inj(mfc)

    def unapply[T[_[_]], A](mf: MapFunc[T, A]): Option[MapFuncDerived[T, A]] =
      Inject[MapFuncDerived[T, ?], MapFunc[T, ?]].prj(mf)
  }

  type FreeQS[T[_[_]]]      = Free[QScriptTotal[T, ?], Hole]
  type FreeMapA[T[_[_]], A] = Free[MapFunc[T, ?], A]
  type FreeMap[T[_[_]]]     = FreeMapA[T, Hole]
  type JoinFunc[T[_[_]]]    = FreeMapA[T, JoinSide]

  type CoEnvQS[T[_[_]], A]      = CoEnv[Hole, QScriptTotal[T, ?], A]
  type CoEnvMapA[T[_[_]], A, B] = CoEnv[A, MapFunc[T, ?], B]
  type CoEnvMap[T[_[_]], A]     = CoEnvMapA[T, Hole, A]

  object ExtractFunc {
    def unapply[T[_[_]], A](fma: FreeMapA[T, A]): Option[MapFuncCore[T, FreeMapA[T, A]]] = fma match {
      case Embed(CoEnv(\/-(MFC(func)))) => Some(func)
      case _ => None
    }
  }

  def HoleF[T[_[_]]]: FreeMap[T] = Free.point[MapFunc[T, ?], Hole](SrcHole)
  def HoleQS[T[_[_]]]: FreeQS[T] = Free.point[QScriptTotal[T, ?], Hole](SrcHole)
  def LeftSideF[T[_[_]]]: JoinFunc[T] =
    Free.point[MapFunc[T, ?], JoinSide](LeftSide)
  def RightSideF[T[_[_]]]: JoinFunc[T] =
    Free.point[MapFunc[T, ?], JoinSide](RightSide)
  def ReduceIndexF[T[_[_]]](i: Int \/ Int): FreeMapA[T, ReduceIndex] =
    Free.point[MapFunc[T, ?], ReduceIndex](ReduceIndex(i))

  def rebase[M[_]: Bind, A](in: M[A], key: M[A]): M[A] = in >> key

  def rebaseT[T[_[_]]: BirecursiveT, F[_]: Traverse](
    target: FreeQS[T])(
    src: T[F])(
    implicit FI: Injectable.Aux[F, QScriptTotal[T, ?]]):
      Option[T[F]] =
    target.as(src.transAna[T[QScriptTotal[T, ?]]](FI.inject)).cata(recover(_.embed)).transAnaM(FI project _)

  def rebaseTCo[T[_[_]]: BirecursiveT, F[_]: Traverse]
    (target: FreeQS[T])
    (srcCo: T[CoEnv[Hole, F, ?]])
    (implicit FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : Option[T[CoEnv[Hole, F, ?]]] =
    // TODO: with the right instances & types everywhere, this should look like
    //       target.transAnaM(_.htraverse(FI project _)) ∘ (_ >> srcCo)
    target.cataM[Option, T[CoEnv[Hole, F, ?]]](
      CoEnv.htraverse(λ[QScriptTotal[T, ?] ~> (Option ∘ F)#λ](FI.project(_))).apply(_) ∘ (_.embed)) ∘
      (targ => (targ.convertTo[Free[F, Hole]] >> srcCo.convertTo[Free[F, Hole]]).convertTo[T[CoEnv[Hole, F, ?]]])

  /** A variant of `repeatedly` that works with `Inject` instances. */
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def injectRepeatedly[F [_], G[_], A]
    (op: F[A] => Option[G[A]])
    (implicit F: F :<: G)
      : F[A] => G[A] =
    fa => op(fa).fold(F.inj(fa))(ga => F.prj(ga).fold(ga)(injectRepeatedly(op)))

  // Helpers for creating `Injectable` instances

  object ::\:: {
    def apply[F[_]] = new Aux[F]

    final class Aux[F[_]] {
      def apply[T[_[_]], G[_]]
        (i: Injectable.Aux[G, QScriptTotal[T, ?]])
        (implicit F: F :<: QScriptTotal[T, ?])
          : Injectable.Aux[Coproduct[F, G, ?], QScriptTotal[T, ?]] =
        Injectable.coproduct(Injectable.inject[F, QScriptTotal[T, ?]], i)
    }
  }

  def ::/::[T[_[_]], F[_], G[_]]
    (implicit F: F :<: QScriptTotal[T, ?], G: G :<: QScriptTotal[T, ?])
      : Injectable.Aux[Coproduct[F, G, ?], QScriptTotal[T, ?]] =
    Injectable.coproduct(
      Injectable.inject[F, QScriptTotal[T, ?]],
      Injectable.inject[G, QScriptTotal[T, ?]])
}
