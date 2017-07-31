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

package quasar

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.{ADir, AFile}
import quasar.ejson.EJson
import quasar.fp._
import quasar.qscript.{provenance => prov}
import quasar.qscript.MapFuncCore._
import quasar.qscript.MapFuncsCore._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import monocle.macros.Lenses
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
  type CoEnvJoin[T[_[_]], A]    = CoEnvMapA[T, JoinSide, A]

  def HoleF[T[_[_]]]: FreeMap[T] = Free.point[MapFunc[T, ?], Hole](SrcHole)
  def HoleQS[T[_[_]]]: FreeQS[T] = Free.point[QScriptTotal[T, ?], Hole](SrcHole)
  def LeftSideF[T[_[_]]]: JoinFunc[T] =
    Free.point[MapFunc[T, ?], JoinSide](LeftSide)
  def RightSideF[T[_[_]]]: JoinFunc[T] =
    Free.point[MapFunc[T, ?], JoinSide](RightSide)
  def ReduceIndexF[T[_[_]]](i: Int \/ Int): FreeMapA[T, ReduceIndex] =
    Free.point[MapFunc[T, ?], ReduceIndex](ReduceIndex(i))

  def EmptyAnn[T[_[_]]]: Ann[T] = Ann[T](Nil, HoleF[T])

  private def concatNaive[T[_[_]]: BirecursiveT: EqualT: ShowT, A: Equal: Show]
    (l: FreeMapA[T, A], r: FreeMapA[T, A])
      : (FreeMapA[T, A], FreeMap[T], FreeMap[T]) = {
    val norm = Normalizable.normalizable[T]

    (norm.freeMF(l), norm.freeMF(r)) match {
      case (newL, newR) if newL ≟ newR => (newL, HoleF[T], HoleF[T])
      case (newL @ Embed(CoEnv(\/-(MFC(Constant(_))))), newR) =>
        (newR, newL >> HoleF, HoleF[T])
      case (newL, newR @ Embed(CoEnv(\/-(MFC(Constant(_)))))) =>
        (newL, HoleF[T], newR >> HoleF)
      case (newL, newR) =>
        (StaticArray(List(newL, newR)),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))))
    }
  }

  def concat[T[_[_]]: BirecursiveT: EqualT: ShowT, A: Equal: Show]
    (l: FreeMapA[T, A], r: FreeMapA[T, A])
      : (FreeMapA[T, A], FreeMap[T], FreeMap[T]) = {
    val norm = Normalizable.normalizable[T]

    val norml = norm.freeMF(l)
    val normr = norm.freeMF(r)

    val leftElems: List[FreeMapA[T, A]] = norml.project match {
      case StaticArray(array) => array
      case _                  => Nil
    }

    val rightElems: List[FreeMapA[T, A]] = normr.project  match {
      case StaticArray(array) => array
      case _                  => Nil
    }

    def projectIndex(idx: Int): FreeMap[T] =
      Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](idx))))

    def indexOf(elems: List[FreeMapA[T ,A]], value: FreeMapA[T, A]): Option[Int] =
      IList.fromList(elems).indexOf(Free.roll(MFC(MakeArray(value))))

    indexOf(leftElems, normr).cata(
      idx => (norml, HoleF[T], projectIndex(idx)),
      indexOf(rightElems, norml).cata(
        idx => (normr, projectIndex(idx), HoleF[T]),
        concatNaive(norml, normr)))
  }

  def concat3[T[_[_]]: BirecursiveT: EqualT: ShowT, A: Equal: Show](
    l: FreeMapA[T, A], c: FreeMapA[T, A], r: FreeMapA[T, A]):
      (FreeMapA[T, A], FreeMap[T], FreeMap[T], FreeMap[T]) = {
    val norm = Normalizable.normalizable[T]

    (norm.freeMF(l), norm.freeMF(c), norm.freeMF(r)) match {
      case (newL, newC, newR) if newL ≟ newC && newL ≟ newR =>
        (newL, HoleF[T], HoleF[T], HoleF[T])

      case (newL @ Embed(CoEnv(\/-(MFC(Constant(_))))), newC @ Embed(CoEnv(\/-(MFC(Constant(_))))), newR) =>
        (newR, newL >> HoleF[T], newC >> HoleF[T], HoleF[T])

      case (newL @ Embed(CoEnv(\/-(MFC(Constant(_))))), newC, newR @ Embed(CoEnv(\/-(MFC(Constant(_)))))) =>
        (newC, newL >> HoleF[T], HoleF[T], newR >> HoleF[T])

      case (newL, newC @ Embed(CoEnv(\/-(MFC(Constant(_))))), newR @ Embed(CoEnv(\/-(MFC(Constant(_)))))) =>
        (newL, HoleF[T], newC >> HoleF[T], newR >> HoleF[T])

      case (newL @ Embed(CoEnv(\/-(MFC(Constant(_))))), newC, newR) =>
        if (newC ≟ newR)
          (newC, newL >> HoleF, HoleF[T], HoleF[T])
        else
          (StaticArray(List(newC, newR)),
            newL >> HoleF,
            Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
            Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))))

      case (newL, newC @ Embed(CoEnv(\/-(MFC(Constant(_))))), newR) =>
        if (newL ≟ newR)
          (newL, HoleF[T], newC >> HoleF, HoleF[T])
        else
          (StaticArray(List(newL, newR)),
            Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
            newC >> HoleF,
            Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))))

      case (newL, newC, newR @ Embed(CoEnv(\/-(MFC(Constant(_)))))) =>
        if (newL ≟ newC)
          (newL, HoleF[T], HoleF[T], newR >> HoleF)
        else
          (StaticArray(List(newL, newC)),
            Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
            Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))),
            newR >> HoleF)

      case (newL, newC, newR) =>
        (StaticArray(List(newL, newC, newR)),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](2)))))
    }
  }

  def concat4[T[_[_]]: BirecursiveT: EqualT: ShowT, A: Equal: Show](
    l: FreeMapA[T, A], c: FreeMapA[T, A], r: FreeMapA[T, A], r2: FreeMapA[T, A]):
      (FreeMapA[T, A], FreeMap[T], FreeMap[T], FreeMap[T], FreeMap[T]) = {
    val norm = Normalizable.normalizable[T]

    (norm.freeMF(l), norm.freeMF(c), norm.freeMF(r), norm.freeMF(r2)) match {
      case (newL, newC, newR, newR2) if newL ≟ newC && newR ≟ newR2 =>
        (StaticArray(List(newL, newR)),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))))

      // TODO: Handle cases with more than one constant.
      case (newL @ Embed(CoEnv(\/-(MFC(Constant(_))))), newC, newR, newR2) =>
        (StaticArray(List(newC, newR, newR2)),
          newL >> HoleF,
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](2)))))

      case (newL, newC @ Embed(CoEnv(\/-(MFC(Constant(_))))), newR, newR2) =>
        (StaticArray(List(newL, newR, newR2)),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
          newC >> HoleF,
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](2)))))

      case (newL, newC, newR @ Embed(CoEnv(\/-(MFC(Constant(_))))), newR2) =>
        (StaticArray(List(newL, newC, newR2)),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))),
          newR >> HoleF,
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](2)))))

      case (newL, newC, newR, newR2 @ Embed(CoEnv(\/-(MFC(Constant(_)))))) =>
        (StaticArray(List(newL, newC, newR)),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](2)))),
          newR2 >> HoleF)

      case (newL, newC, newR, newR2) =>
        (StaticArray(List(newL, newC, newR, newR2)),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](2)))),
          Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](3)))))
    }
  }

  def rebase[M[_]: Bind, A](in: M[A], field: M[A]): M[A] = in >> field

  def rebaseBranch[T[_[_]]: BirecursiveT: EqualT: ShowT](
    br: FreeQS[T],
    fm: FreeMap[T]
  ): FreeQS[T] = {
    val rewrite = new Rewrite[T]

    (br >> Free.roll(Inject[QScriptCore[T, ?], QScriptTotal[T, ?]].inj(
      Map(Free.point[QScriptTotal[T, ?], Hole](SrcHole), fm))))
      .transCata[FreeQS[T]](liftCo(rewrite.normalizeCoEnv[QScriptTotal[T, ?]]))
  }

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

  private def pruneArrays0[T, F[_]: Traverse](
    state: PATypes.RewriteState)(
    implicit
      R: Recursive.Aux[T, F],
      C: Corecursive.Aux[T, F],
      P: PruneArrays[F])
      : T => T = {
    val pa = new PAFindRemap[T, F]
    _.hyloM[State[PATypes.RewriteState, ?], pa.ArrayEnv, T](
      pa.remapIndices[State[PATypes.RewriteState, ?]],
      pa.findIndices[State[PATypes.RewriteState, ?]]
    ).eval(state)
  }

  private def pruneArrays[T, F[_]: Traverse](
    implicit
      R: Recursive.Aux[T, F],
      C: Corecursive.Aux[T, F],
      P: PruneArrays[F])
      : T => T =
    pruneArrays0[T, F](PATypes.Ignore)

  implicit final class BirecursiveOps[T[_[_]], F[_]](val self: T[F]) extends scala.AnyVal {
    final def pruneArraysF(
      implicit
        T: BirecursiveT[T],
        P: PruneArrays[F],
        F: Traverse[F])
        : T[F] =
      pruneArrays[T[F], F].apply(self)
  }

  implicit final class FreeQSOps[T[_[_]]](val self: FreeQS[T]) extends scala.AnyVal {
    final def pruneArraysBranch(
      state: PATypes.RewriteState)(
      implicit
        T: BirecursiveT[T],
        P: PruneArrays[CoEnvQS[T, ?]])
        : FreeQS[T] =
      pruneArrays0[FreeQS[T], CoEnvQS[T, ?]](state).apply(self)
  }
}

package qscript {
  final case class SrcMerge[A, B](src: A, lval: B, rval: B)

  @Lenses final case class Ann[T[_[_]]](provenance: List[prov.Provenance[T]], values: FreeMap[T])

  object Ann {
    implicit def equal[T[_[_]]: BirecursiveT: EqualT](implicit J: Equal[T[EJson]]): Equal[Ann[T]] =
      Equal.equal((a, b) => a.provenance ≟ b.provenance && a.values ≟ b.values)

    implicit def show[T[_[_]]: ShowT]: Show[Ann[T]] =
      Show.show(ann => Cord("Ann(") ++ ann.provenance.show ++ Cord(", ") ++ ann.values.show ++ Cord(")"))
  }

  @Lenses final case class Target[T[_[_]], F[_]](ann: Ann[T], value: T[F])

  object Target {
    implicit def equal[T[_[_]]: BirecursiveT: EqualT, F[_]: Functor](
      implicit F: Delay[Equal, F], J: Equal[T[EJson]]
    ): Equal[Target[T, F]] =
      Equal.equal((a, b) => a.ann ≟ b.ann && a.value ≟ b.value)

    implicit def show[T[_[_]]: ShowT, F[_]: Functor](implicit F: Delay[Show, F])
        : Show[Target[T, F]] =
      Show.show(target =>
        Cord("Target(") ++
          target.ann.shows ++ Cord(", ") ++
          target.value.shows ++ Cord(")"))
  }
}
