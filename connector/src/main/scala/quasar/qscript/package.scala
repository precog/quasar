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
import quasar.qscript.{provenance => prov}

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
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
    */
  type QScriptTotal[T[_[_]], A] =
    (QScriptCore[T, ?] :\: ProjectBucket[T, ?] :\:
      ThetaJoin[T, ?] :\: EquiJoin[T, ?] :\:
      Const[ShiftedRead, ?] :\: Const[Read, ?] :/: Const[DeadEnd, ?])#M[A]

  /** QScript that has not gone through Read conversion. */
  type QScript[T[_[_]], A] =
    (QScriptCore[T, ?] :\: ThetaJoin[T, ?] :/: Const[DeadEnd, ?])#M[A]

  implicit def qScriptToQscriptTotal[T[_[_]]]
      : Injectable.Aux[QScript[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, ThetaJoin[T, ?], Const[DeadEnd, ?]])

  /** QScript that has gone through Read conversion. */
  type QScriptRead[T[_[_]], A] =
    (QScriptCore[T, ?] :\: ThetaJoin[T, ?] :/: Const[Read, ?])#M[A]

  implicit def qScriptReadToQscriptTotal[T[_[_]]]
      : Injectable.Aux[QScriptRead[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, ThetaJoin[T, ?], Const[Read, ?]])

  /** QScript that has gone through Read conversion and shifted conversion */
  type QScriptShiftRead[T[_[_]], A] =
    (QScriptCore[T, ?] :\: ThetaJoin[T, ?] :/: Const[ShiftedRead, ?])#M[A]

  implicit def qScriptShiftReadToQScriptTotal[T[_[_]]]
      : Injectable.Aux[QScriptShiftRead[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, ThetaJoin[T, ?], Const[ShiftedRead, ?]])

  type FreeQS[T[_[_]]]      = Free[QScriptTotal[T, ?], Hole]
  type FreeMapA[T[_[_]], A] = Free[MapFunc[T, ?], A]
  type FreeMap[T[_[_]]]     = FreeMapA[T, Hole]
  type JoinFunc[T[_[_]]]    = FreeMapA[T, JoinSide]

  def HoleF[T[_[_]]]: FreeMap[T] = Free.point[MapFunc[T, ?], Hole](SrcHole)
  def HoleQS[T[_[_]]]: FreeQS[T] = Free.point[QScriptTotal[T, ?], Hole](SrcHole)
  def LeftSideF[T[_[_]]]: JoinFunc[T] =
    Free.point[MapFunc[T, ?], JoinSide](LeftSide)
  def RightSideF[T[_[_]]]: JoinFunc[T] =
    Free.point[MapFunc[T, ?], JoinSide](RightSide)
  def ReduceIndexF[T[_[_]]](i: Int): FreeMapA[T, ReduceIndex] =
    Free.point[MapFunc[T, ?], ReduceIndex](ReduceIndex(i))

  def EmptyAnn[T[_[_]]]: Ann[T] = Ann[T](Nil, HoleF[T])

  def rebase[M[_]: Bind, A](in: M[A], field: M[A]): M[A] = in >> field

  import MapFunc._
  import MapFuncs._

  def concatBuckets[T[_[_]]: BirecursiveT](buckets: List[FreeMap[T]]):
      Option[(FreeMap[T], NEL[FreeMap[T]])] =
    buckets match {
      case Nil => None
      case head :: tail =>
        (ConcatArraysN(buckets.map(b => Free.roll(MakeArray[T, FreeMap[T]](b)))).embed,
          NEL(head, tail).zipWithIndex.map(p =>
            Free.roll(ProjectIndex[T, FreeMap[T]](
              HoleF[T],
              IntLit[T, Hole](p._2))))).some
    }

  def concat[T[_[_]]: BirecursiveT: EqualT: ShowT, A: Equal: Show](
    l: FreeMapA[T, A], r: FreeMapA[T, A]):
      (FreeMapA[T, A], FreeMap[T], FreeMap[T]) = {
    val norm = Normalizable.normalizable[T]

    // NB: Might be better to do this later, after some normalization, part of
    //     array compaction, but this helps us avoid some autojoins.
    (norm.freeMF(l) ≟ norm.freeMF(r)).fold(
      (norm.freeMF(l), HoleF[T], HoleF[T]),
      (Free.roll(ConcatArrays(Free.roll(MakeArray(l)), Free.roll(MakeArray(r)))),
        Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0))),
        Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))))
  }

  def concat3[T[_[_]]: CorecursiveT, A](
    l: FreeMapA[T, A], c: FreeMapA[T, A], r: FreeMapA[T, A]):
      (FreeMapA[T, A], FreeMap[T], FreeMap[T], FreeMap[T]) =
    (Free.roll(ConcatArrays(Free.roll(ConcatArrays(Free.roll(MakeArray(l)), Free.roll(MakeArray(c)))), Free.roll(MakeArray(r)))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](1))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](2))))

  def concat4[T[_[_]]: CorecursiveT, A](
    l: FreeMapA[T, A], c: FreeMapA[T, A], r: FreeMapA[T, A], r2: FreeMapA[T, A]):
      (FreeMapA[T, A], FreeMap[T], FreeMap[T], FreeMap[T], FreeMap[T]) =
    (Free.roll(ConcatArrays(Free.roll(ConcatArrays(Free.roll(ConcatArrays(Free.roll(MakeArray(l)), Free.roll(MakeArray(c)))), Free.roll(MakeArray(r)))), Free.roll(MakeArray(r2)))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](1))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](2))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](3))))

  def rebaseBranch[T[_[_]]: BirecursiveT: EqualT: ShowT]
    (br: FreeQS[T], fm: FreeMap[T]): FreeQS[T] = {
    val rewrite = new Rewrite[T]

    (br >> Free.roll(Inject[QScriptCore[T, ?], QScriptTotal[T, ?]].inj(Map(Free.point[QScriptTotal[T, ?], Hole](SrcHole), fm)))).transCata[FreeQS[T]](
      liftCo(rewrite.normalizeCoEnv[QScriptTotal[T, ?]]))
  }

  def rewriteShift[T[_[_]]: BirecursiveT: EqualT]
    (idStatus: IdStatus, repair: JoinFunc[T])
      : Option[(IdStatus, JoinFunc[T])] =
    (idStatus ≟ IncludeId).option[Option[(IdStatus, JoinFunc[T])]] {
      def makeRef(idx: Int): JoinFunc[T] =
        Free.roll[MapFunc[T, ?], JoinSide](ProjectIndex(RightSideF, IntLit(idx)))

      val zeroRef: JoinFunc[T] = makeRef(0)
      val oneRef: JoinFunc[T] = makeRef(1)
      val rightCount: Int = repair.elgotPara(count(RightSideF))

      if (repair.elgotPara(count(oneRef)) ≟ rightCount)
        // all `RightSide` access is through `oneRef`
        (ExcludeId, repair.transApoT(substitute[JoinFunc[T]](oneRef, RightSideF))).some
      else if (repair.elgotPara(count(zeroRef)) ≟ rightCount)
        // all `RightSide` access is through `zeroRef`
        (IdOnly, repair.transApoT(substitute[JoinFunc[T]](zeroRef, RightSideF))).some
      else
        None
    }.join

  /** A variant of `repeatedly` that works with `Inject` instances. */
  def injectRepeatedly[F [_], G[_], A]
    (op: F[A] => Option[G[A]])
    (implicit F: F :<: G)
      : F[A] => G[A] =
    fa => op(fa).fold(F.inj(fa))(ga => F.prj(ga).fold(ga)(injectRepeatedly(op)))

  // TODO: make this simply a transform itself, rather than a full traversal.
  def shiftRead[T[_[_]]: BirecursiveT: EqualT: ShowT, F[_]: Functor, G[_]: Traverse]
    (implicit QC: QScriptCore[T, ?] :<: G,
              TJ: ThetaJoin[T, ?] :<: G,
              SR: Const[ShiftedRead, ?] :<: G,
              GI: Injectable.Aux[G, QScriptTotal[T, ?]],
              S: ShiftRead.Aux[T, F, G],
              C: Coalesce.Aux[T, G, G],
              N: Normalizable[G])
      : T[F] => T[G] = {
    val rewrite = new Rewrite[T]
    _.codyna(
      rewrite.normalize[G] >>>
        liftFG(injectRepeatedly(C.coalesceSR[G](idPrism))) >>>
        (_.embed),
      ((_: T[F]).project) >>> (S.shiftRead(idPrism.reverseGet)(_)))
  }

  // FIXME: This needs a better name, as it doesn’t currently reflect what it
  //        does at all.
  def simplifyRead[T[_[_]]: BirecursiveT: EqualT: ShowT, F[_]: Functor, G[_]: Traverse, H[_]: Functor]
    (implicit QC: QScriptCore[T, ?] :<: G,
              TJ: ThetaJoin[T, ?] :<: G,
              SR: Const[ShiftedRead, ?] :<: G,
              GI: Injectable.Aux[G, QScriptTotal[T, ?]],
              S: ShiftRead.Aux[T, F, G],
              J: SimplifyJoin.Aux[T, G, H],
              C: Coalesce.Aux[T, G, G],
              N: Normalizable[G])
      : T[F] => T[H] =
    shiftRead[T, F, G].apply(_).transCata[T[H]](J.simplifyJoin(idPrism.reverseGet))

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

  implicit final class BirecursiveOps[T[_[_]], F[_]](val self: T[F]) extends scala.AnyVal {
    final def pruneArrays
      (implicit PA: PruneArrays[F], T: BirecursiveT[T], TF: Traverse[F])
        : T[F] = {
      val pa = new PAFindRemap[T, F]
      self.hyloM[pa.ArrayState, pa.ArrayEnv[F, ?], T[F]](pa.remapIndices, pa.findIndices).run(None)._2
    }
  }
}

package qscript {
  final case class SrcMerge[A, B](src: A, left: B, right: B)

  @Lenses final case class Ann[T[_[_]]](provenance: List[prov.Provenance[T]], values: FreeMap[T])

  object Ann {
    implicit def equal[T[_[_]]: EqualT]: Equal[Ann[T]] =
      Equal.equal((a, b) => a.provenance ≟ b.provenance && a.values ≟ b.values)

    implicit def show[T[_[_]]: ShowT]: Show[Ann[T]] =
      Show.show(ann => Cord("Ann(") ++ ann.provenance.show ++ Cord(", ") ++ ann.values.show ++ Cord(")"))
  }

  @Lenses final case class Target[T[_[_]], F[_]](ann: Ann[T], value: T[F])

  object Target {
    implicit def equal[T[_[_]]: EqualT, F[_]: Functor]
      (implicit F: Delay[Equal, F])
        : Equal[Target[T, F]] =
      Equal.equal((a, b) => a.ann ≟ b.ann && a.value ≟ b.value)

    implicit def show[T[_[_]]: ShowT, F[_]: Functor](implicit F: Delay[Show, F])
        : Show[Target[T, F]] =
      Show.show(target =>
        Cord("Target(") ++
          target.ann.shows ++ Cord(", ") ++
          target.value.shows ++ Cord(")"))
  }
}
