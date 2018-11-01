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

package quasar.qscript.rewrites

import slamdata.Predef.{Map => _, _}
import quasar.RenderTreeT
import quasar.IdStatus, IdStatus.{ExcludeId, IdOnly, IncludeId}
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.{ADir, AFile}
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript._
import quasar.qscript.RecFreeS._
import quasar.qscript.MapFuncCore._
import quasar.qscript.MapFuncsCore._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.{:+: => _, Divide => _, _},
  BijectionT._,
  Leibniz._,
  Scalaz._

class Rewrite[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] extends TTypes[T] {

  def rewriteShift(idStatus: IdStatus, repair: JoinFunc)
      : Option[(IdStatus, JoinFunc)] =
    (idStatus === IncludeId).option[Option[(IdStatus, JoinFunc)]] {
      def makeRef(idx: Int): JoinFunc =
        Free.roll[MapFunc, JoinSide](MFC(ProjectIndex(RightSideF, IntLit(idx))))

      val zeroRef: JoinFunc = makeRef(0)
      val oneRef: JoinFunc = makeRef(1)
      val rightCount: Int = repair.elgotPara[Int](count(RightSideF))

      if (repair.elgotPara[Int](count(oneRef)) === rightCount)
        // all `RightSide` access is through `oneRef`
        (ExcludeId, repair.transApoT(substitute[JoinFunc](oneRef, RightSideF))).some
      else if (repair.elgotPara[Int](count(zeroRef)) ≟ rightCount)
        // all `RightSide` access is through `zeroRef`
        (IdOnly, repair.transApoT(substitute[JoinFunc](zeroRef, RightSideF))).some
      else
        None
    }.join

  // TODO: make this simply a transform itself, rather than a full traversal.
  // FOOBAR 2
  // ShiftRead transforms Read to ShiftedRead
  def shiftRead[F[_]: Functor, G[a] <: ACopK[a]: Traverse]
    (implicit QC: QScriptCore :<<: G,
              TJ: ThetaJoin :<<: G,
              SD: Const[ShiftedRead[ADir], ?] :<<: G,
              SF: Const[ShiftedRead[AFile], ?] :<<: G,
              S: ShiftRead.Aux[T, F, G],
              C: Coalesce.Aux[T, G, G])
      : T[F] => T[G] = {
    _.codyna[G, T[G]](
      normalizeTJ[G] >>>
      repeatedly(C.coalesceSR[G, ADir](idPrism)) >>>
      repeatedly(C.coalesceSR[G, AFile](idPrism)) >>>
      (_.embed),
      ((_: T[F]).project) >>> (S.shiftRead[G](idPrism.reverseGet)(_)))
  }

  def shiftReadDir[F[_]: Functor, G[a] <: ACopK[a]: Traverse](
    implicit
    QC: QScriptCore :<<: G,
    TJ: ThetaJoin :<<: G,
    SD: Const[ShiftedRead[ADir], ?] :<<: G,
    S: ShiftReadDir.Aux[T, F, G],
    C: Coalesce.Aux[T, G, G])
      : T[F] => T[G] =
    _.codyna[G, T[G]](
      normalizeTJ[G] >>>
      repeatedly(C.coalesceSR[G, ADir](idPrism)) >>>
      (_.embed),
      ((_: T[F]).project) >>> (S.shiftReadDir[G](idPrism.reverseGet)(_)))

  // FOOBAR 1
  // SimplifyJoin does not depend on anything else, ThetaJoin => EquiJoin
  def simplifyJoinOnShiftRead[F[_]: Functor, G[a] <: ACopK[a]: Traverse, H[_]: Functor]
    (implicit QC: QScriptCore :<<: G,
              TJ: ThetaJoin :<<: G,
              SD: Const[ShiftedRead[ADir], ?] :<<: G,
              SF: Const[ShiftedRead[AFile], ?] :<<: G,
              S: ShiftRead.Aux[T, F, G],
              J: SimplifyJoin.Aux[T, G, H],
              C: Coalesce.Aux[T, G, G])
      : T[F] => T[H] =
    shiftRead[F, G].apply(_).transCata[T[H]](J.simplifyJoin[J.G](idPrism.reverseGet))


  // TODO: These optimizations should give rise to various property tests:
  //       • elideNopMap ⇒ no `Map(???, HoleF)`
  //       • normalize ⇒ a whole bunch, based on MapFuncsCore
  //       • coalesceMaps ⇒ no `Map(Map(???, ???), ???)`
  //       • coalesceMapJoin ⇒ no `Map(ThetaJoin(???, …), ???)`

  def elideNopQC[F[_]: Functor]: QScriptCore[T[F]] => Option[F[T[F]]] = {
    case Filter(Embed(src), RecBoolLit(true)) => some(src)
    case Map(Embed(src), mf) if mf ≟ HoleR    => some(src)
    case _                                    => none
  }

  val compactQC = λ[QScriptCore ~> (Option ∘ QScriptCore)#λ] {
    case LeftShift(src, struct, id, stpe, undef, repair) =>
      rewriteShift(id, repair) ∘ (xy => LeftShift(src, struct, xy._1, stpe, undef, xy._2))

    case Reduce(src, bucket, reducers, repair0) =>
      // `indices`: the indices into `reducers` that are used
      val Empty   = ReduceIndex(-1.right)
      val used    = repair0.map(_.idx).toList.unite.toSet
      val indices = reducers.indices filter used
      val repair  = repair0 map (r => r.copy(r.idx ∘ indices.indexOf))
      val done    = repair ≟ repair0 || (repair element Empty)

      !done option Reduce(src, bucket, (indices map reducers).toList, repair)

    case _ => None
  }

  // TODO: add reordering
  // - Filter can be moved ahead of Sort
  // - Subset can have a normalized order _if_ their counts are constant
  //   (maybe in some additional cases)

  // The order of optimizations is roughly this:
  // - elide NOPs
  // - read conversion given to us by the filesystem
  // - convert any remaning projects to maps
  // - coalesce nodes
  // - normalize mapfunc
  //
  // FOOBAR 6
  private def applyNormalizations[F[a] <: ACopK[a]: Functor, G[_]: Functor](
    prism: PrismNT[G, F],
    normalizeJoins: F[T[G]] => Option[G[T[G]]])(
    implicit C: Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F):
      F[T[G]] => G[T[G]] = {

    val qcPrism = PrismNT.injectCopK[QScriptCore, F] compose prism

    ftf => repeatedly[G[T[G]]](applyTransforms[G[T[G]]](
      liftFFTrans[QScriptCore, G, T[G]](qcPrism)(compactQC(_: QScriptCore[T[G]])),
      liftFFTrans[F, G, T[G]](prism)(C.coalesceQC[G](prism)),
      liftFGTrans[F, G, T[G]](prism)(normalizeJoins),
      liftFGTrans[QScriptCore, G, T[G]](qcPrism)(elideNopQC[G])
    ))(prism(ftf))
  }

  // FOOBAR 5
  private def normalizeWithBijection[F[a] <: ACopK[a]: Functor, G[_]: Functor, A](
    bij: Bijection[A, T[G]])(
    prism: PrismNT[G, F],
    normalizeJoins: F[T[G]] => Option[G[T[G]]])(
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F):
      F[A] => G[A] =
    fa => applyNormalizations[F, G](prism, normalizeJoins)
      .apply(fa ∘ bij.toK.run) ∘ bij.fromK.run

  private def normalizeEJBijection[F[a] <: ACopK[a]: Functor, G[_]: Functor, A](
    bij: Bijection[A, T[G]])(
    prism: PrismNT[G, F])(
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F,
             EJ: EquiJoin :<<: F):
      F[A] => G[A] = {

    val normEJ: G[T[G]] => Option[G[T[G]]] =
      liftFFTrans[F, G, T[G]](prism)(C.coalesceEJ[G](prism.get))

    normalizeWithBijection[F, G, A](bij)(prism, normEJ compose (prism apply _))
  }

  def normalizeEJ[F[a] <: ACopK[a]: Functor](
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F,
             EJ: EquiJoin :<<: F):
      F[T[F]] => F[T[F]] =
    normalizeEJBijection[F, F, T[F]](bijectionId)(idPrism)

  def normalizeEJCoEnv[F[a] <: ACopK[a]: Functor](
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F,
             EJ: EquiJoin :<<: F):
      F[Free[F, Hole]] => CoEnv[Hole, F, Free[F, Hole]] =
    normalizeEJBijection[F, CoEnv[Hole, F, ?], Free[F, Hole]](coenvBijection)(coenvPrism)

  // FOOBAR 4
  private def normalizeTJBijection[F[a] <: ACopK[a]: Functor, G[_]: Functor, A](
    bij: Bijection[A, T[G]])(
    prism: PrismNT[G, F])(
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F,
             TJ: ThetaJoin :<<: F):
      F[A] => G[A] = {

    val normTJ = applyTransforms(
      liftFFTrans[F, G, T[G]](prism)(C.coalesceTJ[G](prism.get)))

    normalizeWithBijection[F, G, A](bij)(prism, normTJ compose (prism apply _))
  }

  // FOOBAR 3
  def normalizeTJ[F[a] <: ACopK[a]: Traverse](
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F,
             TJ: ThetaJoin :<<: F):
      F[T[F]] => F[T[F]] =
    normalizeTJBijection[F, F, T[F]](bijectionId)(idPrism)

  def normalizeTJCoEnv[F[a] <: ACopK[a]: Traverse](
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F,
             TJ: ThetaJoin :<<: F):
      F[Free[F, Hole]] => CoEnv[Hole, F, Free[F, Hole]] =
    normalizeTJBijection[F, CoEnv[Hole, F, ?], Free[F, Hole]](coenvBijection)(coenvPrism)
}
