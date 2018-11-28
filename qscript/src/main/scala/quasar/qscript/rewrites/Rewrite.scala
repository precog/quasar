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

import quasar.RenderTreeT
import quasar.contrib.matryoshka.applyTransforms
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.implicits._
import matryoshka.patterns.CoEnv
import scalaz._, BijectionT._, Scalaz._

class Rewrite[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] extends TTypes[T] {

  type QSEd[A] = QScriptEducated[A]
  type QSTotal[A] = QScriptTotal[A]

  def normalize[H[_]: Functor](
      implicit J: ThetaToEquiJoin.Aux[T, QSEd, H],
               C: Coalesce.Aux[T, QSEd, QSEd],
               N: Normalizable[QSEd])
      : T[QSEd] => T[H] =
    normalizeAll.apply(_).transCata[T[H]](J.rewrite[J.G](idPrism.reverseGet))

  private def normalizeAll(
      implicit C: Coalesce.Aux[T, QSEd, QSEd],
               N: Normalizable[QSEd])
      : T[QSEd] => T[QSEd] =
    _.cata[T[QSEd]](
      normalizeT >>>
      repeatedly(N.normalizeF(_: QSEd[T[QSEd]])) >>>
      (_.embed))

  private def applyNormalizations[F[a] <: ACopK[a]: Normalizable, G[_]: Functor](
      prism: PrismNT[G, F])(
      implicit C: Coalesce.Aux[T, F, F],
               QC: QScriptCore :<<: F)
      : F[T[G]] => G[T[G]] =
    ftf => repeatedly[G[T[G]]](applyTransforms[G[T[G]]](
      liftFFTrans[F, G, T[G]](prism)(Normalizable[F].normalizeF(_: F[T[G]])),
      liftFFTrans[F, G, T[G]](prism)(C.coalesceQC[G](prism)),
    ))(prism(ftf))

  private def normalizeWithBijection[F[a] <: ACopK[a]: Functor: Normalizable, G[_]: Functor, A](
      bij: Bijection[A, T[G]])(
      prism: PrismNT[G, F])(
      implicit C: Coalesce.Aux[T, F, F],
               QC: QScriptCore :<<: F)
      : F[A] => G[A] =
    fa => applyNormalizations[F, G](prism)
      .apply(fa ∘ bij.toK.run) ∘ bij.fromK.run

  def normalizeT(implicit C: Coalesce.Aux[T, QSEd, QSEd])
      :QSEd[T[QSEd]] => QSEd[T[QSEd]] =
    normalizeWithBijection[QSEd, QSEd, T[QSEd]](bijectionId)(idPrism)

  def normalizeCoEnv(implicit C: Coalesce.Aux[T, QSTotal, QSTotal])
      : QSTotal[Free[QSTotal, Hole]] => CoEnv[Hole, QSTotal, Free[QSTotal, Hole]] =
    normalizeWithBijection[QSTotal, CoEnv[Hole, QSTotal, ?], Free[QSTotal, Hole]](coenvBijection)(coenvPrism)
}
