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
import quasar.contrib.matryoshka._
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.{:+: => _, Divide => _, _},
  BijectionT._,
  Leibniz._,
  Scalaz._

class Rewrite[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] extends TTypes[T] {

  def normTJ[G[a] <: ACopK[a]: Traverse]
    (implicit QC: QScriptCore :<<: G,
              C: Coalesce.Aux[T, G, G],
              N: Normalizable[G])
      : T[G] => T[G] =
    _.cata[T[G]](
      normalizeTJ[G] >>>
      repeatedly(N.normalizeF(_: G[T[G]])) >>>
      (_.embed))

  def simplifyJoinOnNorm[G[a] <: ACopK[a]: Traverse, H[_]: Functor]
    (implicit QC: QScriptCore :<<: G,
              J: SimplifyJoin.Aux[T, G, H],
              C: Coalesce.Aux[T, G, G],
              N: Normalizable[G])
      : T[G] => T[H] =
    normTJ[G].apply(_).transCata[T[H]](J.simplifyJoin[J.G](idPrism.reverseGet))

  private def applyNormalizations[F[a] <: ACopK[a]: Functor: Normalizable, G[_]: Functor](
    prism: PrismNT[G, F])(
    implicit C: Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F):
      F[T[G]] => G[T[G]] =
    ftf => repeatedly[G[T[G]]](applyTransforms[G[T[G]]](
      liftFFTrans[F, G, T[G]](prism)(Normalizable[F].normalizeF(_: F[T[G]])),
      liftFFTrans[F, G, T[G]](prism)(C.coalesceQC[G](prism)),
    ))(prism(ftf))

  private def normalizeWithBijection[F[a] <: ACopK[a]: Functor: Normalizable, G[_]: Functor, A](
    bij: Bijection[A, T[G]])(
    prism: PrismNT[G, F])(
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F):
      F[A] => G[A] =
    fa => applyNormalizations[F, G](prism)
      .apply(fa ∘ bij.toK.run) ∘ bij.fromK.run

  def normalizeTJ[F[a] <: ACopK[a]: Traverse: Normalizable](
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F):
      F[T[F]] => F[T[F]] =
    normalizeWithBijection[F, F, T[F]](bijectionId)(idPrism)

  def normalizeTJCoEnv[F[a] <: ACopK[a]: Traverse: Normalizable](
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F):
      F[Free[F, Hole]] => CoEnv[Hole, F, Free[F, Hole]] =
    normalizeWithBijection[F, CoEnv[Hole, F, ?], Free[F, Hole]](coenvBijection)(coenvPrism)
}
