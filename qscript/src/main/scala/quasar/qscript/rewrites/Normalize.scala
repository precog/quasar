/*
 * Copyright 2014–2020 SlamData Inc.
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
import quasar.contrib.iota._
import quasar.fp.{coenvBijection, coenvPrism, idPrism, liftCo, PrismNT}
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.{~>, Functor, NaturalTransformation}
import scalaz.syntax.either._
import scalaz.syntax.monad._

abstract class Normalize[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] {

  type QST[A] = QScriptTotal[T, A]
  type QSNorm[A] = QScriptNormalized[T, A]
  type OptNorm[A] = Option[QSNorm[A]]

  def norm[F[_], G[_]: Functor](
      FToNorm: F ~> OptNorm,
      NormToG: QSNorm ~> G,
      prismGF: PrismNT[G, F])
      : F[T[G]] => G[T[G]]

  def normQS: QSNorm[T[QSNorm]] => QSNorm[T[QSNorm]] =
    norm[QSNorm, QSNorm](
      λ[QSNorm ~> OptNorm](Some(_)),
      NaturalTransformation.refl[QSNorm],
      idPrism)

  def normQSCoEnv(qst: QST[FreeQS[T]]): CoEnvQS[T, FreeQS[T]] = {
    val runNorm: QST[T[CoEnvQS[T, ?]]] => CoEnvQS[T, T[CoEnvQS[T, ?]]] =
      norm[QST, CoEnvQS[T, ?]](
        SubInject[QSNorm, QST].project,
        λ[QSNorm ~> CoEnvQS[T, ?]](qs => CoEnv.coEnv(
          SubInject[QSNorm, QST].inject(qs).right)),
        coenvPrism)

    runNorm(qst.map(coenvBijection[T, QST, Hole].toK.apply(_)))
      .map(coenvBijection[T, QST, Hole].fromK.apply(_))
  }

  def branchNorm(branch: FreeQS[T]): FreeQS[T] =
    branch.transCata[FreeQS[T]](liftCo(normQSCoEnv))
}
