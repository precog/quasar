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

import scalaz._
import TTypes.simplifiableProjection

/** This optional transformation changes the semantics of [[Read]]. The default
  * semantics return a single value, whereas the transformed version has an
  * implied [[LeftShift]] and therefore returns a set of values, which more
  * closely matches the way many data stores behave.
  */
trait SimplifyProjection[F[_]] {
  type H[A]

  def simplifyProjection: F ~> H
}

object SimplifyProjection {
  type Aux[F[_], G[_]] = SimplifyProjection[F] { type H[A] = G[A] }

  def apply[F[_], G[_]](implicit ev: Aux[F, G]): Aux[F, G]          = ev
  def default[F[_]: Traverse, G[_]](implicit F: F :<: G): Aux[F, G] = make[F, G](F)

  def make[F[_], G[_]](f: F ~> G): Aux[F, G] = new SimplifyProjection[F] {
    type H[A] = G[A]
    val simplifyProjection = f
  }

  // FIXME: if these are only used implicitly they should have names less likely to collide.
  implicit def projectBucket[T[_[_]], G[_]](implicit QC: QScriptCore[T, ?] :<: G): Aux[ProjectBucket[T, ?], G] =
    simplifiableProjection[T].ProjectBucket[G]
  implicit def qscriptCore[T[_[_]], G[_]](implicit QC: QScriptCore[T, ?] :<: G): Aux[QScriptCore[T, ?], G] =
    simplifiableProjection[T].QScriptCore[G]
  implicit def thetaJoin[T[_[_]], G[_]](implicit QC: ThetaJoin[T, ?] :<: G): Aux[ThetaJoin[T, ?], G] =
    simplifiableProjection[T].ThetaJoin[G]
  implicit def equiJoin[T[_[_]], G[_]](implicit QC: EquiJoin[T, ?] :<: G): Aux[EquiJoin[T, ?], G] =
    simplifiableProjection[T].EquiJoin[G]
  implicit def deadEnd[F[_]](implicit DE: Const[DeadEnd, ?] :<: F): Aux[Const[DeadEnd, ?], F] =
    default[Const[DeadEnd, ?], F]
  implicit def read[F[_], A](implicit R: Const[Read[A], ?] :<: F): Aux[Const[Read[A], ?], F] =
    default[Const[Read[A], ?], F]
  implicit def shiftedRead[F[_], A](implicit SR: Const[ShiftedRead[A], ?] :<: F): Aux[Const[ShiftedRead[A], ?], F] =
    default[Const[ShiftedRead[A], ?], F]

  implicit def coproduct[T[_[_]], G[_], I[_], J[_]](implicit I: Aux[I, G], J: Aux[J, G]): Aux[Coproduct[I, J, ?], G] =
    make(λ[Coproduct[I, J, ?] ~> G](fa => fa.run.fold(I.simplifyProjection, J.simplifyProjection)))

  // This assembles the coproduct out of the individual implicits.
  def simplifyQScriptTotal[T[_[_]]]: Aux[QScriptTotal[T, ?], QScriptTotal[T, ?]] = apply
}
