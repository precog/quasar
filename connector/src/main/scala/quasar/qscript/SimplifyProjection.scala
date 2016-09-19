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

import scalaz._

/** This optional transformation changes the semantics of [[Read]]. The default
  * semantics return a single value, whereas the transformed version has an
  * implied [[LeftShift]] and therefore returns a set of values, which more
  * closely matches the way many data stores behave.
  */
trait SimplifyProjection[F[_]] {
  type H[A]

  def simplifyProjection: F ~> H
}

object SimplifyProjection extends SimplifyProjectionInstances {
  type Aux[F[_], G[_]] = SimplifyProjection[F] { type H[A] = G[A] }

  def apply[F[_], G[_]](implicit ev: SimplifyProjection.Aux[F, G]) = ev

  def applyToBranch[T[_[_]]](branch: FreeQS[T]): FreeQS[T] =
    branch.mapSuspension(
      SimplifyProjection[QScriptTotal[T, ?], QScriptTotal[T, ?]].simplifyProjection)

  implicit def projectBucket[T[_[_]], G[_]](implicit QC: QScriptCore[T, ?] :<: G):
      SimplifyProjection.Aux[ProjectBucket[T, ?], G] =
    new SimplifyProjection[ProjectBucket[T, ?]] {
      type H[A] = G[A]
      def simplifyProjection = new (ProjectBucket[T, ?] ~> G) {
        def apply[A](fa: ProjectBucket[T, A]) = fa match {
          case BucketField(src, value, field) =>
            QC.inj(Map(src, Free.roll(MapFuncs.ProjectField(value, field))))
          case BucketIndex(src, value, index) =>
            QC.inj(Map(src, Free.roll(MapFuncs.ProjectIndex(value, index))))
        }
      }
    }

  implicit def qscriptCore[T[_[_]], G[_]](implicit QC: QScriptCore[T, ?] :<: G):
      SimplifyProjection.Aux[QScriptCore[T, ?], G] =
    new SimplifyProjection[QScriptCore[T, ?]] {
      type H[A] = G[A]
      def simplifyProjection = new (QScriptCore[T, ?] ~> G) {
        def apply[A](fa: QScriptCore[T, A]) = QC.inj(fa match {
          case Union(src, lb, rb) =>
            Union(src, applyToBranch(lb), applyToBranch(rb))
          case Drop(src, lb, rb) =>
            Drop(src, applyToBranch(lb), applyToBranch(rb))
          case Take(src, lb, rb) =>
            Take(src, applyToBranch(lb), applyToBranch(rb))
          case _ => fa
        })
      }
    }

  implicit def thetaJoin[T[_[_]], G[_]](implicit TJ: ThetaJoin[T, ?] :<: G):
      SimplifyProjection.Aux[ThetaJoin[T, ?], G] =
    new SimplifyProjection[ThetaJoin[T, ?]] {
      type H[A] = G[A]
      def simplifyProjection = new (ThetaJoin[T, ?] ~> G) {
        def apply[A](tj: ThetaJoin[T, A]) =
          TJ.inj(ThetaJoin(
            tj.src,
            applyToBranch(tj.lBranch),
            applyToBranch(tj.rBranch),
            tj.on,
            tj.f,
            tj.combine))
      }
    }

  implicit def equiJoin[T[_[_]], G[_]](implicit EJ: EquiJoin[T, ?] :<: G):
      SimplifyProjection.Aux[EquiJoin[T, ?], G] =
    new SimplifyProjection[EquiJoin[T, ?]] {
      type H[A] = G[A]
      def simplifyProjection = new (EquiJoin[T, ?] ~> G) {
        def apply[A](ej: EquiJoin[T, A]) =
          EJ.inj(EquiJoin(
            ej.src,
            applyToBranch(ej.lBranch),
            applyToBranch(ej.rBranch),
            ej.lKey,
            ej.rKey,
            ej.f,
            ej.combine))
      }
    }

  implicit def coproduct[T[_[_]], G[_], I[_], J[_]]
    (implicit I: SimplifyProjection.Aux[I, G], J: SimplifyProjection.Aux[J, G]):
      SimplifyProjection.Aux[Coproduct[I, J, ?], G] =
    new SimplifyProjection[Coproduct[I, J, ?]] {
      type H[A] = G[A]
      def simplifyProjection = new (Coproduct[I, J, ?] ~> G) {
        def apply[A](fa: Coproduct[I, J, A]) =
          fa.run.fold(I.simplifyProjection(_), J.simplifyProjection(_))
      }
    }
}

abstract class SimplifyProjectionInstances {
  implicit def inject[F[_]: Traverse, G[_]](implicit F: F :<: G):
      SimplifyProjection.Aux[F, G] =
    new SimplifyProjection[F] {
      type H[A] = G[A]
      def simplifyProjection = F
    }
}
