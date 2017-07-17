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

import quasar.fp._

import scalaz._

/** Centralizes the knowledge of T[_[_]] as well as certain
 *  type classes required for many operations. This is for
 *  compilation performance.
 */

trait TTypes[T[_[_]]] {
  // Partially applying types with the known T.
  // In this context we shouldn't often need to refer to the original type
  // any longer, so reuse the name.
  type EquiJoin[A]       = quasar.qscript.EquiJoin[T, A]
  type QScriptCore[A]    = quasar.qscript.QScriptCore[T, A]
  type QScriptTotal[A]   = quasar.qscript.QScriptTotal[T, A]
  type ProjectBucket[A]  = quasar.qscript.ProjectBucket[T, A]
  type ThetaJoin[A]      = quasar.qscript.ThetaJoin[T, A]
  type MapFuncCore[A]    = quasar.qscript.MapFuncCore[T, A]
  type MapFuncDerived[A] = quasar.qscript.MapFuncDerived[T, A]
  type MapFunc[A]        = quasar.qscript.MapFunc[T, A]
  type FreeMapA[A]       = quasar.qscript.FreeMapA[T, A]
  type FreeMap           = quasar.qscript.FreeMap[T]
  type JoinFunc          = quasar.qscript.JoinFunc[T]
  type CoEnvQS[A]        = quasar.qscript.CoEnvQS[T, A]
  type CoEnvMapA[A, B]   = quasar.qscript.CoEnvMapA[T, A, B]
  type CoEnvMap[A]       = quasar.qscript.CoEnvMap[T, A]
  type CoEnvJoin[A]      = quasar.qscript.CoEnvJoin[T, A]
  type FreeQS            = quasar.qscript.FreeQS[T]
  type Ann               = quasar.qscript.Ann[T]
  type Target[F[_]]      = quasar.qscript.Target[T, F]
}

object TTypes {
  def simplifiableProjection[T[_[_]]] = new SimplifiableProjectionT[T]
}

class SimplifiableProjectionT[T[_[_]]] extends TTypes[T] {
  import SimplifyProjection._

  private lazy val simplify: EndoK[QScriptTotal] = simplifyQScriptTotal[T].simplifyProjection
  private def applyToBranch(branch: FreeQS): FreeQS = branch mapSuspension simplify

  def ProjectBucket[G[_]](implicit QC: QScriptCore :<: G) = make(
    λ[ProjectBucket ~> G] {
      case BucketField(src, value, field) => QC.inj(Map(src, Free.roll(MFC(MapFuncsCore.ProjectField(value, field)))))
      case BucketIndex(src, value, index) => QC.inj(Map(src, Free.roll(MFC(MapFuncsCore.ProjectIndex(value, index)))))
    }
  )

  def QScriptCore[G[_]](implicit QC: QScriptCore :<: G) = make(
    λ[QScriptCore ~> G](fa =>
      QC inj (fa match {
        case Union(src, lb, rb) =>
          Union(src, applyToBranch(lb), applyToBranch(rb))
        case Subset(src, lb, sel, rb) =>
          Subset(src, applyToBranch(lb), sel, applyToBranch(rb))
        case _ => fa
      })
    )
  )
  def ThetaJoin[G[_]](implicit TJ: ThetaJoin :<: G) = make(
    λ[ThetaJoin ~> G](tj =>
      TJ inj quasar.qscript.ThetaJoin(
        tj.src,
        applyToBranch(tj.lBranch),
        applyToBranch(tj.rBranch),
        tj.on,
        tj.f,
        tj.combine
      )
    )
  )
  def EquiJoin[G[_]](implicit EJ: EquiJoin :<: G) = make(
    λ[EquiJoin ~> G](ej =>
      EJ inj quasar.qscript.EquiJoin(
        ej.src,
        applyToBranch(ej.lBranch),
        applyToBranch(ej.rBranch),
        ej.lKey,
        ej.rKey,
        ej.f,
        ej.combine
      )
    )
  )
}
