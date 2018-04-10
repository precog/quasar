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

package quasar.qscript

/** Centralizes the knowledge of T[_[_]].
 *  This is for compilation performance.
 */

trait TTypes[T[_[_]]] {
  // Partially applying types with the known T.
  // In this context we shouldn't often need to refer to the original type
  // any longer, so reuse the name.
  type QScriptCore[A]    = quasar.qscript.QScriptCore[T, A]
  type QScriptTotal[A]   = quasar.qscript.QScriptTotal[T, A]
  type ProjectBucket[A]  = quasar.qscript.ProjectBucket[T, A]
  type ThetaJoin[A]      = quasar.qscript.ThetaJoin[T, A]
  type EquiJoin[A]       = quasar.qscript.EquiJoin[T, A]
  type MapFuncCore[A]    = quasar.qscript.MapFuncCore[T, A]
  type MapFuncDerived[A] = quasar.qscript.MapFuncDerived[T, A]
  type MapFunc[A]        = quasar.qscript.MapFunc[T, A]
  type FreeMapA[A]       = quasar.qscript.FreeMapA[T, A]
  type FreeMap           = quasar.qscript.FreeMap[T]
  type RecFreeMap        = quasar.qscript.RecFreeMap[T]
  type RecFreeMapA[A]    = quasar.qscript.RecFreeMapA[T, A]
  type JoinFunc          = quasar.qscript.JoinFunc[T]
  type CoEnvQS[A]        = quasar.qscript.CoEnvQS[T, A]
  type CoEnvMapA[A, B]   = quasar.qscript.CoEnvMapA[T, A, B]
  type CoEnvMap[A]       = quasar.qscript.CoEnvMap[T, A]
  type FreeQS            = quasar.qscript.FreeQS[T]
}
