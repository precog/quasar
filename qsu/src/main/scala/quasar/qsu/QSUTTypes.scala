/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.qsu

import quasar.qscript.TTypes

trait QSUTTypes[T[_[_]]] extends TTypes[T] {
  type QAuth[P] = quasar.qsu.QAuth[T, P]
  type QProv = quasar.qsu.QProv[T]
  type FreeAccess[A] = quasar.qsu.FreeAccess[T, A]
  type QSUGraph = quasar.qsu.QSUGraph[T]
  type RevIdx = quasar.qsu.QSUGraph.RevIdx[T]
  type RevIdxM[F[_]] = quasar.qsu.RevIdxM[T, F]
  type References = quasar.qsu.References[T]
  type QScriptUniform[A] = quasar.qsu.QScriptUniform[T, A]
}
