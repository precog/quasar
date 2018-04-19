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

package quasar.qsu

import quasar.ejson.EJson
import quasar.qscript.TTypes

trait QSUTTypes[T[_[_]]] extends TTypes[T] {
  type QAuth = quasar.qsu.QAuth[T]
  type QDims = quasar.qsu.QDims[T]
  type QIdAccess = quasar.qsu.QIdAccess[T]
  type QAccess[A] = quasar.qsu.QAccess[T, A]
  type FreeAccess[A] = quasar.qsu.FreeAccess[T, A]
  type QSUGraph = quasar.qsu.QSUGraph[T]
  type RevIdx = quasar.qsu.QSUGraph.RevIdx[T]
  type RevIdxM[F[_]] = quasar.qsu.RevIdxM[T, F]
  type References = quasar.qsu.References[T, T[EJson]]
  type QScriptUniform[A] = quasar.qsu.QScriptUniform[T, A]
  type QScriptEducated[A] = quasar.qscript.QScriptEducated[T, A]
}
