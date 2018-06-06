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

package quasar

import iotaz.TListK.:::
import iotaz.{ CopK, TNilK }

package object ejson {

  /** For _strict_ JSON, you want something like `Obj[Mu[Json]]`.
    */
  type Json[A]    = CopK[Obj ::: Common ::: TNilK, A]
  val ObjJson     = CopK.Inject[Obj, Json]
  val CommonJson  = CopK.Inject[Common, Json]

  type EJsonL     = Extension ::: Common ::: TNilK
  type EJson[A]   = CopK[EJsonL, A]
  val ExtEJson    = CopK.Inject[Extension, EJson]
  val CommonEJson = CopK.Inject[Common, EJson]
}
