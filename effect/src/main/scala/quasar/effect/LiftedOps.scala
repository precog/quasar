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

package quasar.effect

import quasar.fp._

import scalaz._

/** Encapsulates boilerplate useful in defining lifted operations on free
  * monads over effect algebras.
  */
abstract class LiftedOps[G[_], S[_] <: ACopK](implicit S: G :<<: S) {
  type FreeS[A] = Free[S, A]

  def lift[A](ga: G[A]): FreeS[A] =
    free.lift(ga).into[S]
}
