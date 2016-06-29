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

package quasar.fp

import matryoshka.∘
import org.scalacheck._
import scalaz._

object Helpers {
  // NB: Should be exposed via matryoshka-scalacheck, which doesn’t yet exist.
  /** A “direct” Arbitrary from a “deferred” Arbitrary.
    */
  implicit def NTArbitrary[F[_], A](
    implicit F: Arbitrary ~> (Arbitrary ∘ F)#λ, A: Arbitrary[A]):
      Arbitrary[F[A]] =
    F(A)
}
