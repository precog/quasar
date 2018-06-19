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

package quasar.fp.free

import quasar.contrib.iota.{:<<:, ACopK}
import scalaz._

object lift {
  final class LifterAux[F[_], A](fa: F[A]) {

    def into[G[_]](implicit I: F :<: G): Free[G, A] =
      Free.liftF(I.inj(fa))

    def intoCopK[G[a] <: ACopK[a]](implicit I: F :<<: G): Free[G, A] =
      Free.liftF(I.inj(fa))
  }

  def apply[F[_], A](fa: F[A]): LifterAux[F, A] =
    new LifterAux(fa)
}
