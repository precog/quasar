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

package quasar

import slamdata.Predef._
import quasar.contrib.scalaz.{MonadListen_, MonadTell_}

import scalaz._

package object common {
  type PhaseResults = Vector[PhaseResult]
  type PhaseResultW[A] = Writer[PhaseResults, A]
  type PhaseResultT[F[_], A] = WriterT[F, PhaseResults, A]

  type PhaseResultTell[F[_]] = MonadTell_[F, PhaseResults]

  object PhaseResultTell {
    def apply[F[_]](implicit F: PhaseResultTell[F]) = F
  }

  type PhaseResultListen[F[_]] = MonadListen_[F, PhaseResults]

  object PhaseResultListen {
    def apply[F[_]](implicit F: PhaseResultListen[F]) = F
  }
}
