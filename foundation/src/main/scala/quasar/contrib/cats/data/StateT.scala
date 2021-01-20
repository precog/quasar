/*
 * Copyright 2020 Precog Data
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

package quasar.contrib.cats.data

import quasar.contrib.scalaz.{MonadError_, MonadTell_}

import cats.{Applicative, Monad}
import cats.data.StateT

object stateT {
  implicit def catsDataStateTMonadError_[E, F[_]: Monad: MonadError_[*[_], E], S]
      : MonadError_[StateT[F, S, *], E] =
    new MonadError_[StateT[F, S, *], E] {
      def raiseError[B](e: E): StateT[F, S, B] =
        StateT.liftF(MonadError_[F, E].raiseError[B](e))

      def handleError[B](fb: StateT[F, S, B])(f: E => StateT[F, S, B]): StateT[F, S, B] = StateT { (s: S) =>
        MonadError_[F, E].handleError(fb.run(s))(e => f(e).run(s))
      }
    }

  implicit def catsDataStateTMonadTell_[W, F[_]: Applicative: MonadTell_[*[_], W], S]
      : MonadTell_[StateT[F, S, *], W] =
    new MonadTell_[StateT[F, S, *], W] {
      def writer[B](w: W, b: B): StateT[F, S, B] =
        StateT.liftF(MonadTell_[F, W].writer(w, b))
    }
}
