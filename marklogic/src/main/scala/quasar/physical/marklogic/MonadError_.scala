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

package quasar.physical.marklogic

import scalaz.MonadError

/** A version of MonadError that doesn't extend Monad to avoid ambiguous implicits
  * in the presence of multiple "mtl" constraints.
  */
trait MonadError_[F[_], E] { self =>
  def raiseError[A](e: E): F[A]
  def handleError[A](fa: F[A])(f: E => F[A]): F[A]
}

object MonadError_ {
  def apply[F[_], E](implicit F: MonadError_[F, E]): MonadError_[F, E] = F

  implicit def monadErrorNoMonad[F[_], E](implicit F: MonadError[F, E]): MonadError_[F, E] =
    new MonadError_[F, E] {
      def raiseError[A](e: E): F[A] = F.raiseError(e)
      def handleError[A](fa: F[A])(f: E => F[A]): F[A] = F.handleError(fa)(f)
    }
}
