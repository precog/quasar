/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.contrib.cats

import quasar.contrib.scalaz.MonadError_

import scala.util.Either

import cats.{Monad, MonadError}

object monadError {
  // Not implicit as can easily introduce ambiguity
  def monadError_CatsMonadError[F[_], E](
      implicit m: Monad[F], me: MonadError_[F, E])
      : MonadError[F, E] =
    new MonadError[F, E] {
      def raiseError[A](e: E): F[A] = me.raiseError(e)
      def handleErrorWith[A](fa: F[A])(f: E => F[A]): F[A] = me.handleError(fa)(f)
      def pure[A](x: A): F[A] = m.pure(x)
      def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B] = m.flatMap(fa)(f)
      def tailRecM[A, B](a: A)(f: A => F[Either[A,B]]): F[B] = m.tailRecM(a)(f)
    }
}
