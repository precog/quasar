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

package quasar.contrib.scalaz

import slamdata.Predef._

import scalaz.MonadState

/** A version of MonadState that doesn't extend Monad to avoid ambiguous
  * implicits in the presence of multiple "mtl" constraints.
  */
trait MonadState_[F[_], S] { self =>
  def MS: MonadState[F, S]

  def bind[A, B](fa: F[A])(f: (A) ⇒ F[B]): F[B]
  def get: F[S]
  def init: F[S]
  def point[A](a: ⇒ A): F[A]
  def put(s: S): F[Unit]
}

object MonadState_ {
  def apply[F[_], S](implicit F: MonadState_[F, S]): MonadState_[F, S] = F

  implicit def monadStateNoMonad[F[_], S](implicit F: MonadState[F, S])
      : MonadState_[F, S] =
    new MonadState_[F, S] {
      def MS = F

      def bind[A, B](fa: F[A])(f: (A) ⇒ F[B]) = F.bind(fa)(f)
      def get = F.get
      def init = F.init
      def point[A](a: ⇒ A) = F.point(a)
      def put(s: S) = F.put(s)
    }

  implicit def toMonadState[F[_], S](ms: MonadState_[F, S]): MonadState[F, S] =
    ms.MS
}
