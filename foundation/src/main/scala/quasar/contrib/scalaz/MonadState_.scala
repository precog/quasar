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

import scalaz.{Bind, Functor, Monad, MonadState, StateT}
import scalaz.syntax.monad._

/** A version of MonadState that doesn't extend Monad to avoid ambiguous
  * implicits in the presence of multiple "mtl" constraints.
  */
trait MonadState_[F[_], S] {
  def get: F[S]
  def put(s: S): F[Unit]

  def gets[A](f: S => A)(implicit F: Functor[F]): F[A] =
    F.map(get)(f)

  def modify(f: S => S)(implicit F: Bind[F]): F[Unit] =
    F.bind(get)(f andThen put)
}

object MonadState_ {
  def apply[F[_], S](implicit F: MonadState_[F, S]): MonadState_[F, S] = F

  implicit def monadStateNoMonad[F[_], S](implicit F: MonadState[F, S])
      : MonadState_[F, S] =
    new MonadState_[F, S] {
      def get = F.get
      def put(s: S) = F.put(s)
    }

  implicit def monadStateWithinState[F[_]: Monad, S1, S2](implicit F: MonadState_[F, S1])
      : MonadState_[StateT[F, S2, ?], S1] =
    new MonadState_[StateT[F, S2, ?], S1] {
      def get = F.get.liftM[StateT[?[_], S2, ?]]
      def put(s: S1) = F.put(s).liftM[StateT[?[_], S2, ?]]
    }
}
