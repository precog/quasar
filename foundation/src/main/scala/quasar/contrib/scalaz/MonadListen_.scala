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

import scalaz._, Scalaz._

/** A version of MonadListen that doesn't extend Monad to avoid ambiguous implicits
  * in the presence of multiple "mtl" constraints.
  */
trait MonadListen_[F[_], W] {
  def listen[A](fa: F[A]): F[(A, W)]

  def pass[A](fa: F[(A, W => W)])(implicit B: Bind[F], T: MonadTell_[F, W]): F[A] =
    B.bind(listen(fa)) { case ((a, f), w) => T.writer(f(w), a) }
}

object MonadListen_ extends MonadListen_Instances {
  def apply[F[_], W](implicit L: MonadListen_[F, W]): MonadListen_[F, W] = L
}

sealed abstract class MonadListen_Instances extends MonadListen_Instances0 {
  implicit def eitherTMonadListen[F[_]: Functor, W, E](implicit L: MonadListen_[F, W]): MonadListen_[EitherT[F, E, ?], W] =
    new MonadListen_[EitherT[F, E, ?], W] {
      def listen[A](fa: EitherT[F, E, A]) =
        EitherT(L.listen(fa.run) map { case (d, w) => d strengthR w })
    }

  implicit def writerTMonadListen[F[_]: Functor, W1, W2](implicit L: MonadListen_[F, W1]): MonadListen_[WriterT[F, W2, ?], W1] =
    new MonadListen_[WriterT[F, W2, ?], W1] {
      def listen[A](fa: WriterT[F, W2, A]) =
        WriterT(L.listen(fa.run) map { case ((w2, a), w1) => (w2, (a, w1)) })
    }

  implicit def kleisliMonadListen[F[_], R, W](
      implicit F: MonadListen_[F, W]): MonadListen_[Kleisli[F, R, ?], W] =
    new MonadListen_[Kleisli[F, R, ?], W] {
      def listen[A](fa: Kleisli[F, R, A]): Kleisli[F, R, (A, W)] =
        fa.mapK(MonadListen_[F, W].listen[A])
    }
}

sealed abstract class MonadListen_Instances0 {
  implicit def monadListenNoMonad[F[_], W](implicit L: MonadListen[F, W]): MonadListen_[F, W] =
    new MonadListen_[F, W] {
      def listen[A](fa: F[A]) = L.listen(fa)
    }
}
