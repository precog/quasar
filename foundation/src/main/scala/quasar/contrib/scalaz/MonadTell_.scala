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

package quasar.contrib.scalaz

import quasar.Predef.Unit

import scalaz._, Scalaz._

/** A version of MonadTell that doesn't extend Monad to avoid ambiguous implicits
  * in the presence of multiple "mtl" constraints.
  */
trait MonadTell_[F[_], W] {
  def writer[A](w: W, a: A): F[A]
  def tell(w: W): F[Unit] = writer(w, ())
}

object MonadTell_ extends MonadTell_Instances {
  def apply[F[_], W](implicit T: MonadTell_[F, W]): MonadTell_[F, W] = T
}

sealed abstract class MonadTell_Instances extends MonadTell_Instances0 {
  implicit def eitherTMonadTell[F[_]: Functor, W, E](implicit T: MonadTell_[F, W]): MonadTell_[EitherT[F, E, ?], W] =
    new MonadTell_[EitherT[F, E, ?], W] {
      def writer[A](w: W, a: A) = EitherT(T.writer(w, a) map (_.right[E]))
    }

  implicit def writerTMonadTell[F[_]: Functor, W1, W2: Monoid](implicit T: MonadTell_[F, W1]): MonadTell_[WriterT[F, W2, ?], W1] =
    new MonadTell_[WriterT[F, W2, ?], W1] {
      def writer[A](w: W1, a: A) = WriterT(T.writer(w, a) strengthL mzero[W2])
    }
}

sealed abstract class MonadTell_Instances0 {
  implicit def monadTellNoMonad[F[_], W](implicit F: MonadTell[F, W]): MonadTell_[F, W] =
    new MonadTell_[F, W] {
      def writer[A](w: W, a: A) = F.writer(w, a)
    }
}

final class MonadTell_Ops[F[_], W, A] private[scalaz] (self: F[A])(implicit F: MonadTell_[F, W]) {
  final def :++>(w: ⇒ W)(implicit B: Bind[F]): F[A] =
    B.bind(self)(a => B.map(F.tell(w))(_ => a))

  final def :++>>(f: A ⇒ W)(implicit B: Bind[F]): F[A] =
    B.bind(self)(a => B.map(F.tell(f(a)))(_ => a))
}
