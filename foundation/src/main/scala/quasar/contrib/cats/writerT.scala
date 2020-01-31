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

import quasar.contrib.scalaz.{MonadError_, MonadListen_, MonadTell_}

import cats.{Applicative, Functor, Monoid}
import cats.data.WriterT
import cats.syntax.functor._

object writerT extends WriterTInstances {
  implicit def catsWriterTMonadError_[E, F[_]: Functor: MonadError_[?[_], E], W: Monoid]
      : MonadError_[WriterT[F, W, ?], E] =
    new MonadError_[WriterT[F, W, ?], E] {
      def raiseError[A](e: E): WriterT[F, W, A] =
        WriterT(MonadError_[F, E].raiseError[(W, A)](e))

      def handleError[A](fa: WriterT[F, W, A])(f: E => WriterT[F, W, A]): WriterT[F, W, A] =
        WriterT(MonadError_[F, E].handleError(fa.run)(e => f(e).run))
    }

  implicit def catsWriterTNestedMonadTell_[W: Monoid, X, F[_]: Applicative: MonadTell_[?[_], X]]: MonadTell_[WriterT[F, W, ?], X] =
    new MonadTell_[WriterT[F, W, ?], X] {
      def writer[A](x: X, a: A) = WriterT.liftF(MonadTell_[F, X].writer(x, a))
    }
}

sealed abstract class WriterTInstances {
  implicit def catsWriterTMonadListen_[F[_]: Functor, W]: MonadListen_[WriterT[F, W, ?], W] =
    new MonadListen_[WriterT[F, W, ?], W] {
      def listen[A](fa: WriterT[F, W, A]) =
        WriterT(fa.run.map { case (w, a) => (w, (a, w)) })
    }

  implicit def catsWriterTMonadTell_[F[_]: Applicative, W]: MonadTell_[WriterT[F, W, ?], W] =
    new MonadTell_[WriterT[F, W, ?], W] {
      def writer[A](w: W, a: A) = WriterT.put(a)(w)
    }
}
