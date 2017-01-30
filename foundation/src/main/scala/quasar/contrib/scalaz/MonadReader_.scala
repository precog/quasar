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

import scalaz._, Scalaz._

/** A version of MonadReader that doesn't extend Monad to avoid ambiguous implicits
  * in the presence of multiple "mtl" constraints.
  */
trait MonadReader_[F[_], R] {
  def ask: F[R]
  def local[A](f: R => R)(fa: F[A]): F[A]
  def scope[A](k: R)(fa: F[A]): F[A] = local(_ => k)(fa)
  def asks[A](f: R => A)(implicit F: Functor[F]): F[A] = F.map(ask)(f)
}

object MonadReader_ extends MonadReader_Instances {
  def apply[F[_], R](implicit F: MonadReader_[F, R]): MonadReader_[F, R] = F

  implicit def monadReaderNoMonad[F[_], R](implicit F: MonadReader[F, R]): MonadReader_[F, R] =
    new MonadReader_[F, R] {
      def ask = F.ask
      def local[A](f: R => R)(fa: F[A]) = F.local(f)(fa)
    }
}

sealed abstract class MonadReader_Instances {
  implicit def eitherTMonadReader_[F[_]: Functor, E, R](implicit R: MonadReader_[F, R]): MonadReader_[EitherT[F, E, ?], R] =
    new MonadReader_[EitherT[F, E, ?], R] {
      def ask = EitherT(R.ask map (_.right[E]))
      def local[A](f: R => R)(fa: EitherT[F, E, A]) = EitherT(R.local(f)(fa.run))
    }

  implicit def writerTMonadReader_[F[_]: Functor, W: Monoid, R](implicit R: MonadReader_[F, R]): MonadReader_[WriterT[F, W, ?], R] =
    new MonadReader_[WriterT[F, W, ?], R] {
      def ask = WriterT(R.ask strengthL mzero[W])
      def local[A](f: R => R)(fa: WriterT[F, W, A]) = WriterT(R.local(f)(fa.run))
    }

  implicit def readerTInnerMonadReader_[F[_], R1, R2](implicit R: MonadReader_[F, R1]): MonadReader_[ReaderT[F, R2, ?], R1] =
    new MonadReader_[ReaderT[F, R2, ?], R1] {
      def ask = ReaderT(_ => R.ask)
      def local[A](f: R1 => R1)(fa: ReaderT[F, R2, A]) = ReaderT(r2 => R.local(f)(fa.run(r2)))
    }
}
