/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.contrib.cats.effect

import cats.effect.{IO, LiftIO}
import scalaz.{EitherT, Monad, Monoid, ReaderT, StateT, WriterT}
import scalaz.syntax.either._

trait LiftIOScalazInstances {
  implicit def scalazEitherTLiftIO[F[_]: LiftIO, E]: LiftIO[EitherT[F, E, ?]] =
    new LiftIO[EitherT[F, E, ?]] {
      def liftIO[A](ioa: IO[A]): EitherT[F, E, A] =
        EitherT(ioa.map(_.right[E]).to[F])
    }

  implicit def scalazReaderTLiftIO[F[_]: LiftIO, R]: LiftIO[ReaderT[F, R, ?]] =
    new LiftIO[ReaderT[F, R, ?]] {
      def liftIO[A](ioa: IO[A]): ReaderT[F, R, A] =
        ReaderT(_ => ioa.to[F])
    }

  implicit def scalazStateTLiftIO[F[_]: LiftIO: Monad, S]: LiftIO[StateT[F, S, ?]] =
    new LiftIO[StateT[F, S, ?]] {
      def liftIO[A](ioa: IO[A]): StateT[F, S, A] =
        StateT(s => ioa.map((s, _)).to[F])
    }

  implicit def scalazWriterTLiftIO[F[_]: LiftIO, W: Monoid]: LiftIO[WriterT[F, W, ?]] =
    new LiftIO[WriterT[F, W, ?]] {
      def liftIO[A](ioa: IO[A]): WriterT[F, W, A] =
        WriterT.writerT(ioa.map((Monoid[W].zero, _)).to[F])
    }
}

object liftio extends LiftIOScalazInstances
