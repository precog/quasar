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

package quasar.contrib.fs2

import cats.effect.{IO, LiftIO}
import fs2.Stream
import scalaz.ApplicativePlus

trait StreamInstances {
  implicit def streamLiftIO[F[_]: LiftIO]: LiftIO[Stream[F, ?]] =
    new LiftIO[Stream[F, ?]] {
      def liftIO[A](ioa: IO[A]): Stream[F, A] =
        Stream.eval(LiftIO[F].liftIO(ioa))
    }

  implicit def streamApplicativePlus[F[_]]: ApplicativePlus[Stream[F, ?]] =
    new ApplicativePlus[Stream[F, ?]] {
      def plus[A](x: Stream[F, A], y: => Stream[F, A]) =
        x ++ y

      def empty[A] = Stream.empty

      def ap[A, B](fa: => Stream[F, A])(ff: => Stream[F, A => B]) =
        for {
          a <- fa
          f <- ff
        } yield f(a)

      def point[A](a: => A) =
        Stream.emit(a)

      override def map[A, B](fa: Stream[F, A])(f: A => B) =
        fa map f
    }
}

object stream extends StreamInstances
