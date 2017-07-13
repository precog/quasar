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

import quasar.fp.ski._

import scalaz._

trait KleisliInstances {
  implicit def kleisliMonadState[F[_], S, R](implicit F: MonadState[F, S]): MonadState[Kleisli[F, R, ?], S] =
    new MonadState[Kleisli[F, R, ?], S] {
      def init = Kleisli(κ(F.init))
      def get = Kleisli(κ(F.get))
      def put(s: S) = Kleisli(κ(F.put(s)))
      def point[A](a: => A) = Kleisli(κ(F.point(a)))
      override def map[A, B](fa: Kleisli[F, R, A])(f: A => B) = fa map f
      def bind[A, B](fa: Kleisli[F, R, A])(f: A => Kleisli[F, R, B]) = fa flatMap f
    }
}

object kleisli extends KleisliInstances
