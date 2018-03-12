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

package quasar.contrib.spire.random

import scalaz.{Functor, Monad, Zip}
import spire.random.Dist

trait DistInstances {
  implicit val distMonad: Monad[Dist] =
    new Monad[Dist] {
      def point[A](a: => A) =
        Dist.constant(a)

      def bind[A, B](fa: Dist[A])(f: A => Dist[B]) =
        fa flatMap f

      override def map[A, B](fa: Dist[A])(f: A => B) =
        fa map f
    }

  implicit val distZip: Zip[Dist] =
    new Zip[Dist] {
      def zip[A, B](a: => Dist[A], b: => Dist[B]) =
        a zip b

      override def zipWith[A, B, C](a: => Dist[A], b: => Dist[B])(f: (A, B) => C)(implicit F: Functor[Dist]) =
        a.zipWith(b)(f)
    }
}

object dist extends DistInstances
