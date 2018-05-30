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

package quasar.evaluate

import quasar.api.ResourcePath

import monocle.macros.Lenses
import scalaz.{Applicative, Cord, Equal, Order, Show, Traverse}
import scalaz.std.tuple._
import scalaz.syntax.functor._
import scalaz.syntax.show._

@Lenses
final case class Source[A](path: ResourcePath, src: A) {
  def map[B](f: A => B): Source[B] =
    Source(path, f(src))
}

object Source extends SourceInstances

sealed abstract class SourceInstances extends SourceInstances0 {
  implicit def order[A: Order]: Order[Source[A]] =
    Order.orderBy {
      case Source(p, a) => (p, a)
    }

  implicit def show[A: Show]: Show[Source[A]] =
    Show.show {
      case Source(p, a) =>
        Cord("Source(") ++ p.show ++ Cord(", ") ++ a.show ++ Cord(")")
    }

  implicit val traverse: Traverse[Source] =
    new Traverse[Source] {
      override def map[A, B](sa: Source[A])(f: A => B) =
        sa map f

      def traverseImpl[F[_]: Applicative, A, B](fa: Source[A])(f: A => F[B]) =
        f(fa.src) map (Source(fa.path, _))
    }
}

sealed abstract class SourceInstances0 {
  implicit def equal[A: Equal]: Equal[Source[A]] =
    Equal.equalBy {
      case Source(p, a) => (p, a)
    }
}
