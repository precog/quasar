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

package quasar.contrib

import slamdata.Predef._

import _root_.scalaz._, Leibniz._, Scalaz._
import _root_.shapeless._

package object shapeless {
  // TODO generalize this and contribute to shapeless-contrib
  implicit class FuncUtils[A, N <: Nat](val self: Sized[List[A], N]) extends scala.AnyVal {
    def reverse: Sized[List[A], N] =
      Sized.wrap[List[A], N](self.unsized.reverse)

    def foldMap[B](f: A => B)(implicit F: Monoid[B]): B =
      self.unsized.foldMap(f)

    def foldRight[B](z: => B)(f: (A, => B) => B): B =
      Foldable[List].foldRight(self.unsized, z)(f)

    def traverse[G[_], B](f: A => G[B])(implicit G: Applicative[G]): G[Sized[List[B], N]] =
      G.map(self.unsized.traverse(f))(bs => Sized.wrap[List[B], N](bs))

    // Input[Option[B], N] -> Option[Input[B, N]]
    def sequence[G[_], B](implicit ev: A === G[B], G: Applicative[G]): G[Sized[List[B], N]] =
      G.map(self.unsized.sequence(ev, G))(bs => Sized.wrap[List[B], N](bs))

    def zip[B](input: Sized[List[B], N]): Sized[List[(A, B)], N] =
      Sized.wrap[List[(A, B)], N](self.unsized.zip(input))

    def unzip3[X, Y, Z](implicit ev: A === (X, (Y, Z))): (Sized[List[X], N], Sized[List[Y], N], Sized[List[Z], N]) =
      Unzip[List].unzip3[X, Y, Z](ev.subst(self.unsized)) match {
        case (x, y, z) => (Sized.wrap[List[X], N](x), Sized.wrap[List[Y], N](y), Sized.wrap[List[Z], N](z))
      }
  }
}
