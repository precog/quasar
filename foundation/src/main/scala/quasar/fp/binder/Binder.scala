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

package quasar.fp.binder

import quasar.Predef._

import matryoshka.Recursive
import scalaz._
import simulacrum.typeclass

@typeclass trait Binder[F[_]] {
  type G[A]
  def G: Traverse[G]

  def initial[A]: G[A]

  // Extracts bindings from a node:
  def bindings[T, A](t: F[T], b: G[A])(f: F[T] => A)(implicit T: Recursive.Aux[T, F]): G[A]

  // Possibly binds a free term to its definition:
  def subst[T, A](t: F[T], b: G[A])(implicit T: Recursive.Aux[T, F]): Option[A]
}
