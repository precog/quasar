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

package quasar.sql

import quasar.Predef._
import quasar._, RenderTree.ops._

import monocle.macros.Lenses
import scalaz._, Scalaz._

@Lenses final case class Blob[T](expr: T, scope: List[Statement[T]]) {
  def mapExpressionM[M[_]:Functor](f: T => M[T]) =
    f(expr).map(Blob(_, scope))
}

object Blob {
  implicit def renderTree[T:RenderTree]: RenderTree[Blob[T]] =
    new RenderTree[Blob[T]] {
      def render(blob: Blob[T]) =
        NonTerminal("Sql Blob" :: Nil, None, List(blob.scope.render, blob.expr.render))
    }
  implicit val traverse: Traverse[Blob] = new Traverse[Blob] {
    def traverseImpl[G[_]:Applicative,A,B](ba: Blob[A])(f: A => G[B]): G[Blob[B]] =
      (f(ba.expr) |@| ba.scope.traverse(_.traverse(f)))(Blob(_, _))
  }
}
