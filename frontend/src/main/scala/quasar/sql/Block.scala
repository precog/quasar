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

import slamdata.Predef._
import quasar._, RenderTree.ops._

import monocle.macros.Lenses
import scalaz._, Scalaz._

@Lenses final case class Block[T](expr: T, defs: List[FunctionDecl[T]]) {
  def mapExpressionM[M[_]:Functor](f: T => M[T]): M[Block[T]] =
    f(expr).map(Block(_, defs))
}

object Block {
  implicit def renderTree[T:RenderTree]: RenderTree[Block[T]] =
    new RenderTree[Block[T]] {
      def render(block: Block[T]) =
        NonTerminal("Sql Block" :: Nil, None, List(block.defs.render, block.expr.render))
    }
  implicit val traverse: Traverse[Block] = new Traverse[Block] {
    def traverseImpl[G[_]:Applicative,A,B](ba: Block[A])(f: A => G[B]): G[Block[B]] =
      (f(ba.expr) |@| ba.defs.traverse(_.traverse(f)))(Block(_, _))
  }
}
