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

@Lenses final case class Blob[T](expr: T, scope: List[FunctionDecl[T]])

object Blob {
  implicit def renderTree[T:RenderTree]: RenderTree[Blob[T]] =
    new RenderTree[Blob[T]] {
      def render(blob: Blob[T]) =
        NonTerminal("Sql Blob" :: Nil, None, List(blob.scope.render, blob.expr.render))
    }
}
