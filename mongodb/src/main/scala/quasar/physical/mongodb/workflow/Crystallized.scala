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

package quasar.physical.mongodb.workflow

import quasar.RenderTree, RenderTree.ops._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._

/** A type for a `Workflow` which has had `crystallize` applied to it. */
final case class Crystallized[F[_]](op: Fix[F]) {
  def inject[G[_]: Functor](implicit F: Functor[F], I: F :<: G): Crystallized[G] =
    copy(op = op.transCata[Fix[G]](I.inj(_)))
}

object Crystallized {
  implicit def renderTree[F[_]](implicit R: RenderTree[Fix[F]]):
      RenderTree[Crystallized[F]] =
    new RenderTree[Crystallized[F]] {
      def render(v: Crystallized[F]) = v.op.render
    }
}
