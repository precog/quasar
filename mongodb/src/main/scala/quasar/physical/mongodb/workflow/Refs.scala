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

package quasar.physical.mongodb.workflow

import slamdata.Predef._
import quasar.fp._
import quasar.physical.mongodb.expression.DocVar

import scalaz._
import simulacrum.typeclass

@typeclass trait Refs[F[_]] {
  def refs[A](op: F[A]): List[DocVar]
}

object Refs {
  def fromRewrite[F[_]](rewrite: PartialFunction[DocVar, DocVar] => RewriteRefs[F]) = new Refs[F] {
    def refs[A](op: F[A]): List[DocVar] = {
      // FIXME: Sorry world
      val vf = new scala.collection.mutable.ListBuffer[DocVar]
      ignore(rewrite { case v => ignore(vf += v); v } (op))
      vf.toList
    }
  }

  implicit def coproduct[F[_], G[_]](implicit RF: Refs[F], RG: Refs[G]):
      Refs[Coproduct[F, G, ?]] =
    new Refs[Coproduct[F, G, ?]] {
      def refs[A](op: Coproduct[F, G, A]) = op.run.fold(RF.refs, RG.refs)
    }
}
