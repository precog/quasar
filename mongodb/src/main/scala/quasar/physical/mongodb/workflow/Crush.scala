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

import quasar.physical.mongodb.expression.DocVar
import quasar.physical.mongodb.workflowtask.WorkflowTask

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._
import simulacrum.typeclass

/** Operations that are applied to a completed workflow to produce an
  * executable WorkflowTask. NB: when this is applied, information about the
  * the type of plan (i.e. the required MongoDB version) is discarded.
  */
@typeclass trait Crush[F[_]] {
  /** Returns both the final WorkflowTask as well as a DocVar indicating the
    * base of the collection.
    */
  def crush[T[_[_]]: BirecursiveT](op: F[(T[F], (DocVar, WorkflowTask))])
      : (DocVar, WorkflowTask)
}
object Crush {
  implicit def injected[F[_]: Functor, G[_]: Functor](implicit I: F :<: G, CG: Crush[G]):
      Crush[F] =
    new Crush[F] {
      def crush[T[_[_]]: BirecursiveT](op: F[(T[F], (DocVar, WorkflowTask))]) =
        CG.crush(I.inj(op.map(_.leftMap(_.transCata[T[G]](I.inj(_))))))
    }
}
