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
import quasar.fp.ski._

import simulacrum.typeclass
import scalaz._

@typeclass trait Classify[F[_]] {
  def source[A](op: F[A]):          Option[SourceF[F, A]]

  def singleSource[A](op: F[A]):    Option[SingleSourceF[F, A]]
  def pipeline[A](op: F[A]):        Option[PipelineF[F, A]]
  def shapePreserving[A](op: F[A]): Option[ShapePreservingF[F, A]]
}

object Classify {
  implicit def coproductClassify[F[_]: Functor, G[_]: Functor, A]
    (implicit CF: Classify[F], CG: Classify[G])
      : Classify[Coproduct[F, G, ?]] = new Classify[Coproduct[F, G, ?]] {
    def source[A](v: Coproduct[F, G, A]) =
      v.run.fold(
        CF.source(_).map(_.fmap(Coproduct.leftc[F, G, A](_))),
        CG.source(_).map(_.fmap(Coproduct.rightc[F, G, A](_))))

    def singleSource[A](v: Coproduct[F, G, A]) =
      v.run.fold(
        CF.singleSource(_).map(_.fmap(ι, Inject[F, Coproduct[F, G, ?]])),
        CG.singleSource(_).map(_.fmap(ι, Inject[G, Coproduct[F, G, ?]])))

    def pipeline[A](v: Coproduct[F, G, A]) =
      v.run.fold(
        CF.pipeline(_).map(_.fmap(ι, Inject[F, Coproduct[F, G, ?]])),
        CG.pipeline(_).map(_.fmap(ι, Inject[G, Coproduct[F, G, ?]])))

    def shapePreserving[A](v: Coproduct[F, G, A]) =
      v.run.fold(
        CF.shapePreserving(_).map(_.fmap(ι, Inject[F, Coproduct[F, G, ?]])),
        CG.shapePreserving(_).map(_.fmap(ι, Inject[G, Coproduct[F, G, ?]])))
  }
}
