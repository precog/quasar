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

package quasar.mimir.evaluate

import quasar.common.data.Data
import quasar.common.resource.ResourcePath
import quasar.qscript.QScriptEducated

import fs2.Stream
import scalaz.~>

sealed trait QueryAssociate[T[_[_]], F[_]]

object QueryAssociate {
  final case class Lightweight[T[_[_]], F[_]](f: ResourcePath => F[Stream[F, Data]])
      extends QueryAssociate[T, F]

  final case class Heavyweight[T[_[_]], F[_]](f: T[QScriptEducated[T, ?]] => F[Stream[F, Data]])
      extends QueryAssociate[T, F]

  def lightweight[T[_[_]], F[_]](f: ResourcePath => F[Stream[F, Data]])
      : QueryAssociate[T, F] =
    Lightweight(f)

  def heavyweight[T[_[_]], F[_]](f: T[QScriptEducated[T, ?]] => F[Stream[F, Data]])
      : QueryAssociate[T, F] =
    Heavyweight(f)

  def transformResult[T[_[_]], F[_], G[_]](
      qa: QueryAssociate[T, F])(
      f: λ[a => F[Stream[F, a]]] ~> λ[a => G[Stream[G, a]]])
      : QueryAssociate[T, G] =
    qa match {
      case Lightweight(k) => Lightweight(k andThen (f(_)))
      case Heavyweight(k) => Heavyweight(k andThen (f(_)))
    }
}
