/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.impl.datasource

import slamdata.Predef.{Boolean, Option}
import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.Datasource
import quasar.contrib.scalaz.MonadError_

import scalaz.Applicative

final class FailedDatasource[
    E,
    F[_]: Applicative: MonadError_[?[_], E],
    G[_], Q, R, P <: ResourcePathType] private (
    datasourceType: DatasourceType,
    error: E)
    extends Datasource[F, G, Q, R, P] {

  val kind: DatasourceType = datasourceType

  def evaluate(query: Q): F[R] =
    MonadError_[F, E].raiseError(error)

  def pathIsResource(path: ResourcePath): F[Boolean] =
    MonadError_[F, E].raiseError(error)

  def prefixedChildPaths(path: ResourcePath)
      : F[Option[G[(ResourceName, P)]]] =
    MonadError_[F, E].raiseError(error)
}

object FailedDatasource {
  def apply[
      E,
      F[_]: Applicative: MonadError_[?[_], E],
      G[_], Q, R, P <: ResourcePathType](
      kind: DatasourceType,
      error: E)
      : Datasource[F, G, Q, R, P] =
    new FailedDatasource[E, F, G, Q, R, P](kind, error)
}
