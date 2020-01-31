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

import scalaz.{Applicative, PlusEmpty}
import scalaz.std.option._
import scalaz.syntax.applicative._
import scalaz.syntax.plusEmpty._

final class EmptyDatasource[F[_]: Applicative, G[_]: PlusEmpty, Q, R, P <: ResourcePathType] private (
    val kind: DatasourceType,
    emptyResult: R)
    extends Datasource[F, G, Q, R, P] {

  def evaluate(q: Q): F[R] =
    emptyResult.pure[F]

  def pathIsResource(path: ResourcePath): F[Boolean] =
    false.pure[F]

  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[G[(ResourceName, P)]]] =
    ResourcePath.root
      .getOption(prefixPath)
      .as(mempty[G, (ResourceName, P)])
      .pure[F]
}

object EmptyDatasource {
  def apply[F[_]: Applicative, G[_]: PlusEmpty, Q, R, P <: ResourcePathType](
      kind: DatasourceType,
      emptyResult: R)
      : Datasource[F, G, Q, R, P] =
    new EmptyDatasource[F, G, Q, R, P](kind, emptyResult)
}
