/*
 * Copyright 2014–2019 SlamData Inc.
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

import slamdata.Predef.{Boolean, None, Option, Some, Unit}
import quasar.Condition
import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.Datasource
import quasar.contrib.scalaz.MonadError_

import scalaz.Monad

final class ConditionReportingDatasource[
    E, F[_]: Monad: MonadError_[?[_], E], G[_], Q, R, P <: ResourcePathType] private (
    report: Condition[E] => F[Unit],
    val underlying: Datasource[F, G, Q, R, P])
    extends Datasource[F, G, Q, R, P] {

  val kind: DatasourceType = underlying.kind

  def evaluate(query: Q): F[R] =
    reportCondition(underlying.evaluate(query))

  def pathIsResource(path: ResourcePath): F[Boolean] =
    reportCondition(underlying.pathIsResource(path))

  def prefixedChildPaths(path: ResourcePath)
      : F[Option[G[(ResourceName, P)]]] =
    reportCondition(underlying.prefixedChildPaths(path))

  ////

  private def reportCondition[A](fa: F[A]): F[A] =
    MonadError_[F, E].ensuring(fa) {
      case Some(e) => report(Condition.abnormal(e))
      case None    => report(Condition.normal())
    }
}

object ConditionReportingDatasource {
  def apply[E, F[_]: Monad: MonadError_[?[_], E], G[_], Q, R, P <: ResourcePathType](
      f: Condition[E] => F[Unit],
      ds: Datasource[F, G, Q, R, P])
      : Datasource[F, G, Q, R, P] =
    new ConditionReportingDatasource[E, F, G, Q, R, P](f, ds)
}
