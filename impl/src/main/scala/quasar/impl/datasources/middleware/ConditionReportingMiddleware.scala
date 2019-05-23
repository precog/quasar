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

package quasar.impl.datasources.middleware

import slamdata.Predef.{Exception, Unit}
import quasar.Condition
import quasar.api.resource.ResourcePathType
import quasar.connector.Datasource
import quasar.contrib.scalaz.MonadError_
import quasar.impl.datasource.ConditionReportingDatasource
import quasar.impl.datasources.ManagedDatasource

import scalaz.{Monad, ~>}
import scalaz.syntax.functor._

object ConditionReportingMiddleware {
  def apply[F[_], I](onChange: (I, Condition[Exception]) => F[Unit]): PartiallyApplied[F, I] =
    new PartiallyApplied(onChange)

  final class PartiallyApplied[F[_], I](onChange: (I, Condition[Exception]) => F[Unit]) {
    def apply[T[_[_]], G[_], R, P <: ResourcePathType](
        id: I, mds: ManagedDatasource[T, F, G, R, P])(
        implicit
        F0: Monad[F],
        F1: MonadError_[F, Exception])
        : F[ManagedDatasource[T, F, G, R, P]] =
      onChange(id, Condition.normal()) as {
        mds.modify(λ[Datasource[F, G, ?, R, P] ~> Datasource[F, G, ?, R, P]] { ds =>
          ConditionReportingDatasource(onChange(id, _: Condition[Exception]), ds)
        })
      }
  }
}
