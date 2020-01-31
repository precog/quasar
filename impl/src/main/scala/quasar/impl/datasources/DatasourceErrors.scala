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

package quasar.impl.datasources

import slamdata.Predef.{Exception, Option}

import scalaz.{Functor, IMap, Order}
import scalaz.syntax.functor._

/** Allows querying the error status of datasources. */
trait DatasourceErrors[F[_], I] {
  /** Datasources currently in an error state. */
  def erroredDatasources: F[IMap[I, Exception]]

  /** Retrieve the error associated with the specified datasource. */
  def datasourceError(datasourceId: I): F[Option[Exception]]
}

object DatasourceErrors {
  def fromMap[F[_]: Functor, I: Order](
      errors: F[IMap[I, Exception]])
      : DatasourceErrors[F, I] =
    new DatasourceErrors[F, I] {
      val erroredDatasources = errors

      def datasourceError(datasourceId: I) =
        erroredDatasources.map(_.lookup(datasourceId))
    }
}
