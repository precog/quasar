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

package quasar.impl.datasources

import slamdata.Predef.{Exception, Option}
import quasar.api.ResourceName

import scalaz.{Functor, IMap}
import scalaz.syntax.functor._

/** Allows querying the error status of datasources. */
trait DataSourceErrors[F[_]] {
  /** DataSources currently in an error state. */
  def errored: F[IMap[ResourceName, Exception]]

  /** Retrieve the error associated with the named datasource. */
  def lookup(name: ResourceName): F[Option[Exception]]
}

object DataSourceErrors {
  def fromMap[F[_]: Functor](errors: F[IMap[ResourceName, Exception]]): DataSourceErrors[F] =
    new DataSourceErrors[F] {
      val errored = errors
      def lookup(name: ResourceName) = errored.map(_.lookup(name))
    }
}
