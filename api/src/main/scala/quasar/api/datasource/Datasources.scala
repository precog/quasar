/*
 * Copyright 2020 Precog Data
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

package quasar.api.datasource

import quasar.Condition

import scala.{Option, None, Exception}

import scalaz.{\/, ISet}

/** @tparam F effects
  * @tparam G multple results
  * @tparam I identity
  * @tparam C configuration
  */
trait Datasources[F[_], G[_], I, C] {
  import DatasourceError._

  /** Adds the datasource described by the given `DatasourceRef` to the
    * set of datasources, returning its identifier or an error if it could
    * not be added.
    */
  def addDatasource(ref: DatasourceRef[C]): F[CreateError[C] \/ I]

  /** Metadata for all datasources. */
  def allDatasourceMetadata: F[G[(I, DatasourceMeta)]]

  /** Returns the reference to the specified datasource, or an error if
    * it doesn't exist.
    */
  def datasourceRef(datasourceId: I): F[ExistentialError[I] \/ DatasourceRef[C]]

  /** Returns the status of the specified datasource or an error if it doesn't
    * exist.
    */
  def datasourceStatus(datasourceId: I): F[ExistentialError[I] \/ Condition[Exception]]

  /** Removes the specified datasource, making its resources unavailable. */
  def removeDatasource(datasourceId: I): F[Condition[ExistentialError[I]]]

  /** Replaces the reference to the specified datasource. */
  def replaceDatasource(datasourceId: I, ref: DatasourceRef[C])
      : F[Condition[DatasourceError[I, C]]]

  /** Replaces the reference to the specified datasource, applying the patch
    * to the existing configuration.
    */
  def reconfigureDatasource(datasourceId: I, patch: C)
      : F[Condition[DatasourceError[I, C]]]

  /** Renames the reference to the specified datasource.
    */
  def renameDatasource(datasourceId: I, name: DatasourceName)
      : F[Condition[DatasourceError[I, C]]]

  /** The set of supported datasource types. */
  def supportedDatasourceTypes: F[ISet[DatasourceType]]
}
