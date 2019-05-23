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

package quasar.api.datasource

import slamdata.Predef.{Boolean, Exception, Option}
import quasar.Condition
import quasar.api.SchemaConfig
import quasar.api.resource._

import scala.concurrent.duration.FiniteDuration

import scalaz.{\/, ISet}

/** @tparam F effects
  * @tparam G multple results
  * @tparam I identity
  * @tparam C configuration
  */
trait Datasources[F[_], G[_], I, C, S <: SchemaConfig] {
  import DatasourceError._

  type PathType <: ResourcePathType

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

  /** Returns whether or not the specified path refers to a resource in the
    * specified datasource.
    */
  def pathIsResource(datasourceId: I, path: ResourcePath)
      : F[ExistentialError[I] \/ Boolean]

  /** Returns the name and type of the `ResourcePath`s within the specified
    * datasource implied by concatenating each name to `prefixPath`.
    */
  def prefixedChildPaths(datasourceId: I, prefixPath: ResourcePath)
      : F[DiscoveryError[I] \/ G[(ResourceName, PathType)]]

  /** Removes the specified datasource, making its resources unavailable. */
  def removeDatasource(datasourceId: I): F[Condition[ExistentialError[I]]]

  /** Replaces the reference to the specified datasource. */
  def replaceDatasource(datasourceId: I, ref: DatasourceRef[C])
      : F[Condition[DatasourceError[I, C]]]

  /** Retrieves the schema of the resource at the given `path` with the
    * specified datasource according to the provided `schemaConfig`.
    *
    * Execution time is limited to `timeLimit` and any available value will
    * be returned upon expiration. If a "complete" schema, as defined by the
    * specified config, is computed before the time limit, it will be returned
    * immediately.
    *
    * Returns `None` if the resource exists but a schema is not available.
    */
  def resourceSchema(
      datasourceId: I,
      path: ResourcePath,
      schemaConfig: S,
      timeLimit: FiniteDuration)
      : F[DiscoveryError[I] \/ Option[schemaConfig.Schema]]

  /** The set of supported datasource types. */
  def supportedDatasourceTypes: F[ISet[DatasourceType]]
}
