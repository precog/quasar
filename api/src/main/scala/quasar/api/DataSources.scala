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

package quasar.api

import quasar.Condition

import scalaz.{\/, IMap, ISet}

trait DataSources[F[_], C, S] {
  import DataSourceError._

  /** Returns the result of attempting to create a connection to an external
    * datasource, `Unit` indicates the operation was successful.
    *
    * @name an identifier to assign to the datasource, must not exist unless `onConflict` is `Replace`
    * @kind uniquely identifies the type of datasource
    * @config configuration information, the specifics of which are determined by `kind`
    * @onConflict an enumeration describing how to resolve name conflicts
    */
  def createExternal(
      name: ResourceName,
      kind: DataSourceType,
      config: C,
      onConflict: ConflictResolution)
      : F[Condition[ExternalError[C]]]

  /** Returns the result of attempting to create a static datasource containing the
    * given content, `Unit` indicates the operation was successful.
    *
    * @name an identifier to assign to the datasource, must not exist unless `onConflict` is `Replace`
    * @content the static content the datasource will provide
    * @onConflict an enumeration describing how to resolve name conflicts
    */
  def createStatic(
      name: ResourceName,
      content: S,
      onConflict: ConflictResolution)
      : F[Condition[StaticError]]

  /** Returns the metadata and configuration for the specified datasource,
    * or an error if it doesn't exist.
    */
  def lookup(name: ResourceName): F[CommonError[C] \/ (StaticDataSource \/ (ExternalMetadata, C))]

  /** Removes the specified datasource, making its data unavailable. If the
    * specified datasource is static, its content is deleted.
    *
    * An error is returned if no datasource exists having the specified name.
    */
  def remove(name: ResourceName): F[Condition[CommonError[C]]]

  /** Rename `src` to `dst`, handling conflicts at `dst` according to
    * `onConflict`. An error is returned if `src` does not exist.
    */
  def rename(
      src: ResourceName,
      dst: ResourceName,
      onConflict: ConflictResolution)
      : F[Condition[CreateError[C]]]

  /** A map of all datasources to their current status. */
  def status: F[IMap[ResourceName, StaticDataSource \/ ExternalMetadata]]

  /** The set of currently supported external datasources. */
  def supportedExternal: F[ISet[DataSourceType]]
}
