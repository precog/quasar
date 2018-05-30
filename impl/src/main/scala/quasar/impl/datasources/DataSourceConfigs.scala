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

import slamdata.Predef.{Boolean, Option, Unit}
import quasar.api.{DataSourceType, ResourceName}

import scalaz.IMap

/** A primitive facility for managing datasource configurations.
  *
  * All mutating operations overwrite existing configurations as this is intended
  * to be used as a dependency in another context, such as `DefaultDataSources`,
  * that decides whether doing so is appropriate.
  */
trait DataSourceConfigs[F[_], C] {
  /** Associate the given configuration with `name`, replacing any
    * existing association.
    */
  def add(name: ResourceName, config: DataSourceConfig[C]): F[Unit]

  /** The set of configured datasources. */
  def configured: F[IMap[ResourceName, DataSourceType]]

  /** Returns the configuration associated with `name` or `None` if none exists. */
  def lookup(name: ResourceName): F[Option[DataSourceConfig[C]]]

  /** Removes any association with the given name, returning whether one existed. */
  def remove(name: ResourceName): F[Boolean]

  /** Renames the association of `src` to `dst`, replacing any existing
    * association at `dst`.
    */
  def rename(src: ResourceName, dst: ResourceName): F[Unit]
}
