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

import slamdata.Predef.Unit
import quasar.Condition
import quasar.api.{DataSourceType, ResourceName}
import quasar.api.DataSourceError.CreateError

import scalaz.ISet

/** A primitive facility for managing the lifecycle of datasources.
  *
  * All mutating operations replace existing datasources as this is intended
  * to be used as a dependency in another context, such as `DefaultDataSources`,
  * that decides whether doing so is appropriate.
  */
trait DataSourceControl[F[_], C] {
  /** Initialize a datasource as `name` using the provided `config`. If a
    * datasource exists at `name`, it is shut down.
    */
  def init(
      name: ResourceName,
      config: DataSourceConfig[C])
      : F[Condition[CreateError[C]]]

  /** Stop the named datasource, discarding it and freeing any resources it may
    * be using.
    */
  def shutdown(name: ResourceName): F[Unit]

  /** Update the name of the datasource at `src` to `dst`. If a datasource exists
    * at `dst`, it is shut down.
    */
  def rename(src: ResourceName, dst: ResourceName): F[Unit]

  /** The types of datasources supported. */
  def supported: F[ISet[DataSourceType]]
}
