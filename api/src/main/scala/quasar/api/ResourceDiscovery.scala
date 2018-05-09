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

import slamdata.Predef.Boolean
import quasar.api.ResourceError.CommonError

import scalaz.{\/, IMap, Tree}

/** Provides for discovering the resources in a datasource. */
trait ResourceDiscovery[F[_]] {

  /** Returns the children of the specified resource path or an error if it
    * does not exist.
    */
  def children(path: ResourcePath): F[CommonError \/ IMap[ResourceName, ResourcePathType]]

  /** Returns the descendants of the specified resource path or an error if it
    * does not exist.
    */
  def descendants(path: ResourcePath): F[CommonError \/ Tree[(ResourceName, ResourcePathType)]]

  /** Returns whether the specified resource path refers to a resource. */
  def isResource(path: ResourcePath): F[Boolean]
}
