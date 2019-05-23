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

package quasar.impl.destinations

import slamdata.Predef.{Exception, Option, Unit}

import quasar.Condition
import quasar.api.destination.DestinationError.CreateError
import quasar.api.destination.{DestinationRef, DestinationType}
import quasar.connector.Destination

import scalaz.{IMap, ISet}

/** A primitive facility for managing the lifecycle of destinations. */
trait DestinationManager[I, C, F[_]] {
  /** Initialize a destination as `destinationId` using the provided `ref`. If a
    * destination exists at `destinationId`, it is shut down.
    */
  def initDestination(destinationId: I, ref: DestinationRef[C])
      : F[Condition[CreateError[C]]]

  /** Returns the destination having the given id or `None` if not found. */
  def destinationOf(destinationId: I): F[Option[Destination[F]]]

  /** Returns `ref` devoid of any sensitive information (credentials and the like). */
  def sanitizedRef(ref: DestinationRef[C]): DestinationRef[C]

  /** Stop the destination, discarding it and freeing any resources it may
    * be using.
    */
  def shutdownDestination(destinationId: I): F[Unit]

  /** The types of destinations supported. */
  def supportedDestinationTypes: F[ISet[DestinationType]]

  /** Retrieve all errors */
  def errors: F[IMap[I, Exception]]

  /** Returns errors related to the given id */
  def errorsOf(destinationId: I): F[Option[Exception]]

}
