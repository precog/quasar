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

package quasar.api.destination

import slamdata.Predef.Exception

import quasar.Condition

import scalaz.{\/, IMap, ISet}

/** @tparam F effects
  * @tparam G multiple results
  * @tparam I identity
  * @tparam C configuration
  */
trait Destinations[F[_], G[_], I, C] {
  import DestinationError._

  /** Adds the destination described by the given `DestinationRef` to the
    * set of destinations, returning its identifier or an error if it could
    * not be added.
    */
  def addDestination(ref: DestinationRef[C]): F[CreateError[C] \/ I]

  /** Metadata for all destinations. */
  def allDestinationMetadata: F[G[(I, DestinationMeta)]]

  /** Returns the reference to the specified destination, or an error if
    * it doesn't exist.
    */
  def destinationRef(destinationId: I): F[ExistentialError[I] \/ DestinationRef[C]]

  /** Returns the specified destination. Initializes it if necessary */
  def destinationOf(destinationId: I): F[DestinationError[I, C] \/ Destination[F]]

  /** Returns the status of the specified destination or an error if it doesn't
    * exist.
    */
  def destinationStatus(destinationId: I): F[ExistentialError[I] \/ Condition[Exception]]

  /** Shuts down and removes a destination. */
  def removeDestination(destinationId: I): F[Condition[ExistentialError[I]]]

  /** Replaces the reference to the specified destination. */
  def replaceDestination(destinationId: I, ref: DestinationRef[C])
      : F[Condition[DestinationError[I, C]]]

  /** The set of supported destination types. */
  def supportedDestinationTypes: F[ISet[DestinationType]]

  /** Destinations that have errored */
  def errors: F[IMap[I, Exception]]
}
