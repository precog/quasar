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

import slamdata.Predef._

import quasar.Condition
import quasar.api.destination.{DestinationMeta, DestinationRef, DestinationType, Destinations}
import quasar.api.destination.DestinationError
import quasar.api.destination.DestinationError.{CreateError, ExistentialError}
import quasar.contrib.scalaz.MonadState_

import scalaz.syntax.monad._
import scalaz.syntax.either._
import scalaz.syntax.std.either._
import scalaz.{\/, IMap, ISet, Order, Monoid, Monad}
import monocle.macros.Lenses

class MockDestinations[I: Order, C, F[_]: Monad](freshId: F[I], supported: ISet[DestinationType])(
  implicit F: MonadState_[F, MockDestinations.State[I, C]]) extends Destinations[F, List, I, C] {
  import MockDestinations._

  implicit val monadRunningState: MonadState_[F, IMap[I, DestinationRef[C]]] =
    MonadState_.zoom[F](State.running[I, C])

  implicit val monadErroredState: MonadState_[F, IMap[I, Exception]] =
    MonadState_.zoom[F](State.errored[I, C])

  val R = MonadState_[F, IMap[I, DestinationRef[C]]]
  val E = MonadState_[F, IMap[I, Exception]]

  def addDestination(ref: DestinationRef[C]): F[CreateError[C] \/ I] =
    for {
      newId <- freshId
      currentDests <- R.get
      newDests = currentDests.insert(newId, ref)
      _ <- R.put(newDests)
    } yield newId.right

  def allDestinationMetadata: F[List[(I, DestinationMeta)]] =
    for {
      currentDests <- R.gets(_.toList)
      errs <- E.get
      metas = currentDests map {
        case (i, ref) => (i, DestinationMeta.fromOption(ref.kind, ref.name, errs.lookup(i)))
      }
    } yield metas

  def destinationRef(id: I): F[ExistentialError[I] \/ DestinationRef[C]] =
    R.gets(_.lookup(id))
      .map(_.toRight(DestinationError.destinationNotFound(id)).disjunction)

  def destinationStatus(id: I): F[ExistentialError[I] \/ Condition[Exception]] =
    (R.get |@| E.get) {
      case (running, errors) => (running.lookup(id), errors.lookup(id)) match {
        case (Some(_), Some(err)) => Condition.abnormal(err).right[ExistentialError[I]]
        case (Some(_), None) => Condition.normal().right[ExistentialError[I]]
        case _ => DestinationError.destinationNotFound(id).left[Condition[Exception]]
      }
    }

  def removeDestination(id: I): F[Condition[ExistentialError[I]]] =
    R.gets(_.lookup(id).fold(
      Condition.abnormal(DestinationError.destinationNotFound(id)))(_ =>
      Condition.normal()))

  def replaceDestination(id: I, ref: DestinationRef[C]): F[Condition[DestinationError[I, C]]] =
    R.gets(_.lookup(id)) >>= (_.fold(
      Condition.abnormal(DestinationError.destinationNotFound[I, DestinationError[I, C]](id)).point[F])(_ =>
      R.modify(_.insert(id, ref)) *> Condition.normal[DestinationError[I, C]]().point[F]))

  def supportedDestinationTypes: F[ISet[DestinationType]] =
    supported.point[F]

  def errors: F[IMap[I, Exception]] =
    E.get
}

object MockDestinations {
  @Lenses
  case class State[I, C](running: IMap[I, DestinationRef[C]], errored: IMap[I, Exception])

  implicit def mockDestinationStateMonoid[I: Order, C]: Monoid[State[I, C]] = new Monoid[State[I, C]] {
    val zero = State[I, C](IMap.empty, IMap.empty)
    def append(l: State[I, C], r: => State[I, C]): State[I, C] =
      State(l.running union r.running, l.errored union r.errored)
  }

  def apply[I: Order, C, F[_]: Monad: MonadState_[?[_], State[I, C]]](
    freshId: F[I],
    supported: ISet[DestinationType]) =
    new MockDestinations[I, C, F](freshId, supported)
}
