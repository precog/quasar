/*
 * Copyright 2014–2019 SlamData Inc.
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

import slamdata.Predef.{Stream => _, _}

import quasar.ConditionMatchers
import quasar.Qspec
import quasar.api.destination.{
  DestinationError,
  DestinationName,
  DestinationRef,
  DestinationType
}
import quasar.contrib.scalaz.MonadState_

import eu.timepit.refined.auto._
import monocle.Lens
import monocle.macros.Lenses
import scalaz.std.anyVal._
import scalaz.syntax.monad._
import scalaz.{IMap, ISet, State}

final class MockDestinationsSpec extends Qspec with ConditionMatchers {
  import MockDestinationsSpec._

  type M[A] = State[RunState[Int, String], A]

  implicit def monadRunningState: MonadState_[M, MockDestinations.State[Int, String]] =
    MonadState_.zoom[M](runStateMockState[Int, String])

  implicit def monadIndex: MonadState_[M, Int] =
    MonadState_.zoom[M][RunState[Int, String], Int](RunState.currentIndex)

  val mockType = DestinationType("mock", 1L)
  val unsupportedType = DestinationType("unsupported", 1337L)

  def freshId: M[Int] =
    MonadState_[M, Int].modify(_ + 1) >> MonadState_[M, Int].get

  def lastId: M[Int] =
    MonadState_[M, Int].get

  val supportedSet = ISet.singleton(mockType)

  val destinations =
    MockDestinations[Int, String, M](freshId, supportedSet)

  val testRef: DestinationRef[String] =
    DestinationRef(mockType, DestinationName("mock-name"), "foo")

  val unsupportedRef: DestinationRef[String] =
    DestinationRef(unsupportedType, DestinationName("unsupported-name"), "bar")

  def run[A](act: M[A]): A =
    act.eval(initial[Int, String])

  "adds a destination" >> {
    val (addStatus, retrieved) =
      run(for {
        addStatus <- destinations.addDestination(testRef)
        newId <- lastId
        retrieved <- destinations.destinationRef(newId)
      } yield (addStatus, retrieved))

    addStatus must be_\/-
    retrieved must be_\/-(testRef)
  }

  "verifies name uniqueness on creation" >> {
    val (add1, add2) =
      run(for {
        add1 <- destinations.addDestination(testRef)
        add2 <- destinations.addDestination(testRef)
      } yield (add1, add2))

    add1 must be_\/-
    add2 must be_-\/(DestinationError.destinationNameExists(testRef.name))
  }

  "verifies name uniqueness on replace" >> {
    val testRef2 = DestinationRef.name.set(DestinationName("mock-2"))(testRef)
    val testRef3 = DestinationRef.name.set(DestinationName("mock-2"))(testRef)

    val replace =
      run(for {
        _ <- destinations.addDestination(testRef)
        addId <- lastId
        _ <- destinations.addDestination(testRef2)
        replace <- destinations.replaceDestination(addId, testRef3)
      } yield replace)

    replace must beAbnormal(DestinationError.destinationNameExists(testRef2.name))
  }

  "allows replacement with the same name" >> {
    val testRef2 = DestinationRef.config.set("modified")(testRef)

    val (replaceStatus, replaced) =
      run(for {
        _ <- destinations.addDestination(testRef)
        addId <- lastId
        replaceStatus <- destinations.replaceDestination(addId, testRef2)
        replaced <- destinations.destinationRef(addId)
      } yield (replaceStatus, replaced))

    replaceStatus must beNormal
    replaced must be_\/-(testRef2)
  }

  "errors on unsupported when replacing" >> {
    val testRef2 = DestinationRef.kind.set(unsupportedType)(testRef)

    val (replaceStatus, replaced) =
      run(for {
        _ <- destinations.addDestination(testRef)
        addId <- lastId
        replaceStatus <- destinations.replaceDestination(addId, testRef2)
        replaced <- destinations.destinationRef(addId)
      } yield (replaceStatus, replaced))

    replaceStatus must beAbnormal(DestinationError.destinationUnsupported(unsupportedType, supportedSet))
    replaced must be_\/-(testRef)
  }

  "errors on unsupported" >> {
    run(destinations.addDestination(unsupportedRef)) must be_-\/(
      DestinationError.destinationUnsupported(unsupportedType, supportedSet))
  }
}

object MockDestinationsSpec {
  @Lenses
  case class RunState[I, C](running: IMap[I, DestinationRef[C]], errored: IMap[I, Exception], currentIndex: Int)

  def initial[I, C]: RunState[I, C] =
    RunState(IMap.empty, IMap.empty, 0)

  def runStateMockState[I, C]: Lens[RunState[I, C], MockDestinations.State[I, C]] =
    Lens[RunState[I, C], MockDestinations.State[I, C]](runState =>
      MockDestinations.State(runState.running, runState.errored))(mockState => (rs =>
      rs.copy(
        running = mockState.running,
        errored = mockState.errored)))
}
