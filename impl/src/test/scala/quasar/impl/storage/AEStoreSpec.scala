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

package quasar.impl.storage

import slamdata.Predef._
import quasar.concurrent.BlockingContext

import cats.effect.{IO, Resource, ContextShift, Timer}
import cats.syntax.functor._

import io.atomix.core._

import monocle.Prism

import java.util.concurrent.ConcurrentHashMap
import java.nio.charset.StandardCharsets

import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.util.Random
import scalaz.std.string._

import shims._

final class AEStoreSpec extends IndexedStoreSpec[IO, String, String] {
  sequential

  implicit val ec: ExecutionContext = ExecutionContext.global
//  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)
//  implicit val monadQuasarErr: MonadQuasarErr[F] =
//MonadError_.facet[F](QuasarError.throwableP)


  val pool = BlockingContext.cached("aestore-pool")

  val defaultNode = AtomixSetup.NodeInfo("default", "localhost", 6000)

  val bytesStringP: Prism[Array[Byte], String] =
    Prism((bytes: Array[Byte]) => Some(new String(bytes, StandardCharsets.UTF_8)))(_.getBytes)

  val emptyStore: Resource[IO, IndexedStore[IO, String, String]] = for {
    atomix <- Resource.make(AtomixSetup.mkAtomix[IO](defaultNode, List()))((a: Atomix) =>
      AtomixSetup.cfToAsync[IO, java.lang.Void](a.stop) as (()))
    underlying <- Resource.pure[IO, ConcurrentHashMap[String, AEStore.Value]](new ConcurrentHashMap[String, AEStore.Value]())
    store <- AEStore[IO]("default", atomix.getCommunicationService(), atomix.getMembershipService(), underlying, pool)
  } yield store

  val valueA = "A"
  val valueB = "B"
  val freshIndex = IO(Random.nextInt().toString)
}
