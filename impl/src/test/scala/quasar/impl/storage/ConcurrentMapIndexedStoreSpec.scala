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

package quasar.impl.storage

import slamdata.Predef._
import quasar.contrib.cats.effect.stateT.catsStateTEffect
import quasar.concurrent.BlockingContext

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

import cats.data.StateT
import cats.effect.{Sync, IO, Resource}
import scalaz.std.anyVal._
import scalaz.std.string._
import shims._
import java.util.concurrent.ConcurrentHashMap

final class ConcurrentMapIndexedStoreSpec extends
    IndexedStoreSpec[StateT[IO, Int, ?], Int, String] {

  type M[A] = StateT[IO, Int, A]

  val blockingPool: BlockingContext = BlockingContext.cached("concurrent-map-indexed-store-spec")

  val freshIndex: M[Int] = StateT.liftF(IO(Random.nextInt))

  val commit: M[Unit] = StateT.modify { (x: Int) => x + 1 }

  val emptyStore: Resource[M, IndexedStore[M, Int, String]] =
    Resource.liftF(Sync[M].delay { new ConcurrentHashMap[Int, String]() } map { ConcurrentMapIndexedStore(_, commit, blockingPool) })

  val valueA = "A"
  val valueB = "B"

  "check commits works" >> {
    val expected = List(0, 1, 2, 3, 4, 4)

    val stateT = emptyStore use { store => for {
      initial <- StateT.get[IO, Int]

      i1 <- freshIndex
      _ <- store.insert(i1, valueA)
      inserted1 <- StateT.get[IO, Int]

      i2 <- freshIndex
      _ <- store.insert(i2, valueB)
      inserted2 <- StateT.get[IO, Int]

      i3 <- freshIndex
      _ <- store.insert(i3, valueA)
      inserted3 <- StateT.get[IO, Int]

      _ <- store.delete(i1)
      deleted1 <- StateT.get[IO, Int]

      i4 <- freshIndex
      _ <- store.delete(i4)
      deleted2 <- StateT.get[IO, Int]

      actual = List(initial, inserted1, inserted2, inserted3, deleted1, deleted2)
    } yield actual === expected }

    stateT.runA(0).unsafeRunSync
  }
}
