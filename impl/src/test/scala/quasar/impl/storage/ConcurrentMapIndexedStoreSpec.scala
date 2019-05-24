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

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

import cats.effect.IO
import cats.effect.concurrent.Ref
import scalaz.std.anyVal._
import scalaz.std.string._
import shims._
import java.util.concurrent.ConcurrentHashMap

final class ConcurrentMapIndexedStoreSpec extends
    IndexedStoreSpec[IO, Int, String] {

  val blockingPool: BlockingContext = BlockingContext.cached("concurrent-map-indexed-store-spec")

  val freshIndex: IO[Int] = IO(Random.nextInt)

  def commit(ref: Ref[IO, Int]): IO[Unit] = ref update { (x: Int) => x + 1 }

  def mkEmptyStore(ref: Ref[IO, Int]): IO[IndexedStore[IO, Int, String]] =
    IO { new ConcurrentHashMap[Int, String]() } map { ConcurrentMapIndexedStore(_, commit(ref), blockingPool) }

  val emptyStore: IO[IndexedStore[IO, Int, String]] =
    Ref[IO].of(0) flatMap ((ref: Ref[IO, Int]) => mkEmptyStore(ref))

  val valueA = "A"
  val valueB = "B"

  "check commits works" >> {
    val expected = List(0, 1, 2, 3, 4, 4)

    val io = for {
      ref <- Ref[IO].of(0)
      store <- mkEmptyStore(ref)
      initial <- ref.get

      i1 <- freshIndex
      df <- store.insert(i1, valueA)
      _ <- df.get
      inserted1 <- ref.get

      i2 <- freshIndex
      df <- store.insert(i2, valueB)
      _ <- df.get
      inserted2 <- ref.get

      i3 <- freshIndex
      df <- store.insert(i3, valueA)
      _ <- df.get
      inserted3 <- ref.get

      df <- store.delete(i1)
      _ <- df.get
      deleted1 <- ref.get

      i4 <- freshIndex
      df <- store.delete(i4)
      _ <- df.get
      deleted2 <- ref.get

      actual = List(initial, inserted1, inserted2, inserted3, deleted1, deleted2)
    } yield actual === expected

    io.unsafeRunSync
  }
}
