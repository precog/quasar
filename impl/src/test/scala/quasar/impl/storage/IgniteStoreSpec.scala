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

import cats.effect.{IO, Resource}
import cats.effect.concurrent.Deferred
import cats.syntax.functor._

import scalaz.std.anyVal._
import scalaz.std.string._
import scala.util.Random

import org.apache.ignite._

final class IgniteStoreSpec extends IndexedStoreSpec[IO, Int, String] {
  sequential

  val blockingPool: BlockingContext = BlockingContext.cached("ignite-indexed-store-spec")
  val freshIndex: IO[Int] = IO(Random.nextInt)
  val ignite: Resource[IO, Ignite] = {
    val create: IO[Ignite] = for {
      d <- Deferred.tryable[IO, Ignite]
      ignite <- IO(Ignition.start()).start
      _ <- IO(Ignition.addListener(new IgnitionListener {
        def onStateChange(name: String, state: IgniteState): Unit = state match {
          case IgniteState.STARTED =>
            println("================================================================================")
            println("STARTED")
            println("================================================================================")
            d.complete(Ignition.ignite()).unsafeRunSync
          case _ => ()
        }})).start
      i <- d.get
    } yield i
    def stop(i: Ignite): IO[Unit] = for {
      _ <- IO(println("STOPPED"))
      _ <- IO(Ignition.stop(true)).start
      d <- Deferred.tryable[IO, Unit]
      _ <- IO(Ignition.addListener(new IgnitionListener {
        def onStateChange(name: String, state: IgniteState): Unit = state match {
          case IgniteState.STARTED => ()
          case _ => d.complete(()).unsafeRunSync
        }
      })).start
      _ <- d.get
    } yield (())
    Resource.make(create)(stop)
  }
Resource.make(IO(Ignition.start()))(x => IO(Ignition.stop(true)) as (()))
  val emptyStore: Resource[IO, IndexedStore[IO, Int, String]] = ignite evalMap { (ig: Ignite) =>
    println(ig)
    val cache = ig.getOrCreateCache[Int, String]("test")
    println(cache)
    IO(IgniteStore[IO, Int, String](cache, blockingPool))
  }

  val valueA = "A"
  val valueB = "B"
}
