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

package quasar

import slamdata.Predef._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import java.util.UUID

import org.specs2.mutable.Specification

import fs2.Stream

import cats.effect.{IO, Sync, Timer}
import cats.effect.laws.util.TestContext
import cats.kernel.Hash
import cats.implicits._

object RateLimiterSpec extends Specification {

  implicit def executionContext: ExecutionContext = ExecutionContext.Implicits.global
  implicit val timer: Timer[IO] = IO.timer(executionContext)

  def freshKey: IO[UUID] = IO.delay(UUID.randomUUID())

  "rate limiter" should {
    "output events with real time" >> {
      "one event in one window" in {
        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID]).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 1, 1.seconds)).unsafeRunSync()

        val back = Stream.eval_(limit) ++ Stream.emit(1)

        back.compile.toList.unsafeRunSync() mustEqual(List(1))
      }

      "two events in one window" in {
        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID]).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 2, 1.seconds)).unsafeRunSync()

        val back =
          Stream.eval_(limit) ++ Stream.emit(1) ++
            Stream.eval_(limit) ++ Stream.emit(2)

        back.compile.toList.unsafeRunSync() mustEqual(List(1, 2))
      }

      "two events in two windows" in {
        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID]).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 1, 1.seconds)).unsafeRunSync()

        val back =
          Stream.eval_(limit) ++ Stream.emit(1) ++
            Stream.eval_(limit) ++ Stream.emit(2)

        back.compile.toList.unsafeRunSync() mustEqual(List(1, 2))
      }

      "events from two keys" in {
        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID]).unsafeRunSync()

        val RateLimiterEffects(limit1, _) =
          key.flatMap(k => rl(k, 1, 1.seconds)).unsafeRunSync()

        val RateLimiterEffects(limit2, _) =
          key.flatMap(k => rl(k, 1, 1.seconds)).unsafeRunSync()

        val back1 =
          Stream.eval_(limit1) ++ Stream.emit(1) ++
            Stream.eval_(limit1) ++ Stream.emit(2)

        val back2 =
          Stream.eval_(limit2) ++ Stream.emit(3) ++
            Stream.eval_(limit2) ++ Stream.emit(4)

        back1.compile.toList.unsafeRunSync() mustEqual(List(1, 2))
        back2.compile.toList.unsafeRunSync() mustEqual(List(3, 4))
      }
    }

    "output events with simulated time" >> {
      "one event per second" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 1, 1.seconds)).unsafeRunSync()

        var a: Int = 0

        val run =
          limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(4)
      }

      "one event per two seconds" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 1, 2.seconds)).unsafeRunSync()

        var a: Int = 0

        val run =
          limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(4)
      }

      "two events per second" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 2, 1.seconds)).unsafeRunSync()

        var a: Int = 0

        val run =
          limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(6)
      }

      "three events per second" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 3, 1.seconds)).unsafeRunSync()

        var a: Int = 0

        val run =
          limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(6)

        ctx.tick(1.seconds)
        a mustEqual(8)
      }

      "with a caution of 0.75" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](0.75, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 4, 1.seconds)).unsafeRunSync()

        var a: Int = 0

        val run =
          limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(6)

        ctx.tick(1.seconds)
        a mustEqual(8)
      }

      "do not overwrite configs (use existing config)" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val k1 = key.unsafeRunSync()

        val RateLimiterEffects(limit1, _) = rl(k1, 2, 1.seconds).unsafeRunSync()
        val RateLimiterEffects(limit2, _) = rl(k1, 3, 1.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          limit2 >> IO.delay(a += 1) >>
            limit2 >> IO.delay(a += 1) >>
            limit2 >> IO.delay(a += 1) >>
            limit2 >> IO.delay(a += 1) >>
            limit2 >> IO.delay(a += 1) >>
            limit2 >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(6)
      }

      "support two keys on the same schedule" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val RateLimiterEffects(limit1, _) =
          key.flatMap(k => rl(k, 2, 1.seconds)).unsafeRunSync()

        val RateLimiterEffects(limit2, _) =
          key.flatMap(k => rl(k, 3, 1.seconds)).unsafeRunSync()

        var a1: Int = 0
        var a2: Int = 0

        val run1 =
          limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1)

        val run2 =
          limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1)

        run1.unsafeRunAsyncAndForget()
        run2.unsafeRunAsyncAndForget()

        a1 mustEqual(2)
        a2 mustEqual(3)

        ctx.tick(1.seconds)
        a1 mustEqual(4)
        a2 mustEqual(6)

        ctx.tick(1.seconds)
        a1 mustEqual(6)
        a2 mustEqual(8)
      }

      "support two keys on different schedules" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val RateLimiterEffects(limit1, _) =
          key.flatMap(k => rl(k, 2, 1.seconds)).unsafeRunSync()

        val RateLimiterEffects(limit2, _) =
          key.flatMap(k => rl(k, 2, 2.seconds)).unsafeRunSync()

        var a1: Int = 0
        var a2: Int = 0

        val run1 =
          limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1)

        val run2 =
          limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1)

        run1.unsafeRunAsyncAndForget()
        run2.unsafeRunAsyncAndForget()

        a1 mustEqual(2)
        a2 mustEqual(2)

        ctx.tick(1.seconds)
        a1 mustEqual(4)
        a2 mustEqual(2)

        ctx.tick(1.seconds)
        a1 mustEqual(6)
        a2 mustEqual(4)

        ctx.tick(1.seconds)
        a1 mustEqual(6)
        a2 mustEqual(4)

        ctx.tick(1.seconds)
        a1 mustEqual(6)
        a2 mustEqual(6)
      }
    }

    "respect backoff effect" >> {
      "backoff for key that hasn't been limited" in {
        val ctx = TestContext()
        val updater = new TestRateLimitUpdater[UUID]

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, updater)(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val k1 = key.unsafeRunSync()

        val RateLimiterEffects(limit, backoff) = rl(k1, 1, 1.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          backoff >>
          limit >> IO.delay(a += 1) >>
          limit >> IO.delay(a += 1) >>
          limit >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(0)
        updater.waits must containTheSameElementsAs(List(k1))

        ctx.tick(1.seconds)
        a mustEqual(1)
        updater.waits must containTheSameElementsAs(List(k1, k1))

        ctx.tick(1.seconds)
        a mustEqual(2)
        updater.waits must containTheSameElementsAs(List(k1, k1, k1))

        ctx.tick(1.seconds)
        a mustEqual(3)
        updater.waits must containTheSameElementsAs(List(k1, k1, k1))
      }

      "backoff state for known key" in {
        val ctx = TestContext()
        val updater = new TestRateLimitUpdater[UUID]

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, updater)(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val k1 = key.unsafeRunSync()

        val RateLimiterEffects(limit, backoff) = rl(k1, 1, 1.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          limit >> IO.delay(a += 1) >>
          limit >> IO.delay(a += 1) >>
          backoff >>
          limit >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(1)
        updater.waits must containTheSameElementsAs(List(k1))

        ctx.tick(1.seconds)
        a mustEqual(2)
        updater.waits must containTheSameElementsAs(List(k1, k1))

        ctx.tick(1.seconds)
        a mustEqual(3)
        updater.waits must containTheSameElementsAs(List(k1, k1))
      }
    }

    "handle wait request" >> {
      "wait for unknown key has no effect" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val wait = key.flatMap(k => rl.wait(k, 2.seconds))
        val effectsF = key.flatMap(k => rl(k, 1, 1.seconds))

        var a: Int = 0

        val run =
          (wait >> effectsF) flatMap {
            case RateLimiterEffects(limit, _) =>
              limit >> IO.delay(a += 1) >>
              limit >> IO.delay(a += 1) >>
              limit >> IO.delay(a += 1)
          }

        run.unsafeRunAsyncAndForget()

        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(3)
      }

      "wait for known but unused key" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val k: UUID = key.unsafeRunSync()

        val wait = rl.wait(k, 2.seconds)
        val effectsF = rl(k, 1, 1.seconds)

        var a: Int = 0

        val run =
          (wait >> effectsF) flatMap {
            case RateLimiterEffects(limit, _) =>
              wait >>
              limit >> IO.delay(a += 1) >>
              limit >> IO.delay(a += 1) >>
              limit >> IO.delay(a += 1)
          }

        run.unsafeRunAsyncAndForget()

        a mustEqual(0)

        ctx.tick(1.seconds)
        a mustEqual(0)

        ctx.tick(1.seconds)
        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(3)
      }

      "wait state for known key" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val k: UUID = key.unsafeRunSync()

        val wait = rl.wait(k, 2.seconds)
        val effectsF = rl(k, 1, 1.seconds)

        var a: Int = 0

        val run =
          effectsF flatMap {
            case RateLimiterEffects(limit, _) =>
              limit >> IO.delay(a += 1) >>
              limit >> IO.delay(a += 1) >>
              wait >>
              limit >> IO.delay(a += 1)
          }

        run.unsafeRunAsyncAndForget()

        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(3)
      }
    }

    "handle plus one request" >> {
      "modify state for unknown key" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val key1 = key.unsafeRunSync()
        val key2 = key.unsafeRunSync()

        val plusOne = rl.plusOne(key1)
        val effectsF = rl(key2, 1, 1.seconds)

        var a: Int = 0

        val run =
          effectsF flatMap {
            case RateLimiterEffects(limit, _) =>
              plusOne >>
              limit >> IO.delay(a += 1) >>
              limit >> IO.delay(a += 1)
          }

        run.unsafeRunAsyncAndForget()

        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)
      }

      "modify state for known key" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val key1 = key.unsafeRunSync()

        val plusOne = rl.plusOne(key1)
        val effectsF = rl(key1, 1, 1.seconds)

        var a: Int = 0

        val run =
          effectsF flatMap {
            case RateLimiterEffects(limit, _) =>
              plusOne >>
              limit >> IO.delay(a += 1) >>
              limit >> IO.delay(a += 1)
          }

        run.unsafeRunAsyncAndForget()

        a mustEqual(0)

        ctx.tick(1.seconds)
        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)
      }
    }

    "handle configure request" >> {
      "add config for unknown key" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val key1: UUID = key.unsafeRunSync()

        val configure = rl.configure(key1, RateLimiterConfig(2, 1.seconds))
        val effectsF = rl(key1, 1, 1.seconds)

        var a: Int = 0

        val run =
          (configure >> effectsF) flatMap {
            case RateLimiterEffects(limit, _) =>
              limit >> IO.delay(a += 1) >>
              limit >> IO.delay(a += 1) >>
              limit >> IO.delay(a += 1) >>
              limit >> IO.delay(a += 1)
          }

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(4)
      }

      "ignore config added for known key" in {
        val ctx = TestContext()

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, NoopRateLimitUpdater[IO, UUID])(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val key1: UUID = key.unsafeRunSync()

        val configure = rl.configure(key1, RateLimiterConfig(2, 1.seconds))
        val effectsF = rl(key1, 1, 1.seconds)

        var a: Int = 0

        val run =
          effectsF flatMap {
            case RateLimiterEffects(limit, _) =>
              configure >>
              limit >> IO.delay(a += 1) >>
              limit >> IO.delay(a += 1) >>
              limit >> IO.delay(a += 1) >>
              limit >> IO.delay(a += 1)
          }

        run.unsafeRunAsyncAndForget()

        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(4)
      }
    }

    "send updates through the updater" >> {
      "one event per second" in {
        val ctx = TestContext()
        val updater = new TestRateLimitUpdater[UUID]

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, updater)(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val k1 = key.unsafeRunSync()

        val RateLimiterEffects(limit, _) = rl(k1, 1, 1.seconds).unsafeRunSync()

        updater.configs must containTheSameElementsAs(List(k1))

        (limit >> limit >> limit).unsafeRunAsyncAndForget()

        updater.plusOnes must containTheSameElementsAs(List(k1))
        updater.waits must containTheSameElementsAs(List(k1))

        ctx.tick(1.seconds)
        updater.plusOnes must containTheSameElementsAs(List(k1, k1))
        updater.waits must containTheSameElementsAs(List(k1, k1))

        ctx.tick(1.seconds)
        updater.plusOnes must containTheSameElementsAs(List(k1, k1, k1))
        updater.waits must containTheSameElementsAs(List(k1, k1))
      }

      "two events per second" in {
        val ctx = TestContext()
        val updater = new TestRateLimitUpdater[UUID]

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, updater)(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val k1 = key.unsafeRunSync()

        val RateLimiterEffects(limit, _) = rl(k1, 2, 1.seconds).unsafeRunSync()

        updater.configs must containTheSameElementsAs(List(k1))

        (limit >> limit >> limit >> limit).unsafeRunAsyncAndForget()

        updater.plusOnes must containTheSameElementsAs(List(k1, k1))
        updater.waits must containTheSameElementsAs(List(k1))

        ctx.tick(1.seconds)
        updater.plusOnes must containTheSameElementsAs(List(k1, k1, k1, k1))
        updater.waits must containTheSameElementsAs(List(k1))
      }

      "two keys" in {
        val ctx = TestContext()
        val updater = new TestRateLimitUpdater[UUID]

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](1.0, freshKey, updater)(Sync[IO], ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val k1 = key.unsafeRunSync()
        val k2 = key.unsafeRunSync()

        val RateLimiterEffects(limit1, _) = rl(k1, 1, 1.seconds).unsafeRunSync()
        val RateLimiterEffects(limit2, _) = rl(k2, 2, 1.seconds).unsafeRunSync()

        updater.configs must containTheSameElementsAs(List(k1, k2))

        (limit1 >> limit1 >> limit1 >> limit1).unsafeRunAsyncAndForget()
        (limit2 >> limit2 >> limit2 >> limit2).unsafeRunAsyncAndForget()

        updater.plusOnes must containTheSameElementsAs(List(k1, k2, k2))
        updater.waits must containTheSameElementsAs(List(k1, k2))

        ctx.tick(1.seconds)
        updater.plusOnes must containTheSameElementsAs(List(k1, k2, k2, k1, k2, k2))
        updater.waits must containTheSameElementsAs(List(k1, k2, k1))

        ctx.tick(1.seconds)
        updater.plusOnes must containTheSameElementsAs(List(k1, k2, k2, k1, k2, k2, k1))
        updater.waits must containTheSameElementsAs(List(k1, k2, k1, k1))

        ctx.tick(1.seconds)
        updater.plusOnes must containTheSameElementsAs(List(k1, k2, k2, k1, k2, k2, k1, k1))
        updater.waits must containTheSameElementsAs(List(k1, k2, k1, k1))
      }
    }
  }
}
