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

package quasar

import slamdata.Predef._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import org.specs2.mutable.Specification

import fs2.Stream

import cats.effect.{IO, Sync, Timer}
import cats.effect.laws.util.TestContext
import cats.kernel.Hash
import cats.implicits._

object RateLimiterSpec extends Specification {

  implicit def executionContext: ExecutionContext = ExecutionContext.Implicits.global
  implicit val timer: Timer[IO] = IO.timer(executionContext)

  private case object Token1 {
    implicit val eq: Hash[Token1.type] =
      Hash.fromUniversalHashCode[Token1.type]
  }

  private case object Token2 {
    implicit val eq: Hash[Token2.type] =
      Hash.fromUniversalHashCode[Token2.type]
  }

  "rate limiter" should {
    "output events with real time" >> {
      "one event in one window" in {
        val rl = RateLimiter[IO](1.0).unsafeRunSync()

        val effect: IO[Unit] = rl(Token1, 1, 1.seconds).unsafeRunSync()

        val back = Stream.eval_(effect) ++ Stream.emit(1)

        back.compile.toList.unsafeRunSync() mustEqual(List(1))
      }

      "two events in one window" in {
        val rl = RateLimiter[IO](1.0).unsafeRunSync()

        val effect: IO[Unit] = rl(Token1, 2, 1.seconds).unsafeRunSync()

        val back =
          Stream.eval_(effect) ++ Stream.emit(1) ++
            Stream.eval_(effect) ++ Stream.emit(2)

        back.compile.toList.unsafeRunSync() mustEqual(List(1, 2))
      }

      "two events in two windows" in {
        val rl = RateLimiter[IO](1.0).unsafeRunSync()

        val effect: IO[Unit] = rl(Token1, 1, 1.seconds).unsafeRunSync()

        val back =
          Stream.eval_(effect) ++ Stream.emit(1) ++
            Stream.eval_(effect) ++ Stream.emit(2)

        back.compile.toList.unsafeRunSync() mustEqual(List(1, 2))
      }

      "events from two tokens" in {
        val rl = RateLimiter[IO](1.0).unsafeRunSync()

        val effect1: IO[Unit] = rl(Token1, 1, 1.seconds).unsafeRunSync()
        val effect2: IO[Unit] = rl(Token2, 1, 1.seconds).unsafeRunSync()

        val back1 =
          Stream.eval_(effect1) ++ Stream.emit(1) ++
            Stream.eval_(effect1) ++ Stream.emit(2)

        val back2 =
          Stream.eval_(effect2) ++ Stream.emit(3) ++
            Stream.eval_(effect2) ++ Stream.emit(4)

        back1.compile.toList.unsafeRunSync() mustEqual(List(1, 2))
        back2.compile.toList.unsafeRunSync() mustEqual(List(3, 4))
      }
    }

    "output events with simulated time" >> {
      "one event per second" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO](1.0)(Sync[IO], ctx.timer[IO]).unsafeRunSync()

        val effect: IO[Unit] = rl(Token1, 1, 1.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1)

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
        val rl = RateLimiter[IO](1.0)(Sync[IO], ctx.timer[IO]).unsafeRunSync()

        val effect: IO[Unit] = rl(Token1, 1, 2.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1)

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
        val rl = RateLimiter[IO](1.0)(Sync[IO], ctx.timer[IO]).unsafeRunSync()

        val effect: IO[Unit] = rl(Token1, 2, 1.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(6)
      }

      "three events per second" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO](1.0)(Sync[IO], ctx.timer[IO]).unsafeRunSync()

        val effect: IO[Unit] = rl(Token1, 3, 1.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(6)

        ctx.tick(1.seconds)
        a mustEqual(8)
      }

      "with a caution of 0.75" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO](0.75)(Sync[IO], ctx.timer[IO]).unsafeRunSync()

        val effect: IO[Unit] = rl(Token1, 4, 1.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(6)

        ctx.tick(1.seconds)
        a mustEqual(8)
      }

      "do not overwrite configs (use existing config)" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO](1.0)(Sync[IO], ctx.timer[IO]).unsafeRunSync()

        val effect1: IO[Unit] = rl(Token1, 2, 1.seconds).unsafeRunSync()
        val effect2: IO[Unit] = rl(Token1, 3, 1.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          effect2 >> IO.delay(a += 1) >>
            effect2 >> IO.delay(a += 1) >>
            effect2 >> IO.delay(a += 1) >>
            effect2 >> IO.delay(a += 1) >>
            effect2 >> IO.delay(a += 1) >>
            effect2 >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(6)
      }

      "support two tokens on the same schedule" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO](1.0)(Sync[IO], ctx.timer[IO]).unsafeRunSync()

        val effect1: IO[Unit] = rl(Token1, 2, 1.seconds).unsafeRunSync()
        val effect2: IO[Unit] = rl(Token2, 3, 1.seconds).unsafeRunSync()

        var a1: Int = 0
        var a2: Int = 0

        val run1 =
          effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1)

        val run2 =
          effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1)

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

      "support two tokens on different schedules" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO](1.0)(Sync[IO], ctx.timer[IO]).unsafeRunSync()

        val effect1: IO[Unit] = rl(Token1, 2, 1.seconds).unsafeRunSync()
        val effect2: IO[Unit] = rl(Token2, 2, 2.seconds).unsafeRunSync()

        var a1: Int = 0
        var a2: Int = 0

        val run1 =
          effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1)

        val run2 =
          effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1)

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
  }
}
