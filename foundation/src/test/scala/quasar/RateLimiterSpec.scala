/*
 * Copyright 2020 Precog Data
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

import quasar.fp.ski.ι

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import java.util.UUID

import org.specs2.mutable.Specification

import fs2.Stream

import cats.effect._
import cats.effect.laws.util.TestContext
import cats.effect.testing.specs2.CatsIO
import cats.kernel.Hash
import cats.implicits._

object RateLimiterSpec extends Specification with CatsIO {

  import cats.effect.IO._

  implicit def executionContext: ExecutionContext = ExecutionContext.Implicits.global
  implicit val timer: Timer[IO] = IO.timer(executionContext)
  implicit val cs: ContextShift[IO] = IO.contextShift(executionContext)

  def freshKey: IO[UUID] = IO.delay(UUID.randomUUID())

  "rate limiter" should {
    "output events with real time" >> {
      "one event in one window" in {
        RateLimiter[IO, UUID](freshKey) use { limiting =>
          val rl = limiting.limiter
          val key = limiting.freshKey

          for {
            effects <- key.flatMap(k => rl(k, 1, 1.seconds, (_ + 1)))
            stream = Stream.eval_(effects.limit) ++ Stream.emit(1)
            values <- stream.compile.toList
          } yield {
            values mustEqual(List(1))
          }
        }
      }

      "two events in one window" in {
        RateLimiter[IO, UUID](freshKey) use { limiting =>
          val rl = limiting.limiter
          val key = limiting.freshKey

          for {
            effects <- key.flatMap(k => rl(k, 2, 1.seconds, (_ + 1)))
            stream =
              Stream.eval_(effects.limit) ++ Stream.emit(1) ++
                Stream.eval_(effects.limit) ++ Stream.emit(2)
            values <- stream.compile.toList
          } yield {
            values mustEqual(List(1, 2))
          }
        }
      }

      "two events in two windows" in {
        RateLimiter[IO, UUID](freshKey) use { limiting =>
          val rl = limiting.limiter
          val key = limiting.freshKey

          for {
            effects <- key.flatMap(k => rl(k, 1, 1.seconds, (_ + 1)))
            stream =
              Stream.eval_(effects.limit) ++ Stream.emit(1) ++
                Stream.eval_(effects.limit) ++ Stream.emit(2)
            values <- stream.compile.toList
          } yield {
            values mustEqual(List(1, 2))
          }
        }
      }

      "events from two keys" in {
        RateLimiter[IO, UUID](freshKey) use { limiting =>
          val rl = limiting.limiter
          val key = limiting.freshKey

          for {
            effects1 <- key.flatMap(k => rl(k, 1, 1.seconds, (_ + 1)))
            effects2 <- key.flatMap(k => rl(k, 1, 1.seconds, (_ + 1)))

            stream1 =
              Stream.eval_(effects1.limit) ++ Stream.emit(1) ++
                Stream.eval_(effects1.limit) ++ Stream.emit(2)
            stream2 =
              Stream.eval_(effects2.limit) ++ Stream.emit(3) ++
                Stream.eval_(effects2.limit) ++ Stream.emit(4)

            values1 <- stream1.compile.toList
            values2 <- stream2.compile.toList
          } yield {
            values1 mustEqual(List(1, 2))
            values2 mustEqual(List(3, 4))
          }
        }
      }
    }

    "output events with simulated time" >> {
      "one event per second" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        var a: Int = 0

        val run = RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]) use { limiting =>
          val rl = limiting.limiter
          val key = limiting.freshKey

          for {
            effects <- key.flatMap(k => rl(k, 1, 1.seconds, (_ + 1)))
            limit = effects.limit
            back <-
               limit >> IO.delay(a += 1) >>
                 limit >> IO.delay(a += 1) >>
                 limit >> IO.delay(a += 1) >>
                 limit >> IO.delay(a += 1)
          } yield back
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

      "one event per two seconds" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        var a: Int = 0

        val run = RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]) use { limiting =>
          val rl = limiting.limiter
          val key = limiting.freshKey

          for {
            effects <- key.flatMap(k => rl(k, 1, 2.seconds, (_ + 1)))
            limit = effects.limit
            back <-
              limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1)
          } yield back
        }

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
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        var a: Int = 0

        val run = RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]) use { limiting =>
          val rl = limiting.limiter
          val key = limiting.freshKey

          for {
            effects <- key.flatMap(k => rl(k, 2, 1.seconds, (_ + 1)))
            limit = effects.limit
            back <-
              limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1)
          } yield back
        }

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(6)
      }

      "three events per second" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        var a: Int = 0

        val run = RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]) use { limiting =>
          val rl = limiting.limiter
          val key = limiting.freshKey

          for {
            effects <- key.flatMap(k => rl(k, 3, 1.seconds, (_ + 1)))
            limit = effects.limit
            back <-
              limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1)
          } yield back
        }

        run.unsafeRunAsyncAndForget()

        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(6)

        ctx.tick(1.seconds)
        a mustEqual(8)
      }

      "extra sleep so that we don't fill the full request quota" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])
        val timer = ctx.timer[IO]

        var a: Int = 0

        val run = RateLimiter[IO, UUID](freshKey)(concurrent, timer, Hash[UUID]) use { limiting =>
          val rl = limiting.limiter
          val key = limiting.freshKey

          for {
            effects <- key.flatMap(k => rl(k, 3, 1.seconds, (_ + 1)))
            limit = effects.limit
            back <-
              limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                timer.sleep(1.seconds) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1)
          } yield back
        }

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(5)

        ctx.tick(1.seconds)
        a mustEqual(8)
      }

      "concurrently make fewer requests than the limit per window" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        var a: Int = 0

        val run = RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]) use { limiting =>
          val rl = limiting.limiter
          val key = limiting.freshKey

          for {
            effects <- key.flatMap(k => rl(k, 4, 1.seconds, (_ + 1)))
            limit = effects.limit

            run1 =
              limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1)

            run2 =
              limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1)

            run3 =
              limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1)

            run <- concurrent.start(run1) >> concurrent.start(run2) >> run3
          } yield run
        }

        run.unsafeRunAsyncAndForget()

        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(8)

        ctx.tick(1.seconds)
        a mustEqual(12)

        ctx.tick(1.seconds)
        a mustEqual(16)

        ctx.tick(1.seconds)
        a mustEqual(19)
      }

      "concurrently make more requests than the limit per window" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        var a: Int = 0

        val run = RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]) use { limiting =>
          val rl = limiting.limiter
          val key = limiting.freshKey

          for {
            effects <- key.flatMap(k => rl(k, 2, 1.seconds, (_ + 1)))
            limit = effects.limit

            run1 =
              limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1)

            run2 =
              limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1)

            run3 =
              limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1)

            run <- concurrent.start(run1) >> concurrent.start(run2) >> run3
          } yield run
        }

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(6)

        ctx.tick(1.seconds)
        a mustEqual(8)

        ctx.tick(1.seconds)
        a mustEqual(10)

        ctx.tick(1.seconds)
        a mustEqual(12)

        ctx.tick(1.seconds)
        a mustEqual(14)

        ctx.tick(1.seconds)
        a mustEqual(16)

        ctx.tick(1.seconds)
        a mustEqual(18)

        ctx.tick(1.seconds)
        a mustEqual(19)
      }

      "do not overwrite configs (use existing config)" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        var a: Int = 0

        val run = RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]) use { limiting =>
          val rl = limiting.limiter
          val key = limiting.freshKey

          for {
            k1 <- key

            _ <- rl(k1, 2, 1.seconds, (_ + 1))
            effects <- rl(k1, 3, 1.seconds, (_ + 1))

            limit = effects.limit
            back <-
              limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1) >>
                limit >> IO.delay(a += 1)
          } yield back
        }

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(6)
      }

      "support two keys on the same schedule" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        var a1: Int = 0
        var a2: Int = 0

        val run = RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]) use { limiting =>
          val rl = limiting.limiter
          val key = limiting.freshKey

          for {
            effects1 <- key.flatMap(k => rl(k, 2, 1.seconds, (_ + 1)))
            effects2 <- key.flatMap(k => rl(k, 3, 1.seconds, (_ + 1)))

            limit1 = effects1.limit
            limit2 = effects2.limit

            run1 =
              limit1 >> IO.delay(a1 += 1) >>
                limit1 >> IO.delay(a1 += 1) >>
                limit1 >> IO.delay(a1 += 1) >>
                limit1 >> IO.delay(a1 += 1) >>
                limit1 >> IO.delay(a1 += 1) >>
                limit1 >> IO.delay(a1 += 1)

            run2 =
              limit2 >> IO.delay(a2 += 1) >>
                limit2 >> IO.delay(a2 += 1) >>
                limit2 >> IO.delay(a2 += 1) >>
                limit2 >> IO.delay(a2 += 1) >>
                limit2 >> IO.delay(a2 += 1) >>
                limit2 >> IO.delay(a2 += 1) >>
                limit2 >> IO.delay(a2 += 1) >>
                limit2 >> IO.delay(a2 += 1)

            run <- concurrent.start(run1) >> run2
          } yield run
        }

        run.unsafeRunAsyncAndForget()

        ctx.tick(0.seconds) // allow the concurrent fibers to start, see comments in https://github.com/precog/quasar/pull/4743
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
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        var a1: Int = 0
        var a2: Int = 0

        val run = RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]) use { limiting =>
          val rl = limiting.limiter
          val key = limiting.freshKey

          for {
            effects1 <- key.flatMap(k => rl(k, 2, 1.seconds, (_ + 1)))
            effects2 <- key.flatMap(k => rl(k, 2, 2.seconds, (_ + 1)))

            limit1 = effects1.limit
            limit2 = effects2.limit

            run1 =
              limit1 >> IO.delay(a1 += 1) >>
                limit1 >> IO.delay(a1 += 1) >>
                limit1 >> IO.delay(a1 += 1) >>
                limit1 >> IO.delay(a1 += 1) >>
                limit1 >> IO.delay(a1 += 1) >>
                limit1 >> IO.delay(a1 += 1)

            run2 =
              limit2 >> IO.delay(a2 += 1) >>
                limit2 >> IO.delay(a2 += 1) >>
                limit2 >> IO.delay(a2 += 1) >>
                limit2 >> IO.delay(a2 += 1) >>
                limit2 >> IO.delay(a2 += 1) >>
                limit2 >> IO.delay(a2 += 1)

            run <- concurrent.start(run1) >> run2
          } yield run
        }

        run.unsafeRunAsyncAndForget()

        ctx.tick(0.seconds) // allow the concurrent fibers to start, see comments in https://github.com/precog/quasar/pull/4743
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

    "respect backoff effect" in {
      val ctx = TestContext()
      val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

      var a: Int = 0

      val run = RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]) use { limiting =>
        val rl = limiting.limiter
        val key = limiting.freshKey

        for {
          effects <- key.flatMap(k => rl(k, 1, 2.seconds, (_ + 1)))

          limit = effects.limit
          backoff = effects.backoff

          back <-
            backoff >>
            limit >> IO.delay(a += 1) >>
            backoff >>
            limit >> IO.delay(a += 1) >>
            backoff >>
            limit >> IO.delay(a += 1)
        } yield back
      }

      run.unsafeRunAsyncAndForget()

      a mustEqual(0)

      ctx.tick(1.seconds)
      a mustEqual(0)

      ctx.tick(1.seconds)
      a mustEqual(1)

      ctx.tick(1.seconds)
      a mustEqual(1)

      ctx.tick(1.seconds)
      a mustEqual(2)

      ctx.tick(1.seconds)
      a mustEqual(2)

      ctx.tick(1.seconds)
      a mustEqual(3)
    }

    "allows setting limits externally" in {
      val ctx = TestContext()
      val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

      var a: Int = 0

      val run = RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]) use { limiting =>
        val rl = limiting.limiter
        val key = limiting.freshKey

        for {
          effects <- key.flatMap(k => rl(k, 100, 5.seconds, ι))

          limit = effects.limit
          backoff = effects.backoff
          setUsage = effects.setUsage

          back <-
            limit >> IO.delay(a += 1) >>
            setUsage(100) >>
            limit >> IO.delay(a += 1) >>
            setUsage(5) >>
            limit >> IO.delay(a += 1)
        } yield back
      }

      run.unsafeRunAsyncAndForget()

      a mustEqual(1)

      ctx.tick(1.seconds)

      a mustEqual(1)

      ctx.tick(4.seconds)

      a mustEqual(3)
    }

    "external limit is independent of event count" in {
      val ctx = TestContext()
      val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

      var a: Int = 0

      val run = RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]) use { limiting =>
        val rl = limiting.limiter
        val key = limiting.freshKey

        for {
          effects <- key.flatMap(k => rl(k, 3, 5.seconds, ι))

          limit = effects.limit
          backoff = effects.backoff
          setUsage = effects.setUsage

          back <-
            limit >> IO.delay(a += 1) >>
            setUsage(1) >>
            limit >> IO.delay(a += 1) >>
            setUsage(1) >>
            limit >> IO.delay(a += 1) >>
            setUsage(2) >>
            limit >> IO.delay(a += 1) >>
            setUsage(2) >>
            limit >> IO.delay(a += 1)
        } yield back
      }

      run.unsafeRunAsyncAndForget()

      ctx.tick(5.seconds)

      a mustEqual(5) // 5 events with a limit of 3
    }
  }
}
