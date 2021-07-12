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

import quasar.contrib.cats.hash.toHashing
import quasar.contrib.cats.eqv.toEquiv

import java.util.concurrent.TimeUnit

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Queue
import scala.concurrent.duration._

import cats.effect.{Concurrent, Resource, Timer}
import cats.effect.concurrent.{Deferred, Ref}
import cats.kernel.Hash
import cats.implicits._

final class RateLimiter[F[_]: Concurrent: Timer, A: Hash] private () {

  // TODO make this clustering-aware
  private val configs: TrieMap[A, RateLimiterConfig] =
    new TrieMap[A, RateLimiterConfig](toHashing[A], toEquiv[A])

  // TODO make this clustering-aware
  private val states: TrieMap[A, Ref[F, State[F]]] =
    new TrieMap[A, Ref[F, State[F]]](toHashing[A], toEquiv[A])

  val cancelAll: F[Unit] = states.values.foldLeft(().pure[F]) {
    case (eff, ref) => (ref.modify[F[Unit]] {
      case Active(_, _, _, cancel) => (Done(), eff >> cancel)
      case Done() => (Done(), eff)
    }).flatten
  }

  def apply(key: A, max: Int, window: FiniteDuration, setUsage: Int => Int)
      : F[RateLimiterEffects[F]] =
    for {
      config <- Concurrent[F] delay {
        val c = RateLimiterConfig(max, window)
        configs.putIfAbsent(key, c).getOrElse(c)
      }

      now <- nowF
      maybeR <- Ref.of[F, State[F]](Active[F](now, 0, Queue(), ().pure[F]))
      stateRef <- Concurrent[F] delay {
        states.putIfAbsent(key, maybeR).getOrElse(maybeR)
      }
    } yield {
      RateLimiterEffects[F](
        limit(config, stateRef, setUsage),
        backoff(config, stateRef),
        setWindowUsage(stateRef))
    }

  private def setWindowUsage(stateRef: Ref[F, State[F]])(usage: Int)
      : F[Unit] =
    for {
      now <- nowF
      modified <- stateRef update {
        case Active(_, _, queue, cancel) => Active(now, usage, queue, cancel)
        case Done() => Done()
      }
    } yield modified


  /* The backoff function is as a damage control measure, providing a way for
   * information to be communicated back to the rate limiter.
   *
   * It sets the state as if it has reached the rate limit for a window starting
   * now. This will trigger any subsequent limits or drains to sleep.
   */
  private def backoff(config: RateLimiterConfig, stateRef: Ref[F, State[F]])
      : F[Unit] =
    setWindowUsage(stateRef)(config.max)

  /* Recursively drains the queue of deferred requests until it is empty.
   *
   * We check which window we're in and determine if we're within the request limit.
   */
  private def drain(config: RateLimiterConfig, stateRef: Ref[F, State[F]], setUsage: Int => Int)
      : F[Unit] = {
    val window = config.window
    val max = config.max

    val back = for {
      now <- nowF
      modified <- stateRef modify[F[Unit]] {
        case Done() => (Done(), ().pure[F])

        // nothing available to drain, stop the recusion
        case Active(current, count, queue, _) if queue.isEmpty =>
          (Active(current, count, Queue(), ().pure[F]), ().pure[F])

        // something available to drain
        case Active(current, count, queue, cancel) =>
          if (current + window <= now) { // outside current window, reset the state and loop
            val state = Active(current + window, 0, queue, cancel)
            val effect = drain(config, stateRef, setUsage)
            (state, effect)
          } else if (count < max) { // in current window, and within the limit
            val (elem, remaining) = queue.dequeue
            val state = Active(current, setUsage(count), remaining, cancel)
            val effect = elem.complete(()) >> drain(config, stateRef, setUsage)
            (state, effect)
          } else { // in current window, limit exceeded
            val state = Active(current + window, 0, queue, cancel)
            val effect = Timer[F].sleep((current + window) - now) >> drain(config, stateRef, setUsage)
            (state, effect)
          }
      }
    } yield modified

    back.flatten
  }

  /* If the queue is empty, we check which window we're in and determine if we're within the
   * request limit.
   *
   * If the queue is not empty, we enqueue the new request.
   */
  private def limit(config: RateLimiterConfig, stateRef: Ref[F, State[F]], setUsage: Int => Int)
      : F[Unit] = {
    val window = config.window
    val max = config.max

    val back = for {
      now <- nowF
      modified <- stateRef modify[F[Unit]] {
        case Done() => (Done(), ().pure[F])

        // the queue is empty, so attempt to continue
        case Active(current, count, queue, cancel) if queue.isEmpty =>
          if (current + window <= now) { // outside current window, reset the state and loop
            val state = Active[F](current + window, 0, queue, cancel)
            (state, limit(config, stateRef, setUsage))
          } else if (count < max) { // in current window, within the limit
            val state = Active[F](current, setUsage(count), queue, cancel)
            (state, ().pure[F])
          } else { // in current window, limit exceeded
            val deferred = Deferred.unsafe[F, Unit]
            val state = Active[F](current + window, 0, queue.enqueue(deferred), cancel)
            val sleep = Timer[F].sleep((current + window) - now)
            // start draining so we can get the deferred
            val started =
              Concurrent[F].start(sleep >> drain(config, stateRef, setUsage)).flatMap(fib =>
                stateRef modify[F[Unit]] {
                  case Active(start, count, queue, cancel) =>
                    // we shouldn't need to actually run the old cancel, but we do anyways
                    (Active(start, count, queue, fib.cancel), cancel)
                  case Done() =>
                    // avoid the race condition of starting a new fiber post-shutdown
                    (Done(), fib.cancel)
                }).flatten
            val effect = started >> deferred.get
            (state, effect)
          }

        // when the queue is non-empty, we enqueue all new requests
        case Active(current, count, queue, cancel) =>
          val deferred = Deferred.unsafe[F, Unit]
          val state = Active(current, count, queue.enqueue(deferred), cancel)
          (state, deferred.get)

      }
    } yield modified

    back.flatten
  }

  private val nowF: F[FiniteDuration] =
    Timer[F].clock.realTime(TimeUnit.MILLISECONDS).map(_.millis)

  private sealed trait State[F[_]]

  /* @param start the start time of the current window
   * @param count the number of requests made during the current window
   * @param queue the queue of requests deferred while rate limiting
   */
  private case class Active[F[_]](
      start: FiniteDuration,
      count: Int,
      queue: Queue[Deferred[F, Unit]],
      cancel: F[Unit]) extends State[F]

  private case class Done[F[_]]() extends State[F]
}

object RateLimiter {
  def apply[F[_]: Concurrent: Timer, A: Hash](freshKey: F[A])
      : Resource[F, RateLimiting[F, A]] = {

    val acquire = Concurrent[F].delay(RateLimiting[F, A](
      new RateLimiter[F, A](), freshKey))

    def release(rl: RateLimiting[F, A]): F[Unit] = rl.limiter.cancelAll

    Resource.make[F, RateLimiting[F, A]](acquire)(release)
  }
}
