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

import quasar.contrib.cats.hash.toHashing
import quasar.contrib.cats.eqv.toEquiv

import java.util.concurrent.TimeUnit

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._

import cats.effect.{Sync, Timer}
import cats.effect.concurrent.Ref
import cats.kernel.Hash
import cats.implicits._

final class RateLimiter[F[_]: Sync: Timer, A: Hash] private (
    caution: Double,
    updater: RateLimitUpdater[F, A]) {

  // TODO make this clustering-aware
  private val configs: TrieMap[A, RateLimiterConfig] =
    new TrieMap[A, RateLimiterConfig](toHashing[A], toEquiv[A])

  // TODO make this clustering-aware
  private val states: TrieMap[A, Ref[F, State]] =
    new TrieMap[A, Ref[F, State]](toHashing[A], toEquiv[A])

  def configure(key: A, config: RateLimiterConfig)
      : F[Option[RateLimiterConfig]] =
    Sync[F].delay(configs.putIfAbsent(key, config))

  // TODO implement with TrieMap#updateWith when we're on Scala 2.13
  def plusOne(key: A): F[Unit] =
    for {
      ref <- Sync[F].delay(states.get(key))
      now <- nowF
      _ <- ref match {
        case Some(r) =>
          r.modify(s => (s.copy(count = s.count + 1), ()))
        case None =>
          for {
            now <- nowF
            ref <- Ref.of[F, State](State(1, now))
            put <- Sync[F].delay(states.putIfAbsent(key, ref))
            _ <- put match {
              case Some(_) => plusOne(key) // retry
              case None => ().pure[F]
            }
          } yield ()
      }
    } yield ()

  // TODO implement with TrieMap#updateWith when we're on Scala 2.13
  def wait(key: A, duration: FiniteDuration): F[Unit] =
    for {
      ref <- Sync[F].delay(states.get(key))
      now <- nowF
      _ <- ref match {
        case Some(r) =>
          r.update(_ => State(0, now + duration))
        case None =>
          for {
            ref <- Ref.of[F, State](State(0, now + duration))
            put <- Sync[F].delay(states.putIfAbsent(key, ref))
              _ <- put match {
                case Some(_) => wait(key, duration) // retry
                case None => ().pure[F]
              }
          } yield ()
      }
    } yield ()

  def apply(key: A, max: Int, window: FiniteDuration)
      : F[RateLimiterEffects[F]] =
    for {
      config <- Sync[F] delay {
        val c = RateLimiterConfig(max, window)
        configs.putIfAbsent(key, c).getOrElse(c)
      }

      _ <- updater.config(key, config)

      now <- nowF
      maybeR <- Ref.of[F, State](State(0, now))
      stateRef <- Sync[F] delay {
        states.putIfAbsent(key, maybeR).getOrElse(maybeR)
      }
    } yield {
      RateLimiterEffects[F](
        limit(key, config, stateRef),
        backoff(key, config, stateRef))
    }

  // TODO wait smarter (i.e. not for an entire window)
  // the server's window falls in our previous window between
  // max and max+1 requests prior to the server-throttled request
  private def backoff(key: A, config: RateLimiterConfig, stateRef: Ref[F, State])
      : F[Unit] =
    nowF.flatMap(now =>
      stateRef.update(_ => State(0, now + config.window)) >>
        updater.wait(key, config.window))

  private def limit(key: A, config: RateLimiterConfig, stateRef: Ref[F, State])
      : F[Unit] = {
    import config._

    for {
      now <- nowF
      state <- stateRef.get
      back <-
        if (state.start > now) { // waiting
          Timer[F].sleep(state.start - now) >>
            limit(key, config, stateRef)
        } else if (state.start + window < now) { // in the next window
          stateRef.update(_ => State(0, state.start + window)) >>
            limit(key, config, stateRef)
        } else { // in the current window
          stateRef.modify(s => (s.copy(count = s.count + 1), s.count)) flatMap { count =>
            if (count >= max * caution) { // max exceeded
              val duration = (state.start + window) - now
              updater.wait(key, duration) >>
                Timer[F].sleep(duration) >>
                stateRef.update(_ => State(0, state.start + window)) >>
                limit(key, config, stateRef)
            } else { // continue
              updater.plusOne(key)
            }
          }
        }
    } yield back
  }

  private val nowF: F[FiniteDuration] =
    Timer[F].clock.realTime(TimeUnit.MILLISECONDS).map(_.millis)

  private case class State(count: Int, start: FiniteDuration)
}

object RateLimiter {
  def apply[F[_]: Sync: Timer, A: Hash](
      caution: Double,
      freshKey: F[A],
      updater: RateLimitUpdater[F, A])
      : F[RateLimiting[F, A]] =
    Sync[F].delay(RateLimiting[F, A](
      new RateLimiter[F, A](caution, updater),
      freshKey))
}
