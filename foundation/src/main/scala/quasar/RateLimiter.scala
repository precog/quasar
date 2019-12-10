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

import quasar.contrib.cats.hash.toHashing
import quasar.contrib.cats.eqv.toEquiv

import java.util.concurrent.TimeUnit

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.reflect.{ClassTag, classTag}

import cats.effect.{Sync, Timer}
import cats.effect.concurrent.Ref
import cats.kernel.Hash
import cats.implicits._

import skolems._

import RateLimiter.{Config, Key, State}

final class RateLimiter[F[_]: Sync: Timer] private (
    configs: TrieMap[Exists[Key], Config],
    states: TrieMap[Exists[Key], Ref[F, State]])(
    caution: Double) {

  def apply[A: Hash: ClassTag](key: A, max: Int, window: FiniteDuration)
      : F[F[Unit]] =
    for {
      config <- Sync[F] delay {
        val c = Config(max, window)
        configs.putIfAbsent(Key(key, Hash[A], classTag[A]), c).getOrElse(c)
      }

      now <- nowF
      maybeR <- Ref.of[F, State](State(0, now))
      stateRef <- Sync[F] delay {
        states.putIfAbsent(Key(key, Hash[A], classTag[A]), maybeR).getOrElse(maybeR)
      }
    } yield limit(config, stateRef)

  private def limit(config: Config, stateRef: Ref[F, State]): F[Unit] = {
    import config._

    val emptyStateF: F[Boolean] =
      nowF.flatMap(now => stateRef.tryUpdate(_ => State(0, now)))

    for {
      now <- nowF
      state <- stateRef.get
      back <-
        if (state.start + window < now) {
          emptyStateF >> limit(config, stateRef)
        } else {
          stateRef.modify(s => (s.copy(count = s.count + 1), s.count)) flatMap { count =>
            if (count >= max * caution) {
              Timer[F].sleep((state.start + window) - now) >>
                emptyStateF >>
                limit(config, stateRef)
            } else
              ().pure[F]
          }
        }
    } yield back
  }

  private val nowF: F[FiniteDuration] =
    Timer[F].clock.realTime(TimeUnit.MILLISECONDS).map(_.millis)
}

object RateLimiter {

  final case class Config(max: Int, window: FiniteDuration)
  final case class State(count: Int, start: FiniteDuration)

  final case class Key[A](value: A, hash: Hash[A], tag: ClassTag[A])

  object Key {
    implicit def hash: Hash[Exists[Key]] =
      new Hash[Exists[Key]] {

        def hash(k: Exists[Key]) =
          k().hash.hash(k().value)

        def eqv(left: Exists[Key], right: Exists[Key]) = {
          (left().tag == right().tag) &&
            left().hash.eqv(
              left().value,
              right().value.asInstanceOf[left.A])
        }
      }
  }

  def apply[F[_]: Sync: Timer](caution: Double)
      : F[RateLimiter[F]] = {

    val configs: TrieMap[Exists[Key], Config] =
      new TrieMap[Exists[Key], Config](
        toHashing[Exists[Key]],
        toEquiv[Exists[Key]])

    val states: TrieMap[Exists[Key], Ref[F, State]] =
      new TrieMap[Exists[Key], Ref[F, State]](
        toHashing[Exists[Key]],
        toEquiv[Exists[Key]])

    Sync[F].delay(new RateLimiter[F](configs, states)(caution))
  }

  def applyAll[F[_]: Sync: Timer](
      configs: TrieMap[Exists[Key], Config],
      states: TrieMap[Exists[Key], Ref[F, State]])(
      caution: Double)
      : F[RateLimiter[F]] =
    Sync[F].delay(new RateLimiter[F](configs, states)(caution))
}
