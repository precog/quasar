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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration

import cats.effect.IO

class TestRateLimitUpdater[A] extends RateLimitUpdater[IO, A] {
  val plusOnes: ArrayBuffer[A] = ArrayBuffer()
  val waits: ArrayBuffer[A] = ArrayBuffer()
  val configs: ArrayBuffer[A] = ArrayBuffer()

  def plusOne(key: A): IO[Unit] = IO.delay(plusOnes += key)
  def wait(key: A, duration: FiniteDuration): IO[Unit] = IO.delay(waits += key)
  def config(key: A, config: RateLimiterConfig): IO[Unit] = IO.delay(configs += key)
}
