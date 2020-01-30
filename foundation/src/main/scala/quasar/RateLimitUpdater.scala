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

import slamdata.Predef.Unit

import scala.concurrent.duration.FiniteDuration

trait RateLimitUpdater[F[_], A] {
  def plusOne(key: A): F[Unit]
  def wait(key: A, duration: FiniteDuration): F[Unit]
  def config(key: A, config: RateLimiterConfig): F[Unit]
}
