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

package quasar.mimir.evaluate

import cats.effect.Sync
import cats.effect.concurrent.Ref

import scala.{Product, Serializable}

sealed trait Pushdown extends Product with Serializable

object Pushdown {
  final case object EnablePushdown extends Pushdown
  final case object DisablePushdown extends Pushdown
}

final class PushdownControl[F[_]: Sync](ref: Ref[F, Pushdown]) {
  def set(pushdown: Pushdown): F[Unit] = ref.set(pushdown)
  def get: F[Pushdown] = ref.get
}
