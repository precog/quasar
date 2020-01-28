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

package quasar.impl.datasources

import quasar.connector.ByteStore

import cats.Applicative
import cats.implicits._

import scala.Unit

trait ByteStores[F[_], K] {
  /** Returns the `ByteStore` for the specified key. */
  def get(key: K): F[ByteStore[F]]

  /** Removes all associations from the `ByteStore` for the specified key. */
  def clear(key: K): F[Unit]
}

object ByteStores {
  def void[F[_]: Applicative, I]: ByteStores[F, I] =
    new ByteStores[F, I] {
      def get(key: I) = ByteStore.void[F].pure[F]
      def clear(key: I) = ().pure[F]
    }
}
