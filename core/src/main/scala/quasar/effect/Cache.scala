/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.effect

import scalaz._

final case class Cache[F[_], A](cached: F[A]) extends scala.AnyVal

object Cache {
  def apply[F[_], G[_]](f: F ~> G): Cache[F, ?] ~> G =
    f compose run[F]

  def cache[F[_]]: F ~> Cache[F, ?] =
    new (F ~> Cache[F, ?]) {
      def apply[A](fa: F[A]) = Cache(fa)
    }

  def run[F[_]]: Cache[F, ?] ~> F =
    new (Cache[F, ?] ~> F) {
      def apply[A](c: Cache[F, A]) = c.cached
    }

  def cached[S[_], F[_]](implicit F: F :<: S, C: Cache[F, ?] :<: S): S ~> S =
    new (S ~> S) {
      def apply[A](sa: S[A]) =
        F.prj(sa).fold(sa)(C compose cache[F])
    }
}
