/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.contrib.scalaz

import _root_.scalaz._, Scalaz._

trait bind {

  implicit final class MoreBindOps[F[_], A](val self: F[A])(implicit val F: Bind[F]) extends _root_.scalaz.syntax.Ops[F[A]] {
    def <<[B](b: => F[B]): F[A] = F.bind(self)(a => b.as(a))
  }

}

object bind extends bind