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

package quasar.contrib.scalaz

import scalaz._, Scalaz._, Leibniz.===

final class BitraverseOps[F[_, _], A, B] private[scalaz] (self: F[A, B])(implicit F0: Bitraverse[F]) {
  final def uTraverse[G[_]: Applicative, C](f: A => G[C])(implicit ev: B === A): G[F[C, C]] =
    F0.uTraverse.traverse(self rightMap ev)(f)
}

trait ToBitraverseOps {
  implicit def toBitraverseOps[F[_, _]: Bitraverse, A, B](self: F[A, B]): BitraverseOps[F, A, B] =
    new BitraverseOps(self)
}

object bitraverse extends ToBitraverseOps
