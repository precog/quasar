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

import scalaz._

final class DisjunctionOps[A, B] private[scalaz] (val self: A \/ B) extends scala.AnyVal {
  final def liftT[F[_]: Applicative]: EitherT[F, A, B] =
    EitherT.fromDisjunction[F](self)
}

trait ToDisjunctionOps {
  implicit def toDisjunctionOps[A, B](self: A \/ B): DisjunctionOps[A, B] =
    new DisjunctionOps(self)
}

object disjunction extends ToDisjunctionOps
