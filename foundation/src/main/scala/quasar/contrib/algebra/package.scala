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

package quasar.contrib

import _root_.algebra.{Eq, Semigroup => ASemigroup, Monoid => AMonoid, Order => AOrder}
import _root_.scalaz.{Equal, Semigroup, Monoid, Order, Ordering}

package object algebra extends AlgebraInstancesLowPriority {
  implicit def algebraOrder[A](implicit A: AOrder[A]): Order[A] =
    Order.order((x, y) => Ordering.fromInt(A.compare(x, y)))

  implicit def algebraMonoid[A](implicit A: AMonoid[A]): Monoid[A] =
    Monoid.instance((x, y) => A.combine(x, y), A.empty)
}

sealed abstract class AlgebraInstancesLowPriority {
  implicit def algebraEqual[A](implicit A: Eq[A]): Equal[A] =
    Equal.equal(A.eqv)

  implicit def algebraSemigroup[A](implicit A: ASemigroup[A]): Semigroup[A] =
    Semigroup.instance((x, y) => A.combine(x, y))
}
