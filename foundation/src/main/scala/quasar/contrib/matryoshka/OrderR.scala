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

package quasar.contrib.matryoshka

import matryoshka._
import matryoshka.implicits._
import scalaz._

object OrderR {
  def order[T, F[_]: Functor](a: T, b: T)(implicit T: Recursive.Aux[T, F], O: Delay[Order, F]): Ordering =
    O(orderR[T, F](O)).order(a.project, b.project)

  def orderR[T, F[_]: Functor](ordF: Delay[Order, F])(implicit T: Recursive.Aux[T, F]): Order[T] =
    Order.order[T](order[T, F](_, _)(Functor[F], T, ordF))
}
