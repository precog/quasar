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

package quasar.api.destination.param

import cats.{Eq, Show}

import scala.{math, Boolean, Int, Product, Serializable}

sealed trait IntegerStep extends Product with Serializable with (Int => Boolean)

object IntegerStep {
  final case class Factor(init: Int, n: Int) extends IntegerStep {
    def apply(i: Int): Boolean =
      i % n == init
  }

  final case class Power(init: Int, n: Int) extends IntegerStep {
    def apply(i: Int): Boolean =
      math.round(math.log(i.toDouble) / math.log(n.toDouble)) == init
  }

  implicit val equal: Eq[IntegerStep] =
    Eq.fromUniversalEquals[IntegerStep]

  implicit val show: Show[IntegerStep] =
    Show.fromToString[IntegerStep]
}
