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

package quasar.yggdrasil
package util

import quasar.blueeyes._
import quasar.precog.util.NumericComparisons

/**
  * Compare values of different types.
  */
trait HetOrder[@specialized(Boolean, Long, Double, AnyRef) A, @specialized(Boolean, Long, Double, AnyRef) B] {
  def compare(a: A, b: B): Int
}

trait HetOrderLow {
  def reverse[@specialized(Boolean, Long, Double, AnyRef) A, @specialized(Boolean, Long, Double, AnyRef) B](ho: HetOrder[A, B]) =
    new HetOrder[B, A] {
      def compare(b: B, a: A) = {
        val cmp = ho.compare(a, b)
        if (cmp < 0) 1 else if (cmp == 0) 0 else -1
      }
    }

  implicit def fromOrder[@specialized(Boolean, Long, Double, AnyRef) A](implicit o: SpireOrder[A]) = new HetOrder[A, A] {
    def compare(a: A, b: A) = o.compare(a, b)
  }
}

object HetOrder extends HetOrderLow {
  implicit def fromScalazOrder[A](implicit z: ScalazOrder[A]) = new HetOrder[A, A] {
    def compare(a: A, b: A): Int = z.order(a, b).toInt
  }

  implicit def DoubleLongOrder       = reverse(LongDoubleOrder)
  implicit def BigDecimalLongOrder   = reverse(LongBigDecimalOrder)
  implicit def BigDecimalDoubleOrder = reverse(DoubleBigDecimalOrder)

  implicit object LongDoubleOrder extends HetOrder[Long, Double] {
    def compare(a: Long, b: Double): Int = NumericComparisons.compare(a, b)
  }

  implicit object LongBigDecimalOrder extends HetOrder[Long, BigDecimal] {
    def compare(a: Long, b: BigDecimal): Int = NumericComparisons.compare(a, b)
  }

  implicit object DoubleBigDecimalOrder extends HetOrder[Double, BigDecimal] {
    def compare(a: Double, b: BigDecimal): Int = NumericComparisons.compare(a, b)
  }

  @inline final def apply[@specialized(Boolean, Long, Double, AnyRef) A, @specialized(Boolean, Long, Double, AnyRef) B](implicit ho: HetOrder[A, B]) = ho
}
