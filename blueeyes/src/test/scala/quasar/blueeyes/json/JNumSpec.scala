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

package quasar.blueeyes
package json

import quasar.precog.TestSupport._
class JNumSpec extends Specification {
  "JNums must sort correctly" in {
    val s1: JNum = JNumStr("123.456")
    val s2: JNum = JNumStr("-1.0")
    val s3: JNum = JNumStr("3333333.0")
    val s4: JNum = JNumStr("0.00000001")

    val n1: JNum = JNum(123L)
    val n2: JNum = JNum(124L)
    val n3: JNum = JNum(-1L)
    val n4: JNum = JNum(3333333L)
    val n5: JNum = JNum(0L)
    val n6: JNum = JNum(1L)

    val d1: JNum  = JNum(123.455555555555)
    val d2: JNum  = JNum(123.456)
    val d3: JNum  = JNum(123.4560000000923)
    val d4: JNum  = JNum(-1.000000472)
    val d5: JNum  = JNum(-1.0)
    val d6: JNum  = JNum(-0.99999999999)
    val d7: JNum  = JNum(3333333.0)
    val d8: JNum  = JNum(0.0)
    val d9: JNum  = JNum(0.00000001)
    val d10: JNum = JNum(0.00000001110101)

    val b1: JNum  = JNum(BigDecimal("123.45999999999999999999999"))
    val b2: JNum  = JNum(BigDecimal("123.45600000000000000000000"))
    val b3: JNum  = JNum(BigDecimal("123.45600000000000000000001"))
    val b4: JNum  = JNum(BigDecimal("-1.000000000000000000000000000000000001"))
    val b5: JNum  = JNum(BigDecimal("-1.000000000000000000000000000000000000"))
    val b6: JNum  = JNum(BigDecimal("-0.999999999999999999999999999999999999"))
    val b7: JNum  = JNum(BigDecimal("3333333.00000000000001"))
    val b8: JNum  = JNum(BigDecimal("3333333.0"))
    val b9: JNum  = JNum(BigDecimal("3333332.99999999999999"))
    val b10: JNum = JNum(BigDecimal("0.0000000999999999999999999"))
    val b11: JNum = JNum(BigDecimal("0.00000001"))
    val b12: JNum = JNum(BigDecimal("0.0000000100000000000000001"))

    val nums: List[JNum] = (
        s1 :: s2 :: s3 :: s4 ::
          n1 :: n2 :: n3 :: n4 :: n5 :: n6 ::
            d1 :: d2 :: d3 :: d4 :: d5 :: d6 :: d7 :: d8 :: d9 :: d10 ::
              b1 :: b2 :: b3 :: b4 :: b5 :: b6 :: b7 :: b8 :: b9 :: b10 :: b11 :: b12 ::
                Nil
      )

    type Q = (JNum, BigDecimal, Double, Long)
    implicit val ordq = new scala.math.Ordering[Q] {
      def compare(a: Q, b: Q) = a._1 numCompare b._1
    }

    // ensure that for all the numbers given, their BigDecimal, Double, and
    // Long interpretations all agree with their JNum ordering.
    val tpls = nums.map(n => (n, n.toBigDecimal, n.toDouble, n.toLong)).sorted
    tpls.sliding(2) forall {
      case (j1, b1, d1, n1) :: (j2, b2, d2, n2) :: Nil =>
        (b1 compare b2) must be_<(1)
        (d1 compare d2) must be_<(1)
        (n1 compare n2) must be_<(1)

      case _ => sys.error("invalid starting data")
    }
  }
}
