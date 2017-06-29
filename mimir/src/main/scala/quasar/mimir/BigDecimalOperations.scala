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

package quasar.mimir

object BigDecimalOperations {

  /**
    * Newton's approximation to some number of iterations (by default: 50).
    * Ported from a Java example found here: http://www.java2s.com/Code/Java/Language-Basics/DemonstrationofhighprecisionarithmeticwiththeBigDoubleclass.htm
    */
  def sqrt(d: BigDecimal, k: Int = 50): BigDecimal = {
    if (d > 0) {
      lazy val approx = { // could do this with a self map, but it would be much slower
        def gen(x: BigDecimal): Stream[BigDecimal] = {
          val x2 = (d + x * x) / (x * 2)

          lazy val tail =
            if (x2 == x)
              Stream.empty
            else
              gen(x2)

          x2 #:: tail
        }

        gen(d / 3)
      }

      approx take k last
    } else if (d == 0) {
      0
    } else {
      sys.error("square root of a negative number")
    }
  }
}
