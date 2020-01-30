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

package quasar.numeric

import slamdata.Predef._

import spire.math.Real

object NumericConversions {

  /** If the input falls outside the range of a double,
   *  round it to `Double.MaxValue` or `Double.MinValue`.
   */
  def realToDouble(real: Real): Double =
    if (real.compare(Real(Double.MaxValue)) > 0) {
      Double.MaxValue
    } else if (real.compare(Real(Double.MinValue)) < 0) {
      Double.MinValue
    } else {
      real.doubleValue()
    }

  /** If the input parses as positive or negative infinity
   *  round it to `Double.MaxValue` or `Double.MinValue`.
   */
  def stringToDouble(str: String): Double = {
    val result = str.toDouble

    if (result.isPosInfinity) {
      Double.MaxValue
    } else if (result.isNegInfinity) {
      Double.MinValue
    } else {
      result
    }
  }

  /** Widening from a long to a double may result in loss of
   *  precision but will not result in loss of magnitude.
   */
  def longToDouble(long: Long): Double =
    long.toDouble
}
