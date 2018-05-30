/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar

import org.specs2.matcher._

trait ConditionMatchers {
  def beAbnormal[A](check: ValueCheck[A]): Matcher[Condition[A]] =
    new OptionLikeCheckedMatcher[Condition, A, A](
      "Abnormal",
      Condition.abnormal[A].getOption(_),
      check)

  def beNormal[A]: Matcher[Condition[A]] =
    new Matcher[Condition[A]] {
      def apply[S <: Condition[A]](value: Expectable[S]) =
        result(Condition.normal[A].nonEmpty(value.value),
               value.description + " is Normal",
               value.description + " is not Normal",
               value)
    }
}
