/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.fp._
import org.specs2.mutable._
import org.specs2.scalaz.ScalazMatchers
import org.specs2.execute.AsResult
import scalaz._

trait QuasarSpecification extends SpecificationLike with ScalazMatchers {
  // Report all test timings.
  args.report(showtimes=true)

  implicit class Specs2ScalazOps[A : Equal : Show](lhs: A) {
    def must_=(rhs: A) = lhs must equal(rhs)
  }

  /** Allows marking non-deterministically failing tests as such,
   *  in the manner of pendingUntilFixed but such that it will not
   *  fail regardless of whether it seems to pass or fail.
   */
  implicit class FlakyTest[T : AsResult](t: => T) {
    import org.specs2.execute._
    def flakyTest: Result = flakyTest("")
    def flakyTest(m: String): Result = ResultExecution.execute(AsResult(t)) match {
      case s: Success => s
      case r          =>
        val explain = if (m == "") "" else s" ($m)"
        Skipped(s"${r.message}, but test is marked as flaky$explain", r.expected)
    }
  }
}
