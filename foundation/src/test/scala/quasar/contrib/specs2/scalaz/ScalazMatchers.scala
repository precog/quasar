/* MIT License
 * Copyright (c)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

// From typelevel/specs2-scalaz which is released infrequently

package quasar.contrib.specs2.scalaz

import slamdata.Predef._

import org.specs2.matcher._
import scalaz._

trait ScalazMatchers { outer =>

  /** Equality matcher with a [[scalaz.Equal]] typeclass */
  def equal[T : Equal : Show](expected: T): Matcher[T] = new Matcher[T] {
    def apply[S <: T](actual: Expectable[S]): MatchResult[S] = {
      val actualT = actual.value.asInstanceOf[T]
      def test = Equal[T].equal(expected, actualT)
      def koMessage = "%s !== %s".format(Show[T].shows(actualT), Show[T].shows(expected))
      def okMessage = "%s === %s".format(Show[T].shows(actualT), Show[T].shows(expected))
      Matcher.result(test, okMessage, koMessage, actual)
    }
  }

  class ScalazBeHaveMatchers[T : Equal : Show](result: MatchResult[T]) {
    def equal(t: T): MatchResult[T] = result.be(outer.equal[T](t)(Equal[T], Show[T]))
  }

  import scala.language.implicitConversions
  implicit def scalazBeHaveMatcher[T : Equal : Show](result: MatchResult[T]): ScalazBeHaveMatchers[T] =
    new ScalazBeHaveMatchers(result)
}

object ScalazMatchers extends ScalazMatchers


