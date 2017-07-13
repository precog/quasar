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

package quasar

import slamdata.Predef._
import quasar.RenderTree.ops._
import quasar.fp._

import matryoshka._
import org.specs2.matcher._
import scalaz._, Scalaz._

trait TreeMatchers {
  // TODO remove in favor of `beTreeEqual`
  // uses `==`
  def beTree[A: RenderTree](expected: A): Matcher[A] = new Matcher[A] {
    def apply[S <: A](ex: Expectable[S]) = {
      val actual: A = ex.value
      val diff: String = (RenderTree[A].render(actual) diff expected.render).shows
      result(actual == expected, s"trees match:\n$diff", s"trees do not match:\n$diff", ex)
    }
  }

  // uses `scalaz.Equal`
  def beTreeEqual[A: Equal: RenderTree](expected: A): Matcher[A] = new Matcher[A] {
    def apply[S <: A](s: Expectable[S]) = {
      val v: A = s.value
      // TODO: these are unintuitively reversed b/c of the `diff` implementation, should be fixed
      val diff = (RenderTree[A].render(v) diff expected.render).shows
      result(v ≟ expected, s"trees match:\n$diff", s"trees do not match:\n$diff", s)
    }
  }
}
