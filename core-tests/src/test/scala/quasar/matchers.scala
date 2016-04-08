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
import quasar.RenderTree.ops._
import quasar.fp._

import matryoshka._, FunctorT.ops._
import org.specs2.matcher._
import scalaz._, Scalaz._

trait TreeMatchers {
  def beTree[A: RenderTree](expected: A): Matcher[A] = new Matcher[A] {
    def apply[S <: A](s: Expectable[S]) = {
      val v = s.value
      val diff = (RenderTree[A].render(v) diff expected.render).shows
      result(v == expected, s"trees match:\n$diff", s"trees do not match:\n$diff", s)
    }
  }
}

trait TermLogicalPlanMatchers {
  case class equalToPlan(expected: Fix[LogicalPlan])
      extends Matcher[Fix[LogicalPlan]] {
    def apply[S <: Fix[LogicalPlan]](s: Expectable[S]) = {
      val normed = s.value.transCata(repeatedly(Optimizer.simplifyƒ[Fix]))
      val diff = (normed.render diff expected.render).shows
      result(
        expected ≟ normed,
        "\ntrees are equal:\n" + diff,
        "\ntrees are not equal:\n" + diff +
          "\noriginal was:\n" + normed.render.shows,
        s)
    }
  }
}
