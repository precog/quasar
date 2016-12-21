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

import quasar.RenderTree.ops._
import quasar.fp._
import quasar.frontend.logicalplan.{LogicalPlan, Optimizer}

import matryoshka._
import matryoshka.data.Fix
import org.specs2.matcher._
import scalaz._, Scalaz._

trait TermLogicalPlanMatchers {
  case class equalToPlan(expected: Fix[LogicalPlan])
      extends Matcher[Fix[LogicalPlan]] {
    val optimizer = new Optimizer[Fix]

    def apply[S <: Fix[LogicalPlan]](s: Expectable[S]) = {
      val normed = optimizer.simplify(s.value)
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
