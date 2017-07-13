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

trait CondRewriter extends DAG {
  import instructions._
  import dag._

  def rewriteConditionals(node: DepGraph): DepGraph = {
    node mapDown { recurse =>
      {
        case peer @ IUI(true, Filter(leftJoin, left, pred1), Filter(rightJoin, right, Operate(Comp, pred2))) if pred1 == pred2 =>
          Cond(recurse(pred1), recurse(left), leftJoin, recurse(right), rightJoin)(peer.loc)
      }
    }
  }
}
