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

package quasar.mimir

import quasar.precog.common._

import scalaz._
import scalaz.std.anyVal.booleanInstance.disjunction
import scalaz.std.option.optionFirst

trait DAGRewriterSpecs extends EvaluatorSpecification {

  import dag._
  import instructions._

  val evaluator = Evaluator
  import evaluator._
  import library._

  "DAG rewriting" should {
    "compute identities given a relative path" in {
      val input = dag.AbsoluteLoad(Const(CString("/numbers")))

      val ctx = defaultEvaluationContext
      val result = fullRewriteDAG(true, ctx)(input)

      result.identities mustEqual Identities.Specs(Vector(LoadIds("/numbers")))
    }

    "rewrite to have constant" in {
      /*
       * foo := //foo
       * foo.a + count(foo) + foo.c
       */

      val t1 = dag.AbsoluteLoad(Const(CString("/hom/pairs")))

      val input =
        Join(Add, IdentitySort,
          Join(Add, Cross(None),
            Join(DerefObject, Cross(None),
              t1,
              Const(CString("first"))),
            dag.Reduce(Count, t1)),
          Join(DerefObject, Cross(None),
            t1,
            Const(CString("second"))))

      val ctx = defaultEvaluationContext
      val optimize = true

      // The should be a MegaReduce for the Count reduction
      val optimizedDAG = fullRewriteDAG(optimize, ctx)(input)
      val megaReduce = optimizedDAG.foldDown(true) {
        case m@MegaReduce(_, _) => Tag(Some(m)): FirstOption[DepGraph]
      }

      megaReduce.getClass must_== classOf[scala.Some[_]]

      val rewritten = inlineNodeValue(
        optimizedDAG,
        megaReduce.asInstanceOf[Option[DepGraph]].get,
        CNum(42))

      val hasMegaReduce = rewritten.foldDown(false) {
        case m@MegaReduce(_, _) => true
      }(disjunction)
      val hasConst = rewritten.foldDown(false) {
        case m@Const(CNum(n)) if n == 42 => true
      }(disjunction)

      // Must be turned into a Const node
      hasMegaReduce must beFalse
      hasConst must beTrue
    }
  }
}

object DAGRewriterSpecs extends DAGRewriterSpecs
