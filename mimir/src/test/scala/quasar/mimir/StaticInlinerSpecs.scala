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

import quasar.blueeyes._
import quasar.precog.common._
import quasar.yggdrasil.execution.EvaluationContext

import scalaz.Need

trait StaticInlinerSpecs[M[+_]] extends EvaluatorSpecification[M] {

  import dag._
  import instructions._

  object inliner extends StdLibStaticInliner with StdLibOpFinder {
    def MorphContext(ctx: EvaluationContext, node: DepGraph): MorphContext = new MorphContext(ctx, null)
  }
  import inliner._

  "static inlining of Root computation" should {
    "detect and resolve addition" in {
      val line = Line(1, 1, "")

      val input = Join(Add, Cross(None),
        Const(CLong(42))(line),
        Const(CNum(3.14))(line))(line)

      val expected = Const(CNum(45.14))(line)

      inlineStatics(input, defaultEvaluationContext) mustEqual expected
    }

    "detect and resolve operations at depth" in {
      val line = Line(1, 1, "")

      val input = Join(Add, Cross(None),
        Const(CLong(42))(line),
        Join(Mul, Cross(None),
          Const(CNum(3.14))(line),
          Const(CLong(2))(line))(line))(line)

      val expected = Const(CNum(48.28))(line)

      inlineStatics(input, defaultEvaluationContext) mustEqual expected
    }

    "produce CUndefined in cases where the operation is undefined" in {
      val line = Line(1, 1, "")

      val input = Join(Div, Cross(None),
        Const(CLong(42))(line),
        Const(CLong(0))(line))(line)

      val expected = Const(CUndefined)(line)

      inlineStatics(input, defaultEvaluationContext) mustEqual expected
    }

    "propagate through static computations CUndefined when produced at depth" in {
      val line = Line(1, 1, "")

      val input = Join(Add, Cross(None),
        Const(CLong(42))(line),
        Join(Div, Cross(None),
          Const(CNum(3.14))(line),
          Const(CLong(0))(line))(line))(line)

      val expected = Const(CUndefined)(line)

      inlineStatics(input, defaultEvaluationContext) mustEqual expected
    }

    "propagate through non-singleton computations CUndefined when produced at depth" >> {
      val line = Line(1, 1, "")

      "left" >> {
        val input = Join(Add, Cross(None),
          dag.AbsoluteLoad(Const(CString("/foo"))(line))(line),
          Join(Div, Cross(None),
            Const(CNum(3.14))(line),
            Const(CLong(0))(line))(line))(line)

        val expected = Const(CUndefined)(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual expected
      }

      "right" >> {
        val input = Join(Add, Cross(None),
          Join(Div, Cross(None),
            Const(CNum(3.14))(line),
            Const(CLong(0))(line))(line),
          dag.AbsoluteLoad(Const(CString("/foo"))(line))(line))(line)

        val expected = Const(CUndefined)(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual expected
      }
    }

    "reduce filters with static RHS" >> {
      val line = Line(1, 1, "")

      "true" >> {
        val input = Filter(Cross(None),
          dag.AbsoluteLoad(Const(CString("/foo"))(line))(line),
          Const(CTrue)(line))(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual dag.AbsoluteLoad(Const(CString("/foo"))(line))(line)
      }

      "false" >> {
        val input = Filter(Cross(None),
          dag.AbsoluteLoad(Const(CString("/foo"))(line))(line),
          Const(CBoolean(false))(line))(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual Const(CUndefined)(line)
      }
    }

    "detect and resolve array" >> {
      val line = Line(1, 1, "")

      "wrap" >> {
        val input = Operate(WrapArray, Const(CBoolean(false))(line))(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual Const(RArray(CBoolean(false)))(line)
      }

      "deref" >> {
        val input = Join(DerefArray, Cross(None), Const(RArray(CBoolean(false), CTrue))(line), Const(CNum(1))(line))(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual Const(CTrue)(line)
      }

      "join" >> {
        val input = Join(JoinArray, Cross(None), Const(RArray(CTrue))(line), Const(RArray(CBoolean(false)))(line))(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual Const(RArray(CTrue, CBoolean(false)))(line)
      }

      "swap" >> {
        val input = Join(ArraySwap, Cross(None), Const(RArray(CTrue, CBoolean(false), CString("TEST")))(line), Const(CNum(1))(line))(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual Const(RArray(CBoolean(false), CTrue, CString("TEST")))(line)
      }
    }

    "detect and resolve object" >> {
      val line = Line(1, 1, "")

      "wrap" >> {
        val input = Join(WrapObject, Cross(None), Const(CString("k"))(line), Const(CTrue)(line))(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual Const(RObject("k" -> CTrue))(line)
      }

      "deref" >> {
        val input = Join(DerefObject, Cross(None), Const(RObject("k" -> CBoolean(false)))(line), Const(CString("k"))(line))(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual Const(CBoolean(false))(line)
      }

      "join" >> {
        val input = Join(JoinObject, Cross(None), Const(RObject("k" -> CTrue))(line), Const(RObject("l" -> CBoolean(false)))(line))(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual Const(RObject("k" -> CTrue, "l" -> CBoolean(false)))(line)
      }
    }

    "detect and resolve cond" >> {
      val line = Line(1, 1, "")

      "const true" >> {
        val input = Cond(Const(CBoolean(true))(line), Const(CString("j"))(line), Cross(None), Const(CString("k"))(line), Cross(None))(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual Const(CString("j"))(line)
      }

      "const false" >> {
        val input = Cond(Const(CBoolean(false))(line), Const(CString("j"))(line), Cross(None), Const(CString("k"))(line), Cross(None))(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual Const(CString("k"))(line)
      }

      "invalid const" >> {
        val input = Cond(Const(CString("fubar"))(line), Const(CString("j"))(line), Cross(None), Const(CString("k"))(line), Cross(None))(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual Undefined(line)
      }
    }

    "detect and resove union with identical left/right" in {
      val line = Line(1, 1, "")

      val side = Const(CString("j"))(line)
      val input = IUI(true, side, side)(line)

      inlineStatics(input, defaultEvaluationContext) mustEqual side
    }

    "detect and resove intersect with identical left/right" in {
      val line = Line(1, 1, "")

      val side = Const(CString("j"))(line)
      val input = IUI(false, side, side)(line)

      inlineStatics(input, defaultEvaluationContext) mustEqual side
    }

    "rewrite a filter with undefined to undefined" >> {
      "target" >> {
        val line = Line(1, 1, "")
        val input = Filter(IdentitySort, Const(CUndefined)(line), Const(CString("j"))(line))(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual Const(CUndefined)(line)
      }

      "predicate" >> {
        val line = Line(1, 1, "")
        val input = Filter(IdentitySort, Const(CString("j"))(line), Const(CUndefined)(line))(line)

        inlineStatics(input, defaultEvaluationContext) mustEqual Const(CUndefined)(line)
      }
    }
  }
}

object StaticInlinerSpecs extends StaticInlinerSpecs[Need]
