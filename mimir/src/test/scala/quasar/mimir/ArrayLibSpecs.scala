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

import quasar.precog.common._
import quasar.yggdrasil._
import scalaz._

trait ArrayLibSpecs[M[+_]] extends EvaluatorSpecification[M]
    with LongIdMemoryDatasetConsumer[M] { self =>

  import dag._
  import instructions._

  import library._

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(graph, defaultEvaluationContext) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "array utilities" should {
    "flatten a homogeneous set" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Flatten,
        dag.AbsoluteLoad(Const(CString("/hom/arrays"))(line))(line))(line)

      val result = testEval(input)
      result must haveSize(25)

      val values = result collect {
        case (ids, SDecimal(d)) if ids.length == 2 => d
      }

      values mustEqual Set(-9, -42, 42, 87, 4, 7, 6, 12, 0, 1024, 57, 77, 46.2,
        -100, 1, 19, 22, 11, 104, -27, 6, -2790111, 244, 13, 11)
    }

    "flatten a heterogeneous set" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Flatten,
        dag.AbsoluteLoad(Const(CString("/het/arrays"))(line))(line))(line)

      val result = testEval(input)
      result must haveSize(26)

      val values = result collect {
        case (ids, jv) if ids.length == 2 => jv
      }

      values mustEqual Set(SDecimal(-9), SDecimal(-42), SDecimal(42), SDecimal(87),
        SDecimal(4), SDecimal(7), SDecimal(6), SDecimal(12), SDecimal(0),
        SDecimal(1024), SDecimal(57), SDecimal(77), SDecimal(46.2), SDecimal(-100), SDecimal(1),
        SDecimal(19), SDecimal(22), SDecimal(11), SDecimal(104), SDecimal(-27),
        SDecimal(6), SDecimal(-2790111), SDecimal(244), SDecimal(13), SDecimal(11),
        SArray(Vector(SDecimal(-9), SDecimal(-42), SDecimal(42), SDecimal(87), SDecimal(4))))
    }

    "flattened set is related to original set" in {
      val line = Line(1, 1, "")

      val input = dag.Join(JoinObject, IdentitySort,
        dag.Join(WrapObject, Cross(None),
          Const(CString("arr"))(line),
          dag.AbsoluteLoad(Const(CString("/het/arrays"))(line))(line))(line),
        dag.Join(WrapObject, Cross(None),
          Const(CString("val"))(line),
          dag.Morph1(Flatten,
            dag.AbsoluteLoad(Const(CString("/het/arrays"))(line))(line))(line))(line))(line)

      val result = testEval(input)
      result must haveSize(26)

      val values = result collect {
        case (ids, jv) if ids.length == 2 => jv
      }
      values must contain(like[SValue]({
        case SObject(obj) =>
          obj.keySet mustEqual Set("arr", "val")
          val SArray(elems) = obj("arr")
          elems must contain(obj("val"))
        case _ => ko
      })).forall
    }

    "flatten a non-array without exploding" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Flatten,
        Const(CString("/het/arrays"))(line))(line)

      testEval(input) must haveSize(0)
    }

    "flatten an empty set without exploding" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Flatten,
        Undefined(line))(line)

      testEval(input) must haveSize(0)
    }
  }
}

object ArrayLibSpecs extends ArrayLibSpecs[Need]
