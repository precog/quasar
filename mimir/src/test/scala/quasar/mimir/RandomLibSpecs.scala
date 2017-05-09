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
import quasar.yggdrasil._

import scalaz._

trait RandomLibSpecs[M[+_]] extends EvaluatorSpecification[M] with LongIdMemoryDatasetConsumer[M] { self =>

  import dag._
  import instructions._
  import library._

  val line = Line(1, 1, "")

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(graph, defaultEvaluationContext) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "return observed set given a single event with bottom identity" in {
    val uniform = dag.Morph1(UniformDistribution,
      Const(CLong(12))(line))(line)

    val input = dag.Observe(
      Const(CString("foo"))(line),
      uniform)(line)

    val result = testEval(input)

    result must haveSize(1)

    result must haveAllElementsLike {
      case (ids, SDecimal(d)) =>
        ids must haveSize(0)
        d mustEqual(0.2182148468113263)
    }
  }

  "fail to observe if seed for distribution is not a long" in {
    val uniform = dag.Morph1(UniformDistribution,
      Const(CDouble(4.4))(line))(line)

    val input = dag.Observe(
      Const(CString("foo"))(line),
      uniform)(line)

    val result = testEval(input)

    result must beEmpty
  }

  "return observed set given 22 heterogeneous events with identities" in {
    val uniform = dag.Morph1(UniformDistribution,
      Const(CLong(100))(line))(line)

    val input = dag.Observe(
      dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line),
      uniform)(line)

    val result = testEval(input)

    result must haveSize(22)

    val expected = Set(
      0.5782602543202314, 0.8725670189895508, 0.9089304564449439,
      0.5659348849912463, 0.5658210600064535, 0.5260245794435036,
      0.5616114285770709, 0.29996900265360527, 0.1760490091508321,
      0.6328197512142251, 0.8796105992305842, 0.4216360283269056,
      0.5529979602103697, 0.07161817845821428, 0.9674312488914959,
      0.4195344677504308, 0.644432398131965, 0.7187289610972881,
      0.18631410431791728, 0.6891140970302626, 0.9204152760017362,
      0.7976605648079012)

    val actual = result collect {
      case (ids, SDecimal(d)) if ids.size == 1 => d
    }

    actual mustEqual expected
  }

  "return observed set joined with original" in {
    val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line)

    val uniform = dag.Morph1(UniformDistribution,
      Const(CLong(88))(line))(line)

    val observe = dag.Observe( numbers, uniform)(line)

    val randObj = dag.Join(WrapObject, Cross(None),
      Const(CString("rand"))(line),
      observe)(line)

    val numbersObj = dag.Join(WrapObject, Cross(None),
      Const(CString("data"))(line),
      numbers)(line)

    val input = dag.Join(JoinObject, IdentitySort,
      randObj,
      numbersObj)(line)

    val result = testEval(input)

    result must haveSize(10)

    val expected = Set(
      0.39249760038657855, 0.8928583546656026, 0.2502047859103521,
      0.7861686432357328, 0.6239576284000388, 0.7866902522682274,
      0.438254062387199, 0.9800628279464199, 0.6104706956474781,
      0.8637112992116962)

    result must haveAllElementsLike {
      case (ids, SObject(obj)) =>
        ids must haveSize(1)

        obj.keys mustEqual(Set("rand", "data"))
        obj("rand") must beLike {
          case SDecimal(d) => expected must contain(d)
        }
    }
  }
}

object RandomLibSpecs extends RandomLibSpecs[Need]
