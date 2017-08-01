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

trait ReductionLibSpecs[M[+_]] extends EvaluatorSpecification[M]
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

  def determineResult(input: DepGraph, value: Double) = {
    val result = testEval(input)

    result must haveSize(1)

    val result2 = result collect {
      case (ids, SDecimal(d)) if ids.length == 0 => d.toDouble
    }

    result2.toSet must_== Set(value)
  }

  val line = Line(1, 1, "")

  "reduce homogeneous sets" >> {
    "first" >> {
      val input = dag.Reduce(First,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      determineResult(input, 42)
    }

    "last" >> {
      val input = dag.Reduce(Last,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      determineResult(input, 13)
    }

    "singleton count" >> {
      val input = dag.Reduce(Count, Const(CString("alpha"))(line))(line)

      determineResult(input, 1)
    }

    "count" >> {
      val input = dag.Reduce(Count,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      determineResult(input, 5)
    }

    "count het numbers" >> {
      val input = dag.Reduce(Count,
        dag.AbsoluteLoad(Const(CString("/hom/numbersHet"))(line))(line))(line)

      determineResult(input, 13)
    }

    "geometricMean" >> {
      val input = dag.Reduce(GeometricMean,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      determineResult(input, 13.822064739747384)
    }

    "mean" >> {
      val input = dag.Reduce(Mean,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      determineResult(input, 29)
    }

    "mean het numbers" >> {
      val input = dag.Reduce(Mean,
        dag.AbsoluteLoad(Const(CString("/hom/numbersHet"))(line))(line))(line)

      determineResult(input, -37940.51855769231)
    }

    "max" >> {
      val input = dag.Reduce(Max,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      determineResult(input, 77)
    }

    "max het numbers" >> {
      val input = dag.Reduce(Max,
        dag.AbsoluteLoad(Const(CString("/hom/numbersHet"))(line))(line))(line)

      determineResult(input, 9999)
    }

    "min" >> {
      val input = dag.Reduce(Min,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      determineResult(input, 1)
    }

    "min het numbers" >> {
      val input = dag.Reduce(Min,
        dag.AbsoluteLoad(Const(CString("/hom/numbersHet"))(line))(line))(line)

      determineResult(input, -500000)
    }

    "standard deviation" >> {
      val input = dag.Reduce(StdDev,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      determineResult(input, 27.575351312358652)
    }

    "stdDev het numbers" >> {
      val input = dag.Reduce(StdDev,
        dag.AbsoluteLoad(Const(CString("/hom/numbersHet"))(line))(line))(line)

      determineResult(input, 133416.18997644997)
    }

    "sum a singleton" >> {
      val input = dag.Reduce(Sum, Const(CLong(18))(line))(line)

      determineResult(input, 18)
    }

    "sum" >> {
      val input = dag.Reduce(Sum,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      determineResult(input, 145)
    }

    "sumSq" >> {
      val input = dag.Reduce(SumSq,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      determineResult(input, 8007)
    }

    "variance" >> {
      val input = dag.Reduce(Variance,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      determineResult(input, 760.4)
    }

    "forall" >> {
      val input = dag.Reduce(Forall,
        dag.IUI(true,
          Const(CTrue)(line),
          Const(CFalse)(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SBoolean(b)) if ids.length == 0 => b
      }

      result2.toSet must_== Set(false)
    }

    "exists" >> {
      val input = dag.Reduce(Exists,
        dag.IUI(true,
          Const(CTrue)(line),
          Const(CFalse)(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SBoolean(b)) if ids.length == 0 => b
      }

      result2.toSet must_== Set(true)
    }
  }

  "reduce heterogeneous sets" >> {
    "count" >> {
      val input = dag.Reduce(Count,
        dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line))(line)

      determineResult(input, 10)
    }

    "geometricMean" >> {
      val input = dag.Reduce(GeometricMean,
        dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line))(line)

      determineResult(input, 13.822064739747384)
    }

    "mean" >> {
      val input = dag.Reduce(Mean,
        dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line))(line)

      determineResult(input, 29)
    }

    "max" >> {
      val input = dag.Reduce(Max,
        dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line))(line)

      determineResult(input, 77)
    }

    "min" >> {
      val input = dag.Reduce(Min,
        dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line))(line)

      determineResult(input, 1)
    }

    "standard deviation" >> {
      val input = dag.Reduce(StdDev,
        dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line))(line)

      determineResult(input, 27.575351312358652)
    }

    "sum" >> {
      val input = dag.Reduce(Sum,
        dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line))(line)

      determineResult(input, 145)
    }

    "sumSq" >> {
      val input = dag.Reduce(SumSq,
        dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line))(line)

      determineResult(input, 8007)
    }

    "variance" >> {
      val input = dag.Reduce(Variance,
        dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line))(line)

      determineResult(input, 760.4)
    }
  }

  "reduce heterogeneous sets across two slice boundaries (22 elements)" >> {
    "count" >> {
      val input = dag.Reduce(Count,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 22)
    }

    "geometricMean" >> {
      val input = dag.Reduce(GeometricMean,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 0)
    }

    "mean" >> {
      val input = dag.Reduce(Mean,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 1.8888888888888888)
    }

    "max" >> {
      val input = dag.Reduce(Max,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 12)
    }

    "min" >> {
      val input = dag.Reduce(Min,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, -3)
    }

    "standard deviation" >> {
      val input = dag.Reduce(StdDev,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 4.121608220220312)
    }

    "sum" >> {
      val input = dag.Reduce(Sum,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 17)
    }

    "sumSq" >> {
      val input = dag.Reduce(SumSq,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 185)
    }

    "variance" >> {
      val input = dag.Reduce(Variance,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 16.987654320987655)
    }
  }

  "reduce homogeneous sets across two slice boundaries (22 elements)" >> {
    "count" >> {
      val input = dag.Reduce(Count,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 22)
    }

    "geometricMean" >> {
      val input = dag.Reduce(GeometricMean,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 0)
    }

    "mean" >> {
      val input = dag.Reduce(Mean,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 0.9090909090909090909090909090909091)
    }

    "max" >> {
      val input = dag.Reduce(Max,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 15)
    }

    "min" >> {
      val input = dag.Reduce(Min,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, -14)
    }

    "standard deviation" >> {
      val input = dag.Reduce(StdDev,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 10.193175483934386)
    }

    "sum" >> {
      val input = dag.Reduce(Sum,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 20)
    }

    "sumSq" >> {
      val input = dag.Reduce(SumSq,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 2304)
    }

    "variance" >> {
      val input = dag.Reduce(Variance,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 103.9008264462809917355371900826446)
    }
  }
}

object ReductionLibSpecs extends ReductionLibSpecs[Need]
