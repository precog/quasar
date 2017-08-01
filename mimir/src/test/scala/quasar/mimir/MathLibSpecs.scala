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

import quasar.yggdrasil._
import quasar.precog.common._
import scalaz._

trait MathLibSpecs[M[+_]] extends EvaluatorSpecification[M]
    with LongIdMemoryDatasetConsumer[M] { self =>

  import dag._
  import instructions._
  import library._

  val line = Line(1, 1, "")
  val homn4 = "/hom/numbers4"

  def inputOp2(op: Op2, loadFrom: String, const: RValue) = {
    Join(BuiltInFunction2Op(op), Cross(None),
      dag.AbsoluteLoad(Const(CString(loadFrom))(line))(line),
      Const(const)(line))(line)
  }

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(graph, defaultEvaluationContext) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "for sets with numeric values inside arrays and objects" should {
    "compute cos only of the numeric value" in {
      val input = dag.Operate(BuiltInFunction1Op(cos),
        dag.AbsoluteLoad(Const(CString("/het/numbers7"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(1)
    }
  }

  "for homogeneous sets, the appropriate math function" should {   //todo test in particular cases when functions are not defined!!
    "compute sinh" in {
      val input = dag.Operate(BuiltInFunction1Op(sinh),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set[BigDecimal](0, 1.1752011936438014, -1.1752011936438014, 8.696374707602505E17, -4.872401723124452E9)
    }
    "compute sinh on two large(ish) values" in {
      val input = dag.Operate(BuiltInFunction1Op(sinh),
        dag.AbsoluteLoad(Const(CString("/hom/number"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(0)
    }
    "compute toDegrees" in {
      val input = dag.Operate(BuiltInFunction1Op(toDegrees),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 57.29577951308232, -57.29577951308232, 2406.4227395494577, -1317.8029288008934)
    }
    "compute expm1" in {
      val input = dag.Operate(BuiltInFunction1Op(expm1),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)
      result must haveSize(6)

      val result2 = result collect {
        case (Seq(_), SDecimal(d)) => d
      }

      result2.toSet must_=== Set(0.0, 1.718281828459045, -0.6321205588285577, 1.73927494152050099E18, -0.9999999998973812)
    }
    "compute expm1 on two large(ish) values" in {
      val input = dag.Operate(BuiltInFunction1Op(expm1),
        dag.AbsoluteLoad(Const(CString("/hom/number"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(-1.0)
    }
    "compute getExponent" in {
      val input = dag.Operate(BuiltInFunction1Op(getExponent),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(3)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0, 5)
    }
    "compute asin" in {
      val input = dag.Operate(BuiltInFunction1Op(asin),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(4)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.5707963267948966, -1.5707963267948966)
    }
    "compute log10" in {
      val input = dag.Operate(BuiltInFunction1Op(log10),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(3)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.6232492903979006)
    }
    "compute cos" in {
      val input = dag.Operate(BuiltInFunction1Op(cos),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(1.0, 0.5403023058681398, 0.5403023058681398, -0.39998531498835127, -0.5328330203333975)
    }
    "compute exp" in {
      val input = dag.Operate(BuiltInFunction1Op(exp),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (Seq(_), SDecimal(d)) => d
      }

      result2.toSet must_== Set[BigDecimal](1.0, 2.718281828459045, 0.36787944117144233, 1.73927494152050099E18, 1.026187963170189E-10)
    }

    "compute exp on two large(ish) values" in {
      val input = dag.Operate(BuiltInFunction1Op(exp),
        dag.AbsoluteLoad(Const(CString("/hom/number"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0)
    }
    "compute cbrt" in {
      val input = dag.Operate(BuiltInFunction1Op(cbrt),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, -1.0, 3.4760266448864496, -2.8438669798515654)
    }
    "compute atan" in {
      val input = dag.Operate(BuiltInFunction1Op(atan),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 0.7853981633974483, -0.7853981633974483, 1.5469913006098266, -1.5273454314033659)
    }
    "compute ceil" in {
      val input = dag.Operate(BuiltInFunction1Op(ceil),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, -1.0, 42.0, -23.0)
    }
    "compute rint" in {
      val input = dag.Operate(BuiltInFunction1Op(rint),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, -1.0, 42.0, -23.0)
    }
    "compute log1p" in {
      val input = dag.Operate(BuiltInFunction1Op(log1p),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(4)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 0.6931471805599453, 3.7612001156935624)
    }
    "compute sqrt" in {
      val input = dag.Operate(BuiltInFunction1Op(sqrt),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(4)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, 6.48074069840786)
    }
    "compute floor" in {
      val input = dag.Operate(BuiltInFunction1Op(floor),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, -1.0, 42.0, -23.0)
    }
    "compute toRadians" in {
      val input = dag.Operate(BuiltInFunction1Op(toRadians),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 0.017453292519943295, -0.017453292519943295, 0.7330382858376184, -0.40142572795869574)
    }
    "compute tanh" in {
      val input = dag.Operate(BuiltInFunction1Op(tanh),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 0.7615941559557649, -0.7615941559557649, 1.0, -1.0)
    }
    "compute round" in {
      val input = dag.Operate(BuiltInFunction1Op(round),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0, 1, -1, 42, -23)
    }
    "compute cosh" in {
      val input = dag.Operate(BuiltInFunction1Op(cosh),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(1.0, 1.543080634815244, 1.543080634815244, 8.696374707602505E17, 4.872401723124452E9)
    }
    "compute cosh on two large(ish) values" in {
      val input = dag.Operate(BuiltInFunction1Op(cosh),
        dag.AbsoluteLoad(Const(CString("/hom/number"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(0)
    }
    "compute tan" in {
      val input = dag.Operate(BuiltInFunction1Op(tan),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.5574077246549023, -1.5574077246549023, 2.2913879924374863, -1.5881530833912738)
    }
    "compute abs" in {
      val input = dag.Operate(BuiltInFunction1Op(abs),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0, 1, 42, 23)
    }
    "compute sin" in {
      val input = dag.Operate(BuiltInFunction1Op(sin),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 0.8414709848078965, -0.8414709848078965, -0.9165215479156338, 0.8462204041751706)
    }
    "compute log" in {
      val input = dag.Operate(BuiltInFunction1Op(mathlog),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(3)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 3.7376696182833684)
    }
    "compute signum" in {
      val input = dag.Operate(BuiltInFunction1Op(signum),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, -1.0)
    }
    "compute acos" in {
      val input = dag.Operate(BuiltInFunction1Op(acos),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(4)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(1.5707963267948966, 0.0, 3.141592653589793)
    }
    "compute ulp" in {
      val input = dag.Operate(BuiltInFunction1Op(ulp),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(4.9E-324, 2.220446049250313E-16, 2.220446049250313E-16, 7.105427357601002E-15, 3.552713678800501E-15)
    }
    "compute hypot" in {
      val input = Join(BuiltInFunction2Op(hypot), Cross(None),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(7.0, 7.0710678118654755, 7.0710678118654755, 42.579337712087536, 24.041630560342615)
    }
    "compute pow" in {
      val input = Join(BuiltInFunction2Op(pow), Cross(None),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, -1.0, 2.30539333248E11, -3.404825447E9)
    }
    "compute maxOf" in {
      val input = Join(BuiltInFunction2Op(maxOf), Cross(None),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(7, 42)
    }
    "compute atan2" in {
      val input = Join(BuiltInFunction2Op(atan2), Cross(None),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 0.1418970546041639, -0.1418970546041639, 1.4056476493802699, -1.2753554896511767)
    }
    "compute copySign" in {
      val input = Join(BuiltInFunction2Op(copySign), Cross(None),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, 42.0, 23.0)
    }
    "compute IEEEremainder" in {
      val input = Join(BuiltInFunction2Op(IEEEremainder), Cross(None),
        dag.AbsoluteLoad(Const(CString("/hom/numbers4"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, -1.0, -2.0)
    }
    "compute roundTo" in {
      val input = inputOp2(roundTo, "/hom/decimals", CLong(2))

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set[Double](1.24, 123.19, 100.00, 0, 0.50)
    }
    "round to evenly in the face of repeating decimals" in {
      val input =
        Operate(BuiltInFunction1Op(numToString),
          Join(BuiltInFunction2Op(roundTo), Cross(None),
            Join(Mul, Cross(None),
              Join(Div, Cross(None),
                Const(CLong(15))(line),
                Const(CLong(3168))(line))(line),
              Const(CLong(100))(line))(line),
            Const(CLong(2))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SString(str)) if ids.isEmpty => str
      }

      result2.toSet must_== Set("0.47")
    }
  }

  "for heterogeneous sets, the appropriate math function" should {
    "compute sinh" in {
      val input = dag.Operate(BuiltInFunction1Op(sinh),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (Seq(_), SDecimal(d)) => d
      }

      result2.toSet must_=== Set[BigDecimal](0, 1.1752011936438014, -1.1752011936438014, 8.696374707602505E17, -4.872401723124452E9)
    }
    "compute toDegrees" in {
      val input = dag.Operate(BuiltInFunction1Op(toDegrees),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 57.29577951308232, -57.29577951308232, 2406.4227395494577, -1317.8029288008934)
    }
    "compute expm1" in {
      val input = dag.Operate(BuiltInFunction1Op(expm1),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)
      result must haveSize(6)

      val result2 = result collect {
        case (Seq(_), SDecimal(d)) => d
      }

      result2.toSet must_=== Set(0.0, 1.718281828459045, -0.6321205588285577, 1.73927494152050099E18, -0.9999999998973812)
    }
    "compute getExponent" in {
      val input = dag.Operate(BuiltInFunction1Op(getExponent),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(3)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0, 5)
    }
    "compute asin" in {
      val input = dag.Operate(BuiltInFunction1Op(asin),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(4)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.5707963267948966, -1.5707963267948966)
    }
    "compute log10" in {
      val input = dag.Operate(BuiltInFunction1Op(log10),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(3)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.6232492903979006)
    }
    "compute cos" in {
      val input = dag.Operate(BuiltInFunction1Op(cos),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(1.0, 0.5403023058681398, 0.5403023058681398, -0.39998531498835127, -0.5328330203333975)
    }
    "compute exp" in {
      val input = dag.Operate(BuiltInFunction1Op(exp),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set[BigDecimal](1.0, 2.718281828459045, 0.36787944117144233, 1.73927494152050099E18, 1.026187963170189E-10)
    }
    "compute cbrt" in {
      val input = dag.Operate(BuiltInFunction1Op(cbrt),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, -1.0, 3.4760266448864496, -2.8438669798515654)
    }
    "compute atan" in {
      val input = dag.Operate(BuiltInFunction1Op(atan),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 0.7853981633974483, -0.7853981633974483, 1.5469913006098266, -1.5273454314033659)
    }
    "compute ceil" in {
      val input = dag.Operate(BuiltInFunction1Op(ceil),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, -1.0, 42.0, -23.0)
    }
    "compute rint" in {
      val input = dag.Operate(BuiltInFunction1Op(rint),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, -1.0, 42.0, -23.0)
    }
    "compute log1p" in {
      val input = dag.Operate(BuiltInFunction1Op(log1p),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(4)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 0.6931471805599453, 3.7612001156935624)
    }
    "compute sqrt" in {
      val input = dag.Operate(BuiltInFunction1Op(sqrt),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(4)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, 6.48074069840786)
    }
    "compute floor" in {
      val input = dag.Operate(BuiltInFunction1Op(floor),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, -1.0, 42.0, -23.0)
    }
    "compute toRadians" in {
      val input = dag.Operate(BuiltInFunction1Op(toRadians),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 0.017453292519943295, -0.017453292519943295, 0.7330382858376184, -0.40142572795869574)
    }
    "compute tanh" in {
      val input = dag.Operate(BuiltInFunction1Op(tanh),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 0.7615941559557649, -0.7615941559557649, 1.0, -1.0)
    }
    "compute round" in {
      val input = dag.Operate(BuiltInFunction1Op(round),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0, 1, -1, 42, -23)
    }
    "compute cosh" in {
      val input = dag.Operate(BuiltInFunction1Op(cosh),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(1.0, 1.543080634815244, 1.543080634815244, 8.696374707602505E17, 4.872401723124452E9)
    }
    "compute tan" in {
      val input = dag.Operate(BuiltInFunction1Op(tan),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.5574077246549023, -1.5574077246549023, 2.2913879924374863, -1.5881530833912738)
    }
    "compute abs" in {
      val input = dag.Operate(BuiltInFunction1Op(abs),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0, 1, 42, 23)
    }
    "compute sin" in {
      val input = dag.Operate(BuiltInFunction1Op(sin),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 0.8414709848078965, -0.8414709848078965, -0.9165215479156338, 0.8462204041751706)
    }
    "compute log" in {
      val input = dag.Operate(BuiltInFunction1Op(mathlog),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(3)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 3.7376696182833684)
    }
    "compute signum" in {
      val input = dag.Operate(BuiltInFunction1Op(signum),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, -1.0)
    }
    "compute acos" in {
      val input = dag.Operate(BuiltInFunction1Op(acos),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(4)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(1.5707963267948966, 0.0, 3.141592653589793)
    }
    "compute ulp" in {
      val input = dag.Operate(BuiltInFunction1Op(ulp),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(4.9E-324, 2.220446049250313E-16, 2.220446049250313E-16, 7.105427357601002E-15, 3.552713678800501E-15)
    }
    "compute hypot" in {
      val input = Join(BuiltInFunction2Op(hypot), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(7.0, 7.0710678118654755, 7.0710678118654755, 42.579337712087536, 24.041630560342615)
    }
    "compute pow" in {
      val input = Join(BuiltInFunction2Op(pow), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, -1.0, 2.30539333248E11, -3.404825447E9)
    }
    "compute maxOf" in {
      val input = Join(BuiltInFunction2Op(maxOf), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(7, 42)
    }
    "compute maxOf over numeric arrays (doesn't map over arrays)" in {
      val input = Join(BuiltInFunction2Op(maxOf), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/arrays"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(47)
    }
    "compute maxOf over numeric arrays and numeric objects (doesn't map over arrays or objects)" in {
      val input = Join(BuiltInFunction2Op(maxOf), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/numbers7"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(7)
    }
    "compute maxOf over numeric arrays and numeric objects (using Map2)" in {
      val input = Join(BuiltInFunction2Op(maxOf), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/numbers7"))(line))(line),
        dag.AbsoluteLoad(Const(CString("/het/arrays"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 2  => d
      }

      result2.toSet must_== Set(47)
    }
    "compute atan2" in {
      val input = Join(BuiltInFunction2Op(atan2), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 0.1418970546041639, -0.1418970546041639, 1.4056476493802699, -1.2753554896511767)
    }
    "compute copySign" in {
      val input = Join(BuiltInFunction2Op(copySign), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, 42.0, 23.0)
    }
    "compute IEEEremainder" in {
      val input = Join(BuiltInFunction2Op(IEEEremainder), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/numbers4"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0.0, 1.0, -1.0, -2.0)
    }
    "compute roundTo" in {
      val input = inputOp2(roundTo, "/het/numbers4", CLong(-1))

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(0, 0, 40, -20)
    }
  }

  "for homogeneous sets across two slice boundaries (22 elements)" should {
    "compute sinh" in {
      val input = dag.Operate(BuiltInFunction1Op(sinh),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 601302.1420819727, -601302.1420819727, 29937.07084924806, -221206.6960033301, 221206.6960033301, 548.3161232732465, -548.3161232732465, -74.20321057778875, -10.017874927409903, 11013.232874703393, 201.71315737027922, -4051.54190208279, 1634508.6862359024, -81377.39570642984)
    }
    "compute toDegrees" in {
      val input = dag.Operate(BuiltInFunction1Op(toDegrees),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 630.2535746439056, 572.9577951308232, -401.07045659157626, 401.07045659157626, 802.1409131831525, -802.1409131831525, 859.4366926962349, -515.662015617741, -687.5493541569879, -744.8451336700703, 744.8451336700703, 343.77467707849394, -286.4788975654116, -171.88733853924697)
    }
    "compute expm1" in {
      val input = dag.Operate(BuiltInFunction1Op(expm1),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, -0.9999991684712809, -0.9932620530009145, 3269016.3724721107, 22025.465794806718, -0.950212931632136, 402.4287934927351, -0.9999938557876467, 1095.6331584284585, -0.999997739670593, 59873.14171519782, -0.9998765901959134, 442412.3920089205, -0.9990881180344455, 1202603.2841647768)
    }
    "compute getExponent" in {
      val input = dag.Operate(BuiltInFunction1Op(getExponent),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(3, 2)
    }
    "compute asin" in {
      val input = dag.Operate(BuiltInFunction1Op(asin),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0)
    }
    "compute log10" in {
      val input = dag.Operate(BuiltInFunction1Op(log10),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(1.146128035678238, 0.8450980400142568, 1.0, 1.1139433523068367, 1.1760912590556813, 0.7781512503836436, 1.0413926851582251)
    }
    "compute cos" in {
      val input = dag.Operate(BuiltInFunction1Op(cos),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.9601702866503661, -0.8390715290764524, 1.0, -0.9111302618846769, 0.1367372182078336, 0.7539022543433046, 0.8438539587324921, -0.9899924966004454, 0.9074467814501962, -0.7596879128588213, 0.28366218546322625, 0.004425697988050785)
    }
    "compute exp" in {
      val input = dag.Operate(BuiltInFunction1Op(exp),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.00000614421235332821, 1.0, 0.049787068367863944, 1096.6331584284585, 3269017.3724721107, 442413.3920089205, 0.0000022603294069810542, 1202604.2841647768, 403.4287934927351, 0.0009118819655545162, 8.315287191035679E-7, 0.00012340980408667956, 59874.14171519782, 22026.465794806718, 0.006737946999085467)
    }
    "compute cbrt" in {
      val input = dag.Operate(BuiltInFunction1Op(cbrt),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, -1.709975946676697, -1.4422495703074083, -2.2894284851066637, 1.8171205928321397, -1.9129311827723892, 1.9129311827723892, -2.3513346877207573, 2.154434690031884, 2.3513346877207573, 2.4662120743304703, 2.2239800905693157, -2.41014226417523, 2.41014226417523, -2.080083823051904)
    }
    "compute atan" in {
      val input = dag.Operate(BuiltInFunction1Op(atan),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, -1.4876550949064553, -1.4994888620096063, 1.4994888620096063, -1.460139105621001, 1.5042281630190728, 1.4288992721907328, -1.4288992721907328, 1.4711276743037347, 1.4056476493802699, -1.4940244355251187, 1.4940244355251187, 1.4801364395941514, -1.2490457723982544, -1.373400766945016)
    }
    "compute ceil" in {
      val input = dag.Operate(BuiltInFunction1Op(ceil),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 10, -7, 14, -3, -12, 6, 13, -5, 7, -14, 11, -9, -13, 15)
    }
    "compute rint" in {
      val input = dag.Operate(BuiltInFunction1Op(rint),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 10, -7, 14, -3, -12, 6, 13, -5, 7, -14, 11, -9, -13, 15)
    }
    "compute log1p" in {
      val input = dag.Operate(BuiltInFunction1Op(log1p),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(11)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(2.70805020110221, 0.0, 1.9459101490553132, 2.4849066497880004, 2.3978952727983707, 2.0794415416798357, 2.639057329615259, 2.772588722239781)
    }
    "compute sqrt" in {
      val input = dag.Operate(BuiltInFunction1Op(sqrt),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(11)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 3.3166247903554, 2.449489742783178, 2.6457513110645907, 3.7416573867739413, 3.872983346207417, 3.1622776601683795, 3.605551275463989)
    }
    "compute floor" in {
      val input = dag.Operate(BuiltInFunction1Op(floor),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 10, -7, 14, -3, -12, 6, 13, -5, 7, -14, 11, -9, -13, 15)
    }
    "compute toRadians" in {
      val input = dag.Operate(BuiltInFunction1Op(toRadians),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, -0.08726646259971647, -0.20943951023931953, 0.19198621771937624, 0.22689280275926282, 0.10471975511965977, -0.22689280275926282, 0.17453292519943295, -0.12217304763960307, 0.12217304763960307, -0.05235987755982988, -0.15707963267948966, -0.24434609527920614, 0.24434609527920614, 0.2617993877991494)
    }
    "compute tanh" in {
      val input = dag.Operate(BuiltInFunction1Op(tanh),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 0.9999999999998128, -0.9950547536867305, -0.9999999999244973, 0.9999877116507956, -0.9999092042625951, 0.9999999958776927, 0.9999999994421064, 0.9999999999986171, -0.9999999999986171, -0.999999969540041, -0.9999983369439447, 0.9999983369439447, -0.9999999999897818, 0.9999999999897818)
    }
    "compute round" in {
      val input = dag.Operate(BuiltInFunction1Op(round),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 10, -7, 14, -3, -12, 6, 13, -5, 7, -14, 11, -9, -13, 15)
    }
    "compute cosh" in {
      val input = dag.Operate(BuiltInFunction1Op(cosh),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(1634508.6862362083, 4051.5420254925943, 29937.070865949758, 221206.6960055904, 10.067661995777765, 1.0, 81377.39571257407, 548.317035155212, 74.20994852478785, 11013.232920103324, 201.7156361224559, 601302.1420828041)
    }
    "compute tan" in {
      val input = dag.Operate(BuiltInFunction1Op(tan),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, -0.8559934009085188, -0.4630211329364896, 0.4630211329364896, -7.2446066160948055, 7.2446066160948055, 0.6483608274590866, 3.380515006246586, -0.8714479827243187, 0.8714479827243187, 0.1425465430742778, 0.45231565944180985, 0.6358599286615808, -0.29100619138474915, -225.95084645419513)
    }
    "compute abs" in {
      val input = dag.Operate(BuiltInFunction1Op(abs),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 5, 10, 14, 6, 9, 13, 12, 7, 3, 11, 15)
    }
    "compute sin" in {
      val input = dag.Operate(BuiltInFunction1Op(sin),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 0.5365729180004349, -0.4121184852417566, -0.6569865987187891, 0.6569865987187891, -0.9999902065507035, 0.9906073556948704, -0.9906073556948704, -0.5440211108893698, 0.6502878401571168, 0.9589242746631385, 0.4201670368266409, -0.4201670368266409, -0.1411200080598672, -0.27941549819892586)
    }
    "compute log" in {
      val input = dag.Operate(BuiltInFunction1Op(mathlog),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(2.70805020110221, 1.9459101490553132, 2.6390573296152584, 2.3978952727983707, 2.5649493574615367, 2.302585092994046, 1.791759469228055)
    }
    "compute signum" in {
      val input = dag.Operate(BuiltInFunction1Op(signum),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 1, -1)
    }
    "compute acos" in {
      val input = dag.Operate(BuiltInFunction1Op(acos),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(1.5707963267948966)
    }
    "compute ulp" in {
      val input = dag.Operate(BuiltInFunction1Op(ulp),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(1.7763568394002505E-15, 8.881784197001252E-16, 4.440892098500626E-16, 4.9E-324)
    }
    "compute hypot" in {
      val input = Join(BuiltInFunction2Op(hypot), Cross(None),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(13.892443989449804, 11.40175425099138, 8.602325267042627, 12.206555615733702, 16.55294535724685, 9.219544457292887, 15.652475842498529, 14.7648230602334, 7.0, 7.615773105863909, 13.038404810405298, 9.899494936611665)
    }
    "compute pow" in {
      val input = Join(BuiltInFunction2Op(pow), Cross(None),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 170859375, 62748517, 19487171, -2187.0, -35831808, -4782969.0, 823543.0, -62748517, 279936.0, -105413504, 1.0E+7, -78125.0, -823543.0, 105413504)
    }
    "compute maxOf" in {
      val input = Join(BuiltInFunction2Op(maxOf), Cross(None),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(10, 14, 13, 7, 11, 15)
    }
    "compute atan2" in {
      val input = Join(BuiltInFunction2Op(atan2), Cross(None),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 1.1341691669813554, -0.6202494859828215, 1.1071487177940904, -1.1071487177940904, -0.9097531579442097, 1.0040671092713902, 0.7086262721276703, -0.4048917862850834, 1.0768549578753155, -1.0768549578753155, 0.960070362405688, -1.042721878368537, 0.7853981633974483, -0.7853981633974483)
    }
    "compute copySign" in {
      val input = Join(BuiltInFunction2Op(copySign), Cross(None),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 5, 10, 14, 6, 9, 13, 12, 7, 3, 11, 15)
    }
    "compute IEEEremainder" in {
      val input = Join(BuiltInFunction2Op(IEEEremainder), Cross(None),
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (Seq(_), SDecimal(d)) => d
      }

      result2.toVector.sorted must_== Vector(0, -3, 1, 2, -1, -2).sorted
    }.pendingUntilFixed
    "compute roundTo" in {
      val input = inputOp2(roundTo, "/hom/numbersAcrossSlices", CLong(0))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(-7, 15, -13, 11, 7, 11, -7, 0, 14, -3, 6, -12, 10, -9, 15, -5, -13, -14, 11, -5, -5, 13)
    }
  }

  "for heterogeneous sets across two slice boundaries (22 elements)" should {
    "compute sinh" in {
      val input = dag.Operate(BuiltInFunction1Op(sinh),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set[Double](0.0, 3.626860407847019, 74.20321057778875, -10.017874927409903, 81377.39570642984, 1.1752011936438014, -1.1752011936438014)
    }
    "compute toDegrees" in {
      val input = dag.Operate(BuiltInFunction1Op(toDegrees),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 114.59155902616465, -57.29577951308232, 57.29577951308232, 687.5493541569879, 286.4788975654116, -171.88733853924697)
    }
    "compute expm1" in {
      val input = dag.Operate(BuiltInFunction1Op(expm1),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(6.38905609893065, 0.0, 147.4131591025766, 1.718281828459045, -0.950212931632136, 162753.79141900392, -0.6321205588285577)
    }
    "compute getExponent" in {
      val input = dag.Operate(BuiltInFunction1Op(getExponent),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(1, 0, 3, 2)
    }
    "compute asin" in {
      val input = dag.Operate(BuiltInFunction1Op(asin),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 1.5707963267948966, -1.5707963267948966)
    }
    "compute log10" in {
      val input = dag.Operate(BuiltInFunction1Op(log10),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 0.6989700043360189, 1.0791812460476249, 0.3010299956639812)
    }
    "compute cos" in {
      val input = dag.Operate(BuiltInFunction1Op(cos),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(1.0, 0.5403023058681398, 0.8438539587324921, -0.9899924966004454, -0.4161468365471424, 0.28366218546322625)
    }
    "compute exp" in {
      val input = dag.Operate(BuiltInFunction1Op(exp),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(1.0, 0.049787068367863944, 2.718281828459045, 162754.79141900392, 148.4131591025766, 0.36787944117144233, 7.38905609893065)
    }
    "compute cbrt" in {
      val input = dag.Operate(BuiltInFunction1Op(cbrt),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 1.709975946676697, 1.0, -1.4422495703074083, 2.2894284851066637, 1.2599210498948732, -1.0)
    }
    "compute atan" in {
      val input = dag.Operate(BuiltInFunction1Op(atan),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 1.4876550949064553, 1.1071487177940904, -1.2490457723982544, 0.7853981633974483, -0.7853981633974483, 1.373400766945016)
    }
    "compute ceil" in {
      val input = dag.Operate(BuiltInFunction1Op(ceil),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 5, -3, 1, 2, 12, -1)
    }
    "compute rint" in {
      val input = dag.Operate(BuiltInFunction1Op(rint),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 5, -3, 1, 2, 12, -1)
    }
    "compute log1p" in {
      val input = dag.Operate(BuiltInFunction1Op(log1p),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(7)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 0.6931471805599453, 2.5649493574615367, 1.0986122886681096, 1.791759469228055)
    }
    "compute sqrt" in {
      val input = dag.Operate(BuiltInFunction1Op(sqrt),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(7)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 1.4142135623730951, 1.0, 3.4641016151377544, 2.23606797749979)
    }
    "compute floor" in {
      val input = dag.Operate(BuiltInFunction1Op(floor),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 5, -3, 1, 2, 12, -1)
    }
    "compute toRadians" in {
      val input = dag.Operate(BuiltInFunction1Op(toRadians),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, 0.08726646259971647, 0.20943951023931953, -0.05235987755982988, 0.03490658503988659, 0.017453292519943295, -0.017453292519943295)
    }
    "compute tanh" in {
      val input = dag.Operate(BuiltInFunction1Op(tanh),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, -0.9950547536867305, 0.9999999999244973, -0.7615941559557649, 0.7615941559557649, 0.9999092042625951, 0.9640275800758169)
    }
    "compute round" in {
      val input = dag.Operate(BuiltInFunction1Op(round),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 5, -3, 1, 2, 12, -1)
    }
    "compute cosh" in {
      val input = dag.Operate(BuiltInFunction1Op(cosh),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(10.067661995777765, 1.0, 81377.39571257407, 74.20994852478785, 3.7621956910836314, 1.543080634815244)
    }
    "compute tan" in {
      val input = dag.Operate(BuiltInFunction1Op(tan),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, -2.185039863261519, -3.380515006246586, 1.5574077246549023, -1.5574077246549023, 0.1425465430742778, -0.6358599286615808)
    }
    "compute abs" in {
      val input = dag.Operate(BuiltInFunction1Op(abs),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 5, 1, 2, 12, 3)
    }
    "compute sin" in {
      val input = dag.Operate(BuiltInFunction1Op(sin),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.0, -0.5365729180004349, 0.8414709848078965, -0.8414709848078965, 0.9092974268256817, -0.9589242746631385, -0.1411200080598672)
    }
    "compute log" in {
      val input = dag.Operate(BuiltInFunction1Op(mathlog),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.6931471805599453, 0.0, 2.4849066497880004, 1.6094379124341003)
    }
    "compute signum" in {
      val input = dag.Operate(BuiltInFunction1Op(signum),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 1, -1)
    }
    "compute acos" in {
      val input = dag.Operate(BuiltInFunction1Op(acos),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(1.5707963267948966, 0.0, 3.141592653589793)
    }
    "compute ulp" in {
      val input = dag.Operate(BuiltInFunction1Op(ulp),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(2.220446049250313E-16, 4.9E-324, 1.7763568394002505E-15, 8.881784197001252E-16, 4.440892098500626E-16)
    }
    "compute hypot" in {
      val input = Join(BuiltInFunction2Op(hypot), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(13.892443989449804, 8.602325267042627, 7.280109889280518, 7.0710678118654755, 7.0, 7.615773105863909)
    }
    "compute pow" in {
      val input = Join(BuiltInFunction2Op(pow), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 1, -2187, 78125, 35831808, 128, -1)
    }
    "compute maxOf" in {
      val input = Join(BuiltInFunction2Op(maxOf), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(7, 12)
    }
    "compute atan2" in {
      val input = Join(BuiltInFunction2Op(atan2), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0.1418970546041639, 0.0, -0.1418970546041639, 0.6202494859828215, 0.27829965900511133, -0.4048917862850834, 1.042721878368537)
    }
    "compute copySign" in {
      val input = Join(BuiltInFunction2Op(copySign), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, 5, 1, 2, 12, 3)
    }
    "compute IEEEremainder" in {
      val input = Join(BuiltInFunction2Op(IEEEremainder), Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Const(CLong(7))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }

      result2.toSet must_== Set(0, -3, 1, 2, -1, -2)
    }
    "compute roundTo" in {
      val input = inputOp2(roundTo, "/het/numbersAcrossSlices", CLong(0))

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }

      result2.toSet must_== Set(5, 0, 1, -1, 1, 12, 0, 2, -3)
    }
  }
}

object MathLibSpecs extends MathLibSpecs[Need]
