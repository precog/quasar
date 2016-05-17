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

package quasar.std

import quasar.Predef._
import quasar.TypeArbitrary
import quasar.specs2.PendingWithAccurateCoverage

import matryoshka.Fix
import org.scalacheck.Arbitrary
import org.specs2.mutable._
import org.specs2.scalaz._
import org.specs2.ScalaCheck
import org.threeten.bp.{Instant, Duration}
import scalaz.ValidationNel
import scalaz.Validation.FlatMap._
import shapeless._

class MathSpec extends Specification with ScalaCheck with TypeArbitrary with ValidationMatchers with PendingWithAccurateCoverage {
  import MathLib._
  import quasar.Type
  import quasar.Type.Const
  import quasar.Type.Obj
  import quasar.Data._
  import quasar.SemanticError
  import quasar.LogicalPlan, LogicalPlan._

  "MathLib" should {
    "type simple add with ints" in {
      val expr = Add.tpe(Sized[IS](Type.Int, Type.Int))
      expr should beSuccessful(Type.Int)
    }

    "type simple add with decs" in {
      val expr = Add.tpe(Sized[IS](Type.Dec, Type.Dec))
      expr should beSuccessful(Type.Dec)
    }

    "type simple add with promotion" in {
      val expr = Add.tpe(Sized[IS](Type.Int, Type.Dec))
      expr should beSuccessful(Type.Dec)
    }

    "type simple add with zero" in {
      val expr = Add.tpe(Sized[IS](Type.Numeric, TZero()))
      expr should beSuccessful(Type.Numeric)
    }

    "fold simple add with int constants" in {
      val expr = Add.tpe(Sized[IS](TOne(), Const(Int(2))))
      expr should beSuccessful(Const(Int(3)))
    }

    "fold simple add with decimal constants" in {
      val expr = Add.tpe(Sized[IS](Const(Dec(1.0)), Const(Dec(2.0))))
      expr should beSuccessful(Const(Dec(3)))
    }

    "fold simple add with promotion" in {
      val expr = Add.tpe(Sized[IS](TOne(), Const(Dec(2.0))))
      expr should beSuccessful(Const(Dec(3)))
    }

    "simplify add with zero" in {
      Add.simplify(Add.apply0(LogicalPlan.Constant(Int(0)), Free('x))) should
        beSome(FreeF[Fix[LogicalPlan]]('x))
    }

    "simplify add with Dec zero" in {
      Add.simplify(Add.apply0(Free('x), LogicalPlan.Constant(Dec(0.0)))) should
        beSome(FreeF[Fix[LogicalPlan]]('x))
    }

    "eliminate multiply by dec zero (on the right)" ! prop { (c : Const) =>
      val expr = Multiply.tpe(Sized[IS](c, Const(Dec(0.0))))
      expr should beSuccessful(TZero())
    }

    "eliminate multiply by zero (on the left)" ! prop { (c : Const) =>
      val expr = Multiply.tpe(Sized[IS](TZero(), c))
      expr should beSuccessful(TZero())
    }

    "fold simple division" in {
      val expr = Divide.tpe(Sized[IS](Const(Int(6)), Const(Int(3))))
      expr should beSuccessful(Const(Int(2)))
    }

    "fold truncating division" in {
      val expr = Divide.tpe(Sized[IS](Const(Int(5)), Const(Int(2))))
      expr should beSuccessful(Const(Int(2)))
    }

    "fold simple division (dec)" in {
      val expr = Divide.tpe(Sized[IS](Const(Int(6)), Const(Dec(3.0))))
      expr should beSuccessful(Const(Dec(2.0)))
    }

    "fold division (dec)" in {
      val expr = Divide.tpe(Sized[IS](Const(Int(5)), Const(Dec(2))))
      expr should beSuccessful(Const(Dec(2.5)))
    }

    "divide by zero" in {
      val expr = Divide.tpe(Sized[IS](TOne(), TZero()))
      expr must beFailing
    }

    "divide by zero (dec)" in {
      val expr = Divide.tpe(Sized[IS](Const(Dec(1.0)), Const(Dec(0.0))))
      expr must beFailing
    }

    "fold simple modulo" in {
      val expr = Modulo.tpe(Sized[IS](Const(Int(6)), Const(Int(3))))
      expr should beSuccessful(TZero())
    }

    "fold non-zero modulo" in {
      val expr = Modulo.tpe(Sized[IS](Const(Int(5)), Const(Int(2))))
      expr should beSuccessful(TOne())
    }

    "fold simple modulo (dec)" in {
      val expr = Modulo.tpe(Sized[IS](Const(Int(6)), Const(Dec(3.0))))
      expr should beSuccessful(Const(Dec(0.0)))
    }

    "fold non-zero modulo (dec)" in {
      val expr = Modulo.tpe(Sized[IS](Const(Int(5)), Const(Dec(2.2))))
      expr should beSuccessful(Const(Dec(0.6)))
    }

    "modulo by zero" in {
      val expr = Modulo.tpe(Sized[IS](TOne(), TZero()))
      expr must beFailing
    }

    "modulo by zero (dec)" in {
      val expr = Modulo.tpe(Sized[IS](Const(Dec(1.0)), Const(Dec(0.0))))
      expr must beFailing
    }

    "typecheck number raised to 0th power" ! prop { (t: Type) =>
      Power.tpe(Sized[IS](t, TZero())) should beSuccessful(TOne())
    }.setArbitrary(arbitraryNumeric)

    "typecheck 0 raised to any (non-zero) power" ! prop { (t: Type) =>
      (t != TZero()) ==>
        (Power.tpe(Sized[IS](TZero(), t)) should beSuccessful(TZero()))
    }.setArbitrary(arbitraryNumeric)

    "typecheck any number raised to 1st power" ! prop { (t: Type) =>
      Power.tpe(Sized[IS](t, TOne())) should beSuccessful(t)
    }.setArbitrary(arbitraryNumeric)

    "typecheck constant raised to int constant" in {
      Power.tpe(Sized[IS](Const(Dec(7.2)), Const(Int(2)))) should beSuccessful(Const(Dec(51.84)))
    }

    "simplify expression raised to 1st power" in {
      Power.simplify(Power.apply0(Free('x), Constant(Int(1)))) should
        beSome(FreeF[Fix[LogicalPlan]]('x))
    }

    "fold a complex expression (10-4)/3 + (5*8)" in {
      val expr = for {
        x1 <- Subtract.tpe(Sized[IS](
                Const(Int(10)),
                Const(Int(4))));
        x2 <- Divide.tpe(Sized[IS](x1,
                Const(Int(3))));
        x3 <- Multiply.tpe(Sized[IS](
                Const(Int(5)),
                Const(Int(8))))
        x4 <- Add.tpe(Sized[IS](x2, x3))
      } yield x4
      expr should beSuccessful(Const(Int(42)))
    }

    "fail with mismatched constants" in {
      val expr = Add.tpe(Sized[IS](TOne(), Const(Str("abc"))))
      expr should beFailing
    }

    "fail with object and int constant" in {
      val expr = Add.tpe(Sized[IS](Obj(Map("x" -> Type.Int), None), TOne()))
      expr should beFailing
    }

    "add timestamp and interval" in {
      val expr = Add.tpe(Sized[IS](
        Type.Const(Timestamp(Instant.parse("2015-01-21T00:00:00Z"))),
        Type.Const(Interval(Duration.ofHours(9)))))
      expr should beSuccessful(Type.Const(Timestamp(Instant.parse("2015-01-21T09:00:00Z"))))
    }

    def permute(f: quasar.Func.Input[Type, nat._2] => ValidationNel[SemanticError, Type], t1: Const, t2: Const)(exp1: Const, exp2: Type) = {
      f(Sized[IS](t1, t2)) should beSuccessful(exp1)
      f(Sized[IS](t1, t2.value.dataType)) should beSuccessful(exp2)
      f(Sized[IS](t1.value.dataType, t2)) should beSuccessful(exp2)
      f(Sized[IS](t1.value.dataType, t2.value.dataType)) should beSuccessful(exp2)

      f(Sized[IS](t2, t1)) should beSuccessful(exp1)
      f(Sized[IS](t2.value.dataType, t1)) should beSuccessful(exp2)
      f(Sized[IS](t2, t1.value.dataType)) should beSuccessful(exp2)
      f(Sized[IS](t2.value.dataType, t1.value.dataType)) should beSuccessful(exp2)
    }

    "add with const and non-const Ints" in {
      permute(Add.tpe(_), TOne(), Const(Int(2)))(Const(Int(3)), Type.Int)
    }

    "add with const and non-const Int and Dec" in {
      permute(Add.tpe(_), TOne(), Const(Dec(2.0)))(Const(Dec(3.0)), Type.Dec)
    }

    // TODO: tests for unapply() in general
  }

  implicit def genConst : Arbitrary[Const] = Arbitrary {
    for { i <- Arbitrary.arbitrary[scala.Int] }
      yield Const(Int(i))
  }
}
