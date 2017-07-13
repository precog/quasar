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

package quasar.std

import quasar.Func
import slamdata.Predef._
import quasar.TypeArbitrary

import org.scalacheck.{Arbitrary, Gen, Prop}, Arbitrary.arbitrary
import scalaz.Validation, Validation.FlatMap._

class RelationsSpec extends quasar.Qspec with TypeArbitrary {
  import RelationsLib._
  import quasar.Type
  import quasar.Type.Const
  import quasar.Data.Bool
  import quasar.Data.Dec
  import quasar.Data.Int
  import quasar.Data.Str

  val comparisonOps = Gen.oneOf(Eq, Neq, Lt, Lte, Gt, Gte)

  "RelationsLib" should {

    "type eq with matching arguments" >> prop { (t : Type) =>
      val expr = Eq.tpe(Func.Input2(t, t))
      t match {
        case Const(_) => expr should beSuccessful(Const(Bool(true)))
        case _ => expr should beSuccessful(Type.Bool)
      }
    }

    "fold integer eq" in {
      val expr = Eq.tpe(Func.Input2(Const(Int(1)), Const(Int(1))))
      expr should beSuccessful(Const(Bool(true)))
    }

    "fold eq with mixed numeric type" in {
      val expr = Eq.tpe(Func.Input2(Const(Int(1)), Const(Dec(1.0))))
      expr should beSuccessful(Const(Bool(true)))
    }

    "fold eq with mixed type" in {
      val expr = Eq.tpe(Func.Input2(Const(Int(1)), Const(Str("a"))))
      expr should beSuccessful(Const(Bool(false)))
    }

    "type Eq with Top" >> prop { (t : Type) =>
      Eq.tpe(Func.Input2(Type.Top, t)) should beSuccessful(Type.Bool)
      Eq.tpe(Func.Input2(t, Type.Top)) should beSuccessful(Type.Bool)
    }

    "type Neq with Top" >> prop { (t : Type) =>
      Neq.tpe(Func.Input2(Type.Top, t)) should beSuccessful(Type.Bool)
      Neq.tpe(Func.Input2(t, Type.Top)) should beSuccessful(Type.Bool)
    }

    "fold neq with mixed type" in {
      val expr = Neq.tpe(Func.Input2(Const(Int(1)), Const(Str("a"))))
      expr should beSuccessful(Const(Bool(true)))
    }

    // TODO: similar for the rest of the simple relations

    "fold cond with true" >> prop { (t1 : Type, t2 : Type) =>
      val expr = Cond.tpe(Func.Input3(Const(Bool(true)), t1, t2))
      expr must beSuccessful(t1)
    }

    "fold cond with false" >> prop { (t1 : Type, t2 : Type) =>
      val expr = Cond.tpe(Func.Input3(Const(Bool(false)), t1, t2))
      expr must beSuccessful(t2)
    }

    "find lub for cond with int" in {
      val expr = Cond.tpe(Func.Input3(Type.Bool, Type.Int, Type.Int))
      expr must beSuccessful(Type.Int)
    }

    "find lub for cond with arbitrary args" >> prop { (t1 : Type, t2 : Type) =>
      val expr = Cond.tpe(Func.Input3(Type.Bool, t1, t2))
      expr must beSuccessful(Type.lub(t1, t2))
    }

    "flip comparison ops" >> Prop.forAll(comparisonOps, arbitrary[BigInt], arbitrary[BigInt]) {
        case (func, left, right) =>
          flip(func).map(
            _.tpe(Func.Input2(Type.Const(Int(right)), Type.Const(Int(left))))) must
            beSome(func.tpe(Func.Input2(Type.Const(Int(left)), Type.Const(Int(right)))))
    }

    "flip boolean ops" >> Prop.forAll(Gen.oneOf(And, Or), arbitrary[Boolean], arbitrary[Boolean]) {
        case (func, left, right) =>
          flip(func).map(
            _.tpe(Func.Input2(Type.Const(Bool(right)), Type.Const(Bool(left))))) must
            beSome(func.tpe(Func.Input2(Type.Const(Bool(left)), Type.Const(Bool(right)))))
    }

    "negate comparison ops" >> Prop.forAll(comparisonOps, arbitrary[BigInt], arbitrary[BigInt]) {
        case (func, left, right) =>
          RelationsLib.negate(func).map(
            _.tpe(Func.Input2(Type.Const(Int(left)), Type.Const(Int(right))))) must
          beSome(func.tpe(Func.Input2(Type.Const(Int(left)), Type.Const(Int(right)))).flatMap(x => Not.tpe(Func.Input1(x))))
    }
  }
}
