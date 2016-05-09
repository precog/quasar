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
import quasar.{Data, Type}
import quasar.DataArbitrary, DataArbitrary._
import quasar.specs2.ValidationMatchers

import org.specs2.{mutable, ScalaCheck}
import org.threeten.bp.Duration
import scalaz.scalacheck.ScalazArbitrary._
import scalaz.NonEmptyList
import scalaz.std.anyVal._
import scalaz.syntax.foldable1._

class AggLibSpec extends mutable.Specification with ScalaCheck with ValidationMatchers {
  import AggLib._

  "Arbitrary" should {
    "type a nonempty constant set to the constant first value" ! prop { xs: NonEmptyList[Data] =>
      Arbitrary.tpe(Type.Const(Data.Set(xs.list.toList)) :: Nil) must beSuccessful(Type.Const(xs.head))
    }

    "error when applied to an empty constant set" >> {
      Arbitrary.tpe(Type.Const(Data.Set(Nil)) :: Nil) must beFailing
    }
  }

  "Avg" should {
    "type a constant Int set to the constant average of values" ! prop { n: BigInt =>
      val s = Data.Set(List(Data.Int(n), Data.Int(n + 2)))
      Avg.tpe(Type.Const(s) :: Nil) must beSuccessful(Type.Const(Data.Dec(BigDecimal(n + 1))))
    }

    "type a constant Dec set to the constant average of values" ! prop { n0: Double =>
      val n = BigDecimal(n0)
      val s = Data.Set(List(Data.Dec(n), Data.Dec(n / 2.0)))
      Avg.tpe(Type.Const(s) :: Nil) must beSuccessful(Type.Const(Data.Dec(n * 0.75)))
    }

    "type a constant Interval set to the constant average of values" ! prop { n: Int =>
      val dur = Duration.ofMillis(n.toLong)
      val s = Data.Set(List(Data.Interval(dur), Data.Interval(dur.plusMillis(2))))
      Avg.tpe(Type.Const(s) :: Nil) must beSuccessful(Type.Const(Data.Dec(n.toLong + 1)))
    }

    "error when applied to an empty constant set" >> {
      Avg.tpe(Type.Const(Data.Set(Nil)) :: Nil) must beFailing
    }
  }

  "Count" should {
    "type a constant set as the constant length" ! prop { xs: List[Int] =>
      Count.tpe(Type.Const(Data.Set(xs.map(Data.Int(_)))) :: Nil) must beSuccessful(Type.Const(Data.Int(xs.length)))
    }
  }

  "Max" should {
    "type a constant set as the constant max value" ! prop { xs: NonEmptyList[Int] =>
      Max.tpe(Type.Const(Data.Set(xs.list.toList.map(Data.Int(_)))) :: Nil) must
        beSuccessful(Type.Const(Data.Int(xs.maximum1)))
    }

    "error when applied to an empty constant set" >> {
      Max.tpe(Type.Const(Data.Set(Nil)) :: Nil) must beFailing
    }
  }

  "Min" should {
    "type a constant set as the constant min value" ! prop { xs: NonEmptyList[Int] =>
      Min.tpe(Type.Const(Data.Set(xs.list.toList.map(Data.Int(_)))) :: Nil) must
        beSuccessful(Type.Const(Data.Int(xs.minimum1)))
    }

    "error when applied to an empty constant set" >> {
      Min.tpe(Type.Const(Data.Set(Nil)) :: Nil) must beFailing
    }
  }

  "Sum" should {
    "type a constant Int set to the constant Int sum of values" ! prop { xs: NonEmptyList[BigInt] =>
      val s = Data.Set(xs.list.toList map (Data.Int(_)))
      Sum.tpe(Type.Const(s) :: Nil) must beSuccessful(Type.Const(Data.Int(xs.list.toList.sum)))
    }

    "type a constant Dec set to the constant Dec sum of values" ! prop { xs: NonEmptyList[Double] =>
      val ys = xs.list.toList map (BigDecimal(_))
      val s = Data.Set(ys map (Data.Dec(_)))
      Sum.tpe(Type.Const(s) :: Nil) must beSuccessful(Type.Const(Data.Dec(ys.sum)))
    }

    "type a constant Interval set to the constant Interval sum of values" ! prop { xs: NonEmptyList[Int] =>
      val milliss = xs.list.toList map (_.toLong)
      val s = Data.Set(milliss map (n => Data.Interval(Duration.ofMillis(n))))
      Sum.tpe(Type.Const(s) :: Nil) must beSuccessful(Type.Const(Data.Interval(Duration.ofMillis(milliss.sum))))
    }

    "type an empty constant set to constant Int(0)" >> {
      Sum.tpe(Type.Const(Data.Set(Nil)) :: Nil) must beSuccessful(Type.Const(Data.Int(0)))
    }
  }
}
