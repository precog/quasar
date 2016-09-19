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

package quasar

import quasar.Predef._
import quasar.fp._

import matryoshka.Fix
import org.scalacheck._
import org.specs2.scalaz._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._
import shapeless.contrib.scalaz.instances._
import pathy.Path._

class LogicalPlanSpecs extends Spec {
  import LogicalPlan._

  implicit val arbLogicalPlan: Arbitrary ~> λ[α => Arbitrary[LogicalPlan[α]]] =
    new (Arbitrary ~> λ[α => Arbitrary[LogicalPlan[α]]]) {
      def apply[α](arb: Arbitrary[α]): Arbitrary[LogicalPlan[α]] =
        Arbitrary {
          Gen.oneOf(readGen[α], addGen(arb), constGen[α], letGen(arb), freeGen[α](Nil))
        }
    }

  // Switch this to parameterize over Funcs as well
  def addGen[A: Arbitrary]: Gen[LogicalPlan[A]] = for {
    l <- Arbitrary.arbitrary[A]
    r <- Arbitrary.arbitrary[A]
  } yield InvokeF(std.MathLib.Add, Func.Input2(l, r))

  def letGen[A: Arbitrary]: Gen[LogicalPlan[A]] = for {
    n            <- Gen.choose(0, 1000)
    (form, body) <- Arbitrary.arbitrary[(A, A)]
  } yield LetF(Symbol("tmp" + n), form, body)

  def readGen[A]: Gen[LogicalPlan[A]] = Gen.const(ReadF(rootDir </> file("foo")))

  import DataArbitrary._

  def constGen[A]: Gen[LogicalPlan[A]] = for {
    data <- Arbitrary.arbitrary[Data]
  } yield ConstantF(data)

  def freeGen[A](vars: List[Symbol]): Gen[LogicalPlan[A]] = for {
    n <- Gen.choose(0, 1000)
  } yield FreeF(Symbol("tmp" + n))

  implicit val arbIntLP = arbLogicalPlan(Arbitrary.arbInt)

  checkAll(traverse.laws[LogicalPlan])

  import quasar.std.StdLib._, relations._, quasar.std.StdLib.set._, structural._, math._

  "normalizeTempNames" should {
    "rename simple nested lets" in {
      LogicalPlan.normalizeTempNames(
        Let('foo, Read(file("foo")),
          Let('bar, Read(file("bar")),
            Fix(MakeObjectN(
              Constant(Data.Str("x")) -> Fix(ObjectProject(Free('foo), Constant(Data.Str("x")))),
              Constant(Data.Str("y")) -> Fix(ObjectProject(Free('bar), Constant(Data.Str("y"))))))))) must_==
        Let('__tmp0, Read(file("foo")),
          Let('__tmp1, Read(file("bar")),
            Fix(MakeObjectN(
              Constant(Data.Str("x")) -> Fix(ObjectProject(Free('__tmp0), Constant(Data.Str("x")))),
              Constant(Data.Str("y")) -> Fix(ObjectProject(Free('__tmp1), Constant(Data.Str("y"))))))))
    }

    "rename shadowed name" in {
      LogicalPlan.normalizeTempNames(
        Let('x, Read(file("foo")),
          Let('x, Fix(MakeObjectN(
              Constant(Data.Str("x")) -> Fix(ObjectProject(Free('x), Constant(Data.Str("x")))))),
            Free('x)))) must_==
        Let('__tmp0, Read(file("foo")),
          Let('__tmp1, Fix(MakeObjectN(
              Constant(Data.Str("x")) -> Fix(ObjectProject(Free('__tmp0), Constant(Data.Str("x")))))),
            Free('__tmp1)))
    }
  }

  "normalizeLets" should {
    "re-nest" in {
      LogicalPlan.normalizeLets(
        Let('bar,
          Let('foo,
            Read(file("foo")),
            Fix(Filter(Free('foo), Fix(Eq(Fix(ObjectProject(Free('foo), Constant(Data.Str("x")))), Constant(Data.Str("z"))))))), 
          Fix(MakeObjectN(
            Constant(Data.Str("y")) -> Fix(ObjectProject(Free('bar), Constant(Data.Str("y")))))))) must_==
        Let('foo,
          Read(file("foo")),
          Let('bar,
            Fix(Filter(Free('foo), Fix(Eq(Fix(ObjectProject(Free('foo), Constant(Data.Str("x")))), Constant(Data.Str("z")))))), 
            Fix(MakeObjectN(
              Constant(Data.Str("y")) -> Fix(ObjectProject(Free('bar), Constant(Data.Str("y"))))))))
    }

    "re-nest deep" in {
      LogicalPlan.normalizeLets(
        Let('baz,
          Let('bar,
            Let('foo,
              Read(file("foo")),
              Fix(Filter(Free('foo), Fix(Eq(Fix(ObjectProject(Free('foo), Constant(Data.Str("x")))), Constant(Data.Int(0))))))),
            Fix(Filter(Free('bar), Fix(Eq(Fix(ObjectProject(Free('foo), Constant(Data.Str("y")))), Constant(Data.Int(1))))))),
          Fix(MakeObjectN(
            Constant(Data.Str("z")) -> Fix(ObjectProject(Free('bar), Constant(Data.Str("z")))))))) must_==
        Let('foo,
          Read(file("foo")),
          Let('bar,
            Fix(Filter(Free('foo), Fix(Eq(Fix(ObjectProject(Free('foo), Constant(Data.Str("x")))), Constant(Data.Int(0)))))),
            Let('baz,
              Fix(Filter(Free('bar), Fix(Eq(Fix(ObjectProject(Free('foo), Constant(Data.Str("y")))), Constant(Data.Int(1)))))),
              Fix(MakeObjectN(
                Constant(Data.Str("z")) -> Fix(ObjectProject(Free('bar), Constant(Data.Str("z")))))))))
    }

    "hoist multiple Lets" in {
      LogicalPlan.normalizeLets(
        Fix(Add(
          Let('x, Constant(Data.Int(0)), Fix(Add(Free('x), Constant(Data.Int(1))))),
          Let('y, Constant(Data.Int(2)), Fix(Add(Free('y), Constant(Data.Int(3)))))))) must_==
        Let('x, Constant(Data.Int(0)),
          Let('y, Constant(Data.Int(2)),
            Fix(Add(
              Fix(Add(Free('x), Constant(Data.Int(1)))),
              Fix(Add(Free('y), Constant(Data.Int(3))))))))
    }

    "hoist deep Let by one level" in {
      LogicalPlan.normalizeLets(
        Fix(Add(
          Constant(Data.Int(0)),
          Fix(Add(
            Let('x, Constant(Data.Int(1)), Fix(Add(Free('x), Constant(Data.Int(2))))),
            Constant(Data.Int(3))))))) must_==
        Fix(Add(
          Constant(Data.Int(0)),
          Let('x, Constant(Data.Int(1)),
            Fix(Add(
              Fix(Add(Free('x), Constant(Data.Int(2)))),
              Constant(Data.Int(3)))))))
    }
  }
}
