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

package quasar.frontend.logicalplan

import slamdata.Predef._
import quasar.{Data, Func}
import quasar.DataGenerators._
import quasar.fp._
import quasar.std

import matryoshka._
import matryoshka.data.Fix
import org.scalacheck._
import org.specs2.scalaz.{ScalazMatchers, Spec}
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties.{equal => _, _}
import pathy.Path._

class LogicalPlanSpecs extends Spec with ScalazMatchers {
  val lpf = new LogicalPlanR[Fix[LogicalPlan]]

  implicit val arbLogicalPlan: Delay[Arbitrary, LogicalPlan] =
    new Delay[Arbitrary, LogicalPlan] {
      def apply[A](arb: Arbitrary[A]) =
        Arbitrary {
          Gen.oneOf(readGen[A], addGen(arb), constGen[A], letGen(arb), freeGen[A](Nil))
        }
    }

  // Switch this to parameterize over Funcs as well
  def addGen[A: Arbitrary]: Gen[LogicalPlan[A]] = for {
    l <- Arbitrary.arbitrary[A]
    r <- Arbitrary.arbitrary[A]
  } yield Invoke(std.MathLib.Add, Func.Input2(l, r))

  def letGen[A: Arbitrary]: Gen[LogicalPlan[A]] = for {
    n            <- Gen.choose(0, 1000)
    (form, body) <- Arbitrary.arbitrary[(A, A)]
  } yield let(Symbol("tmp" + n), form, body)

  def readGen[A]: Gen[LogicalPlan[A]] = Gen.const(read(rootDir </> file("foo")))

  def constGen[A]: Gen[LogicalPlan[A]] = for {
    data <- Arbitrary.arbitrary[Data]
  } yield constant(data)

  def freeGen[A](vars: List[Symbol]): Gen[LogicalPlan[A]] = for {
    n <- Gen.choose(0, 1000)
  } yield free(Symbol("tmp" + n))

  implicit val arbIntLP = arbLogicalPlan(Arbitrary.arbInt)

  checkAll(traverse.laws[LogicalPlan])

  import quasar.std.StdLib._, relations._, quasar.std.StdLib.set._, structural._, math._

  "normalizeTempNames" should {
    "rename simple nested lets" in {
      lpf.normalizeTempNames(
        lpf.let('foo, lpf.read(file("foo")),
          lpf.let('bar, lpf.read(file("bar")),
            Fix(MakeMapN(
              lpf.constant(Data.Str("x")) -> Fix(MapProject(lpf.free('foo), lpf.constant(Data.Str("x")))),
              lpf.constant(Data.Str("y")) -> Fix(MapProject(lpf.free('bar), lpf.constant(Data.Str("y"))))))))) must equal(
        lpf.let('__tmp0, lpf.read(file("foo")),
          lpf.let('__tmp1, lpf.read(file("bar")),
            Fix(MakeMapN(
              lpf.constant(Data.Str("x")) -> Fix(MapProject(lpf.free('__tmp0), lpf.constant(Data.Str("x")))),
              lpf.constant(Data.Str("y")) -> Fix(MapProject(lpf.free('__tmp1), lpf.constant(Data.Str("y")))))))))
    }

    "rename shadowed name" in {
      lpf.normalizeTempNames(
        lpf.let('x, lpf.read(file("foo")),
          lpf.let('x, Fix(MakeMapN(
              lpf.constant(Data.Str("x")) -> Fix(MapProject(lpf.free('x), lpf.constant(Data.Str("x")))))),
            lpf.free('x)))) must equal(
        lpf.let('__tmp0, lpf.read(file("foo")),
          lpf.let('__tmp1, Fix(MakeMapN(
              lpf.constant(Data.Str("x")) -> Fix(MapProject(lpf.free('__tmp0), lpf.constant(Data.Str("x")))))),
            lpf.free('__tmp1))))
    }
  }

  "normalizeLets" should {
    "re-nest" in {
      lpf.normalizeLets(
        lpf.let('bar,
          lpf.let('foo,
            lpf.read(file("foo")),
            Fix(Filter(lpf.free('foo), Fix(Eq(Fix(MapProject(lpf.free('foo), lpf.constant(Data.Str("x")))), lpf.constant(Data.Str("z"))))))),
          Fix(MakeMapN(
            lpf.constant(Data.Str("y")) -> Fix(MapProject(lpf.free('bar), lpf.constant(Data.Str("y")))))))) must equal(
        lpf.let('foo,
          lpf.read(file("foo")),
          lpf.let('bar,
            Fix(Filter(lpf.free('foo), Fix(Eq(Fix(MapProject(lpf.free('foo), lpf.constant(Data.Str("x")))), lpf.constant(Data.Str("z")))))),
            Fix(MakeMapN(
              lpf.constant(Data.Str("y")) -> Fix(MapProject(lpf.free('bar), lpf.constant(Data.Str("y")))))))))
    }

    "re-nest deep" in {
      lpf.normalizeLets(
        lpf.let('baz,
          lpf.let('bar,
            lpf.let('foo,
              lpf.read(file("foo")),
              Fix(Filter(lpf.free('foo), Fix(Eq(Fix(MapProject(lpf.free('foo), lpf.constant(Data.Str("x")))), lpf.constant(Data.Int(0))))))),
            Fix(Filter(lpf.free('bar), Fix(Eq(Fix(MapProject(lpf.free('foo), lpf.constant(Data.Str("y")))), lpf.constant(Data.Int(1))))))),
          Fix(MakeMapN(
            lpf.constant(Data.Str("z")) -> Fix(MapProject(lpf.free('bar), lpf.constant(Data.Str("z")))))))) must equal(
        lpf.let('foo,
          lpf.read(file("foo")),
          lpf.let('bar,
            Fix(Filter(lpf.free('foo), Fix(Eq(Fix(MapProject(lpf.free('foo), lpf.constant(Data.Str("x")))), lpf.constant(Data.Int(0)))))),
            lpf.let('baz,
              Fix(Filter(lpf.free('bar), Fix(Eq(Fix(MapProject(lpf.free('foo), lpf.constant(Data.Str("y")))), lpf.constant(Data.Int(1)))))),
              Fix(MakeMapN(
                lpf.constant(Data.Str("z")) -> Fix(MapProject(lpf.free('bar), lpf.constant(Data.Str("z"))))))))))
    }

    "hoist multiple Lets" in {
      lpf.normalizeLets(
        Fix(Add(
          lpf.let('x, lpf.constant(Data.Int(0)), Fix(Add(lpf.free('x), lpf.constant(Data.Int(1))))),
          lpf.let('y, lpf.constant(Data.Int(2)), Fix(Add(lpf.free('y), lpf.constant(Data.Int(3)))))))) must equal(
        lpf.let('x, lpf.constant(Data.Int(0)),
          lpf.let('y, lpf.constant(Data.Int(2)),
            Fix(Add(
              Fix(Add(lpf.free('x), lpf.constant(Data.Int(1)))),
              Fix(Add(lpf.free('y), lpf.constant(Data.Int(3)))))))))
    }

    "hoist deep Let by one level" in {
      lpf.normalizeLets(
        Fix(Add(
          lpf.constant(Data.Int(0)),
          Fix(Add(
            lpf.let('x, lpf.constant(Data.Int(1)), Fix(Add(lpf.free('x), lpf.constant(Data.Int(2))))),
            lpf.constant(Data.Int(3))))))) must equal(
        Fix(Add(
          lpf.constant(Data.Int(0)),
          lpf.let('x, lpf.constant(Data.Int(1)),
            Fix(Add(
              Fix(Add(lpf.free('x), lpf.constant(Data.Int(2)))),
              lpf.constant(Data.Int(3))))))))
    }
  }
}
