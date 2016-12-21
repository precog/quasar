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
import quasar.common.SortDir
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}
import quasar.sql.CompilerHelpers
import quasar.std._, StdLib.structural._

import matryoshka._
import matryoshka.data.Fix
import org.scalacheck._
import pathy.Path._
import scalaz.{Free => _, _}, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.scalacheck.ScalazProperties._

class OptimizerSpec extends quasar.Qspec with CompilerHelpers with TreeMatchers {
  import quasar.frontend.fixpoint._

  "simplify" should {

    "inline trivial binding" in {
      optimizer.simplify(lpf.let('tmp0, read("foo"), lpf.free('tmp0))) must
        beTreeEqual(read("foo"))
    }

    "not inline binding that's used twice" in {
      optimizer.simplify(lpf.let('tmp0, read("foo"),
        makeObj(
          "bar" -> lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("bar"))),
          "baz" -> lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("baz")))))) must
        beTreeEqual(
          lpf.let('tmp0, read("foo"),
            makeObj(
              "bar" -> lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("bar"))),
              "baz" -> lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("baz"))))))
    }

    "completely inline stupid lets" in {
      optimizer.simplify(lpf.let('tmp0, read("foo"), lpf.let('tmp1, lpf.free('tmp0), lpf.free('tmp1)))) must
        beTreeEqual(read("foo"))
    }

    "inline correct value for shadowed binding" in {
      optimizer.simplify(lpf.let('tmp0, read("foo"),
        lpf.let('tmp0, read("bar"),
          makeObj(
            "bar" -> lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("bar"))))))) must
        beTreeEqual(
          makeObj(
            "bar" -> lpf.invoke2(ObjectProject, read("bar"), lpf.constant(Data.Str("bar")))))
    }

    "inline a binding used once, then shadowed once" in {
      optimizer.simplify(lpf.let('tmp0, read("foo"),
        lpf.invoke2(ObjectProject, lpf.free('tmp0),
          lpf.let('tmp0, read("bar"),
            makeObj(
              "bar" -> lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("bar")))))))) must
        beTreeEqual(
          lpf.invoke(ObjectProject, Func.Input2(
            read("foo"),
            makeObj(
              "bar" -> lpf.invoke2(ObjectProject, read("bar"), lpf.constant(Data.Str("bar")))))))
    }

    "inline a binding used once, then shadowed twice" in {
      optimizer.simplify(lpf.let('tmp0, read("foo"),
        lpf.invoke2(ObjectProject, lpf.free('tmp0),
          lpf.let('tmp0, read("bar"),
            makeObj(
              "bar" -> lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("bar"))),
              "baz" -> lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("baz")))))))) must
        beTreeEqual(
          lpf.invoke(ObjectProject, Func.Input2(
            read("foo"),
            lpf.let('tmp0, read("bar"),
              makeObj(
                "bar" -> lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("bar"))),
                "baz" -> lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("baz"))))))))
    }

    "partially inline a more interesting case" in {
      optimizer.simplify(lpf.let('tmp0, read("person"),
        lpf.let('tmp1,
          makeObj(
            "name" -> lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("name")))),
          lpf.let('tmp2,
            lpf.sort(
              lpf.free('tmp1),
              (lpf.invoke2(ObjectProject, lpf.free('tmp1), lpf.constant(Data.Str("name"))), SortDir.asc).wrapNel),
            lpf.free('tmp2))))) must
        beTreeEqual(
          lpf.let('tmp1,
            makeObj(
              "name" ->
                lpf.invoke2(ObjectProject, read("person"), lpf.constant(Data.Str("name")))),
            lpf.sort(
              lpf.free('tmp1),
              (lpf.invoke2(ObjectProject, lpf.free('tmp1), lpf.constant(Data.Str("name"))), SortDir.asc).wrapNel)))
    }
  }

  "preferProjections" should {
    "ignore a delete with unknown shape" in {
      optimizer.preferProjections(
        lpf.invoke2(DeleteField, lpf.read(file("zips")),
          lpf.constant(Data.Str("pop")))) must
        beTreeEqual[Fix[LP]](
          lpf.invoke2(DeleteField, lpf.read(file("zips")),
            lpf.constant(Data.Str("pop"))))
    }

    "convert a delete after a projection" in {
      optimizer.preferProjections(
        lpf.let('meh, lpf.read(file("zips")),
          lpf.invoke2(DeleteField,
            makeObj(
              "city" -> lpf.invoke2(ObjectProject, lpf.free('meh), lpf.constant(Data.Str("city"))),
              "pop"  -> lpf.invoke2(ObjectProject, lpf.free('meh), lpf.constant(Data.Str("pop")))),
            lpf.constant(Data.Str("pop"))))) must
      beTreeEqual(
        lpf.let('meh, lpf.read(file("zips")),
          makeObj(
            "city" ->
              lpf.invoke2(ObjectProject,
                makeObj(
                  "city" ->
                    lpf.invoke2(ObjectProject, lpf.free('meh), lpf.constant(Data.Str("city"))),
                  "pop" ->
                    lpf.invoke2(ObjectProject, lpf.free('meh), lpf.constant(Data.Str("pop")))),
                lpf.constant(Data.Str("city"))))))
    }

    "convert a delete when the shape is hidden by a Free" in {
      optimizer.preferProjections(
        lpf.let('meh, lpf.read(file("zips")),
          lpf.let('meh2,
            makeObj(
              "city" -> lpf.invoke2(ObjectProject, lpf.free('meh), lpf.constant(Data.Str("city"))),
              "pop"  -> lpf.invoke2(ObjectProject, lpf.free('meh), lpf.constant(Data.Str("pop")))),
            makeObj(
              "orig" -> lpf.free('meh2),
              "cleaned" ->
                lpf.invoke2(DeleteField, lpf.free('meh2), lpf.constant(Data.Str("pop"))))))) must
      beTreeEqual(
        lpf.let('meh, lpf.read(file("zips")),
          lpf.let('meh2,
            makeObj(
              "city" -> lpf.invoke2(ObjectProject, lpf.free('meh), lpf.constant(Data.Str("city"))),
              "pop"  -> lpf.invoke2(ObjectProject, lpf.free('meh), lpf.constant(Data.Str("pop")))),
            makeObj(
              "orig" -> lpf.free('meh2),
              "cleaned" ->
                makeObj(
                  "city" ->
                    lpf.invoke2(ObjectProject, lpf.free('meh2), lpf.constant(Data.Str("city"))))))))
    }
  }

  "Component" should {
    implicit def componentArbitrary[A: Arbitrary]: Arbitrary[Component[Fix[LP], A]] =
      Arbitrary(Arbitrary.arbitrary[A]) ∘ (NeitherCond(_))

    implicit def ArbComponentInt: Arbitrary[Component[Fix[LP], Int]] =
      componentArbitrary[Int]

    implicit def ArbComponentInt2Int: Arbitrary[Component[Fix[LP], Int => Int]] =
      componentArbitrary[Int => Int]

    // FIXME: this test isn't really testing much at this point because
    //        we cannot test the equality of two functions
    implicit def EqualComponent: Equal[Component[Fix[LP], Int]] = new Equal[Component[Fix[LP], Int]] {
      def equal(a1: Component[Fix[LP], Int], a2: Component[Fix[LP], Int]): Boolean = true
    }

    "obey applicative laws" in {
      applicative.laws[Component[Fix[LP], ?]]
    }
  }
}
