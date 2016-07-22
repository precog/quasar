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
import quasar.std._

import matryoshka._, FunctorT.ops._
import pathy.Path._
import scalaz._, Scalaz._

import org.scalacheck._
import org.specs2.ScalaCheck
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.scalacheck.ScalazProperties._

class OptimizerSpec extends quasar.QuasarSpecification with CompilerHelpers with TreeMatchers with ScalaCheck {
  import StdLib.structural._
  import StdLib.set._
  import LogicalPlan._

  "simplify" should {

    "inline trivial binding" in {
      Let('tmp0, read("foo"), Free('tmp0))
        .transCata(repeatedly(Optimizer.simplifyƒ[Fix])) must
        beTree(read("foo"))
    }

    "not inline binding that's used twice" in {
      Let('tmp0, read("foo"),
        makeObj(
          "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
          "baz" -> ObjectProject(Free('tmp0), Constant(Data.Str("baz")))))
        .transCata(repeatedly(Optimizer.simplifyƒ[Fix])) must
        beTree(
          Let('tmp0, read("foo"),
            makeObj(
              "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
              "baz" -> ObjectProject(Free('tmp0), Constant(Data.Str("baz"))))))
    }

    "completely inline stupid lets" in {
      Let('tmp0, read("foo"), Let('tmp1, Free('tmp0), Free('tmp1)))
        .transCata(repeatedly(Optimizer.simplifyƒ[Fix])) must
        beTree(read("foo"))
    }

    "inline correct value for shadowed binding" in {
      Let('tmp0, read("foo"),
        Let('tmp0, read("bar"),
          makeObj(
            "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))))))
        .transCata(repeatedly(Optimizer.simplifyƒ[Fix])) must
        beTree(
          makeObj(
            "bar" -> ObjectProject(read("bar"), Constant(Data.Str("bar")))))
    }

    "inline a binding used once, then shadowed once" in {
      Let('tmp0, read("foo"),
        ObjectProject(Free('tmp0),
          Let('tmp0, read("bar"),
            makeObj(
              "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar")))))))
        .transCata(repeatedly(Optimizer.simplifyƒ[Fix])) must
        beTree(
          Invoke(ObjectProject, Func.Input2(
            read("foo"),
            makeObj(
              "bar" -> ObjectProject(read("bar"), Constant(Data.Str("bar")))))))
    }

    "inline a binding used once, then shadowed twice" in {
      Let('tmp0, read("foo"),
        ObjectProject(Free('tmp0),
          Let('tmp0, read("bar"),
            makeObj(
              "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
              "baz" -> ObjectProject(Free('tmp0), Constant(Data.Str("baz")))))))
        .transCata(repeatedly(Optimizer.simplifyƒ[Fix])) must
        beTree(
          Invoke(ObjectProject, Func.Input2(
            read("foo"),
            Let('tmp0, read("bar"),
              makeObj(
                "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                "baz" -> ObjectProject(Free('tmp0), Constant(Data.Str("baz"))))))))
    }

    "partially inline a more interesting case" in {
      Let('tmp0, read("person"),
        Let('tmp1,
          makeObj(
            "name" -> ObjectProject(Free('tmp0), Constant(Data.Str("name")))),
          Let('tmp2,
            OrderBy[FLP](
              Free('tmp1),
              MakeArray[FLP](
                ObjectProject(Free('tmp1), Constant(Data.Str("name")))),
              Constant(Data.Str("foobar"))),
            Free('tmp2))))
        .transCata(repeatedly(Optimizer.simplifyƒ[Fix])) must
        beTree(
          Let('tmp1,
            makeObj(
              "name" ->
                ObjectProject(read("person"), Constant(Data.Str("name")))),
            OrderBy[FLP](
              Free('tmp1),
              MakeArray[FLP](
                ObjectProject(Free('tmp1), Constant(Data.Str("name")))),
              Constant(Data.Str("foobar")))))
    }
  }

  "preferProjections" should {
    "ignore a delete with unknown shape" in {
      Optimizer.preferProjections(
        DeleteField(Read(file("zips")),
          Constant(Data.Str("pop")))) must
        beTree[Fix[LogicalPlan]](
          DeleteField(Read(file("zips")),
            Constant(Data.Str("pop"))))
    }

    "convert a delete after a projection" in {
      Optimizer.preferProjections(
        Let('meh, Read(file("zips")),
          DeleteField[FLP](
            makeObj(
              "city" -> ObjectProject(Free('meh), Constant(Data.Str("city"))),
              "pop"  -> ObjectProject(Free('meh), Constant(Data.Str("pop")))),
            Constant(Data.Str("pop"))))) must
      beTree(
        Let('meh, Read(file("zips")),
          makeObj(
            "city" ->
              ObjectProject(
                makeObj(
                  "city" ->
                    ObjectProject(Free('meh), Constant(Data.Str("city"))),
                  "pop" ->
                    ObjectProject(Free('meh), Constant(Data.Str("pop")))),
                Constant(Data.Str("city"))))))
    }

    "convert a delete when the shape is hidden by a Free" in {
      Optimizer.preferProjections(
        Let('meh, Read(file("zips")),
          Let('meh2,
            makeObj(
              "city" -> ObjectProject(Free('meh), Constant(Data.Str("city"))),
              "pop"  -> ObjectProject(Free('meh), Constant(Data.Str("pop")))),
            makeObj(
              "orig" -> Free('meh2),
              "cleaned" ->
                DeleteField(Free('meh2), Constant(Data.Str("pop"))))))) must
      beTree(
        Let('meh, Read(file("zips")),
          Let('meh2,
            makeObj(
              "city" -> ObjectProject(Free('meh), Constant(Data.Str("city"))),
              "pop"  -> ObjectProject(Free('meh), Constant(Data.Str("pop")))),
            makeObj(
              "orig" -> Free('meh2),
              "cleaned" ->
                makeObj(
                  "city" ->
                    ObjectProject(Free('meh2), Constant(Data.Str("city"))))))))
    }
  }

  "Component" should {
    implicit def componentArbitrary[A: Arbitrary]: Arbitrary[Optimizer.Component[A]] =
      Arbitrary(Arbitrary.arbitrary[A]) ∘ (Optimizer.NeitherCond(_))

    implicit def ArbComponentInt: Arbitrary[Optimizer.Component[Int]] =
      componentArbitrary[Int]

    implicit def ArbComponentInt2Int: Arbitrary[Optimizer.Component[Int => Int]] =
      componentArbitrary[Int => Int]

    // FIXME this test isn't really testing much at this point because
    // we cannot test the equality of two functions
    implicit def EqualComponent: Equal[Optimizer.Component[Int]] = new Equal[Optimizer.Component[Int]] {
      def equal(a1: Optimizer.Component[Int], a2: Optimizer.Component[Int]): Boolean = true
    }

    "obey applicative laws" in {
      applicative.laws[Optimizer.Component]
    }
  }
}
