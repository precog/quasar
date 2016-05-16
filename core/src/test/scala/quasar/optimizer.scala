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
import org.specs2.mutable._
import pathy.Path._
import shapeless.{Data => _, _}

class OptimizerSpec extends Specification with CompilerHelpers with TreeMatchers {
  import StdLib._
  import structural._
  import set._

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
          "bar" -> ObjectProject.apply0(Free('tmp0), Constant(Data.Str("bar"))),
          "baz" -> ObjectProject.apply0(Free('tmp0), Constant(Data.Str("baz")))))
        .transCata(repeatedly(Optimizer.simplifyƒ[Fix])) must
        beTree(
          Let('tmp0, read("foo"),
            makeObj(
              "bar" -> ObjectProject.apply0(Free('tmp0), Constant(Data.Str("bar"))),
              "baz" -> ObjectProject.apply0(Free('tmp0), Constant(Data.Str("baz"))))))
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
            "bar" -> ObjectProject.apply0(Free('tmp0), Constant(Data.Str("bar"))))))
        .transCata(repeatedly(Optimizer.simplifyƒ[Fix])) must
        beTree(
          makeObj(
            "bar" -> ObjectProject.apply0(read("bar"), Constant(Data.Str("bar")))))
    }

    "inline a binding used once, then shadowed once" in {
      Let('tmp0, read("foo"),
        ObjectProject.apply0(Free('tmp0),
          Let('tmp0, read("bar"),
            makeObj(
              "bar" -> ObjectProject.apply0(Free('tmp0), Constant(Data.Str("bar")))))))
        .transCata(repeatedly(Optimizer.simplifyƒ[Fix])) must
        beTree(
          Invoke(ObjectProject, Sized[IS](
            read("foo"),
            makeObj(
              "bar" -> ObjectProject.apply0(read("bar"), Constant(Data.Str("bar")))))))
    }

    "inline a binding used once, then shadowed twice" in {
      Let('tmp0, read("foo"),
        ObjectProject.apply0(Free('tmp0),
          Let('tmp0, read("bar"),
            makeObj(
              "bar" -> ObjectProject.apply0(Free('tmp0), Constant(Data.Str("bar"))),
              "baz" -> ObjectProject.apply0(Free('tmp0), Constant(Data.Str("baz")))))))
        .transCata(repeatedly(Optimizer.simplifyƒ[Fix])) must
        beTree(
          Invoke(ObjectProject, Sized[IS](
            read("foo"),
            Let('tmp0, read("bar"),
              makeObj(
                "bar" -> ObjectProject.apply0(Free('tmp0), Constant(Data.Str("bar"))),
                "baz" -> ObjectProject.apply0(Free('tmp0), Constant(Data.Str("baz"))))))))
    }

    "partially inline a more interesting case" in {
      Let('tmp0, read("person"),
        Let('tmp1,
          makeObj(
            "name" -> ObjectProject.apply0(Free('tmp0), Constant(Data.Str("name")))),
          Let('tmp2,
            OrderBy.apply0[FLP](
              Free('tmp1),
              MakeArray.apply0[FLP](
                ObjectProject.apply0(Free('tmp1), Constant(Data.Str("name")))),
                Constant(Data.Str("foobar"))),
            Free('tmp2))))
        .transCata(repeatedly(Optimizer.simplifyƒ[Fix])) must
        beTree(
          Let('tmp1,
            makeObj(
              "name" ->
                ObjectProject.apply0(read("person"), Constant(Data.Str("name")))),
            OrderBy.apply0[FLP](
              Free('tmp1),
              MakeArray.apply0[FLP](
                ObjectProject.apply0(Free('tmp1), Constant(Data.Str("name")))),
                Constant(Data.Str("foobar")))))
    }
  }

  "preferProjections" should {
    "ignore a delete with unknown shape" in {
      Optimizer.preferProjections(
        DeleteField.apply0(Read(file("zips")),
          Constant(Data.Str("pop")))) must
        beTree[Fix[LogicalPlan]](
          DeleteField.apply0(Read(file("zips")),
            Constant(Data.Str("pop"))))
    }

    "convert a delete after a projection" in {
      Optimizer.preferProjections(
        Let('meh, Read(file("zips")),
          DeleteField.apply0[FLP](
            makeObj(
              "city" -> ObjectProject.apply0(Free('meh), Constant(Data.Str("city"))),
              "pop"  -> ObjectProject.apply0(Free('meh), Constant(Data.Str("pop")))),
            Constant(Data.Str("pop"))))) must
      beTree(
        Let('meh, Read(file("zips")),
          makeObj(
            "city" ->
              ObjectProject.apply0(
                makeObj(
                  "city" ->
                    ObjectProject.apply0(Free('meh), Constant(Data.Str("city"))),
                  "pop" ->
                    ObjectProject.apply0(Free('meh), Constant(Data.Str("pop")))),
                Constant(Data.Str("city"))))))
    }

    "convert a delete when the shape is hidden by a Free" in {
      Optimizer.preferProjections(
        Let('meh, Read(file("zips")),
          Let('meh2,
            makeObj(
              "city" -> ObjectProject.apply0(Free('meh), Constant(Data.Str("city"))),
              "pop"  -> ObjectProject.apply0(Free('meh), Constant(Data.Str("pop")))),
            makeObj(
              "orig" -> Free('meh2),
              "cleaned" ->
                DeleteField.apply0(Free('meh2), Constant(Data.Str("pop"))))))) must
      beTree(
        Let('meh, Read(file("zips")),
          Let('meh2,
            makeObj(
              "city" -> ObjectProject.apply0(Free('meh), Constant(Data.Str("city"))),
              "pop"  -> ObjectProject.apply0(Free('meh), Constant(Data.Str("pop")))),
            makeObj(
              "orig" -> Free('meh2),
              "cleaned" ->
                makeObj(
                  "city" ->
                    ObjectProject.apply0(Free('meh2), Constant(Data.Str("city"))))))))
    }
  }
}
