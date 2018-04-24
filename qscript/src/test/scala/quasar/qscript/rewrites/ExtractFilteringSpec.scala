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

package quasar.qscript.rewrites

import quasar.{Qspec, TreeMatchers, Type}
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp.coproductEqual
import quasar.qscript.{construction, Hole, TTypes}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._

final class ExtractFilteringSpec extends Qspec with TTypes[Fix] with TreeMatchers {

  val func = construction.Func[Fix]
  val ejs = Fixed[Fix[EJson]]

  val extractFiltering =
    (_: FreeMap).transCata[FreeMap](repeatedly(ExtractFiltering[Fix, Hole]))

  "extracting filtering conditionals" should {

    "extract filtering success Guard" >> {
      val expr =
        func.ProjectKeyS(
          func.Guard(
            func.ProjectIndexI(func.Hole, 1),
            Type.AnyObject,
            func.ProjectIndexI(func.Hole, 1),
            func.Undefined),
          "foo")

      val expected =
          func.Guard(
            func.ProjectIndexI(func.Hole, 1),
            Type.AnyObject,
            func.ProjectKeyS(func.ProjectIndexI(func.Hole, 1), "foo"),
            func.Undefined)

      extractFiltering(expr) must beTreeEqual(expected)
    }

    "extract filtering failure Guard" >> {
      val expr =
        func.ProjectKeyS(
          func.Guard(
            func.ProjectIndexI(func.Hole, 1),
            Type.AnyObject,
            func.Undefined,
            func.ProjectIndexI(func.Hole, 1)),
          "foo")

      val expected =
          func.Guard(
            func.ProjectIndexI(func.Hole, 1),
            Type.AnyObject,
            func.Undefined,
            func.ProjectKeyS(func.ProjectIndexI(func.Hole, 1), "foo"))

      extractFiltering(expr) must beTreeEqual(expected)
    }

    "extract multiple Guards" >> {
      val expr =
        func.ProjectKey(
          func.Guard(
            func.ProjectKeyS(func.Hole, "foo"),
            Type.AnyObject,
            func.ProjectKeyS(func.Hole, "foo"),
            func.Undefined),
          func.Guard(
            func.ProjectKeyS(func.Hole, "bar"),
            Type.Str,
            func.ProjectKeyS(func.Hole, "bar"),
            func.Undefined))

      val expected =
          func.Guard(
            func.ProjectKeyS(func.Hole, "foo"),
            Type.AnyObject,
            func.Guard(
              func.ProjectKeyS(func.Hole, "bar"),
              Type.Str,
              func.ProjectKey(
                func.ProjectKeyS(func.Hole, "foo"),
                func.ProjectKeyS(func.Hole, "bar")),
              func.Undefined),
            func.Undefined)

      extractFiltering(expr) must beTreeEqual(expected)
    }

    "extract filtering true Cond" >> {
      val expr =
        func.ProjectKeyS(
          func.Cond(
            func.Eq(func.Hole, func.Constant(ejs.int(42))),
            func.ProjectIndexI(func.Hole, 1),
            func.Undefined),
          "foo")

      val expected =
        func.Cond(
          func.Eq(func.Hole, func.Constant(ejs.int(42))),
          func.ProjectKeyS(func.ProjectIndexI(func.Hole, 1), "foo"),
          func.Undefined)

      extractFiltering(expr) must beTreeEqual(expected)
    }

    "extract filtering false Cond" >> {
      val expr =
        func.ProjectKeyS(
          func.Cond(
            func.Eq(func.Hole, func.Constant(ejs.int(42))),
            func.Undefined,
            func.ProjectIndexI(func.Hole, 1)),
          "foo")

      val expected =
        func.Cond(
          func.Eq(func.Hole, func.Constant(ejs.int(42))),
          func.Undefined,
          func.ProjectKeyS(func.ProjectIndexI(func.Hole, 1), "foo"))

      extractFiltering(expr) must beTreeEqual(expected)
    }

    "extract multiple Conds" >> {
      val expr =
        func.Add(
          func.Cond(
            func.Eq(
              func.ProjectKeyS(func.Hole, "bar"),
              func.Constant(ejs.str("foo"))),
            func.Undefined,
            func.ProjectKeyS(func.Hole, "price")),
          func.Cond(
            func.Gt(
              func.ProjectKeyS(func.Hole, "rate"),
              func.Constant(ejs.dec(3.42))),
            func.ProjectKeyS(func.Hole, "rate"),
            func.Undefined))

      val expected =
          func.Cond(
            func.Eq(
              func.ProjectKeyS(func.Hole, "bar"),
              func.Constant(ejs.str("foo"))),
            func.Undefined,
            func.Cond(
              func.Gt(
                func.ProjectKeyS(func.Hole, "rate"),
                func.Constant(ejs.dec(3.42))),
              func.Add(
                func.ProjectKeyS(func.Hole, "price"),
                func.ProjectKeyS(func.Hole, "rate")),
              func.Undefined))

      extractFiltering(expr) must beTreeEqual(expected)
    }

    "extract a mixture of Cond and Guard" >> {
      val expr =
        func.Add(
          func.Cond(
            func.Eq(
              func.ProjectKeyS(func.Hole, "bar"),
              func.Constant(ejs.str("foo"))),
            func.Undefined,
            func.ProjectKeyS(func.Hole, "price")),
          func.Guard(
            func.ProjectKeyS(func.Hole, "rate"),
            Type.Numeric,
            func.ProjectKeyS(func.Hole, "rate"),
            func.Undefined))

      val expected =
        func.Cond(
          func.Eq(
            func.ProjectKeyS(func.Hole, "bar"),
            func.Constant(ejs.str("foo"))),
          func.Undefined,
          func.Guard(
            func.ProjectKeyS(func.Hole, "rate"),
            Type.Numeric,
            func.Add(
              func.ProjectKeyS(func.Hole, "price"),
              func.ProjectKeyS(func.Hole, "rate")),
            func.Undefined))

      extractFiltering(expr) must beTreeEqual(expected)
    }
  }
}
