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

package quasar.qscript

import slamdata.Predef._
import quasar.{Qspec, TreeMatchers, Type}
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.contrib.iota.{copkEqual, copkTraverse, copkShow}

import matryoshka.{delayEqual, delayShow}
import matryoshka.data.Fix
import matryoshka.data.free._
import matryoshka.implicits._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.either._

final class MapFuncCoreSpec extends Qspec with TTypes[Fix] with TreeMatchers {
  import MapFuncCore.{ConcatMapsN, StaticMap, StaticMapSuffix}

  val ejs = Fixed[Fix[EJson]]
  val func = construction.Func[Fix]

  val staticAssocs =
    List(
      ejs.str("foo") -> func.Add(func.ProjectKeyS(func.Hole, "bar"), func.Constant(ejs.int(3)))
    , ejs.int(1)     -> func.ProjectKeyS(func.Hole, "baz")
    , ejs.char('n')  -> func.Now[Hole])

  val staticMapExpr =
      func.ConcatMaps(
        func.ConcatMaps(
          func.MakeMapS(
            "foo",
            func.Add(func.ProjectKeyS(func.Hole, "bar"), func.Constant(ejs.int(3)))),
          func.MakeMap(
            func.Constant(ejs.int(1)),
            func.ProjectKeyS(func.Hole, "baz"))),
        func.MakeMap(
          func.Constant(ejs.char('n')),
          func.Now))

  "StaticMap" >> {
    "constructor should construct a map with static keys" >> {
      StaticMap(staticAssocs) must beTreeEqual(staticMapExpr)
    }

    "extractor should return static key/value pairs" >> {
      StaticMap.unapply(staticMapExpr.project) must beSome(equal(staticAssocs))
    }

    "extractor should fail when not completely static" >> {
      val dynamic =
        func.ConcatMaps(
          staticMapExpr,
          func.ProjectKeyS(func.Hole, "dyn"))

      StaticMap.unapply(dynamic.project) must beNone
    }
  }

  "ConcatMapsN" >> {
    val constMap =
      ejs.map(List(
        ejs.int(1)    -> ejs.str("foo")
      , ejs.char('b') -> ejs.dec(1.2)))

    val mapExprs =
      List(
        func.MakeMapS("baz", func.Now[Hole])
      , func.ProjectKeyS(func.Hole, "map1")
      , func.Constant[Hole](constMap))

    val concatExpr =
      func.ConcatMaps(
        func.ConcatMaps(
          func.MakeMapS("baz", func.Now)
        , func.ProjectKeyS(func.Hole, "map1"))
      , func.Constant(constMap))

    "constructor concatenates arguments" >> {
      ConcatMapsN(mapExprs).embed must beTreeEqual(concatExpr)
    }

    "extractor returns individual maps" >> {
      ConcatMapsN.unapply(concatExpr.project) must beSome(equal(mapExprs))
    }

    "extractor matches a single makeMap" >> {
      val x = func.MakeMapS("foo", func.Hole)
      ConcatMapsN.unapply(x.project) must beSome(equal(List(x)))
    }

    "extractor matches a constant map" >> {
      val x = func.Constant[Hole](constMap)
      ConcatMapsN.unapply(x.project) must beSome(equal(List(x)))
    }
  }

  "StaticMapSuffix" >> {
    "equals StaticMap when expression is a static map" >> {
      val x = staticMapExpr.project
      StaticMapSuffix.unapply(x) must_= StaticMap.unapply(x).map((Nil, _))
    }

    "emits no static part when map is completely dynamic" >> {
      val x = func.Hole
      val y = func.ProjectKeyS(func.Hole, "baz")

      StaticMapSuffix.unapply(func.ConcatMaps(x, y).project) must_=
        Some((List(x.right, y.right), List()))
    }

    "emit static suffix when present" >> {
      val dynMap =
        func.ProjectKeyS(func.Hole, "dyn")

      val mixed =
        func.ConcatMaps(dynMap, staticMapExpr)

      StaticMapSuffix.unapply(mixed.project) must_= Some((List(dynMap.right), staticAssocs))
    }
  }

  "normalization" >> {
    def normalize(fm: FreeMap): FreeMap =
      MapFuncCore.normalized(fm)

    "delete key in a static map" >> {
      val expr =
        func.DeleteKeyS(
          func.ConcatMaps(
            func.ConcatMaps(
              func.MakeMapS("foo", func.Hole),
              func.MakeMapS("baz", func.Hole)),
            func.MakeMapS("quux", func.Hole)),
          "baz")

      val expect =
          func.ConcatMaps(
            func.MakeMapS("foo", func.Hole),
            func.MakeMapS("quux", func.Hole))

      normalize(expr) must beTreeEqual(expect)
    }

    "delete keys in static suffix of a map" >> {
      val expr =
        func.DeleteKeyS(
          func.ConcatMaps(
            func.ConcatMaps(
              func.ConcatMaps(
                func.MakeMapS("foo", func.Hole),
                func.ProjectKeyS(func.Hole, "bar")),
              func.MakeMapS("baz", func.Hole)),
            func.MakeMapS("quux", func.Hole)),
          "baz")

      val expect =
        func.DeleteKeyS(
          func.ConcatMaps(
            func.ConcatMaps(
              func.MakeMapS("foo", func.Hole),
              func.ProjectKeyS(func.Hole, "bar")),
            func.MakeMapS("quux", func.Hole)),
          "baz")

      normalize(expr) must beTreeEqual(expect)
    }

    "statically project key in static map" >> {
      val expr =
        func.ProjectKeyS(
          func.ConcatMaps(
            func.ConcatMaps(
              func.MakeMapS("foo", func.Constant[Hole](ejs.char('a'))),
              func.MakeMapS("baz", func.Constant(ejs.char('b')))),
            func.MakeMapS("quux", func.Constant(ejs.char('c')))),
          "baz")

      normalize(expr) must beTreeEqual(func.Constant[Hole](ejs.char('b')))
    }

    "statically project a key appearing in the suffix" >> {
      val expr =
        func.ProjectKeyS(
          func.ConcatMaps(
            func.ConcatMaps(
              func.ConcatMaps(
                func.MakeMapS("foo", func.Constant[Hole](ejs.char('a'))),
                func.ProjectKeyS(func.Hole, "bar")),
              func.MakeMapS("baz", func.Constant(ejs.char('b')))),
            func.MakeMapS("quux", func.Constant(ejs.char('c')))),
          "baz")

      normalize(expr) must beTreeEqual(func.Constant[Hole](ejs.char('b')))
    }

    "elide all static parts when requested key not present" >> {
      val expr =
        func.ProjectKeyS(
          func.ConcatMaps(
            func.ConcatMaps(
              func.ConcatMaps(
                func.MakeMapS("foo", func.Hole),
                func.ProjectKeyS(func.Hole, "bar")),
              func.MakeMapS("baz", func.Hole)),
            func.MakeMapS("quux", func.Hole)),
          "baat")

      val expect =
        func.ProjectKeyS(func.ProjectKeyS(func.Hole, "bar"), "baat")

      normalize(expr) must beTreeEqual(expect)
    }

    "elide static suffix when requested key in prefix" >> {
      val expr =
        func.ProjectKeyS(
          func.ConcatMaps(
            func.ConcatMaps(
              func.ConcatMaps(
                func.MakeMapS("foo", func.Hole),
                func.ProjectKeyS(func.Hole, "bar")),
              func.MakeMapS("baz", func.Hole)),
            func.MakeMapS("quux", func.Hole)),
          "foo")

      val expect =
        func.ProjectKeyS(
          func.ConcatMaps(
            func.MakeMapS("foo", func.Hole),
            func.ProjectKeyS(func.Hole, "bar")),
          "foo")

      normalize(expr) must beTreeEqual(expect)
    }

    "simplify projecting from a Cond with static suffix in both branches" >> {
      val c =
        StaticMap(List(
          ejs.char('a') -> func.ProjectKeyS(func.Hole, "bar")
        , ejs.char('b') -> func.Constant[Hole](ejs.dec(42.42))))

      val a =
        StaticMapSuffix(
          List(
            (ejs.int(1), func.Constant[Hole](ejs.str("blah"))).left
          , func.ProjectIndexI(func.Hole, 3).right),
          List(
            ejs.char('b') -> func.ProjectKeyS(func.Hole, "baz"),
            ejs.int(7) -> func.Constant[Hole](ejs.str("meh"))))

      val expr =
        func.ProjectKey(
          func.Cond(
            func.ProjectKeyS(func.Hole, "test"),
            c,
            a),
          func.Constant[Hole](ejs.char('b')))

      val expect =
        func.Cond(
          func.ProjectKeyS(func.Hole, "test"),
          func.Constant[Hole](ejs.dec(42.42)),
          func.ProjectKeyS(func.Hole, "baz"))

      normalize(expr) must beTreeEqual(expect)
    }

    "project key from consequent when projecting from a Cond with an undefined alternate" >> {
      val expr =
        func.ProjectKeyS(
          func.ProjectKeyS(
            func.Cond(
              func.ProjectKeyS(func.Hole, "test"),
              func.ConcatMaps(
                func.ProjectKeyS(func.Hole, "bar"),
                func.MakeMapS("b", func.ProjectKeyS(func.Hole, "foo"))),
              func.Undefined),
            "b"),
          "c")

      val expect =
        func.Cond(
          func.ProjectKeyS(func.Hole, "test"),
          func.ProjectKeyS(func.ProjectKeyS(func.Hole, "foo"), "c"),
          func.Undefined)

      normalize(expr) must beTreeEqual(expect)
    }

    "project index from consequent when projecting from a Cond with an undefined alternate" >> {
      val expr =
        func.ProjectIndexI(
          func.ProjectIndexI(
            func.Cond(
              func.ProjectKeyS(func.Hole, "test"),
              func.ProjectKeyS(func.Hole, "foo"),
              func.Undefined),
            7),
          3)

      val expect =
        func.Cond(
          func.ProjectKeyS(func.Hole, "test"),
          func.ProjectIndexI(func.ProjectIndexI(func.ProjectKeyS(func.Hole, "foo"), 7), 3),
          func.Undefined)

      normalize(expr) must beTreeEqual(expect)
    }

    "leave inner guard in defined expression when different from inner nested" >> {
      val guardA =
        func.Guard(
          func.ProjectKeyS(func.Hole, "foo"),
          Type.AnyObject,
          func.ProjectKeyS(func.ProjectKeyS(func.Hole, "foo"), "bar"),
          func.Undefined)

      val guardB =
        func.Guard(
          func.ProjectKeyS(func.Hole, "quux"),
          Type.Numeric,
          func.Add(func.ProjectKeyS(func.Hole, "quux"), func.Constant(ejs.int(3))),
          func.Undefined)

      val nestedGuards =
        func.Guard(guardA, Type.Str, guardB, func.Undefined)

      normalize(nestedGuards) must beTreeEqual(nestedGuards)
    }

    "leave inner guard with different type test in outer defined expression" >> {
      val guardA =
        func.Guard(
          func.ProjectKeyS(func.Hole, "foo"),
          Type.AnyObject,
          func.ProjectKeyS(func.ProjectKeyS(func.Hole, "foo"), "bar"),
          func.Undefined)

      val guardB =
        func.Guard(
          func.ProjectKeyS(func.Hole, "foo"),
          Type.Numeric,
          func.Add(func.ProjectKeyS(func.Hole, "quux"), func.Constant(ejs.int(3))),
          func.Undefined)

      val nestedGuards =
        func.Guard(guardA, Type.Str, guardB, func.Undefined)

      normalize(nestedGuards) must beTreeEqual(nestedGuards)
    }
  }
}
