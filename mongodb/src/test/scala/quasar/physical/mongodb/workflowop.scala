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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.RenderTree
import quasar.TreeMatchers
import quasar.common.SortDir
import quasar.fp._
import quasar.javascript._
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.workflow._

import matryoshka.data.Fix
import org.scalacheck._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._
import shapeless.contrib.scalaz.instances._
import org.specs2.matcher.MustMatchers._

class WorkflowFSpec extends org.specs2.scalaz.Spec {
  implicit val arbIdHandling: Arbitrary[IdHandling] =
    Arbitrary(Gen.oneOf(ExcludeId, IncludeId, IgnoreId))

  checkAll("IdHandling", monoid.laws[IdHandling])

  implicit val arbCardinalExpr:
      Arbitrary ~> λ[α => Arbitrary[CardinalExpr[α]]] =
    new (Arbitrary ~> λ[α => Arbitrary[CardinalExpr[α]]]) {
      def apply[α](arb: Arbitrary[α]): Arbitrary[CardinalExpr[α]] =
        Arbitrary(arb.arbitrary.flatMap(a =>
          Gen.oneOf(MapExpr(a), FlatExpr(a))))
    }

  implicit val arbIntCardinalExpr = arbCardinalExpr(Arbitrary.arbInt)

  checkAll("CardinalExpr", traverse.laws[CardinalExpr])
  checkAll("CardinalExpr", comonad.laws[CardinalExpr])
}

class WorkflowSpec extends quasar.Qspec with TreeMatchers {
  import CollectionUtil._
  import fixExprOp._

  val readFoo = $read[WorkflowF](collection("db", "foo"))

  val cry = Crystallize[WorkflowF]
  import cry.crystallize

  "smart constructors" should {
    "put match before sort" in {
      val given = chain[Workflow](
        readFoo,
        $sort(NonEmptyList(BsonField.Name("city") -> SortDir.Descending)),
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000)))))
      val expected = chain[Workflow](
        readFoo,
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000)))),
        $sort(NonEmptyList(BsonField.Name("city") -> SortDir.Descending)))

      given must beTree(expected)
    }

    "choose smallest limit" in {
      val expected = chain[Workflow](readFoo, $limit(5))
      chain[Workflow](readFoo, $limit(10), $limit(5)) must_== expected
      chain[Workflow](readFoo, $limit(5), $limit(10)) must_== expected
    }

    "sum skips" in {
      chain[Workflow](readFoo, $skip(10), $skip(5)) must beTree(chain[Workflow](readFoo, $skip(15)))
    }

    "flatten foldLefts when possible" in {
      val given = $foldLeft[WorkflowF](
        $foldLeft(
          readFoo,
          $read(collection("db", "zips"))),
        $read(collection("db", "olympics")))
      val expected = $foldLeft[WorkflowF](
        readFoo,
        $read(collection("db", "zips")),
        $read(collection("db", "olympics")))

      given must beTree(expected)
    }

    "flatten project into group/unwind" in {
      val given = chain[Workflow](
        readFoo,
        $group(
          Grouped(ListMap(
            BsonField.Name("value") -> $push($field("rIght")))),
          \/-($field("lEft"))),
        $unwind(DocField(BsonField.Name("value"))),
        $project(Reshape(ListMap(
          BsonField.Name("city") -> \/-($field("value", "city")))),
          IncludeId))

      val expected = chain[Workflow](
        readFoo,
        $group(
          Grouped(ListMap(
            BsonField.Name("city") -> $push($field("rIght", "city")))),
          \/-($field("lEft"))),
        $unwind(DocField(BsonField.Name("city"))))

      given must beTree(expected: Workflow)
    }.pendingUntilFixed("SD-538")

    "not flatten project into group/unwind with _id excluded" in {
      val given = chain[Workflow](
        readFoo,
        $group(
          Grouped(ListMap(
            BsonField.Name("value") -> $push($field("rIght")))),
          \/-($field("lEft"))),
        $unwind(DocField(BsonField.Name("value"))),
        $project(Reshape(ListMap(
          BsonField.Name("city") -> \/-($field("value", "city")))),
          ExcludeId))

      given must beTree(given: Workflow)
    }

    "resolve `Include`" in {
      chain[Workflow]($read(collection("db", "zips")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> \/-($include()))),
          ExcludeId),
        $project(Reshape(ListMap(
          BsonField.Name("_id") -> \/-($field("bar")))),
          IncludeId)) must_==
      chain[Workflow]($read(collection("db", "zips")),
        $project(Reshape(ListMap(
          BsonField.Name("_id") -> \/-($field("bar")))),
          IncludeId))
    }

    "traverse `Include`" in {
      chain[Workflow]($read(collection("db", "zips")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") ->
            \/-($divide(
              $field("baz"),
              $literal(Bson.Int32(92)))))),
          IncludeId),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> \/-($include()))),
          IncludeId)) must_==
      chain[Workflow]($read(collection("db", "zips")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") ->
            \/-($divide($field("baz"), $literal(Bson.Int32(92)))))),
          IncludeId))
    }

    "resolve implied `_id`" in {
      chain[Workflow]($read(collection("db", "zips")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> \/-($field("bar")),
          BsonField.Name("_id") ->
            \/-($field("baz")))),
          IncludeId),
        $project(Reshape(ListMap(
          BsonField.Name("bar") ->
            \/-($field("bar")))),
          IncludeId)) must_==
      chain[Workflow]($read(collection("db", "zips")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> \/-($field("bar")),
          BsonField.Name("_id") ->
            \/-($field("baz")))),
          IncludeId))
    }

    "not resolve excluded `_id`" in {
      chain[Workflow]($read(collection("db", "zips")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> \/-($field("bar")),
          BsonField.Name("_id") ->
            \/-($field("baz")))),
          IncludeId),
        $project(Reshape(ListMap(
          BsonField.Name("bar") ->
            \/-($field("bar")))),
          ExcludeId)) must_==
      chain[Workflow]($read(collection("db", "zips")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") ->
            \/-($field("bar")))),
          ExcludeId))
    }

    "inline $project with field reference" in {
      chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          Reshape(ListMap(
            BsonField.Name("__tmp0") -> \/-($$ROOT))),
          IgnoreId),
        $project(
          Reshape(ListMap(
            BsonField.Name("__tmp1") -> \/-($field("__tmp0", "foo")))),
          IgnoreId)) must_==
      chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          Reshape(ListMap(
            BsonField.Name("__tmp1") -> \/-($field("foo")))),
          IgnoreId))
    }

    "not inline $projects with nesting" in {
      // NB: simulates a pair of type-checks, which cannot be inlined in a simple way
      // because the second digs into the structure created by the first.

      val op = chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          Reshape(ListMap(
            BsonField.Name("__tmp0") -> \/-(
              $cond($literal(Bson.Bool(true)), $$ROOT, $literal(Bson.Int32(0)))))),
          IgnoreId),
        $project(
          Reshape(ListMap(
            BsonField.Name("__tmp1") -> \/-(
              $cond($literal(Bson.Bool(true)), $field("__tmp0", "foo"), $literal(Bson.Int32(1)))))),
          IgnoreId))

      (op.unFix match {
        case $project(Fix($project(_, Reshape(s1), _)), Reshape(s2), _) =>
          s1.keys must_== Set(BsonField.Name("__tmp0"))
          s2.keys must_== Set(BsonField.Name("__tmp1"))
        case _ => failure
      }): org.specs2.execute.Result
    }

    "not inline $project with a bad reference" in {
      // NB: in a case like this, the original structure is preserved so it
      // can be debugged more easily.

      val op = chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          Reshape(ListMap(
            BsonField.Name("__tmp0") -> \/-($$ROOT))),
          IgnoreId),
        $project(
          Reshape(ListMap(
            BsonField.Name("__tmp1") -> \/-($field("foo")))),
          IgnoreId))

      (op.unFix match {
        case $project(Fix($project(_, Reshape(s1), _)), Reshape(s2), _) =>
          s1.keys must_== Set(BsonField.Name("__tmp0"))
          s2.keys must_== Set(BsonField.Name("__tmp1"))
        case _ => failure
      }): org.specs2.execute.Result
    }
  }

  "crystallize" should {
    import quasar.jscore, jscore._

    val readZips = $read[WorkflowF](collection("db", "zips"))

    "coalesce previous projection into a map" in {
      val given = chain[Workflow](
        readZips,
        $project(Reshape(ListMap(
          BsonField.Name("value") -> \/-($$ROOT))),
          IncludeId),
        $simpleMap((MapExpr(JsFn(Name("x"), BinOp(Add, jscore.Literal(Js.Num(4, false)), Select(ident("x"), "value")))):CardinalExpr[JsFn]).wrapNel, ListMap()))

      val expected = chain[Workflow](
        readZips,
        $simpleMap(
          NonEmptyList(
            MapExpr(JsFn(Name("x"), BinOp(Add, jscore.Literal(Js.Num(4, false)), ident("x"))))),
          ListMap()))

      crystallize(given) must beTree(Crystallized(expected))
    }

    "coalesce previous projection into a flatMap" in {
      val given = chain[Workflow](
        readZips,
        $project(Reshape(ListMap(
          BsonField.Name("value") -> \/-($$ROOT))),
          IncludeId),
        $simpleMap(
          (FlatExpr(JsFn(Name("x"), Select(ident("x"), "foo"))):CardinalExpr[JsFn]).wrapNel,
          ListMap()))

      val expected = chain[Workflow](
        readZips,
        $simpleMap(
          NonEmptyList(
            MapExpr(JsFn(Name("x"), Obj(ListMap(Name("value") -> ident("x"))))),
            FlatExpr(JsFn(Name("x"), Select(ident("x"), "foo")))),
          ListMap()))

      crystallize(given) must beTree(Crystallized(expected))
    }

    "convert previous projection before a reduce" in {
      val given = chain[Workflow](
        readZips,
        $project(Reshape(ListMap(
          BsonField.Name("value") -> \/-($$ROOT))),
          IncludeId),
        $reduce($ReduceF.reduceNOP, ListMap()))

      val expected = chain[Workflow](
        readZips,
        $simpleMap(
          (MapExpr(JsFn(Name("x"), Obj(ListMap(Name("value") -> ident("x"))))):CardinalExpr[JsFn]).wrapNel,
          ListMap()),
        $reduce($ReduceF.reduceNOP, ListMap()))

      crystallize(given) must beTree(Crystallized(expected))
    }

    "coalesce previous unwind into a map" in {
      val given = chain[Workflow](
        readZips,
        $unwind(DocVar.ROOT(BsonField.Name("loc"))),
        $simpleMap((MapExpr(JsFn(Name("x"),
          BinOp(Add, jscore.Literal(Js.Num(4, false)), ident("x")))):CardinalExpr[JsFn]).wrapNel, ListMap()))

      val expected = chain[Workflow](
        readZips,
        $simpleMap(
          NonEmptyList(
            FlatExpr(JsFn(Name("x"), Select(ident("x"), "loc"))),
            MapExpr(JsFn(Name("x"),
              BinOp(Add, jscore.Literal(Js.Num(4, false)), ident("x"))))),
          ListMap()))

      crystallize(given) must beTree(Crystallized(expected))
    }

    "coalesce previous unwind into a flatMap" in {
      val given = chain[Workflow](
        readZips,
        $unwind(DocVar.ROOT(BsonField.Name("loc"))),
        $simpleMap(
          (FlatExpr(JsFn(Name("x"), Select(ident("x"), "lat"))):CardinalExpr[JsFn]).wrapNel,
          ListMap()))

      val expected = chain[Workflow](
        readZips,
        $simpleMap(
          NonEmptyList(
            FlatExpr(JsFn(Name("x"), Select(ident("x"), "loc"))),
            FlatExpr(JsFn(Name("x"), Select(ident("x"), "lat")))),
          ListMap()))

      crystallize(given) must beTree(Crystallized(expected))
    }

    "convert previous unwind before a reduce" in {
      val given = chain[Workflow](
        readZips,
        $unwind(DocVar.ROOT(BsonField.Name("loc"))),
        $reduce($ReduceF.reduceNOP, ListMap()))

      val expected = chain[Workflow](
        readZips,
        $simpleMap(
          (FlatExpr(JsFn(Name("x"), Select(ident("x"), "loc"))):CardinalExpr[JsFn]).wrapNel,
          ListMap()),
        $reduce($ReduceF.reduceNOP, ListMap()))

      crystallize(given) must beTree(Crystallized(expected))
    }

    "patch $FoldLeftF" in {
      val given = $foldLeft[WorkflowF](readZips, readZips)

      val expected = $foldLeft(
        chain[Workflow](readZips, $project(Reshape(ListMap(
          BsonField.Name("value") -> \/-($$ROOT))),
          IncludeId)),
        chain[Workflow](readZips, $reduce($ReduceF.reduceFoldLeft, ListMap())))

      crystallize(given) must beTree(Crystallized(expected))
    }

    "patch $FoldLeftF with existing reduce" in {
      val given = $foldLeft[WorkflowF](
        readZips,
        chain[Workflow](readZips, $reduce($ReduceF.reduceNOP, ListMap())))

      val expected = $foldLeft(
        chain[Workflow](
          readZips,
          $project(Reshape(ListMap(
            BsonField.Name("value") -> \/-($$ROOT))),
            IncludeId)),
        chain[Workflow](readZips, $reduce($ReduceF.reduceNOP, ListMap())))

      crystallize(given) must beTree(Crystallized(expected))
    }

    "avoid dangling map with known shape" in {
      crystallize(chain[Workflow](
        $read(collection("db", "zips")),
        $simpleMap(
          NonEmptyList(MapExpr(JsFn(Name("x"), obj(
            "first" -> Select(ident("x"), "pop"),
            "second" -> Select(ident("x"), "city"))))),
          ListMap()))) must
      beTree(Crystallized(chain[Workflow](
        $read(collection("db", "zips")),
        $simpleMap(
          NonEmptyList(MapExpr(JsFn(Name("x"), obj(
            "first" -> Select(ident("x"), "pop"),
            "second" -> Select(ident("x"), "city"))))),
          ListMap()),
        $project(Reshape(ListMap(
          BsonField.Name("first") -> \/-($include()),
          BsonField.Name("second") -> \/-($include()))),
          IgnoreId))))
    }

    "avoid dangling flatMap with known shape" in {
      crystallize(chain[Workflow](
        $read(collection("db", "zips")),
        $simpleMap(
          NonEmptyList(
            MapExpr(JsFn(Name("x"), obj(
              "first"  -> Select(ident("x"), "loc"),
              "second" -> ident("x")))),
            FlatExpr(JsFn(Name("x"), Select(ident("x"), "city")))),
          ListMap()))) must
      beTree(Crystallized(chain[Workflow](
        $read(collection("db", "zips")),
        $simpleMap(
          NonEmptyList(
            MapExpr(JsFn(Name("x"), obj(
              "first"  -> Select(ident("x"), "loc"),
              "second" -> ident("x")))),
            FlatExpr(JsFn(Name("x"), Select(ident("x"), "city")))),
          ListMap()),
        $project(Reshape(ListMap(
          BsonField.Name("first") -> \/-($include()),
          BsonField.Name("second") -> \/-($include()))),
          IgnoreId))))
    }

    "fold unwind into SimpleMap" in {
      crystallize(chain[Workflow](
        $read(collection("db", "zips")),
        $unwind(DocField(BsonField.Name("loc"))),
        $simpleMap(
          NonEmptyList(MapExpr(JsFn(Name("x"), obj("0" -> Select(ident("x"), "loc"))))),
          ListMap()))) must
      beTree(Crystallized(chain[Workflow](
        $read(collection("db", "zips")),
        $simpleMap(
          NonEmptyList(
            FlatExpr(JsFn(Name("x"), Select(ident("x"), "loc"))),
            MapExpr(JsFn(Name("x"), obj("0" -> Select(ident("x"), "loc"))))),
          ListMap()),
        $project(
          Reshape(ListMap(
            BsonField.Name("0") -> \/-($include()))),
          IgnoreId))))
    }

    "not fold unwind into SimpleMap with preceding pipeline op" in {
      crystallize(chain[Workflow](
        $read(collection("db", "zips")),
        $project(Reshape(ListMap(
            BsonField.Name("loc") -> \/-($var(DocField(BsonField.Name("loc")))))),
          IgnoreId),
        $unwind(DocField(BsonField.Name("loc"))),
        $simpleMap(
          NonEmptyList(MapExpr(JsFn(Name("x"), obj("0" -> Select(ident("x"), "loc"))))),
          ListMap()))) must
        beTree(Crystallized(chain[Workflow](
          $read(collection("db", "zips")),
          $project(Reshape(ListMap(
              BsonField.Name("loc") -> \/-($field("loc")))),
            IgnoreId),
          $unwind(DocField(BsonField.Name("loc"))),
          $simpleMap(
            NonEmptyList(
              MapExpr(JsFn(Name("x"), obj("0" -> Select(ident("x"), "loc"))))),
            ListMap()),
          $project(
            Reshape(ListMap(
              BsonField.Name("0") -> \/-($include()))),
            IgnoreId))))
    }

    "fold multiple unwinds into a SimpleMap" in {
      crystallize(chain[Workflow](
        $read(collection("db", "foo")),
        $unwind(DocField(BsonField.Name("bar"))),
        $unwind(DocField(BsonField.Name("baz"))),
        $simpleMap(
          NonEmptyList(MapExpr(JsFn(Name("x"),
            obj(
              "0" -> Select(ident("x"), "bar"),
              "1" -> Select(ident("x"), "baz"))))),
          ListMap()))) must
      beTree(Crystallized(chain[Workflow](
        $read(collection("db", "foo")),
        $simpleMap(
          NonEmptyList(
            FlatExpr(JsFn(Name("x"), Select(ident("x"), "bar"))),
            FlatExpr(JsFn(Name("x"), Select(ident("x"), "baz"))),
            MapExpr(JsFn(Name("x"), obj(
              "0" -> Select(ident("x"), "bar"),
              "1" -> Select(ident("x"), "baz"))))),
          ListMap()),
        $project(
          Reshape(ListMap(
            BsonField.Name("0") -> \/-($include()),
            BsonField.Name("1") -> \/-($include()))),
          IgnoreId))))
    }

    "not fold multiple unwinds into SimpleMap with preceding pipeline op" in {
      crystallize(chain[Workflow](
        $read(collection("db", "foo")),
        $project(Reshape(ListMap(
            BsonField.Name("loc") -> \/-($var(DocField(BsonField.Name("loc")))))),
          IgnoreId),
        $unwind(DocField(BsonField.Name("bar"))),
        $unwind(DocField(BsonField.Name("baz"))),
        $simpleMap(
          NonEmptyList(MapExpr(JsFn(Name("x"),
            obj(
              "0" -> Select(ident("x"), "bar"),
              "1" -> Select(ident("x"), "baz"))))),
          ListMap()))) must
      beTree(Crystallized(chain[Workflow](
        $read(collection("db", "foo")),
        $project(Reshape(ListMap(
            BsonField.Name("loc") -> \/-($field("loc")))),
          IgnoreId),
        $unwind(DocField(BsonField.Name("bar"))),
        $unwind(DocField(BsonField.Name("baz"))),
        $simpleMap(
          NonEmptyList(
            MapExpr(JsFn(Name("x"), obj(
              "0" -> Select(ident("x"), "bar"),
              "1" -> Select(ident("x"), "baz"))))),
          ListMap()),
        $project(
          Reshape(ListMap(
            BsonField.Name("0") -> \/-($include()),
            BsonField.Name("1") -> \/-($include()))),
          IgnoreId))))
    }
  }

  "task" should {
    import quasar.physical.mongodb.workflowtask._
    import quasar.jscore._

    "convert $match with $where into map/reduce" in {
      task(crystallize(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.Where(Js.BinOp("<",
          Js.Select(Js.Select(Js.Ident("this"), "city"), "length"),
          Js.Num(4, false))))))) must
      beTree[WorkflowTask](
        MapReduceTask(
          ReadTask(collection("db", "zips")),
          MapReduce($MapF.mapFn($MapF.mapNOP), $ReduceF.reduceNOP,
            selection = Some(Selector.Where(Js.BinOp("<",
              Js.Select(Js.Select(Js.Ident("this"), "city"), "length"),
              Js.Num(4, false))))),
          None))
    }

    "always pipeline unconverted aggregation ops" in {
      // Tricky: don't want to actually finalize here, just testing `task` behavior
      task(Crystallized(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("__sd_tmp_1") -> $push($field("lEft")))),
          \/-($literal(Bson.Int32(1)))),
        $project(Reshape(ListMap(
          BsonField.Name("a")      -> \/-($include()),
          BsonField.Name("b")      -> \/-($include()),
          BsonField.Name("equal?") -> \/-($eq($field("a"), $field("b"))))),
          IncludeId),
        $match(Selector.Doc(
          BsonField.Name("equal?") -> Selector.Eq(Bson.Bool(true)))),
        $sort(NonEmptyList(BsonField.Name("a") -> SortDir.Descending)),
        $limit(100),
        $skip(5),
        $project(Reshape(ListMap(
          BsonField.Name("a") -> \/-($include()),
          BsonField.Name("b") -> \/-($include()))),
          IncludeId)))) must
      beTree[WorkflowTask](
        PipelineTask(ReadTask(collection("db", "zips")),
          List(
            $GroupF((),
              Grouped(ListMap(
                BsonField.Name("__sd_tmp_1") -> $push($field("lEft")))),
              \/-($literal(Bson.Null))).pipeline,
            $ProjectF((),
              Reshape(ListMap(
                BsonField.Name("a")      -> \/-($include()),
                BsonField.Name("b")      -> \/-($include()),
                BsonField.Name("equal?") -> \/-($eq($field("a"), $field("b"))))),
              IncludeId).pipeline,
            $MatchF((),
              Selector.Doc(
                BsonField.Name("equal?") -> Selector.Eq(Bson.Bool(true)))).shapePreserving,
            $SortF((), NonEmptyList(BsonField.Name("a") -> SortDir.Descending)).shapePreserving,
            $LimitF((), 100).shapePreserving,
            $SkipF((), 5).shapePreserving,
            $ProjectF((),
              Reshape(ListMap(
                BsonField.Name("a") -> \/-($include()),
                BsonField.Name("b") -> \/-($include()))),
              IncludeId).pipeline)
          .map(PipelineOp(_))))
    }

    "create maximal map/reduce" in {
      task(crystallize(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.Doc(
          BsonField.Name("loc") \ BsonField.Name("0") ->
            Selector.Lt(Bson.Int64(-73)))),
        $sort(NonEmptyList(BsonField.Name("city") -> SortDir.Descending)),
        $limit(100),
        $map($MapF.mapMap("value",
          Js.Access(Js.Ident("value"), Js.Num(0, false))),
          ListMap()),
        $reduce($ReduceF.reduceFoldLeft, ListMap()),
        $simpleMap(NonEmptyList(MapExpr(JsFn.identity)), ListMap())))) must
      beTree[WorkflowTask](
        MapReduceTask(
          ReadTask(collection("db", "zips")),
          MapReduce(
            $MapF.mapFn($MapF.mapMap("value",
              Js.Access(Js.Ident("value"), Js.Num(0, false)))),
            $ReduceF.reduceFoldLeft,
            selection = Some(Selector.Doc(
              BsonField.Name("loc") \ BsonField.Name("0") ->
                Selector.Lt(Bson.Int64(-73)))),
            inputSort =
              Some(NonEmptyList(BsonField.Name("city") -> SortDir.Descending)),
            limit = Some(100),
            finalizer = Some($MapF.finalizerFn(JsFn.identity))),
          None))
    }

    "create maximal map/reduce with flatMap" in {
      task(crystallize(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.Doc(
          BsonField.Name("loc") \ BsonField.Name("0") ->
            Selector.Lt(Bson.Int64(-73)))),
        $sort(NonEmptyList(BsonField.Name("city") -> SortDir.Descending)),
        $limit(100),
        $flatMap(Js.AnonFunDecl(List("key", "value"), List(
          Js.AnonElem(List(
            Js.AnonElem(List(Js.Ident("key"), Js.Ident("value"))))))),
          ListMap()),
        $reduce($ReduceF.reduceFoldLeft, ListMap()),
        $simpleMap(NonEmptyList(MapExpr(JsFn.identity)), ListMap())))) must
      beTree[WorkflowTask](
        MapReduceTask(
          ReadTask(collection("db", "zips")),
          MapReduce(
            $FlatMapF.mapFn(Js.AnonFunDecl(List("key", "value"), List(
              Js.AnonElem(List(
                Js.AnonElem(List(Js.Ident("key"), Js.Ident("value")))))))),
            $ReduceF.reduceFoldLeft,
            selection = Some(Selector.Doc(
              BsonField.Name("loc") \ BsonField.Name("0") ->
                Selector.Lt(Bson.Int64(-73)))),
            inputSort =
              Some(NonEmptyList(BsonField.Name("city") -> SortDir.Descending)),
            limit = Some(100),
            finalizer = Some($MapF.finalizerFn(JsFn.identity))),
          None))
    }

    "create map/reduce without map" in {
      task(crystallize(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.Doc(
          BsonField.Name("loc") \ BsonField.Name("0") ->
            Selector.Lt(Bson.Int64(-73)))),
        $sort(NonEmptyList(BsonField.Name("city") -> SortDir.Descending)),
        $limit(100),
        $reduce($ReduceF.reduceFoldLeft, ListMap()),
        $simpleMap(NonEmptyList(MapExpr(JsFn.identity)), ListMap())))) must
      beTree[WorkflowTask](
        MapReduceTask(
          ReadTask(collection("db", "zips")),
          MapReduce(
            $MapF.mapFn($MapF.mapNOP),
            $ReduceF.reduceFoldLeft,
            selection = Some(Selector.Doc(
              BsonField.Name("loc") \ BsonField.Name("0") ->
                Selector.Lt(Bson.Int64(-73)))),
            inputSort =
              Some(NonEmptyList(BsonField.Name("city") -> SortDir.Descending)),
            limit = Some(100),
            finalizer = Some($MapF.finalizerFn(JsFn.identity))),
          None))
    }

    "fold unwind into SimpleMap (when finalize is used)" in {
      task(crystallize(chain[Workflow](
        $read(collection("db", "zips")),
        $unwind(DocField(BsonField.Name("loc"))),
        $simpleMap(
          NonEmptyList(MapExpr(JsFn(Name("x"), obj("0" -> Select(ident("x"), "loc"))))),
          ListMap())))) must
      beTree[WorkflowTask](
        PipelineTask(
          MapReduceTask(
            ReadTask(collection("db", "zips")),
            MapReduce(
              Js.AnonFunDecl(Nil, List(
                Js.Call(
                  Js.Select(
                    Js.Call(
                      Js.AnonFunDecl(List("key", "value"), List(
                        Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
                        Js.ForIn(Js.Ident("elem"), Select(ident("value"), "loc").toJs,
                          Js.Block(List(
                            Js.VarDef(List("each0" -> Js.Call(Js.Ident("clone"), List(Js.Ident("value"))))),
                            unsafeAssign(Select(ident("each0"), "loc"), Access(Select(ident("value"), "loc"), ident("elem"))),
                            Js.Block(List(
                              Js.VarDef(List("each1" ->
                                obj("0" -> Select(ident("each0"), "loc")).toJs)),
                              Js.Call(Js.Select(Js.Ident("rez"), "push"), List(
                                Js.AnonElem(List(
                                  Js.Call(Js.Ident("ObjectId"), Nil),
                                  Js.Ident("each1")))))))))),
                        Js.Return(Js.Ident("rez")))),
                      List(Js.Select(Js.This, IdLabel), Js.This)),
                    "map"),
                  List(
                    Js.AnonFunDecl(List("__rez"), List(
                      Js.Call(Js.Select(Js.Ident("emit"), "apply"), List(Js.Null, Js.Ident("__rez"))))))))),
              $ReduceF.reduceNOP,
              scope = $SimpleMapF.implicitScope(Set("clone"))),
            None),
          List(
            PipelineOp($ProjectF((),
              Reshape(ListMap(
                BsonField.Name("0") -> \/-($field("value", "0")))),
              IgnoreId).pipeline))))
    }

    "fold multiple unwinds into SimpleMap (when finalize is used)" in {
      task(crystallize(chain[Workflow](
        $read(collection("db", "foo")),
        $unwind(DocField(BsonField.Name("bar"))),
        $unwind(DocField(BsonField.Name("baz"))),
        $simpleMap(
          NonEmptyList(MapExpr(JsFn(Name("x"),
            obj(
              "0" -> Select(ident("x"), "bar"),
              "1" -> Select(ident("x"), "baz"))))),
          ListMap())))) must
      beTree[WorkflowTask](
        PipelineTask(
          MapReduceTask(
            ReadTask(collection("db", "foo")),
            MapReduce(
              Js.AnonFunDecl(Nil, List(
                Js.Call(
                  Js.Select(
                    Js.Call(
                      Js.AnonFunDecl(List("key", "value"), List(
                        Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
                        Js.ForIn(Js.Ident("elem"), Select(ident("value"), "bar").toJs,
                          Js.Block(List(
                            Js.VarDef(List("each0" -> Js.Call(Js.Ident("clone"), List(Js.Ident("value"))))),
                            unsafeAssign(Select(ident("each0"), "bar"), Access(Select(ident("value"), "bar"), ident("elem"))),
                            Js.ForIn(Js.Ident("elem"), Select(ident("each0"), "baz").toJs,
                              Js.Block(List(
                                Js.VarDef(List("each1" -> Js.Call(Js.Ident("clone"), List(Js.Ident("each0"))))),
                                unsafeAssign(Select(ident("each1"), "baz"), Access(Select(ident("each0"), "baz"), ident("elem"))),
                                Js.Block(List(
                                  Js.VarDef(List("each2" ->
                                    obj(
                                      "0" -> Select(ident("each1"), "bar"),
                                      "1" -> Select(ident("each1"), "baz")).toJs)),
                                  Js.Call(Js.Select(Js.Ident("rez"), "push"), List(
                                    Js.AnonElem(List(
                                      Js.Call(Js.Ident("ObjectId"), Nil),
                                      Js.Ident("each2"))))))))))))),
                        Js.Return(Js.Ident("rez")))),
                      List(Js.Select(Js.This, IdLabel), Js.This)),
                    "map"),
                  List(
                    Js.AnonFunDecl(List("__rez"), List(
                      Js.Call(Js.Select(Js.Ident("emit"), "apply"), List(Js.Null, Js.Ident("__rez"))))))))),
              $ReduceF.reduceNOP,
              scope = $SimpleMapF.implicitScope(Set("clone"))),
            None),
          List(
            PipelineOp($ProjectF((),
              Reshape(ListMap(
                BsonField.Name("0") -> \/-($field("value", "0")),
                BsonField.Name("1") -> \/-($field("value", "1")))),
              IgnoreId).pipeline))))
    }
  }

  "SimpleMap" should {
    import quasar.jscore._

    "raw" should {
      "extract one" in {
        val op = $SimpleMapF((),
          NonEmptyList(MapExpr(JsFn(Name("x"), Select(ident("x"), "foo")))),
          ListMap())
        (op.raw match {
          case $MapF(_, fn, _) =>
            fn.pprint(0) must_== "function (key, value) { return [key, value.foo] }"
          case _ => failure
        }): org.specs2.execute.Result
      }

      "flatten one" in {
        val op = $SimpleMapF((),
          NonEmptyList(FlatExpr(JsFn(Name("x"), Select(ident("x"), "foo")))),
          ListMap())
        (op.raw match {
          case $FlatMapF(_, fn, _) =>
            fn.pprint(0) must_==
              """function (key, value) {
                |  var rez = [];
                |  for (var elem in (value.foo)) {
                |    var each0 = clone(value);
                |    each0.foo = value.foo[elem];
                |    rez.push([ObjectId(), each0])
                |  };
                |  return rez
                |}""".stripMargin
          case _ => failure
        }): org.specs2.execute.Result
      }
    }
  }

  "$redact" should {

    "render result variables" in {
      $RedactF.DESCEND.bson must_== Bson.Text("$$DESCEND")
      $RedactF.PRUNE.bson   must_== Bson.Text("$$PRUNE")
      $RedactF.KEEP.bson    must_== Bson.Text("$$KEEP")
    }
  }

  "RenderTree[Workflow]" should {
    def render(op: Workflow)(implicit RO: RenderTree[Workflow]): String = RO.render(op).draw.mkString("\n")

    "render read" in {
      render(readFoo) must_== "$ReadF(db; foo)"
    }

    "render simple project" in {
      val op = chain[Workflow](readFoo,
        $project(
          Reshape(ListMap(
            BsonField.Name("bar") -> \/-($field("baz")))),
          IncludeId))

      render(op) must_==
        """Chain
          |├─ $ReadF(db; foo)
          |╰─ $ProjectF
          |   ├─ Name("bar" -> "$baz")
          |   ╰─ IncludeId""".stripMargin
    }

    "render nested project" in {
      val op = chain[Workflow](readFoo,
        $project(
          Reshape(ListMap(
            BsonField.Name("bar") -> -\/(Reshape(ListMap(
              BsonField.Name("0") -> \/-($field("baz"))))))),
          IncludeId))

      render(op) must_==
        """Chain
          |├─ $ReadF(db; foo)
          |╰─ $ProjectF
          |   ├─ Name("bar")
          |   │  ╰─ Name("0" -> "$baz")
          |   ╰─ IncludeId""".stripMargin
    }

    "render map/reduce ops" in {
      val op = chain[Workflow](readFoo,
        $map(Js.AnonFunDecl(List("key"), Nil), ListMap()),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> \/-($field("baz")))),
          IncludeId),
        $flatMap(Js.AnonFunDecl(List("key"), Nil), ListMap()),
        $reduce(
          Js.AnonFunDecl(List("key", "values"),
            List(Js.Return(Js.Access(Js.Ident("values"), Js.Num(1, false))))),
          ListMap()))

      render(op) must_==
        """Chain
          |├─ $ReadF(db; foo)
          |├─ $MapF
          |│  ├─ JavaScript(function (key) {})
          |│  ╰─ Scope(Map())
          |├─ $ProjectF
          |│  ├─ Name("bar" -> "$baz")
          |│  ╰─ IncludeId
          |├─ $FlatMapF
          |│  ├─ JavaScript(function (key) {})
          |│  ╰─ Scope(Map())
          |╰─ $ReduceF
          |   ├─ JavaScript(function (key, values) { return values[1] })
          |   ╰─ Scope(Map())""".stripMargin
    }

    "render unchained" in {
      val op =
        $foldLeft(
          chain[Workflow](readFoo,
            $project(Reshape(ListMap(
              BsonField.Name("bar") -> \/-($field("baz")))),
              IncludeId)),
          chain[Workflow](readFoo,
            $map(Js.AnonFunDecl(List("key"), Nil), ListMap()),
            $reduce($ReduceF.reduceNOP, ListMap())))

      render(op) must_==
      """$FoldLeftF
        |├─ Chain
        |│  ├─ $ReadF(db; foo)
        |│  ╰─ $ProjectF
        |│     ├─ Name("bar" -> "$baz")
        |│     ╰─ IncludeId
        |╰─ Chain
        |   ├─ $ReadF(db; foo)
        |   ├─ $MapF
        |   │  ├─ JavaScript(function (key) {})
        |   │  ╰─ Scope(Map())
        |   ╰─ $ReduceF
        |      ├─ JavaScript(function (key, values) { return values[0] })
        |      ╰─ Scope(Map())""".stripMargin
    }
  }
}
