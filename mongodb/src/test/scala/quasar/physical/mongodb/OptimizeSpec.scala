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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.TreeMatchers
import quasar.common.SortDir
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.optimize.pipeline._
import quasar.physical.mongodb.selector.Selector
import quasar.physical.mongodb.workflow._

import scalaz._, Scalaz._

class OptimizeSpecs extends quasar.Qspec with TreeMatchers {
  import CollectionUtil._
  import fixExprOp._

  "simplifyGroup" should {
    "elide useless reduction" in {
      val op =
        chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("city") -> $last($field("city")))),
            $field("city").right))

      val exp =
        chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            Grouped(ListMap()),
            $field("city").right),
          $project(
            Reshape(ListMap(
              BsonField.Name("city") -> $field("_id").right)),
            IgnoreId))

      simplifyGroup[WorkflowF](op) must beTree(exp)
    }

    "elide useless reduction with complex id" in {
      val op =
        chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("city") -> $max($field("city")))),
            Reshape(ListMap(
              BsonField.Name("0") -> $field("city").right,
              BsonField.Name("1") -> $field("state").right)).left))

      val exp =
        chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            Grouped(ListMap()),
            Reshape(ListMap(
              BsonField.Name("0") -> $field("city").right,
              BsonField.Name("1") -> $field("state").right)).left),
          $project(
            Reshape(ListMap(
              BsonField.Name("city") -> $field("_id", "0").right)),
            IgnoreId))

      simplifyGroup[WorkflowF](op) must beTree(exp)
    }

    "preserve useless-but-array-creating reduction" in {
      val op = chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("city") -> $push($field("city")))),
          Reshape(ListMap(
            BsonField.Name("0") -> $field("city").right)).left))

      simplifyGroup[WorkflowF](op) must beTree(op)
    }
  }

  "reorder" should {
    "push $skip before $project" in {
      val op = chain[Workflow](
       $read(collection("db", "zips")),
       $project(
         Reshape(ListMap(
           BsonField.Name("0") -> \/-($toLower($var(DocField(BsonField.Name("city"))))))),
         IgnoreId),
       $skip(5))
     val exp = chain[Workflow](
      $read(collection("db", "zips")),
      $skip(5),
      $project(
        Reshape(ListMap(
          BsonField.Name("0") -> \/-($toLower($var(DocField(BsonField.Name("city"))))))),
        IgnoreId))

      reorderOps(op) must beTree(exp)
    }

    "push $skip before $simpleMap" in {
      import quasar.jscore._

      val op = chain[Workflow](
       $read(collection("db", "zips")),
       $simpleMap(
         NonEmptyList(MapExpr(JsFn(Name("x"), obj(
           "0" -> Select(ident("x"), "length"))))),
         ListMap()),
       $skip(5))
     val exp = chain[Workflow](
      $read(collection("db", "zips")),
      $skip(5),
      $simpleMap(
        NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "0" -> Select(ident("x"), "length"))))),
        ListMap()))

      reorderOps(op) must beTree(exp)
    }

    "not push $skip before flattening $simpleMap" in {
      import quasar.jscore._

      val op = chain[Workflow](
       $read(collection("db", "zips")),
       $simpleMap(
         NonEmptyList(
           MapExpr(JsFn(Name("x"), obj(
             "0" -> Select(ident("x"), "length")))),
           FlatExpr(JsFn(Name("x"), Select(ident("x"), "loc")))),
         ListMap()),
       $skip(5))

      reorderOps(op) must beTree(op)
    }

    "push $limit before $project" in {
      val op = chain[Workflow](
       $read(collection("db", "zips")),
       $project(
         Reshape(ListMap(
           BsonField.Name("0") -> \/-($toLower($var(DocField(BsonField.Name("city"))))))),
         IgnoreId),
       $limit(10))
     val exp = chain[Workflow](
      $read(collection("db", "zips")),
      $limit(10),
      $project(
        Reshape(ListMap(
          BsonField.Name("0") -> \/-($toLower($var(DocField(BsonField.Name("city"))))))),
        IgnoreId))

      reorderOps(op) must beTree(exp)
    }

    "push $limit before $simpleMap" in {
      import quasar.jscore._

      val op = chain[Workflow](
       $read(collection("db", "zips")),
       $simpleMap(
         NonEmptyList(MapExpr(JsFn(Name("x"), obj(
           "0" -> Select(ident("x"), "length"))))),
         ListMap()),
       $limit(10))
     val exp = chain[Workflow](
      $read(collection("db", "zips")),
      $limit(10),
      $simpleMap(
        NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "0" -> Select(ident("x"), "length"))))),
        ListMap()))

      reorderOps(op) must beTree(exp)
    }

    "not push $limit before flattening $simpleMap" in {
      import quasar.jscore._

      val op = chain[Workflow](
       $read(collection("db", "zips")),
       $simpleMap(
         NonEmptyList(
           MapExpr(JsFn(Name("x"), obj(
             "0" -> Select(ident("x"), "length")))),
           FlatExpr(JsFn(Name("x"), Select(ident("x"), "loc")))),
         ListMap()),
       $limit(10))

      reorderOps(op) must beTree(op)
    }

    "push $match before $project" in {
      val op = chain[Workflow](
       $read(collection("db", "zips")),
       $project(
         Reshape(ListMap(
           BsonField.Name("city") -> \/-($var(DocField(BsonField.Name("address") \ BsonField.Name("city")))))),
         IgnoreId),
       $match(Selector.Doc(
         BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER")))))
     val exp = chain[Workflow](
      $read(collection("db", "zips")),
      $match(Selector.Doc(
        (BsonField.Name("address") \ BsonField.Name("city")) -> Selector.Eq(Bson.Text("BOULDER")))),
      $project(
        Reshape(ListMap(
          BsonField.Name("city") -> \/-($var(DocField(BsonField.Name("address") \ BsonField.Name("city")))))),
        IgnoreId))

      reorderOps(op) must beTree(exp)
    }

    "push $match before $project with deep reference" in {
      val op = chain[Workflow](
       $read(collection("db", "zips")),
       $project(
         Reshape(ListMap(
           BsonField.Name("__tmp0") -> \/-($var(DocField(BsonField.Name("address")))))),
         IgnoreId),
       $match(Selector.Doc(
         BsonField.Name("__tmp0") \ BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER")))))
     val exp = chain[Workflow](
      $read(collection("db", "zips")),
      $match(Selector.Doc(
        (BsonField.Name("address") \ BsonField.Name("city")) -> Selector.Eq(Bson.Text("BOULDER")))),
      $project(
        Reshape(ListMap(
          BsonField.Name("__tmp0") -> \/-($var(DocField(BsonField.Name("address")))))),
        IgnoreId))

      reorderOps(op) must beTree(exp)
    }

    "not push $match before $project with dependency" in {
      val op = chain[Workflow](
       $read(collection("db", "zips")),
       $project(
         Reshape(ListMap(
           BsonField.Name("city") -> \/-($var(DocField(BsonField.Name("city")))),
           BsonField.Name("__tmp0") -> \/-($toLower($var(DocField(BsonField.Name("city"))))))),
         IgnoreId),
       $match(Selector.Doc(
         BsonField.Name("__tmp0") -> Selector.Eq(Bson.Text("boulder")))))

      reorderOps(op) must beTree(op)
    }

    "push $match before $simpleMap" in {
      import quasar.jscore._

      val op = chain[Workflow](
       $read(collection("db", "zips")),
       $simpleMap(
         NonEmptyList(MapExpr(JsFn(Name("x"), obj(
           "__tmp0" -> ident("x"),
           "city" -> Select(ident("x"), "city"))))),
         ListMap()),
       $match(Selector.Doc(
         BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER")))))
     val exp = chain[Workflow](
      $read(collection("db", "zips")),
      $match(Selector.Doc(
        (BsonField.Name("city")) -> Selector.Eq(Bson.Text("BOULDER")))),
      $simpleMap(
        NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "__tmp0" -> ident("x"),
          "city" -> Select(ident("x"), "city"))))),
        ListMap()))

      reorderOps(op) must beTree(exp)
    }

    "push $match with deep reference before $simpleMap" in {
      import quasar.jscore._

      val op = chain[Workflow](
       $read(collection("db", "zips")),
       $simpleMap(
         NonEmptyList(MapExpr(JsFn(Name("x"), obj(
           "__tmp0" -> ident("x"),
           "pop" -> Select(ident("x"), "pop"))))),
         ListMap()),
       $match(Selector.Doc(
         BsonField.Name("__tmp0") \ BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER")))))
     val exp = chain[Workflow](
      $read(collection("db", "zips")),
      $match(Selector.Doc(
        (BsonField.Name("city")) -> Selector.Eq(Bson.Text("BOULDER")))),
      $simpleMap(
        NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "__tmp0" -> ident("x"),
          "pop" -> Select(ident("x"), "pop"))))),
        ListMap()))

      reorderOps(op) must beTree(exp)
    }

    "push $match before splicing $simpleMap" in {
      import quasar.jscore._

      val op = chain[Workflow](
       $read(collection("db", "zips")),
       $simpleMap(
         NonEmptyList(
           MapExpr(JsFn(Name("x"),
             SpliceObjects(List(
               ident("x"),
               obj(
                 "city" -> Select(ident("x"), "city"))))))),
         ListMap()),
       $match(Selector.Doc(
         BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER")))))
     val exp = chain[Workflow](
      $read(collection("db", "zips")),
      $match(Selector.Doc(
        (BsonField.Name("city")) -> Selector.Eq(Bson.Text("BOULDER")))),
      $simpleMap(
        NonEmptyList(MapExpr(JsFn(Name("x"),
          SpliceObjects(List(
            ident("x"),
            obj(
              "city" -> Select(ident("x"), "city"))))))),
        ListMap()))

      reorderOps(op) must beTree(exp)
    }

    "not push $match before $simpleMap with dependency" in {
      import quasar.jscore._

      val op = chain[Workflow](
       $read(collection("db", "zips")),
       $simpleMap(
         NonEmptyList(MapExpr(JsFn(Name("x"), obj(
           "__tmp0" -> ident("x"),
           "city" -> Select(ident("x"), "city"),
           "__sd_tmp_0" -> Select(Select(ident("x"), "city"), "length"))))),
         ListMap()),
       $match(Selector.Doc(
         BsonField.Name("__sd_tmp_0") -> Selector.Lt(Bson.Int32(1000)))))

      reorderOps(op) must beTree(op)
    }

    "not push $match before flattening $simpleMap" in {
      import quasar.jscore._

      val op = chain[Workflow](
       $read(collection("db", "zips")),
       $simpleMap(
         NonEmptyList(
           MapExpr(JsFn(Name("x"), obj(
             "city" -> Select(Select(ident("x"), "__tmp0"), "city")))),
           FlatExpr(JsFn(Name("x"), Select(ident("x"), "loc")))),
         ListMap()),
       $match(Selector.Doc(
         BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER")))))

      reorderOps(op) must beTree(op)
    }

    "not push $sort up" in {
      val op = chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          Reshape(ListMap(
            BsonField.Name("city") -> \/-($var(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city")))))),
          IgnoreId),
        $sort(NonEmptyList(BsonField.Name("city") -> SortDir.Ascending)))

      reorderOps(op) must beTree(op)
    }
  }

  "inline" should {
    "inline simple project on group" in {
      val inlined = inlineProjectGroup(
        Reshape(ListMap(
          BsonField.Name("foo") -> \/-($var(DocField(BsonField.Name("value")))))),
        Grouped(ListMap(BsonField.Name("value") -> $sum($literal(Bson.Int32(1))))))

      inlined must beSome(Grouped(ListMap(BsonField.Name("foo") -> $sum($literal(Bson.Int32(1))))))
    }

    "inline multiple projects on group, dropping extras" in {
        val inlined = inlineProjectGroup(
          Reshape(ListMap(
            BsonField.Name("foo") -> \/-($var(DocField(BsonField.Name("__sd_tmp_1")))),
            BsonField.Name("bar") -> \/-($var(DocField(BsonField.Name("__sd_tmp_2")))))),
          Grouped(ListMap(
            BsonField.Name("__sd_tmp_1") -> $sum($literal(Bson.Int32(1))),
            BsonField.Name("__sd_tmp_2") -> $sum($literal(Bson.Int32(2))),
            BsonField.Name("__sd_tmp_3") -> $sum($literal(Bson.Int32(3))))))

          inlined must beSome(Grouped(ListMap(
            BsonField.Name("foo") -> $sum($literal(Bson.Int32(1))),
            BsonField.Name("bar") -> $sum($literal(Bson.Int32(2))))))
    }

    "inline project on group with nesting" in {
        val inlined = inlineProjectGroup(
          Reshape(ListMap(
            BsonField.Name("bar") -> \/-($var(DocField(BsonField.Name("value") \ BsonField.Name("bar")))),
            BsonField.Name("baz") -> \/-($var(DocField(BsonField.Name("value") \ BsonField.Name("baz")))))),
          Grouped(ListMap(
            BsonField.Name("value") -> $push($var(DocField(BsonField.Name("foo")))))))

          inlined must beNone
    }
  }
}
