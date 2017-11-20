/*
 * Copyright 2014–2017 SlamData Inc.
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
import quasar._
import quasar.common.{Map => _, _}
import quasar.contrib.specs2.PendingWithActualTracking
import quasar.fs._
import quasar.javascript._
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner._
import quasar.physical.mongodb.workflow._
import quasar.sql._

import scala.Either

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

class PlannerSpec extends
    PlannerWorkflowHelpers with
    PendingWithActualTracking {

  import Grouped.grouped
  import Reshape.reshape
  import jscore._
  import CollectionUtil._

  import fixExprOp._
  import PlannerHelpers._, expr3_2Fp._

  def plan(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    PlannerHelpers.plan(query)

  "plan from query string" should {

    "plan filter with both index and field projections" in {
      plan(sqlE"""select count(parents[0].sha) as count from slamengine_commits where parents[0].sha = "56d1caf5d082d1a6840090986e277d36d03f1859" """) must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "slamengine_commits")),
          $match(Selector.Where(
            If(
              BinOp(jscore.And,
                Call(Select(ident("Array"), "isArray"), List(Select(ident("this"), "parents"))),
                BinOp(jscore.And,
                  Call(ident("isObject"), List(
                    Access(
                      Select(ident("this"), "parents"),
                      jscore.Literal(Js.Num(0, false))))),
                  UnOp(jscore.Not,
                    Call(Select(ident("Array"), "isArray"), List(
                      Access(
                        Select(ident("this"), "parents"),
                        jscore.Literal(Js.Num(0, false)))))))),
              BinOp(jscore.Eq,
                Select(
                  Access(
                    Select(ident("this"), "parents"),
                    jscore.Literal(Js.Num(0, false))),
                  "sha"),
                jscore.Literal(Js.Str("56d1caf5d082d1a6840090986e277d36d03f1859"))),
              ident("undefined")).toJs)),
          // NB: This map _looks_ unnecessary, but is actually simpler than the
          //     default impl that would be triggered by the $where selector
          //     above.
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"), obj()))),
            ListMap()),
          $group(
            grouped("count" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null)))))
    }.pendingWithActual(notOnPar, testFile("plan filter with both index and field projections"))

    "plan having with multiple projections" in {
      plan(sqlE"select city, sum(pop) from zips group by city having sum(pop) > 50000") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          grouped(
            "1" ->
              $sum(
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          -\/(reshape("0" -> $field("city")))),
        $match(Selector.Doc(
          BsonField.Name("1") -> Selector.Gt(Bson.Int32(50000)))),
        $project(
          reshape(
            "city" -> $field("_id", "0"),
            "1"    -> $include()),
          IgnoreId)))
    }.pendingWithActual(notOnPar, testFile("plan having with multiple projections"))

    "prefer projection+filter over JS filter" in {
      plan(sqlE"select * from zips where city <> state") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "0" -> $neq($field("city"), $field("state")),
            "src" -> $$ROOT),
          ExcludeId),
        $match(
          Selector.Doc(
            BsonField.Name("0") -> Selector.Eq(Bson.Bool(true)))),
        $project(
          reshape(sigil.Quasar -> $field("src")),
          ExcludeId)))
    }

    "prefer projection+filter over nested JS filter" in {
      plan(sqlE"select * from zips where city <> state and pop < 10000") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.Or(
          Selector.Doc(BsonField.Name("pop") -> Selector.Type(BsonType.Int32)),
          Selector.Doc(BsonField.Name("pop") -> Selector.Type(BsonType.Int64)),
          Selector.Doc(BsonField.Name("pop") -> Selector.Type(BsonType.Dec)),
          Selector.Doc(BsonField.Name("pop") -> Selector.Type(BsonType.Text)),
          Selector.Doc(BsonField.Name("pop") -> Selector.Type(BsonType.Date)),
          Selector.Doc(BsonField.Name("pop") -> Selector.Type(BsonType.Bool))
        )),
        $project(
          reshape(
            "0"   -> $neq($field("city"), $field("state")),
            "1"   -> $field("pop"),
            "src" -> $$ROOT),
          ExcludeId),
        $match(Selector.And(
          Selector.Doc(BsonField.Name("0") -> Selector.Eq(Bson.Bool(true))),
          Selector.Doc(BsonField.Name("1") -> Selector.Lt(Bson.Int32(10000))))),
        $project(
          reshape(sigil.Quasar -> $field("src")),
          ExcludeId)))
    }

    "select partially-applied substring" in {
      plan3_2(sqlE"""select substring("abcdefghijklmnop", 5, trunc(pop / 10000)) from zips""") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $project(
            reshape(
              sigil.Quasar ->
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $cond(
                    $and(
                      $lt($literal(Bson.Null),
                        $cond(
                          $eq($literal(Bson.Int32(10000)), $literal(Bson.Int32(0))),
                          $cond(
                            $eq($field("pop"), $literal(Bson.Int32(0))),
                            $literal(Bson.Dec(Double.NaN)),
                            $cond(
                              $gt($field("pop"), $literal(Bson.Int32(0))),
                              $literal(Bson.Dec(Double.PositiveInfinity)),
                              $literal(Bson.Dec(Double.NegativeInfinity)))
                          ),
                          $divide($field("pop"), $literal(Bson.Int32(10000))))),
                        $lt(
                          $cond(
                            $eq($literal(Bson.Int32(10000)), $literal(Bson.Int32(0))),
                            $cond(
                              $eq($field("pop"), $literal(Bson.Int32(0))),
                              $literal(Bson.Dec(Double.NaN)),
                              $cond(
                                $gt($field("pop"), $literal(Bson.Int32(0))),
                                $literal(Bson.Dec(Double.PositiveInfinity)),
                                $literal(Bson.Dec(Double.NegativeInfinity)))),
                            $divide($field("pop"), $literal(Bson.Int32(10000)))),
                          $literal(Bson.Text(""))
                        )),
                    $substr(
                      $literal(Bson.Text("fghijklmnop")),
                      $literal(Bson.Int32(0)),
                      $trunc(
                        $cond(
                          $eq($literal(Bson.Int32(10000)), $literal(Bson.Int32(0))),
                          $cond(
                            $eq($field("pop"), $literal(Bson.Int32(0))),
                            $literal(Bson.Dec(Double.NaN)),
                            $cond(
                              $gt($field("pop"), $literal(Bson.Int32(0))),
                              $literal(Bson.Dec(Double.PositiveInfinity)),
                              $literal(Bson.Dec(Double.NegativeInfinity)))),
                          $divide($field("pop"), $literal(Bson.Int32(10000)))))),
                  $literal(Bson.Undefined)),
                $literal(Bson.Undefined))))))
    }

    "plan sort with wildcard and expression in key" in {
      plan(sqlE"select * from zips order by pop*10 desc") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("__val"), SpliceObjects(List(
              ident("__val"),
              obj(
                "__sd__0" ->
                  jscore.If(
                    BinOp(jscore.Or,
                      Call(ident("isNumber"), List(Select(ident("__val"), "pop"))),
                      BinOp(jscore.Or,
                        BinOp(Instance, Select(ident("__val"), "pop"), ident("NumberInt")),
                        BinOp(Instance, Select(ident("__val"), "pop"), ident("NumberLong")))),
                    BinOp(Mult, Select(ident("__val"), "pop"), jscore.Literal(Js.Num(10, false))),
                    ident("undefined")))))))),
            ListMap()),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("__val"), obj(
              "__tmp2" ->
                Call(ident("remove"),
                  List(ident("__val"), jscore.Literal(Js.Str("__sd__0")))),
              "__tmp3" -> Select(ident("__val"), "__sd__0"))))),
            ListMap()),
          $sort(NonEmptyList(BsonField.Name("__tmp3") -> SortDir.Descending)),
          $project(
            reshape(sigil.Quasar -> $field("__tmp2")),
            ExcludeId)))
    }.pendingWithActual(notOnPar, testFile("plan sort with wildcard and expression in key"))

    "plan sort with expression and alias" in {
      plan(sqlE"select pop/1000 as popInK from zips order by popInK") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $project(
            reshape(
              "popInK" ->
                $cond(
                  $or(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $and(
                      $lte($literal(Check.minDate), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                  divide($field("pop"), $literal(Bson.Int32(1000))),
                  $literal(Bson.Undefined))),
            IgnoreId),
          $sort(NonEmptyList(BsonField.Name("popInK") -> SortDir.Ascending))))
    }.pendingWithActual(notOnPar, testFile("plan sort with expression and alias")) // at least on agg now

    "plan sort with expression, alias, and filter" in {
      plan(sqlE"select pop/1000 as popInK from zips where pop >= 1000 order by popInK") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.And(
            isNumeric(BsonField.Name("pop")),
            Selector.Doc(BsonField.Name("pop") -> Selector.Gte(Bson.Int32(1000))))),
          $project(
            reshape(
              "popInK" ->
                $cond(
                  $or(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $and(
                      $lte($literal(Check.minDate), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                  divide($field("pop"), $literal(Bson.Int32(1000))),
                  $literal(Bson.Undefined))),
            ExcludeId),
          $sort(NonEmptyList(BsonField.Name("popInK") -> SortDir.Ascending))))
    }.pendingWithActual(notOnPar, testFile("plan sort with expression, alias, and filter")) // at least on agg now

    "plan count and js expr" in {
      plan(sqlE"SELECT COUNT(*) as cnt, LENGTH(city) FROM zips") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
              "1" ->
                If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                  Call(ident("NumberLong"),
                    List(Select(Select(ident("x"), "city"), "length"))),
                  ident("undefined")))))),
              ListMap()),
            $group(
              grouped(
                "cnt" -> $sum($literal(Bson.Int32(1))),
                "1"   -> $push($field("1"))),
              \/-($literal(Bson.Null))),
            $unwind(DocField("1")))
        }
    }.pendingWithActual(notOnPar, testFile("plan count and js expr"))

    "plan useful group by" in {
      plan(sqlE"""select city || ", " || state, sum(pop) from zips group by city, state""") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          grouped(
            "__tmp10" ->
              $first(
                $cond(
                  $or(
                    $and(
                      $lte($literal(Bson.Arr()), $field("city")),
                      $lt($field("city"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                    $and(
                      $lte($literal(Bson.Text("")), $field("city")),
                      $lt($field("city"), $literal(Bson.Doc())))),
                  $field("city"),
                  $literal(Bson.Undefined))),
            "__tmp11" ->
              $first(
                $cond(
                  $or(
                    $and(
                      $lte($literal(Bson.Arr()), $field("state")),
                      $lt($field("state"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                    $and(
                      $lte($literal(Bson.Text("")), $field("state")),
                      $lt($field("state"), $literal(Bson.Doc())))),
                  $field("state"),
                  $literal(Bson.Undefined))),
            "1" ->
              $sum(
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          -\/(reshape(
            "0" -> $field("city"),
            "1" -> $field("state")))),
        $project(
          reshape(
            "0" ->
              $concat(
                $concat($field("__tmp10"), $literal(Bson.Text(", "))),
                $field("__tmp11")),
            "1" -> $field("1")),
          IgnoreId)))
    }.pendingWithActual(notOnPar, testFile("plan useful group by"))

    "plan group by expression" in {
      plan(sqlE"select city, sum(pop) from zips group by lower(city)") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          grouped(
            "city" -> $push($field("city")),
            "1" ->
              $sum(
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          -\/(reshape(
            "0" ->
              $cond(
                $and(
                  $lte($literal(Bson.Text("")), $field("city")),
                  $lt($field("city"), $literal(Bson.Doc()))),
                $toLower($field("city")),
                $literal(Bson.Undefined))))),
        $unwind(DocField(BsonField.Name("city")))))
    }.pendingWithActual(notOnPar, testFile("plan group by expression"))

    "plan group by month" in {
      plan(sqlE"""select avg(score) as a, DATE_PART("month", `date`) as m from caloriesBurnedData group by DATE_PART("month", `date`)""") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "caloriesBurnedData")),
          $group(
            grouped(
              "a" ->
                $avg(
                  $cond(
                    $and(
                      $lt($literal(Bson.Null), $field("score")),
                      $lt($field("score"), $literal(Bson.Text("")))),
                    $field("score"),
                    $literal(Bson.Undefined)))),
            -\/(reshape(
              "0" ->
                $cond(
                  $and(
                    $lte($literal(Check.minDate), $field("date")),
                    $lt($field("date"), $literal(Bson.Regex("", "")))),
                  $month($field("date")),
                  $literal(Bson.Undefined))))),
          $project(
            reshape(
              "a" -> $include(),
              "m" -> $field("_id", "0")),
            IgnoreId)))
    }.pendingWithActual(notOnPar, testFile("plan group by month"))

    // FIXME: Needs an actual expectation
    "plan expr3 with grouping" in {
      plan(sqlE"select case when pop > 1000 then city else lower(city) end, count(*) from zips group by city") must
        beRight
    }

    "plan count and sum grouped by single field" in {
      plan(sqlE"select count(*) as cnt, sum(pop) as sm from zips group by state") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "f0" -> $sum($literal(Bson.Int32(1))),
                "f1" ->
                  $sum(
                    $cond(
                      $and(
                        $lt($literal(Bson.Null), $field("pop")),
                        $lt($field("pop"), $literal(Bson.Text("")))),
                      $field("pop"),
                      $literal(Bson.Undefined)))),
              -\/(reshape("0" -> $arrayLit(List($field("state")))))),
            $project(
              reshape(
                "cnt" -> $field("f0"),
                "sm" -> $field("f1")),
              ExcludeId))
        }
    }

    "collect unaggregated fields into single doc when grouping" in {
      plan(sqlE"select city, state, sum(pop) from zips") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "__tmp3" -> reshape(
              "city"  -> $field("city"),
              "state" -> $field("state")),
            "__tmp4" -> reshape(
              "__tmp2" ->
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          IgnoreId),
        $group(
          grouped(
            "2" -> $sum($field("__tmp4", "__tmp2")),
            "__tmp3" -> $push($field("__tmp3"))),
          \/-($literal(Bson.Null))),
        $unwind(DocField("__tmp3")),
        $project(
          reshape(
            "city"  -> $field("__tmp3", "city"),
            "state" -> $field("__tmp3", "state"),
            "2"     -> $field("2")),
          IgnoreId)))
    }.pendingWithActual(notOnPar, testFile("collect unaggregated fields into single doc when grouping"))

    "plan unaggregated field when grouping, second case" in {
      // NB: the point being that we don't want to push $$ROOT
      plan(sqlE"select max(pop)/1000, pop from zips") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "__tmp3" ->
                  $max($cond(
                    $or(
                      $and(
                        $lt($literal(Bson.Null), $field("pop")),
                        $lt($field("pop"), $literal(Bson.Text("")))),
                      $and(
                        $lte($literal(Check.minDate), $field("pop")),
                        $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                    $field("pop"),
                    $literal(Bson.Undefined))),
                "pop"    -> $push($field("pop"))),
              \/-($literal(Bson.Null))),
            $unwind(DocField("pop")),
            $project(
              reshape(
                "0"   -> divide($field("__tmp3"), $literal(Bson.Int32(1000))),
                "pop" -> $field("pop")),
              IgnoreId))
        }
    }.pendingWithActual(notOnPar, testFile("plan unaggregated field when grouping, second case"))

    "plan double aggregation with another projection" in {
      plan(sqlE"select sum(avg(pop)), min(city) from zips group by foo") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "1"    ->
                  $min($cond(
                    $or(
                      $and(
                        $lt($literal(Bson.Null), $field("city")),
                        $lt($field("city"), $literal(Bson.Doc()))),
                      $and(
                        $lte($literal(Bson.Bool(false)), $field("city")),
                        $lt($field("city"), $literal(Bson.Regex("", ""))))),
                    $field("city"),
                    $literal(Bson.Undefined))),
                "__tmp10" ->
                  $avg($cond(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $field("pop"),
                    $literal(Bson.Undefined)))),
              -\/(reshape("0" -> $field("foo")))),
            $group(
              grouped(
                "0" -> $sum($field("__tmp10")),
                "1" -> $push($field("1"))),
              \/-($literal(Bson.Null))),
            $unwind(DocField("1")))
        }
    }.pendingWithActual(notOnPar, testFile("plan double aggregation with another projection"))

    "plan multiple expressions using same field" in {
      plan(sqlE"select pop, sum(pop), pop/1000 from zips") must
      beWorkflow0(chain[Workflow](
        $read (collection("db", "zips")),
        $project(
          reshape(
            "__tmp5" -> reshape(
              "pop" -> $field("pop"),
              "2"   ->
                $cond(
                  $or(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $and(
                      $lte($literal(Check.minDate), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                  divide($field("pop"), $literal(Bson.Int32(1000))),
                  $literal(Bson.Undefined))),
            "__tmp6" -> reshape(
              "__tmp2" ->
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          IgnoreId),
        $group(
          grouped(
            "1" -> $sum($field("__tmp6", "__tmp2")),
            "__tmp5" -> $push($field("__tmp5"))),
          \/-($literal(Bson.Null))),
        $unwind(DocField("__tmp5")),
        $project(
          reshape(
            "pop" -> $field("__tmp5", "pop"),
            "1"   -> $field("1"),
            "2"   -> $field("__tmp5", "2")),
          IgnoreId)))
    }.pendingWithActual(notOnPar, testFile("plan multiple expressions using same field"))

    "plan sum of expression in expression with another projection when grouped" in {
      plan(sqlE"select city, sum(pop-1)/1000 from zips group by city") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          grouped(
            "__tmp6" ->
              $sum(
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $subtract($field("pop"), $literal(Bson.Int32(1))),
                  $literal(Bson.Undefined)))),
          -\/(reshape("0" -> $field("city")))),
        $project(
          reshape(
            "city" -> $field("_id", "0"),
            "1"    -> divide($field("__tmp6"), $literal(Bson.Int32(1000)))),
          IgnoreId)))
    }.pendingWithActual(notOnPar, testFile("plan sum of expression in expression with another projection when grouped"))

    "plan length of min (JS on top of reduce)" in {
      plan3_2(sqlE"select state, length(min(city)) as shortest from zips group by state") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            grouped(
              "__tmp6" ->
                $min(
                  $cond(
                    $and(
                      $lte($literal(Bson.Text("")), $field("city")),
                      $lt($field("city"), $literal(Bson.Doc()))),
                    $field("city"),
                    $literal(Bson.Undefined)))),
            -\/(reshape("0" -> $field("state")))),
          $project(
            reshape(
              "state"  -> $field("_id", "0"),
              "__tmp6" -> $include()),
            IgnoreId),
          $simpleMap(NonEmptyList(
            MapExpr(JsFn(Name("x"), obj(
              "state" -> Select(ident("x"), "state"),
              "shortest" ->
                Call(ident("NumberLong"),
                  List(Select(Select(ident("x"), "__tmp6"), "length"))))))),
            ListMap()),
          $project(
            reshape(
              "state"    -> $include(),
              "shortest" -> $include()),
            IgnoreId)))
    }.pendingWithActual(notOnPar, testFile("plan length of min (JS on top of reduce)"))

    "plan js expr grouped by js expr" in {
      plan3_2(sqlE"select length(city) as len, count(*) as cnt from zips group by length(city)") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              obj(
                "f0" ->
                  If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                    Call(ident("NumberLong"),
                      List(Select(Select(ident("x"), "city"), "length"))),
                    ident("undefined")),
                "b0" ->
                  Arr(List(If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                    Call(ident("NumberLong"),
                      List(Select(Select(ident("x"), "city"), "length"))),
                    ident("undefined")))))))),
            ListMap()),
          $group(
            grouped(
              "f0" -> $first($field("f0")),
              "f1" -> $sum($literal(Bson.Int32(1)))),
            -\/(reshape("0" -> $field("b0")))),
          $project(
            reshape(
              "len" -> $field("f0"),
              "cnt" -> $field("f1")),
            ExcludeId)))
    }

    "plan simple JS inside expression" in {
      plan3_2(sqlE"select length(city) + 1 from zips") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
            underSigil(If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                BinOp(jscore.Add,
                  Call(ident("NumberLong"),
                    List(Select(Select(ident("x"), "city"), "length"))),
                  jscore.Literal(Js.Num(1, false))),
                ident("undefined")))))),
            ListMap())))
    }

    "plan expressions with ~"in {
      plan(sqlE"""select foo ~ "bar.*", "abc" ~ "a|b", "baz" ~ regex, target ~ regex from a""") must beWorkflow(chain[Workflow](
        $read(collection("db", "a")),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "0" -> If(Call(ident("isString"), List(Select(ident("x"), "foo"))),
              Call(
                Select(New(Name("RegExp"), List(jscore.Literal(Js.Str("bar.*")), jscore.Literal(Js.Str("m")))), "test"),
                List(Select(ident("x"), "foo"))),
              ident("undefined")),
            "1" -> jscore.Literal(Js.Bool(true)),
            "2" -> If(Call(ident("isString"), List(Select(ident("x"), "regex"))),
              Call(
                Select(New(Name("RegExp"), List(Select(ident("x"), "regex"), jscore.Literal(Js.Str("m")))), "test"),
                List(jscore.Literal(Js.Str("baz")))),
              ident("undefined")),
            "3" ->
              If(
                BinOp(jscore.And,
                  Call(ident("isString"), List(Select(ident("x"), "regex"))),
                  Call(ident("isString"), List(Select(ident("x"), "target")))),
                Call(
                  Select(New(Name("RegExp"), List(Select(ident("x"), "regex"), jscore.Literal(Js.Str("m")))), "test"),
                  List(Select(ident("x"), "target"))),
                ident("undefined")))))),
          ListMap()),
        $project(
          reshape(
            "0" -> $include(),
            "1" -> $include(),
            "2" -> $include(),
            "3" -> $include()),
          ExcludeId)))
    }

    "plan object flatten" in {
      plan(sqlE"select geo{*} from usa_factbook") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "usa_factbook")),
            $simpleMap(
              NonEmptyList(
                MapExpr(JsFn(Name("x"), obj(
                  "__tmp2" ->
                    If(
                      BinOp(jscore.And,
                        Call(ident("isObject"), List(Select(ident("x"), "geo"))),
                        UnOp(jscore.Not,
                          Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "geo"))))),
                      Select(ident("x"), "geo"),
                      Obj(ListMap(Name("") -> ident("undefined"))))))),
                FlatExpr(JsFn(Name("x"), Select(ident("x"), "__tmp2")))),
              ListMap()),
            $project(
              reshape(sigil.Quasar -> $field("__tmp2")),
              ExcludeId))
        }
    }.pendingWithActual(notOnPar, testFile("plan object flatten"))

    "plan array concat with filter" in {
      plan(sqlE"""select loc || [ pop ] from zips where city = "BOULDER" """) must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $match(Selector.Doc(
              BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER")))),
            $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
              "__tmp4" ->
                If(
                  BinOp(jscore.Or,
                    Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "loc"))),
                    Call(ident("isString"), List(Select(ident("x"), "loc")))),
                  SpliceArrays(List(
                    jscore.Select(ident("x"), "loc"),
                    jscore.Arr(List(jscore.Select(ident("x"), "pop"))))),
                  ident("undefined")))))),
              ListMap()),
            $project(
              reshape(sigil.Quasar -> $field("__tmp4")),
              ExcludeId))
        }
    }.pendingWithActual(notOnPar, testFile("plan array concat with filter"))

    "plan array flatten" in {
      plan(sqlE"select loc[*] from zips") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "__tmp2" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Arr(List())), $field("loc")),
                      $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                    $field("loc"),
                    $literal(Bson.Arr(List(Bson.Undefined))))),
              IgnoreId),
            $unwind(DocField(BsonField.Name("__tmp2"))),
            $project(
              reshape(sigil.Quasar -> $field("__tmp2")),
              ExcludeId))
        }
    }.pendingWithActual(notOnPar, testFile("plan array flatten"))

    "plan array concat" in {
      plan(sqlE"select loc || [ 0, 1, 2 ] from zips") must beWorkflow0 {
        chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(NonEmptyList(
            MapExpr(JsFn(Name("x"),
              Obj(ListMap(
                Name("__tmp4") ->
                  If(
                    BinOp(jscore.Or,
                      Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "loc"))),
                      Call(ident("isString"), List(Select(ident("x"), "loc")))),
                    SpliceArrays(List(
                      Select(ident("x"), "loc"),
                      Arr(List(
                        jscore.Literal(Js.Num(0, false)),
                        jscore.Literal(Js.Num(1, false)),
                        jscore.Literal(Js.Num(2, false)))))),
                    ident("undefined"))))))),
            ListMap()),
          $project(
            reshape(sigil.Quasar -> $field("__tmp4")),
            ExcludeId))
      }
    }.pendingWithActual(notOnPar, testFile("plan array concat"))

    "plan array flatten with unflattened field" in {
      plan(sqlE"SELECT `_id` as zip, loc as loc, loc[*] as coord FROM zips") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "__tmp2" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Arr(List())), $field("loc")),
                      $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                    $field("loc"),
                    $literal(Bson.Arr(List(Bson.Undefined)))),
                "__tmp3" -> $$ROOT),
              IgnoreId),
            $unwind(DocField(BsonField.Name("__tmp2"))),
            $project(
              reshape(
                "zip"   -> $field("__tmp3", "_id"),
                "loc"   -> $field("__tmp3", "loc"),
                "coord" -> $field("__tmp2")),
              IgnoreId))
        }
    }.pendingWithActual(notOnPar, testFile("plan array flatten with unflattened field"))

    "unify flattened fields" in {
      plan(sqlE"select loc[*] from zips where loc[*] < 0") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "__tmp6" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("loc")),
                  $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                $field("loc"),
                $literal(Bson.Arr(List(Bson.Undefined))))),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp6"))),
        $match(Selector.Doc(
          BsonField.Name("__tmp6") -> Selector.Lt(Bson.Int32(0)))),
        $project(
          reshape(sigil.Quasar -> $field("__tmp6")),
          ExcludeId)))
    }.pendingWithActual(notOnPar, testFile("unify flattened fields"))

    "group by flattened field" in {
      plan(sqlE"select substring(parents[*].sha, 0, 1), count(*) from slamengine_commits group by substring(parents[*].sha, 0, 1)") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "slamengine_commits")),
        $project(
          reshape(
            "__tmp12" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("parents")),
                  $lt($field("parents"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                $field("parents"),
                $literal(Bson.Arr(List(Bson.Undefined))))),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp12"))),
        $group(
          grouped(
            "1" -> $sum($literal(Bson.Int32(1)))),
          -\/(reshape(
            "0" ->
              $cond(
                $and(
                  $lte($literal(Bson.Text("")), $field("__tmp12", "sha")),
                  $lt($field("__tmp12", "sha"), $literal(Bson.Doc()))),
                $substr($field("__tmp12", "sha"), $literal(Bson.Int32(0)), $literal(Bson.Int32(1))),
                $literal(Bson.Undefined))))),
        $project(
          reshape(
            "0" -> $field("_id", "0"),
            "1" -> $include()),
          IgnoreId)))
    }.pendingWithActual(notOnPar, testFile("group by flattened field"))

    "unify flattened fields with unflattened field" in {
      plan(sqlE"select `_id` as zip, loc[*] from zips order by loc[*]") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "__tmp2" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("loc")),
                  $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                $field("loc"),
                $literal(Bson.Arr(List(Bson.Undefined)))),
            "__tmp3" -> $$ROOT),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp2"))),
        $project(
          reshape(
            "zip" -> $field("__tmp3", "_id"),
            "loc" -> $field("__tmp2")),
          IgnoreId),
        $sort(NonEmptyList(BsonField.Name("loc") -> SortDir.Ascending))))
    }.pendingWithActual(notOnPar, testFile("unify flattened fields with unflattened field"))

    "unify flattened with double-flattened" in {
      plan(sqlE"""select * from user_comments where (comments[*].id LIKE "%Dr%" OR comments[*].replyTo[*] LIKE "%Dr%")""") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "user_comments")),
        $project(
          reshape(
            "__tmp14" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("comments")),
                  $lt($field("comments"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                $field("comments"),
                $literal(Bson.Arr(List(Bson.Undefined)))),
            "__tmp15" -> $$ROOT),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp14"))),
        $project(
          reshape(
            "__tmp18" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("__tmp14", "replyTo")),
                  $lt($field("__tmp14", "replyTo"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                $field("__tmp14", "replyTo"),
                $literal(Bson.Arr(List(Bson.Undefined)))),
            "__tmp19" -> $$ROOT),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp18"))),
        $match(Selector.Or(
          Selector.And(
            Selector.Doc(
              BsonField.Name("__tmp19") \ BsonField.Name("__tmp14") \ BsonField.Name("id") -> Selector.Type(BsonType.Text)),
            Selector.Doc(
              BsonField.Name("__tmp19") \ BsonField.Name("__tmp14") \ BsonField.Name("id") -> Selector.Regex("^.*Dr.*$", false, true, false, false))),
          Selector.Doc(
            BsonField.Name("__tmp18") -> Selector.Regex("^.*Dr.*$", false, true, false, false)))),
        $project(
          reshape(sigil.Quasar -> $field("__tmp19", "__tmp15")),
          ExcludeId)))
    }.pendingWithActual(notOnPar, testFile("unify flattened with double-flattened"))

    "plan complex group by with sorting and limiting" in {
      plan(sqlE"SELECT city, SUM(pop) AS pop FROM zips GROUP BY city ORDER BY pop") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "pop"  ->
                  $sum(
                    $cond(
                      $and(
                        $lt($literal(Bson.Null), $field("pop")),
                        $lt($field("pop"), $literal(Bson.Text("")))),
                      $field("pop"),
                      $literal(Bson.Undefined)))),
              -\/(reshape("0" -> $field("city")))),
            $project(
              reshape(
                "city" -> $field("_id", "0"),
                "pop"  -> $include()),
              IgnoreId),
            $sort(NonEmptyList(BsonField.Name("pop") -> SortDir.Ascending)))
        }
    }.pendingWithActual(notOnPar, testFile("plan complex group by with sorting and limiting"))

    "plan implicit group by with filter" in {
      plan(sqlE"""select avg(pop), min(city) from zips where state = "CO" """) must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.Doc(
            BsonField.Name("state") -> Selector.Eq(Bson.Text("CO")))),
          $group(
            grouped(
              "f0" ->
                $avg(
                  $cond(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $field("pop"),
                    $literal(Bson.Undefined))),
              "f1" ->
                $min(
                  $cond($or(
                    $and(
                      $lt($literal(Bson.Null), $field("city")),
                      $lt($field("city"), $literal(Bson.Doc()))),
                    $and(
                      $lte($literal(Bson.Bool(false)), $field("city")),
                      $lt($field("city"), $literal(Bson.Regex("", ""))))),
                    $field("city"),
                    $literal(Bson.Undefined)))),
            \/-($literal(Bson.Null))),
          $project(
            reshape(
              "0" -> $field("f0"),
              "1" -> $field("f1")),
            ExcludeId)))
    }

    "plan distinct as expression" in {
      plan(sqlE"select count(distinct(city)) from zips") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            grouped(),
            -\/(reshape("0" -> $field("city")))),
          $group(
            grouped("__tmp2" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null))),
          $project(
            reshape(sigil.Quasar -> $field("__tmp2")),
            ExcludeId)))
    }.pendingWithActual(notOnPar, testFile("plan distinct as expression"))

    "plan distinct of expression as expression" in {
      plan(sqlE"select count(distinct substring(city, 0, 1)) from zips") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            grouped(),
            -\/(reshape(
              "0" ->
                $cond(
                  $and(
                    $lte($literal(Bson.Text("")), $field("city")),
                    $lt($field("city"), $literal(Bson.Doc()))),
                  $substr(
                    $field("city"),
                    $literal(Bson.Int32(0)),
                    $literal(Bson.Int32(1))),
                  $literal(Bson.Undefined))))),
          $group(
            grouped("__tmp8" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null))),
          $project(
            reshape(sigil.Quasar -> $field("__tmp8")),
            ExcludeId)))
    }.pendingWithActual(notOnPar, testFile("plan distinct of expression as expression"))

    "plan distinct of wildcard" in {
      plan(sqlE"select distinct * from zips") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
            obj(
              "__tmp1" ->
                Call(ident("remove"),
                  List(ident("x"), jscore.Literal(Js.Str("_id")))))))),
            ListMap()),
          $group(
            grouped(),
            -\/(reshape("0" -> $field("__tmp1")))),
          $project(
            reshape(sigil.Quasar -> $field("_id", "0")),
            ExcludeId)))
    }.pendingWithActual(notOnPar, testFile("plan distinct of wildcard"))

    "plan distinct of wildcard as expression" in {
      plan(sqlE"select count(distinct *) from zips") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
            obj(
              "__tmp4" ->
                Call(ident("remove"),
                  List(ident("x"), jscore.Literal(Js.Str("_id")))))))),
            ListMap()),
          $group(
            grouped(),
            -\/(reshape("0" -> $field("__tmp4")))),
          $group(
            grouped("__tmp6" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null))),
          $project(
            reshape(sigil.Quasar -> $field("__tmp6")),
            ExcludeId)))
    }.pendingWithActual(notOnPar, testFile("plan distinct of wildcard as expression"))

    "plan distinct with simple order by" in {
      plan(sqlE"select distinct city from zips order by city") must
        beWorkflow0(
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape("city" -> $field("city")),
              IgnoreId),
            $sort(NonEmptyList(BsonField.Name("city") -> SortDir.Ascending)),
            $group(
              grouped(),
              -\/(reshape("0" -> $field("city")))),
            $project(
              reshape("city" -> $field("_id", "0")),
              IgnoreId),
            $sort(NonEmptyList(BsonField.Name("city") -> SortDir.Ascending))))
      //at least on agg now, but there's an unnecessary array element selection
      //Name("0" -> { "$arrayElemAt": [["$_id.0", "$f0"], { "$literal": NumberInt("1") }] })
    }.pendingWithActual(notOnPar, testFile("plan distinct with simple order by"))

    "plan distinct with unrelated order by" in {
      plan(sqlE"select distinct city from zips order by pop desc") must
        beWorkflow0(
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "__sd__0" -> $field("pop"),
                "city"    -> $field("city")),
              IgnoreId),
            $sort(NonEmptyList(
              BsonField.Name("__sd__0") -> SortDir.Descending)),
            $group(
              grouped("__tmp2" -> $first($$ROOT)),
              -\/(reshape("city" -> $field("city")))),
            $project(
              reshape(
                "city" -> $field("__tmp2", "city"),
                "__tmp0" -> $field("__tmp2", "__sd__0")),
              IgnoreId),
            $sort(NonEmptyList(
              BsonField.Name("__tmp0") -> SortDir.Descending)),
            $project(
              reshape("city" -> $field("city")),
              ExcludeId)))
    }.pendingWithActual(notOnPar, testFile("plan distinct with unrelated order by"))

    "plan distinct as function with group" in {
      plan(sqlE"select state, count(distinct(city)) from zips group by state") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            grouped("__tmp0" -> $first($$ROOT)),
            -\/(reshape("0" -> $field("city")))),
          $group(
            grouped(
              "state" -> $first($field("__tmp0", "state")),
              "1"     -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null)))))
    }.pendingWithActual(notOnPar, testFile("plan distinct as function with group"))

    "plan distinct with sum and group" in {
      plan(sqlE"SELECT DISTINCT SUM(pop) AS totalPop, city, state FROM zips GROUP BY city") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            grouped(
              "totalPop" ->
                $sum(
                  $cond(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $field("pop"),
                    $literal(Bson.Undefined))),
              "state"    -> $push($field("state"))),
            -\/(reshape("0" -> $field("city")))),
          $project(
            reshape(
              "totalPop" -> $include(),
              "city"     -> $field("_id", "0"),
              "state"    -> $include()),
            IgnoreId),
          $unwind(DocField("state")),
          $group(
            grouped(),
            -\/(reshape(
              "0" -> $field("totalPop"),
              "1" -> $field("city"),
              "2" -> $field("state")))),
          $project(
            reshape(
              "totalPop" -> $field("_id", "0"),
              "city"     -> $field("_id", "1"),
              "state"    -> $field("_id", "2")),
            IgnoreId)))
    }.pendingWithActual(notOnPar, testFile("plan distinct with sum and group"))

    "plan distinct with sum, group, and orderBy" in {
      plan(sqlE"SELECT DISTINCT SUM(pop) AS totalPop, city, state FROM zips GROUP BY city ORDER BY totalPop DESC") must
        beWorkflow0(
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "totalPop" ->
                  $sum(
                    $cond(
                      $and(
                        $lt($literal(Bson.Null), $field("pop")),
                        $lt($field("pop"), $literal(Bson.Text("")))),
                      $field("pop"),
                      $literal(Bson.Undefined))),
                "state"    -> $push($field("state"))),
              -\/(reshape("0" -> $field("city")))),
            $project(
              reshape(
                "totalPop" -> $include(),
                "city"     -> $field("_id", "0"),
                "state"    -> $include()),
              IgnoreId),
            $unwind(DocField("state")),
            $sort(NonEmptyList(BsonField.Name("totalPop") -> SortDir.Descending)),
            $group(
              grouped(),
              -\/(reshape(
                "0" -> $field("totalPop"),
                "1" -> $field("city"),
                "2" -> $field("state")))),
            $project(
              reshape(
                "totalPop" -> $field("_id", "0"),
                "city"     -> $field("_id", "1"),
                "state"    -> $field("_id", "2")),
              IgnoreId),
            $sort(NonEmptyList(BsonField.Name("totalPop") -> SortDir.Descending))))

    }.pendingWithActual(notOnPar, testFile("plan distinct with sum, group, and orderBy"))

    "plan simple sort on map-reduce with mapBeforeSort" in {
      plan3_2(sqlE"select length(city) from zips order by city") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), Let(Name("__val"),
            Arr(List(
              obj("0" -> If(
                Call(ident("isString"), List(Select(ident("x"), "city"))),
                Call(ident("NumberLong"),
                  List(Select(Select(ident("x"), "city"), "length"))),
                ident("undefined"))),
              ident("x"))),
            obj("0" -> Select(Access(ident("__val"), jscore.Literal(Js.Num(1, false))), "city"),
                "src" -> ident("__val")))))),
            ListMap()),
          $sort(NonEmptyList(BsonField.Name("0") -> SortDir.Ascending)),
          $project(
            reshape(sigil.Quasar -> $arrayElemAt($field("src"), $literal(Bson.Int32(0)))),
            ExcludeId)))
    }

    "plan time_of_day (JS)" in {
      plan(sqlE"select time_of_day(ts) from days") must
        beRight // NB: way too complicated to spell out here, and will change as JS generation improves
    }

    def joinStructure0(
      left: Workflow, leftName: String, leftBase: Fix[ExprOp], right: Workflow,
      leftKey: Reshape.Shape[ExprOp], rightKey: (String, Fix[ExprOp], Reshape.Shape[ExprOp]) \/ JsCore,
      fin: FixOp[WorkflowF],
      swapped: Boolean) = {

      val (leftLabel, rightLabel) =
        if (swapped) (JoinDir.Right.name, JoinDir.Left.name) else (JoinDir.Left.name, JoinDir.Right.name)
      def initialPipeOps(
        src: Workflow, name: String, base: Fix[ExprOp], key: Reshape.Shape[ExprOp], mainLabel: String, otherLabel: String):
          Workflow =
        chain[Workflow](
          src,
          $group(grouped(name -> $push(base)), key),
          $project(
            reshape(
              mainLabel  -> $field(name),
              otherLabel -> $literal(Bson.Arr(List())),
              "_id"      -> $include()),
            IncludeId))
      fin(
        $foldLeft(
          initialPipeOps(left, leftName, leftBase, leftKey, leftLabel, rightLabel),
          chain[Workflow](
            right,
            rightKey.fold(
              rk => initialPipeOps(_, rk._1, rk._2, rk._3, rightLabel, leftLabel),
              rk => $map($MapF.mapKeyVal(("key", "value"),
                rk.toJs,
                Js.AnonObjDecl(List(
                  (leftLabel, Js.AnonElem(List())),
                  (rightLabel, Js.AnonElem(List(Js.Ident("value"))))))),
                ListMap())),
            $reduce(
              Js.AnonFunDecl(List("key", "values"),
                List(
                  Js.VarDef(List(
                    ("result", Js.AnonObjDecl(List(
                      (leftLabel, Js.AnonElem(List())),
                      (rightLabel, Js.AnonElem(List()))))))),
                  Js.Call(Js.Select(Js.Ident("values"), "forEach"),
                    List(Js.AnonFunDecl(List("value"),
                      List(
                        Js.BinOp("=",
                          Js.Select(Js.Ident("result"), leftLabel),
                          Js.Call(
                            Js.Select(Js.Select(Js.Ident("result"), leftLabel), "concat"),
                            List(Js.Select(Js.Ident("value"), leftLabel)))),
                        Js.BinOp("=",
                          Js.Select(Js.Ident("result"), rightLabel),
                          Js.Call(
                            Js.Select(Js.Select(Js.Ident("result"), rightLabel), "concat"),
                            List(Js.Select(Js.Ident("value"), rightLabel)))))))),
                  Js.Return(Js.Ident("result")))),
              ListMap()))))
    }

    def joinStructure(
        left: Workflow, leftName: String, leftBase: Fix[ExprOp], right: Workflow,
        leftKey: Reshape.Shape[ExprOp], rightKey: (String, Fix[ExprOp], Reshape.Shape[ExprOp]) \/ JsCore,
        fin: FixOp[WorkflowF],
        swapped: Boolean) =
      Crystallize[WorkflowF].crystallize(joinStructure0(left, leftName, leftBase, right, leftKey, rightKey, fin, swapped))

    "plan simple join (map-reduce)" in {
      plan2_6(sqlE"select zips2.city from zips join zips2 on zips.`_id` = zips2.`_id`") must
        beWorkflow0(
          joinStructure(
            $read(collection("db", "zips")), "__tmp0", $$ROOT,
            $read(collection("db", "zips2")),
            reshape("0" -> $field("_id")),
            Obj(ListMap(Name("0") -> Select(ident("value"), "_id"))).right,
            chain[Workflow](_,
              $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
                JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
              $unwind(DocField(JoinHandler.LeftName)),
              $unwind(DocField(JoinHandler.RightName)),
              $project(
                reshape(sigil.Quasar -> $field(JoinDir.Right.name, "city")),
                ExcludeId)),
            false).op)
    }.pendingWithActual("#1560", testFile("plan simple join (map-reduce)"))

    "plan simple join ($lookup)" in {
      plan(sqlE"select zips2.city from zips join zips2 on zips.`_id` = zips2.`_id`") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.Doc(
            BsonField.Name("_id") -> Selector.Exists(true))),
          $project(reshape(JoinDir.Left.name -> $$ROOT)),
          $lookup(
            CollectionName("zips2"),
            JoinHandler.LeftName \ BsonField.Name("_id"),
            BsonField.Name("_id"),
            JoinHandler.RightName),
          $unwind(DocField(JoinHandler.RightName)),
          $project(
            reshape(sigil.Quasar -> $field(JoinDir.Right.name, "city")),
            ExcludeId)))
    }.pendingWithActual("#1560", testFile("plan simple join ($lookup)"))

    "plan simple join with sharded inputs" in {
      // NB: cannot use $lookup, so fall back to the old approach
      val query = sqlE"select zips2.city from zips join zips2 on zips.`_id` = zips2.`_id`"
      plan3_4(query,
        c => Map(
          collection("db", "zips") -> CollectionStatistics(10, 100, true),
          collection("db", "zips2") -> CollectionStatistics(15, 150, true)).get(c),
        defaultIndexes,
        emptyDoc) must_==
        plan2_6(query)
    }.pendingUntilFixed(notOnPar)

    "plan simple join with sources in different DBs" in {
      // NB: cannot use $lookup, so fall back to the old approach
      val query = sqlE"select zips2.city from `/db1/zips` join `/db2/zips2` on zips.`_id` = zips2.`_id`"
      plan(query) must_== plan2_6(query)
    }.pendingUntilFixed(notOnPar)

    "plan simple join with no index" in {
      // NB: cannot use $lookup, so fall back to the old approach
      val query = sqlE"select zips2.city from zips join zips2 on zips.pop = zips2.pop"
      plan(query) must_== plan2_6(query)
    }.pendingUntilFixed(notOnPar)

    "plan non-equi join" in {
      plan(sqlE"select zips2.city from zips join zips2 on zips.`_id` < zips2.`_id`") must
      beWorkflow0(
        joinStructure(
          $read(collection("db", "zips")), "__tmp0", $$ROOT,
          $read(collection("db", "zips2")),
          $literal(Bson.Null),
          jscore.Literal(Js.Null).right,
          chain[Workflow](_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
              JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(JoinHandler.LeftName)),
            $unwind(DocField(JoinHandler.RightName)),
            $project(
              reshape(
                "__tmp11"   ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name),
                    $literal(Bson.Undefined)),
                "__tmp12" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr(List())))),
                    $cond(
                      $or(
                        $and(
                          $lt($literal(Bson.Null), $field(JoinDir.Right.name, "_id")),
                          $lt($field(JoinDir.Right.name, "_id"), $literal(Bson.Doc()))),
                        $and(
                          $lte($literal(Bson.Bool(false)), $field(JoinDir.Right.name, "_id")),
                          $lt($field(JoinDir.Right.name, "_id"), $literal(Bson.Regex("", ""))))),
                      $cond(
                        $and(
                          $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                          $lt($field(JoinDir.Left.name), $literal(Bson.Arr(List())))),
                        $cond(
                          $or(
                            $and(
                              $lt($literal(Bson.Null), $field(JoinDir.Left.name, "_id")),
                              $lt($field(JoinDir.Left.name, "_id"), $literal(Bson.Doc()))),
                            $and(
                              $lte($literal(Bson.Bool(false)), $field(JoinDir.Left.name, "_id")),
                              $lt($field(JoinDir.Left.name, "_id"), $literal(Bson.Regex("", ""))))),
                          $lt($field(JoinDir.Left.name, "_id"), $field(JoinDir.Right.name, "_id")),
                          $literal(Bson.Undefined)),
                        $literal(Bson.Undefined)),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined))),
              IgnoreId),
            $match(
              Selector.Doc(
                BsonField.Name("__tmp12") -> Selector.Eq(Bson.Bool(true)))),
            $project(
              reshape(sigil.Quasar -> $field("__tmp11", "city")),
              ExcludeId)),
          false).op)
    }.pendingWithActual("#1560", testFile("plan non-equi join"))

    "plan simple inner equi-join (map-reduce)" in {
      plan2_6(
        sqlE"select foo.name, bar.address from foo join bar on foo.id = bar.foo_id") must
      beWorkflow0(
        joinStructure(
          $read(collection("db", "foo")), "__tmp0", $$ROOT,
          $read(collection("db", "bar")),
          reshape("0" -> $field("id")),
          Obj(ListMap(Name("0") -> Select(ident(sigil.Quasar), "foo_id"))).right,
          chain[Workflow](_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
              JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(JoinHandler.LeftName)),
            $unwind(DocField(JoinHandler.RightName)),
            $project(
              reshape(
                "name"    ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Left.name, "name"),
                    $literal(Bson.Undefined)),
                "address" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name, "address"),
                    $literal(Bson.Undefined))),
              IgnoreId)),
          false).op)
    }.pendingWithActual("#1560", testFile("plan simple inner equi-join (map-reduce)"))

    "plan simple inner equi-join ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address from foo join bar on foo.id = bar.foo_id",
        defaultStats,
        indexes(collection("db", "bar") -> BsonField.Name("foo_id")),
        emptyDoc) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "foo")),
        $match(Selector.Doc(
          BsonField.Name("id") -> Selector.Exists(true))),
        $project(reshape(JoinDir.Left.name -> $$ROOT)),
        $lookup(
          CollectionName("bar"),
          JoinHandler.LeftName \ BsonField.Name("id"),
          BsonField.Name("foo_id"),
          JoinHandler.RightName),
        $unwind(DocField(JoinHandler.RightName)),
        $project(reshape(
          "name" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                $lt($field(JoinDir.Left.name), $literal(Bson.Arr(Nil)))),
              $field(JoinDir.Left.name, "name"),
              $literal(Bson.Undefined)),
          "address" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                $lt($field(JoinDir.Right.name), $literal(Bson.Arr(Nil)))),
              $field(JoinDir.Right.name, "address"),
              $literal(Bson.Undefined))),
          IgnoreId)))
    }.pendingWithActual("#1560", testFile("plan simple inner equi-join ($lookup)"))

    "plan simple inner equi-join with expression ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address from foo join bar on lower(foo.id) = bar.foo_id",
        defaultStats,
        indexes(collection("db", "bar") -> BsonField.Name("foo_id")),
        emptyDoc) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "foo")),
        $project(reshape(
          JoinDir.Left.name -> $$ROOT,
          "__tmp0" -> $toLower($field("id"))),
          IgnoreId),
        $lookup(
          CollectionName("bar"),
          BsonField.Name("__tmp0"),
          BsonField.Name("foo_id"),
          JoinHandler.RightName),
        $project(reshape(
          JoinDir.Left.name -> $field(JoinDir.Left.name),
          JoinDir.Right.name -> $field(JoinDir.Right.name))),
        $unwind(DocField(JoinHandler.RightName)),
        $project(reshape(
          "name" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
              $field(JoinDir.Left.name, "name"),
              $literal(Bson.Undefined)),
          "address" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
              $field(JoinDir.Right.name, "address"),
              $literal(Bson.Undefined))),
          IgnoreId)))
    }.pendingWithActual("#1560", testFile("plan simple inner equi-join with expression ($lookup)"))

    "plan simple inner equi-join with pre-filtering ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address from foo join bar on foo.id = bar.foo_id where bar.rating >= 4",
        defaultStats,
        indexes(collection("db", "foo") -> BsonField.Name("id")),
        emptyDoc) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "bar")),
        $match(
          Selector.And(
            isNumeric(BsonField.Name("rating")),
            Selector.Doc(
              BsonField.Name("rating") -> Selector.Gte(Bson.Int32(4))))),
        $project(reshape(
          JoinDir.Right.name -> $$ROOT,
          "__tmp2" -> $field("foo_id")),
          ExcludeId),
        $lookup(
          CollectionName("foo"),
          BsonField.Name("__tmp2"),
          BsonField.Name("id"),
          JoinHandler.LeftName),
        $project(reshape(
          JoinDir.Right.name -> $field(JoinDir.Right.name),
          JoinDir.Left.name -> $field(JoinDir.Left.name))),
        $unwind(DocField(JoinHandler.LeftName)),
        $project(reshape(
          "name" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
              $field(JoinDir.Left.name, "name"),
              $literal(Bson.Undefined)),
          "address" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
              $field(JoinDir.Right.name, "address"),
              $literal(Bson.Undefined))),
          IgnoreId)))
    }.pendingWithActual("#1560", testFile("plan simple inner equi-join with pre-filtering ($lookup)"))

    "plan simple outer equi-join with wildcard" in {
      plan(sqlE"select * from foo full join bar on foo.id = bar.foo_id") must
      beWorkflow0(
        joinStructure(
          $read(collection("db", "foo")), "__tmp0", $$ROOT,
          $read(collection("db", "bar")),
          reshape("0" -> $field("id")),
          Obj(ListMap(Name("0") -> Select(ident("value"), "foo_id"))).right,
          chain[Workflow](_,
            $project(
              reshape(
                JoinDir.Left.name ->
                  $cond($eq($size($field(JoinDir.Left.name)), $literal(Bson.Int32(0))),
                    $literal(Bson.Arr(List(Bson.Doc()))),
                    $field(JoinDir.Left.name)),
                JoinDir.Right.name ->
                  $cond($eq($size($field(JoinDir.Right.name)), $literal(Bson.Int32(0))),
                    $literal(Bson.Arr(List(Bson.Doc()))),
                    $field(JoinDir.Right.name))),
              IgnoreId),
            $unwind(DocField(JoinHandler.LeftName)),
            $unwind(DocField(JoinHandler.RightName)),
            $simpleMap(
              NonEmptyList(
                MapExpr(JsFn(Name("x"),
                  Obj(ListMap(
                    Name("__tmp7") ->
                      If(
                        BinOp(jscore.And,
                          Call(ident("isObject"), List(Select(ident("x"), JoinDir.Right.name))),
                          UnOp(jscore.Not,
                            Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), JoinDir.Right.name))))),
                        If(
                          BinOp(jscore.And,
                            Call(ident("isObject"), List(Select(ident("x"), JoinDir.Left.name))),
                            UnOp(jscore.Not,
                              Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), JoinDir.Left.name))))),
                          SpliceObjects(List(
                            Select(ident("x"), JoinDir.Left.name),
                            Select(ident("x"), JoinDir.Right.name))),
                          ident("undefined")),
                        ident("undefined"))))))),
              ListMap()),
            $project(
              reshape(sigil.Quasar -> $field("__tmp7")),
              ExcludeId)),
          false).op)
    }.pendingWithActual("#1560", testFile("plan simple outer equi-join with wildcard"))

    "plan simple left equi-join (map-reduce)" in {
      plan(
        sqlE"select foo.name, bar.address from foo left join bar on foo.id = bar.foo_id") must
      beWorkflow0(
        joinStructure(
          $read(collection("db", "foo")), "__tmp0", $$ROOT,
          $read(collection("db", "bar")),
          reshape("0" -> $field("id")),
          Obj(ListMap(Name("0") -> Select(ident("value"), "foo_id"))).right,
          chain[Workflow](_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0))))),
            $project(
              reshape(
                JoinDir.Left.name  -> $field(JoinDir.Left.name),
                JoinDir.Right.name ->
                  $cond($eq($size($field(JoinDir.Right.name)), $literal(Bson.Int32(0))),
                    $literal(Bson.Arr(List(Bson.Doc()))),
                    $field(JoinDir.Right.name))),
              IgnoreId),
            $unwind(DocField(JoinHandler.LeftName)),
            $unwind(DocField(JoinHandler.RightName)),
            $project(
              reshape(
                "name"    ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Left.name, "name"),
                    $literal(Bson.Undefined)),
                "address" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name, "address"),
                    $literal(Bson.Undefined))),
              IgnoreId)),
          false).op)
    }.pendingWithActual("#1560", testFile("plan simple left equi-join (map-reduce)"))

    "plan simple left equi-join ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address from foo left join bar on foo.id = bar.foo_id",
        defaultStats,
        indexes(collection("db", "bar") -> BsonField.Name("foo_id")),
        emptyDoc) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "foo")),
        $project(reshape(JoinDir.Left.name -> $$ROOT)),
        $lookup(
          CollectionName("bar"),
          JoinHandler.LeftName \ BsonField.Name("id"),
          BsonField.Name("foo_id"),
          JoinHandler.RightName),
        $unwind(DocField(JoinHandler.RightName)),  // FIXME: need to preserve docs with no match
        $project(reshape(
          "name" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
              $field(JoinDir.Left.name, "name"),
              $literal(Bson.Undefined)),
          "address" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
              $field(JoinDir.Right.name, "address"),
              $literal(Bson.Undefined))),
          IgnoreId)))
    }.pendingWithActual("TODO: left/right joins in $lookup", testFile("plan simple left equi-join ($lookup)"))

    "plan simple right equi-join ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address from foo right join bar on foo.id = bar.foo_id",
        defaultStats,
        indexes(collection("db", "bar") -> BsonField.Name("foo_id")),
        emptyDoc) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "bar")),
        $project(reshape(JoinDir.Right.name -> $$ROOT)),
        $lookup(
          CollectionName("foo"),
          JoinHandler.RightName \ BsonField.Name("foo_id"),
          BsonField.Name("id"),
          JoinHandler.LeftName),
        $unwind(DocField(JoinHandler.LeftName)),  // FIXME: need to preserve docs with no match
        $project(reshape(
          "name" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
              $field(JoinDir.Left.name, "name"),
              $literal(Bson.Undefined)),
          "address" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
              $field(JoinDir.Right.name, "address"),
              $literal(Bson.Undefined))),
          IgnoreId)))
    }.pendingWithActual("TODO: left/right joins in $lookup", testFile("plan simple right equi-join ($lookup)"))

    "plan 3-way right equi-join (map-reduce)" in {
      plan2_6(
        sqlE"select foo.name, bar.address, baz.zip from foo join bar on foo.id = bar.foo_id right join baz on bar.id = baz.bar_id") must
      beWorkflow0(
        joinStructure(
          $read(collection("db", "baz")), "__tmp1", $$ROOT,
          joinStructure0(
            $read(collection("db", "foo")), "__tmp0", $$ROOT,
            $read(collection("db", "bar")),
            reshape("0" -> $field("id")),
            Obj(ListMap(Name("0") -> Select(ident("value"), "foo_id"))).right,
            chain[Workflow](_,
              $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
                JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
              $unwind(DocField(JoinHandler.LeftName)),
              $unwind(DocField(JoinHandler.RightName))),
            false),
          reshape("0" -> $field("bar_id")),
          Obj(ListMap(Name("0") -> Select(Select(ident("value"), JoinDir.Right.name), "id"))).right,
          chain[Workflow](_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0))))),
            $project(
              reshape(
                JoinDir.Right.name ->
                  $cond($eq($size($field(JoinDir.Right.name)), $literal(Bson.Int32(0))),
                    $literal(Bson.Arr(List(Bson.Doc()))),
                    $field(JoinDir.Right.name)),
                JoinDir.Left.name -> $field(JoinDir.Left.name)),
              IgnoreId),
            $unwind(DocField(JoinHandler.RightName)),
            $unwind(DocField(JoinHandler.LeftName)),
            $project(
              reshape(
                "name"    ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $cond(
                      $and(
                        $lte($literal(Bson.Doc()), $field(JoinDir.Left.name, JoinDir.Left.name)),
                        $lt($field(JoinDir.Left.name, JoinDir.Left.name), $literal(Bson.Arr()))),
                      $field(JoinDir.Left.name, JoinDir.Left.name, "name"),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined)),
                "address" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $cond(
                      $and(
                        $lte($literal(Bson.Doc()), $field(JoinDir.Left.name, JoinDir.Right.name)),
                        $lt($field(JoinDir.Left.name, JoinDir.Right.name), $literal(Bson.Arr()))),
                      $field(JoinDir.Left.name, JoinDir.Right.name, "address"),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined)),
                "zip"     ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name, "zip"),
                    $literal(Bson.Undefined))),
              IgnoreId)),
          true).op)
    }.pendingWithActual("#1560", testFile("plan 3-way right equi-join (map-reduce)"))

    "plan 3-way equi-join ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address, baz.zip from foo join bar on foo.id = bar.foo_id join baz on bar.id = baz.bar_id",
        defaultStats,
        indexes(
          collection("db", "bar") -> BsonField.Name("foo_id"),
          collection("db", "baz") -> BsonField.Name("bar_id")),
        emptyDoc) must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "foo")),
          $match(Selector.Doc(
            BsonField.Name("id") -> Selector.Exists(true))),
          $project(reshape(JoinDir.Left.name -> $$ROOT)),
          $lookup(
            CollectionName("bar"),
            JoinHandler.LeftName \ BsonField.Name("id"),
            BsonField.Name("foo_id"),
            JoinHandler.RightName),
          $unwind(DocField(JoinHandler.RightName)),
          $match(Selector.Doc(
            JoinHandler.RightName \ BsonField.Name("id") -> Selector.Exists(true))),
          $project(reshape(JoinDir.Left.name -> $$ROOT)),
          $lookup(
            CollectionName("baz"),
            JoinHandler.LeftName \ JoinHandler.RightName \ BsonField.Name("id"),
            BsonField.Name("bar_id"),
            JoinHandler.RightName),
          $unwind(DocField(JoinHandler.RightName)),
          $project(reshape(
            "name" ->
              $cond(
                $and(
                  $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                  $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                $cond(
                  $and(
                    $lte($literal(Bson.Doc()), $field(JoinDir.Left.name, JoinDir.Left.name)),
                    $lt($field(JoinDir.Left.name, JoinDir.Left.name), $literal(Bson.Arr()))),
                  $field(JoinDir.Left.name, JoinDir.Left.name, "name"),
                  $literal(Bson.Undefined)),
                $literal(Bson.Undefined)),
            "address" ->
              $cond(
                $and(
                  $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                  $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                $cond(
                  $and(
                    $lte($literal(Bson.Doc()), $field(JoinDir.Left.name, JoinDir.Right.name)),
                    $lt($field(JoinDir.Left.name, JoinDir.Right.name), $literal(Bson.Arr()))),
                  $field(JoinDir.Left.name, JoinDir.Right.name, "address"),
                  $literal(Bson.Undefined)),
                $literal(Bson.Undefined)),
            "zip" ->
              $cond(
                $and(
                  $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                  $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                $field(JoinDir.Right.name, "zip"),
                $literal(Bson.Undefined))),
            IgnoreId)))
    }.pendingWithActual("#1560", testFile("plan 3-way equi-join ($lookup)"))

    "plan count of $lookup" in {
      plan3_4(
        sqlE"select tp.`_id`, count(*) from `zips` as tp join `largeZips` as ti on tp.`_id` = ti.TestProgramId group by tp.`_id`",
        defaultStats,
        indexes(),
        emptyDoc) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "largeZips")),
        $match(Selector.Doc(
          BsonField.Name("TestProgramId") -> Selector.Exists(true))),
        $project(reshape("right" -> $$ROOT), IgnoreId),
        $lookup(
          CollectionName("zips"),
          JoinHandler.RightName \ BsonField.Name("TestProgramId"),
          BsonField.Name("_id"),
          JoinHandler.LeftName),
        $unwind(DocField(JoinHandler.LeftName)),
        $group(
          grouped("1" -> $sum($literal(Bson.Int32(1)))),
          -\/(reshape("0" -> $cond(
            $and(
              $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
              $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
            $field(JoinDir.Left.name, "_id"),
            $literal(Bson.Undefined))))),
        $project(
          reshape(
            "_id" -> $field("_id", "0"),
            "1"   -> $include),
          IgnoreId)))
    }.pendingWithActual(notOnPar, testFile("plan count of $lookup"))

    "plan join with multiple conditions" in {
      plan(sqlE"select l.sha as child, l.author.login as c_auth, r.sha as parent, r.author.login as p_auth from slamengine_commits as l join slamengine_commits as r on r.sha = l.parents[0].sha and l.author.login = r.author.login") must
      beWorkflow0(
        joinStructure(
          chain[Workflow](
            $read(collection("db", "slamengine_commits")),
            $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
              obj(
                "__tmp4" -> Select(Access(Select(ident("x"), "parents"), jscore.Literal(Js.Num(0, false))), "sha"),
                "__tmp5" -> ident("x"),
                "__tmp6" -> Select(Select(ident("x"), "author"), "login"))))),
              ListMap())),
          "__tmp7", $field("__tmp5"),
          $read(collection("db", "slamengine_commits")),
          reshape(
            "0" -> $field("__tmp4"),
            "1" -> $field("__tmp6")),
          obj(
            "0" -> Select(ident("value"), "sha"),
            "1" -> Select(Select(ident("value"), "author"), "login")).right,
          chain[Workflow](_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
              JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(JoinHandler.LeftName)),
            $unwind(DocField(JoinHandler.RightName)),
            $project(
              reshape(
                "child"  ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Left.name, "sha"),
                    $literal(Bson.Undefined)),
                "c_auth" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $cond(
                      $and(
                        $lte($literal(Bson.Doc()), $field(JoinDir.Left.name, "author")),
                        $lt($field(JoinDir.Left.name, "author"), $literal(Bson.Arr()))),
                      $field(JoinDir.Left.name, "author", "login"),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined)),
                "parent" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name, "sha"),
                    $literal(Bson.Undefined)),
                "p_auth" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $cond(
                      $and(
                        $lte($literal(Bson.Doc()), $field(JoinDir.Right.name, "author")),
                        $lt($field(JoinDir.Right.name, "author"), $literal(Bson.Arr()))),
                      $field(JoinDir.Right.name, "author", "login"),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined))),
              IgnoreId)),
        false).op)
    }.pendingWithActual("#1560", testFile("plan join with multiple conditions"))

    "plan join with non-JS-able condition" in {
      plan(sqlE"select z1.city as city1, z1.loc, z2.city as city2, z2.pop from zips as z1 join zips as z2 on z1.loc[*] = z2.loc[*]") must
      beWorkflow0(
        joinStructure(
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "__tmp0" -> $field("loc"),
                "__tmp1" -> $$ROOT),
              IgnoreId),
            $unwind(DocField(BsonField.Name("__tmp0")))),
          "__tmp2", $field("__tmp1"),
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "__tmp3" -> $field("loc"),
                "__tmp4" -> $$ROOT),
              IgnoreId),
            $unwind(DocField(BsonField.Name("__tmp3")))),
          reshape("0" -> $field("__tmp0")),
          ("__tmp5", $field("__tmp4"), reshape(
            "0" -> $field("__tmp3")).left).left,
          chain[Workflow](_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
              JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(JoinHandler.LeftName)),
            $unwind(DocField(JoinHandler.RightName)),
            $project(
              reshape(
                "city1" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Left.name, "city"),
                    $literal(Bson.Undefined)),
                "loc" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Left.name, "loc"),
                    $literal(Bson.Undefined)),
                "city2" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name, "city"),
                    $literal(Bson.Undefined)),
                "pop" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name, "pop"),
                    $literal(Bson.Undefined))),
              IgnoreId)),
          false).op)
    }.pendingWithActual("#1560", testFile("plan join with non-JS-able condition"))

    "plan simple cross" in {
      plan(sqlE"select zips2.city from zips, zips2 where zips.pop < zips2.pop") must
      beWorkflow0(
        joinStructure(
          $read(collection("db", "zips")), "__tmp0", $$ROOT,
          $read(collection("db", "zips2")),
          $literal(Bson.Null),
          jscore.Literal(Js.Null).right,
          chain[Workflow](_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
              JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(JoinHandler.LeftName)),
            $unwind(DocField(JoinHandler.RightName)),
            $project(
              reshape(
                "__tmp11"   ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name),
                    $literal(Bson.Undefined)),
                "__tmp12" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr(List())))),
                    $cond(
                      $or(
                        $and(
                          $lt($literal(Bson.Null), $field(JoinDir.Right.name, "pop")),
                          $lt($field(JoinDir.Right.name, "pop"), $literal(Bson.Doc()))),
                        $and(
                          $lte($literal(Bson.Bool(false)), $field(JoinDir.Right.name, "pop")),
                          $lt($field(JoinDir.Right.name, "pop"), $literal(Bson.Regex("", ""))))),
                      $cond(
                        $and(
                          $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                          $lt($field(JoinDir.Left.name), $literal(Bson.Arr(List())))),
                        $cond(
                          $or(
                            $and(
                              $lt($literal(Bson.Null), $field(JoinDir.Left.name, "pop")),
                              $lt($field(JoinDir.Left.name, "pop"), $literal(Bson.Doc()))),
                            $and(
                              $lte($literal(Bson.Bool(false)), $field(JoinDir.Left.name, "pop")),
                              $lt($field(JoinDir.Left.name, "pop"), $literal(Bson.Regex("", ""))))),
                          $lt($field(JoinDir.Left.name, "pop"), $field(JoinDir.Right.name, "pop")),
                          $literal(Bson.Undefined)),
                        $literal(Bson.Undefined)),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined))),
              IgnoreId),
            $match(
              Selector.Doc(
                BsonField.Name("__tmp12") -> Selector.Eq(Bson.Bool(true)))),
            $project(
              reshape(sigil.Quasar -> $field("__tmp11", "city")),
              ExcludeId)),
          false).op)
    }.pendingWithActual("#1560", testFile("plan simple cross"))

    "SD-1263 specific case of plan multiple reducing projections (all, distinct, orderBy)" in {
      val q = sqlE"select distinct loc || [pop - 1] as p1, pop - 1 as p2 from zips group by territory order by p2".project.asInstanceOf[Select[Fix[Sql]]]

      plan(q.embed) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        countAccumOps(wf) must_== 1
        countUnwindOps(wf) must_== 0
        countMatchOps(wf) must_== 0
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        appropriateColumns(wf, q)
        rootPushes(wf) must_== Nil
      }
    }

  }
}
