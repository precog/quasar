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
import quasar.contrib.pathy._
import quasar.contrib.specs2._
import quasar.ejson.{EJson, Fixed}
import quasar.fp.ski._
import quasar.fs._
import quasar.javascript._
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner._
import quasar.physical.mongodb.workflow._
import quasar.sql._

import java.time.Instant
import scala.Either

import eu.timepit.refined.auto._
import matryoshka.data.Fix
import org.specs2.execute._
import pathy.Path._
import scalaz._, Scalaz._

/**
 * Tests of which the workflow expectation should be fairly stable
 * and/or fairly easy to maintain by hand
 */
class PlannerSql2ExactSpec extends
    PlannerHelpers with
    PendingWithActualTracking {

  //to write the new actuals:
  // override val mode = WriteMode

  import Grouped.grouped
  import Reshape.reshape
  import jscore._
  import CollectionUtil._

  import fixExprOp._
  import PlannerHelpers._, expr3_0Fp._, expr3_2Fp._, expr3_4Fp._

  val dsl =
    quasar.qscript.construction.mkDefaults[Fix, fs.MongoQScript[Fix, ?]]
  import dsl._

  val json = Fixed[Fix[EJson]]

  def plan(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    PlannerHelpers.plan(query)

  val specs = List(
    PlanSpec(
      "simple join ($lookup)",
      sqlToWf = Ok,
      sqlE"select smallZips.city from zips join smallZips on zips.`_id` = smallZips.`_id`",
      QsSpec(
        sqlToQs = Ok,
        qsToWf = Ok,
        fix.EquiJoin(
          fix.Unreferenced,
          free.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), qscript.ExcludeId),
          free.ShiftedRead[AFile](rootDir </> dir("db") </> file("smallZips"), qscript.ExcludeId),
          List((
            func.Guard(func.Hole, Type.AnyObject, func.ProjectKeyS(func.Hole, "_id"), func.Undefined),
            func.Guard(func.Hole, Type.AnyObject, func.ProjectKeyS(func.Hole, "_id"), func.Undefined))),
          JoinType.Inner,
          func.Guard(func.RightSide, Type.AnyObject, func.ProjectKeyS(func.RightSide, "city"), func.Undefined))).some,
      chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.Doc(
          BsonField.Name("_id") -> Selector.Exists(true))),
        $project(reshape(JoinDir.Left.name -> $$ROOT)),
        $lookup(
          CollectionName("smallZips"),
          JoinHandler.LeftName \ BsonField.Name("_id"),
          BsonField.Name("_id"),
          JoinHandler.RightName),
        $unwind(DocField(JoinHandler.RightName)),
        $project(
          reshape(sigil.Quasar -> $field(JoinDir.Right.name, "city")),
          ExcludeId))),

    PlanSpec(
      "simple inner equi-join ($lookup)",
      sqlToWf = Ok,
      sqlE"select cars.name, cars2.year from cars join cars2 on cars.`_id` = cars2.`_id`",
      QsSpec(
        sqlToQs = Ok,
        qsToWf = Ok,
        fix.EquiJoin(
          fix.Unreferenced,
          free.ShiftedRead[AFile](rootDir </> dir("db") </> file("cars"), qscript.ExcludeId),
          free.ShiftedRead[AFile](rootDir </> dir("db") </> file("cars2"), qscript.ExcludeId),
          List((
            func.Guard(func.Hole, Type.AnyObject, func.ProjectKeyS(func.Hole, "_id"), func.Undefined),
            func.Guard(func.Hole, Type.AnyObject, func.ProjectKeyS(func.Hole, "_id"), func.Undefined))),
          JoinType.Inner,
          func.ConcatMaps(
            func.MakeMap(
              func.Constant(json.str("name")),
              func.Guard(
                func.Guard(func.LeftSide, Type.AnyObject, func.LeftSide, func.Undefined),
                Type.AnyObject,
                func.Guard(func.LeftSide, Type.AnyObject, func.ProjectKeyS(func.LeftSide, "name"), func.Undefined),
                func.Undefined)),
            func.MakeMap(
              func.Constant(json.str("year")),
              func.Guard(
                func.Guard(func.RightSide, Type.AnyObject, func.RightSide, func.Undefined),
                Type.AnyObject,
                func.Guard(func.RightSide, Type.AnyObject, func.ProjectKeyS(func.RightSide, "year"), func.Undefined),
                func.Undefined))))).some,
      chain[Workflow](
        $read(collection("db", "cars")),
        $match(Selector.Doc(
          BsonField.Name("_id") -> Selector.Exists(true))),
        $project(reshape(JoinDir.Left.name -> $$ROOT)),
        $lookup(
          CollectionName("cars2"),
          JoinHandler.LeftName \ BsonField.Name("_id"),
          BsonField.Name("_id"),
          JoinHandler.RightName),
        $unwind(DocField(JoinHandler.RightName)),
        $project(reshape(
          "name" -> $field(JoinDir.Left.name, "name"),
          "year" -> $field(JoinDir.Right.name, "year")),
          ExcludeId)))
  )

  for (s <- specs) {
    testPlanSpec(s)
  }

  "plan from query string" should {

    "plan simple select *" in {
      plan(sqlE"select * from smallZips") must beWorkflow(
        $read[WorkflowF](collection("db", "smallZips")))
    }

    "plan count(*)" in {
      plan(sqlE"select count(*) from zips") must beWorkflow(
        chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            grouped("f0" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null))),
          $project(
            reshape(sigil.Quasar -> $field("f0")),
            ExcludeId)))
    }

    "plan metal count(*)" in {
      planMetal(sqlE"select count(*) from zips") must beSome("db.zips.count();\n")
    }

    "plan metal count(*) limit 10" in {
      planMetal(sqlE"select count(*) from zips limit 10") must beSome("db.zips.count();\n")
    }

    "plan metal count(*) alias" in {
      planMetal(sqlE"select count(*) as c from zips") must beSome("db.zips.count();\n")
    }

    "plan metal count(*) alias limit" in {
      planMetal(sqlE"select count(*) as c from zips limit 10") must beSome("db.zips.count();\n")
    }

    "plan metal count(*) filter" in {
      planMetal(sqlE"""select count(*) from zips where city = "HAMPDEN" """) must beSome(
        """db.zips.count({ "city": "HAMPDEN" });
          |""".stripMargin)
    }

    "plan metal count(*) filter limit 10" in {
      planMetal(sqlE"""select count(*) from zips where city = "HAMPDEN" limit 10""") must beSome(
        """db.zips.count({ "city": "HAMPDEN" });
          |""".stripMargin)
    }

    "plan metal count(*) filter alias" in {
      planMetal(sqlE"""select count(*) as c from zips where city = "HAMPDEN" """) must beSome(
        """db.zips.count({ "city": "HAMPDEN" });
          |""".stripMargin)
    }

    "plan metal count(*) filter alias limit" in {
      planMetal(sqlE"""select count(*) as c from zips where city = "HAMPDEN" limit 10""") must beSome(
        """db.zips.count({ "city": "HAMPDEN" });
          |""".stripMargin)
    }

    "plan simple field projection on single set" in {
      plan(sqlE"select cars.name from cars") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "cars")),
          $project(
            reshape(sigil.Quasar -> $field("name")),
            ExcludeId)))
    }

    "plan simple field projection on single set when table name is inferred" in {
      plan(sqlE"select name from cars") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "cars")),
         $project(
           reshape(sigil.Quasar -> $field("name")),
           ExcludeId)))
    }

    "plan multiple field projection on single set when table name is inferred" in {
      plan(sqlE"select name, year from cars") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "cars")),
         $project(
           reshape(
             "name" -> $field("name"),
             "year" -> $field("year")),
           ExcludeId)))
    }

    "plan simple addition on two fields" in {
      plan(sqlE"select val1 + val2 from divide") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "divide")),
         $project(
           reshape(sigil.Quasar ->
             $cond(
               $and(
                 $lt($literal(Bson.Null), $field("val2")),
                 $lt($field("val2"), $literal(Bson.Text("")))),
               $cond(
                 $or(
                   $and(
                     $lt($literal(Bson.Null), $field("val1")),
                     $lt($field("val1"), $literal(Bson.Text("")))),
                   $and(
                     $lte($literal(Check.minDate), $field("val1")),
                     $lt($field("val1"), $literal(Bson.Regex("", ""))))),
                 $add($field("val1"), $field("val2")),
                 $literal(Bson.Undefined)),
               $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan concat (3.0-)" in {
      plan3_0(sqlE"select concat(city, state) from extraSmallZips") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "extraSmallZips")),
         $project(
           reshape(sigil.Quasar ->
             $cond(
               $and(
                 $lte($literal(Bson.Text("")), $field("state")),
                 $lt($field("state"), $literal(Bson.Doc()))),
               $cond(
                 $and(
                   $lte($literal(Bson.Text("")), $field("city")),
                   $lt($field("city"), $literal(Bson.Doc()))),
                 $concat($field("city"), $field("state")),
                 $literal(Bson.Undefined)),
               $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan concat (3.2+)" in {
      plan3_2(sqlE"select concat(city, state) from extraSmallZips") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "extraSmallZips")),
         $project(
           reshape(sigil.Quasar ->
             $cond(
               $and(
                 $lte($literal(Bson.Text("")), $field("state")),
                 $lt($field("state"), $literal(Bson.Doc()))),
               $cond(
                 $and(
                   $lte($literal(Bson.Text("")), $field("city")),
                   $lt($field("city"), $literal(Bson.Doc()))),
                 $let(ListMap(DocVar.Name("a1") -> $field("city"), DocVar.Name("a2") -> $field("state")),
                   $cond($and($isArray($field("$a1")), $isArray($field("$a2"))),
                     $concatArrays(List($field("$a1"), $field("$a2"))),
                     $concat($field("$a1"), $field("$a2")))),
                 $literal(Bson.Undefined)),
               $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan concat strings with ||" in {
      plan(sqlE"""select city || ", " || state from extraSmallZips""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "extraSmallZips")),
         $project(
           reshape(
             sigil.Quasar ->
               $cond(
                 $or(
                   $and(
                     $lte($literal(Bson.Arr()), $field("state")),
                     $lt($field("state"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                   $and(
                     $lte($literal(Bson.Text("")), $field("state")),
                     $lt($field("state"), $literal(Bson.Doc())))),
                 $cond(
                   $or(
                     $and(
                       $lte($literal(Bson.Arr()), $field("city")),
                       $lt($field("city"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                     $and(
                       $lte($literal(Bson.Text("")), $field("city")),
                       $lt($field("city"), $literal(Bson.Doc())))),
                   $let( // TODO: ideally, this would be a single $concat
                     ListMap(
                       DocVar.Name("a1") ->
                         $let(
                           ListMap(
                             DocVar.Name("a1") -> $field("city"),
                             DocVar.Name("a2") -> $literal(Bson.Text(", "))),
                           $cond($and($isArray($field("$a1")), $isArray($field("$a2"))),
                             $concatArrays(List($field("$a1"), $field("$a2"))),
                             $concat($field("$a1"), $field("$a2")))),
                       DocVar.Name("a2") -> $field("state")),
                     $cond($and($isArray($field("$a1")), $isArray($field("$a2"))),
                       $concatArrays(List($field("$a1"), $field("$a2"))),
                       $concat($field("$a1"), $field("$a2")))),
                   $literal(Bson.Undefined)),
                 $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan concat strings with ||, constant on the right" in {
      plan(sqlE"""select city || state || "..." from extraSmallZips""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "extraSmallZips")),
         $project(
           reshape(
             "0" ->
               $cond(
                 $or(
                   $and(
                     $lte($literal(Bson.Arr()), $field("state")),
                     $lt($field("state"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                   $and(
                     $lte($literal(Bson.Text("")), $field("state")),
                     $lt($field("state"), $literal(Bson.Doc())))),
                 $cond(
                   $or(
                     $and(
                       $lte($literal(Bson.Arr()), $field("city")),
                       $lt($field("city"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                     $and(
                       $lte($literal(Bson.Text("")), $field("city")),
                       $lt($field("city"), $literal(Bson.Doc())))),
                   $concat( // TODO: ideally, this would be a single $concat
                     $concat($field("city"), $field("state")),
                     $literal(Bson.Text("..."))),
                   $literal(Bson.Undefined)),
                 $literal(Bson.Undefined))),
           ExcludeId)))
    }.pendingUntilFixed("SD-639")

    "plan concat with unknown types" in {
      plan(sqlE"select city || state from extraSmallZips") must
        beRight
    }.pendingUntilFixed("SD-639")

    "plan lower" in {
      plan(sqlE"select lower(city) from extraSmallZips") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "extraSmallZips")),
        $project(
          reshape(sigil.Quasar ->
            $cond(
              $and(
                $lte($literal(Bson.Text("")), $field("city")),
                $lt($field("city"), $literal(Bson.Doc()))),
              $toLower($field("city")),
              $literal(Bson.Undefined))),
          ExcludeId)))
    }

    "plan coalesce" in {
      plan(sqlE"select coalesce(val, name) from nullsWithMissing") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "nullsWithMissing")),
         $project(
           reshape(sigil.Quasar ->
             $cond(
               $eq($field("val"), $literal(Bson.Null)),
               $field("name"),
               $field("val"))),
           ExcludeId)))
    }

    "plan select array" in {
      plan(sqlE"select [city, loc, pop] from extraSmallZips") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "extraSmallZips")),
         $project(
           reshape(sigil.Quasar ->
             $arrayLit(List($field("city"), $field("loc"), $field("pop")))),
           ExcludeId)))
    }

    "plan select array (map-reduce)" in {
      plan2_6(sqlE"select [city, loc, pop] from extraSmallZips") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "extraSmallZips")),
         $simpleMap(
           NonEmptyList(MapExpr(JsFn(Name("x"),
             underSigil(Arr(List(Select(ident("x"), "city"), Select(ident("x"), "loc"), Select(ident("x"), "pop"))))))),
             ListMap())))
    }

    "plan select map" in {
      plan(sqlE"""select { "p": pop } from extraSmallZips""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "extraSmallZips")),
         $project(
           reshape("p" -> $field("pop")),
           ExcludeId)))
    }

    "plan select map with field" in {
      plan(sqlE"""select { "p": pop }, state from extraSmallZips""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "extraSmallZips")),
         $project(
           reshape(
             "0" -> objectLit("p" -> $field("pop")),
             "state" -> $field("state")),
           ExcludeId)))
    }

    "plan now() with a literal timestamp" in {
      val time = Instant.parse("2016-08-25T00:00:00.000Z")
      val bsTime = Bson.Date.fromInstant(time).get

      planAt(time, sqlE"""select NOW(), name from cars""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "cars")),
         $project(
           reshape("0" -> $literal(bsTime), "name" -> $field("name")))))
    }

    "plan date field extraction" in {
      plan(sqlE"""select date_part("day", ts) from days""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "days")),
         $project(
           reshape(sigil.Quasar ->
             $cond(
               $and(
                 $lte($literal(Check.minDate), $field("ts")),
                 $lt($field("ts"), $literal(Bson.Regex("", "")))),
               $dayOfMonth($field("ts")),
               $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan complex date field extraction" in {
      plan(sqlE"""select date_part("quarter", ts) from days""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "days")),
         $project(
           reshape(
             sigil.Quasar ->
               $cond(
                 $and(
                   $lte($literal(Check.minDate), $field("ts")),
                   $lt($field("ts"), $literal(Bson.Regex("", "")))),
                 $trunc(
                   $add(
                     $divide(
                       $subtract($month($field("ts")), $literal(Bson.Int32(1))),
                       $literal(Bson.Int32(3))),
                     $literal(Bson.Int32(1)))),
                 $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan date field extraction: \"dow\"" in {
      plan(sqlE"""select date_part("dow", ts) from days""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "days")),
         $project(
           reshape(
             sigil.Quasar ->
               $cond(
                 $and(
                   $lte($literal(Check.minDate), $field("ts")),
                   $lt($field("ts"), $literal(Bson.Regex("", "")))),
                 $subtract($dayOfWeek($field("ts")), $literal(Bson.Int32(1))),
                 $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan date field extraction: \"isodow\"" in {
      plan(sqlE"""select date_part("isodow", ts) from days""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "days")),
         $project(
           reshape(
             sigil.Quasar ->
               $cond(
                 $and(
                   $lte($literal(Check.minDate), $field("ts")),
                   $lt($field("ts"), $literal(Bson.Regex("", "")))),
                 $cond($eq($dayOfWeek($field("ts")), $literal(Bson.Int32(1))),
                   $literal(Bson.Int32(7)),
                   $subtract($dayOfWeek($field("ts")), $literal(Bson.Int32(1)))),
                 $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan filter by date field (SD-1508)" in {
      plan(sqlE"""select * from days where date_part("year", ts) = 2016""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "days")),
         $match(Selector.Doc(BsonField.Name("ts") -> Selector.Type(BsonType.Date))),
         $project(
           reshape(
             "0"   -> $year($field("ts")),
             "src" -> $$ROOT),
           ExcludeId),
         $match(Selector.Doc(
           BsonField.Name("0") -> Selector.Eq(Bson.Int32(2016)))),
         $project(
           reshape(
             sigil.Quasar -> $field("src")),
           ExcludeId)))
    }

    "plan filter array element" in {
      plan(sqlE"select loc from extraSmallZips where loc[0] < -73") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "extraSmallZips")),
        $match(Selector.Doc(BsonField.Name("loc") -> Selector.ElemMatch(Selector.Exists(true).right))),
        $project(
          reshape("0" -> $arrayElemAt($field("loc"), $literal(Bson.Int32(0))), "src" -> $$ROOT),
          ExcludeId),
        $match(Selector.Doc(BsonField.Name("0") -> Selector.Lt(Bson.Int32(-73)))),
        $project(
          reshape(sigil.Quasar -> $field("src", "loc")),
          ExcludeId)))
    }

    "plan select array element (3.2+)" in {
      plan3_2(sqlE"select loc[0] from extraSmallZips") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "extraSmallZips")),
        $project(
          reshape(
            sigil.Quasar ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("loc")),
                  $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                $arrayElemAt($field("loc"), $literal(Bson.Int32(0))),
                $literal(Bson.Undefined))),
          ExcludeId)))
    }

    "plan array length" in {
      plan(sqlE"select array_length(loc, 1) from extraSmallZips") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "extraSmallZips")),
        $project(
          reshape(sigil.Quasar ->
            $cond(
              $and($lte($literal(Bson.Arr()), $field("loc")),
                $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
              $size($field("loc")),
              $literal(Bson.Undefined))),
          ExcludeId)))
    }

    "plan array length 3.2" in {
      plan3_2(sqlE"select array_length(loc, 1) from extraSmallZips") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "extraSmallZips")),
         $simpleMap(
           NonEmptyList(MapExpr(JsFn(Name("x"),
             underSigil(If(
               Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "loc"))),
               Call(ident("NumberLong"), List(Select(Select(ident("x"), "loc"), "length"))),
               ident(Js.Undefined.ident)))))),
             ListMap())))
    }

    "plan array flatten" in {
      plan(sqlE"select loc[*] from zips") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "0" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Arr(List())), $field("loc")),
                      $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                    $field("loc"),
                    $arrayLit(List($literal(Bson.Undefined))))),
              ExcludeId),
            $unwind(DocField(BsonField.Name("0"))),
            $project(
              reshape(sigil.Quasar -> $field("0")),
              ExcludeId))
        }
    }

    "plan array concat" in {
      plan(sqlE"select loc || [ 0, 1, 2 ] from zips") must beWorkflow0 {
        chain[Workflow](
          $read(collection("db", "zips")),
          $project(
            reshape(
              sigil.Quasar -> $cond(
                $or(
                  $and(
                    $lte($literal(Bson.Arr(List())), $field("loc")),
                    $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                  $and(
                    $lte($literal(Bson.Text("")), $field("loc")),
                    $lt($field("loc"), $literal(Bson.Doc())))),
                $let(ListMap(
                  DocVar.Name("a1") -> $field("loc"),
                  DocVar.Name("a2") -> $literal(Bson.Arr(Bson.Int32(0), Bson.Int32(1), Bson.Int32(2)))),
                  $cond($and($isArray($field("$a1")), $isArray($field("$a2"))),
                    $concatArrays(List($field("$a1"), $field("$a2"))),
                    $concat($field("$a1"), $field("$a2")))),
                $literal(Bson.Undefined))),
            ExcludeId))
      }
    }

    "plan sum in expression" in {
      plan(sqlE"select sum(pop) * 100 from zips") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          grouped("f0" ->
            $sum(
              $cond(
                $and(
                  $lt($literal(Bson.Null), $field("pop")),
                  $lt($field("pop"), $literal(Bson.Text("")))),
                $field("pop"),
                $literal(Bson.Undefined)))),
          \/-($literal(Bson.Null))),
        $project(
          reshape(sigil.Quasar -> $multiply($field("f0"), $literal(Bson.Int32(100)))),
          ExcludeId)))
    }

    "plan conditional" in {
      plan(sqlE"select case when pop < 10000 then city else loc end from extraSmallZips") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "extraSmallZips")),
         $project(
           reshape(
             sigil.Quasar ->
               $cond(
                 $or(
                   $and(
                     $lt($literal(Bson.Null), $field("pop")),
                     $lt($field("pop"), $literal(Bson.Doc()))),
                   $and(
                     $lte($literal(Bson.Bool(false)), $field("pop")),
                     $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                 $cond($lt($field("pop"), $literal(Bson.Int32(10000))),
                   $field("city"),
                   $field("loc")),
                 $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan negate" in {
      plan(sqlE"select -val1 from divide") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "divide")),
         $project(
           reshape(sigil.Quasar ->
             $cond(
               $and(
                 $lt($literal(Bson.Null), $field("val1")),
                 $lt($field("val1"), $literal(Bson.Text("")))),
               $multiply($literal(Bson.Int32(-1)), $field("val1")),
               $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan simple filter" in {
      plan(sqlE"select * from extraSmallZips where pop > 10000") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "extraSmallZips")),
         $match(Selector.And(
           isNumeric(BsonField.Name("pop")),
           Selector.Doc(
             BsonField.Name("pop") -> Selector.Gt(Bson.Int32(10000)))))))
    }

    "plan simple reversed filter" in {
      plan(sqlE"select * from extraSmallZips where 10000 < pop") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "extraSmallZips")),
         $match(Selector.And(
           isNumeric(BsonField.Name("pop")),
           Selector.Doc(
             BsonField.Name("pop") -> Selector.Gt(Bson.Int32(10000)))))))
    }

    "plan simple filter with expression in projection" in {
      plan(sqlE"select val1 + val3 from divide where val2 > 2") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "divide")),
         $match(Selector.And(
           isNumeric(BsonField.Name("val2")),
           Selector.Doc(
             BsonField.Name("val2") -> Selector.Gt(Bson.Int32(2))))),
         $project(
           reshape(sigil.Quasar ->
             $cond(
               $and(
                 $lt($literal(Bson.Null), $field("val3")),
                 $lt($field("val3"), $literal(Bson.Text("")))),
               $cond(
                 $or(
                   $and(
                     $lt($literal(Bson.Null), $field("val1")),
                     $lt($field("val1"), $literal(Bson.Text("")))),
                   $and(
                     $lte($literal(Check.minDate), $field("val1")),
                     $lt($field("val1"), $literal(Bson.Regex("", ""))))),
                 $add($field("val1"), $field("val3")),
                 $literal(Bson.Undefined)),
               $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan simple js filter 3.2" in {
      plan3_2(sqlE"select * from zips where length(city) < 4") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.Doc(BsonField.Name("city") -> Selector.Type(BsonType.Text))),

        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
            "0" -> Call(ident("NumberLong"), List(Select(Select(ident("x"), "city"), "length"))),
            "src" -> ident("x")) ))),
          ListMap()),
        $match(Selector.Doc(BsonField.Name("0") -> Selector.Lt(Bson.Int32(4)))),
        $project(
          reshape(sigil.Quasar -> $field("src")),
          ExcludeId)))
    }

    "plan filter with js and non-js 3.2" in {
      plan3_2(sqlE"select * from zips where length(city) < 4 and pop < 20000") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.And(
          Selector.Or(
            Selector.Doc(BsonField.Name("pop") -> Selector.Type(BsonType.Int32)),
            Selector.Doc(BsonField.Name("pop") -> Selector.Type(BsonType.Int64)),
            Selector.Doc(BsonField.Name("pop") -> Selector.Type(BsonType.Dec)),
            Selector.Doc(BsonField.Name("pop") -> Selector.Type(BsonType.Text)),
            Selector.Doc(BsonField.Name("pop") -> Selector.Type(BsonType.Date)),
            Selector.Doc(BsonField.Name("pop") -> Selector.Type(BsonType.Bool))),
          Selector.Doc(BsonField.Name("city") -> Selector.Type(BsonType.Text)))),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
            "0" -> Call(ident("NumberLong"), List(Select(Select(ident("x"), "city"), "length"))),
            "1" -> Select(ident("x"), "pop"),
            "src" -> ident("x"))))),
          ListMap()),
        $match(
          Selector.And(
            Selector.Doc(BsonField.Name("0") -> Selector.Lt(Bson.Int32(4))),
            Selector.Doc(BsonField.Name("1") -> Selector.Lt(Bson.Int32(20000))))),
        $project(
          reshape(sigil.Quasar -> $field("src")),
          ExcludeId)))
    }

    "plan filter with between" in {
      plan(sqlE"select * from extraSmallZips where pop between 10000 and 12000") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "extraSmallZips")),
         $match(
           Selector.And(
             isNumeric(BsonField.Name("pop")),
             Selector.And(
               Selector.Doc(
                 BsonField.Name("pop") -> Selector.Gte(Bson.Int32(10000))),
               Selector.Doc(
                 BsonField.Name("pop") -> Selector.Lte(Bson.Int32(12000))))))))
    }

    "plan filter with like" in {
      plan(sqlE"""select * from extraSmallZips where city like "A.%" """) must
       beWorkflow(chain[Workflow](
         $read(collection("db", "extraSmallZips")),
         $match(Selector.And(
           Selector.Doc(BsonField.Name("city") ->
             Selector.Type(BsonType.Text)),
           Selector.Doc(
             BsonField.Name("city") ->
               Selector.Regex("^A\\..*$", false, true, false, false))))))
    }

    "plan filter with LIKE and OR" in {
      plan(sqlE"""select * from smallZips where city like "A%" or city like "Z%" """) must
       beWorkflow(chain[Workflow](
         $read(collection("db", "smallZips")),
         $match(
           Selector.And(
             Selector.Or(
               Selector.Doc(
                 BsonField.Name("city") -> Selector.Type(BsonType.Text)),
               Selector.Doc(
                 BsonField.Name("city") -> Selector.Type(BsonType.Text))),
             Selector.Or(
               Selector.Doc(
                 BsonField.Name("city") -> Selector.Regex("^A.*$", false, true, false, false)),
               Selector.Doc(
                 BsonField.Name("city") -> Selector.Regex("^Z.*$", false, true, false, false)))))))
    }

    "plan filter with field in constant set" in {
      plan(sqlE"""select * from smallZips where state in ("AZ", "CO")""") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "smallZips")),
          $match(Selector.Doc(BsonField.Name("state") ->
            Selector.In(Bson.Arr(List(Bson.Text("AZ"), Bson.Text("CO"))))))))
    }

    "plan filter with field containing constant value" in {
      plan(sqlE"select * from zips where 43.058514 in loc[_]") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.And(
            Selector.Doc(BsonField.Name("loc") -> Selector.ElemMatch(Selector.Exists(true).right)),
            Selector.Doc(BsonField.Name("loc") -> Selector.ElemMatch(Selector.In(Bson.Arr(List(Bson.Dec(43.058514)))).right))
          ))))
    }

    "filter field in single-element set" in {
      plan(sqlE"""select * from zips where state in ("NV")""") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.Doc(BsonField.Name("state") ->
            Selector.Eq(Bson.Text("NV"))))))
    }

    "filter field “in” a bare value" in {
      plan(sqlE"""select * from zips where state in "PA"""") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.Doc(BsonField.Name("state") ->
            Selector.Eq(Bson.Text("PA"))))))
    }

    "plan filter with field containing other field" in {
      import jscore._
      plan(sqlE"select * from zips where pop in loc[_]") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.Doc(BsonField.Name("loc") -> Selector.ElemMatch(Selector.Exists(true).right))),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
            "0" ->  BinOp(jscore.Neq, Literal(Js.Num(-1, false)), Call(Select(Select(ident("x"), "loc"), "indexOf"), List(Select(ident("x"), "pop")))),
            "src" -> ident("x"))))), ListMap()),
          $match(Selector.Doc(BsonField.Name("0") -> Selector.Eq(Bson.Bool(true)))),
          $project(
            reshape(sigil.Quasar -> $field("src")),
            ExcludeId)))
    }

    "plan filter with ~" in {
      plan(sqlE"""select * from zips where city ~ "^B[AEIOU]+LD.*" """) must beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("city") -> Selector.Type(BsonType.Text)),
          Selector.Doc(
            BsonField.Name("city") -> Selector.Regex("^B[AEIOU]+LD.*", false, true, false, false))))))
    }

    "plan filter with ~*" in {
      plan(sqlE"""select * from zips where city ~* "^B[AEIOU]+LD.*" """) must beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("city") -> Selector.Type(BsonType.Text)),
          Selector.Doc(
            BsonField.Name("city") -> Selector.Regex("^B[AEIOU]+LD.*", true, true, false, false))))))
    }

    "plan filter with !~" in {
      plan(sqlE"""select * from zips where city !~ "^B[AEIOU]+LD.*" """) must beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("city") -> Selector.Type(BsonType.Text)),
          Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
            BsonField.Name("city") -> Selector.NotExpr(Selector.Regex("^B[AEIOU]+LD.*", false, true, false, false))))))))
    }

    "plan filter with !~*" in {
      plan(sqlE"""select * from zips where city !~* "^B[AEIOU]+LD.*" """) must beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("city") -> Selector.Type(BsonType.Text)),
          Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
            BsonField.Name("city") -> Selector.NotExpr(Selector.Regex("^B[AEIOU]+LD.*", true, true, false, false))))))))
    }

    "plan filter with alternative ~" in {
      plan(sqlE"""select * from a where "foo" ~ pattern or target ~ pattern""") must beWorkflow0(chain[Workflow](
        $read(collection("db", "a")),
        $match(
          Selector.Or(
            Selector.Doc(BsonField.Name("pattern") -> Selector.Type(BsonType.Text)),
            Selector.And(
              Selector.Doc(BsonField.Name("pattern") -> Selector.Type(BsonType.Text)),
              Selector.Doc(BsonField.Name("target") -> Selector.Type(BsonType.Text))))),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "0" -> Call(
            Select(New(Name("RegExp"), List(Select(ident("x"), "pattern"), jscore.Literal(Js.Str("m")))), "test"),
            List(jscore.Literal(Js.Str("foo")))),
          "1" -> Call(
            Select(New(Name("RegExp"), List(Select(ident("x"), "pattern"), jscore.Literal(Js.Str("m")))), "test"),
            List(Select(ident("x"), "target"))),
          "src" -> ident("x"))))),
          ListMap()),
        $match(Selector.Or(
          Selector.Doc(BsonField.Name("0") -> Selector.Eq(Bson.Bool(true))),
          Selector.Doc(BsonField.Name("1") -> Selector.Eq(Bson.Bool(true))))),
        $project(
          reshape(sigil.Quasar -> $field("src")),
          ExcludeId)))
    }

    "plan filter with negate(s)" in {
      plan(sqlE"select * from foo where bar != -10 and baz > -1.0") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $match(
           Selector.And(
             isNumeric(BsonField.Name("baz")),
             Selector.And(
               Selector.Doc(
                 BsonField.Name("bar") -> Selector.Neq(Bson.Int32(-10))),
               Selector.Doc(
                 BsonField.Name("baz") -> Selector.Gt(Bson.Dec(-1.0))))))))
    }

    "plan complex filter" in {
      plan(sqlE"""select * from foo where bar > 10 and (baz = "quux" or foop = "zebra")""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $match(
           Selector.And(
             isNumeric(BsonField.Name("bar")),
             Selector.And(
               Selector.Doc(BsonField.Name("bar") -> Selector.Gt(Bson.Int32(10))),
               Selector.Or(
                 Selector.Doc(
                   BsonField.Name("baz") -> Selector.Eq(Bson.Text("quux"))),
                 Selector.Doc(
                   BsonField.Name("foop") -> Selector.Eq(Bson.Text("zebra")))))))))
    }

    "plan filter with not" in {
      plan(sqlE"select * from zips where not (pop > 0 and pop < 1000)") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "zips")),
         $match(
           Selector.And(
             // TODO: eliminate duplication
             isNumeric(BsonField.Name("pop")),
             Selector.And(
               isNumeric(BsonField.Name("pop")),
               Selector.Or(
                 Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                   BsonField.Name("pop") -> Selector.NotExpr(Selector.Gt(Bson.Int32(0))))),
                 Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                   BsonField.Name("pop") -> Selector.NotExpr(Selector.Lt(Bson.Int32(1000)))))))))))
    }

    "plan filter with not and equality" in {
      plan(sqlE"select * from zips where not (pop = 0)") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(
            Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("pop") -> Selector.NotExpr(Selector.Eq(Bson.Int32(0))))))))
    }

    "plan filter with \"is not null\"" in {
      plan(sqlE"select * from zips where city is not null") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(
            Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("city") -> Selector.Expr(Selector.Neq(Bson.Null)))))))
    }

    "filter on constant true" in {
      plan(sqlE"select * from zips where true") must
        beWorkflow($read(collection("db", "zips")))
    }

    "drop nothing" in {
      plan(sqlE"select * from zips limit 5 offset 0") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $limit(5)))
    }

    "concat with empty string" in {
      plan(sqlE"""select "" || city || "" from zips""") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $project(
            reshape(sigil.Quasar ->
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
            ExcludeId)))
    }

    "plan simple sort with field in projection" in {
      plan(sqlE"select bar from foo order by bar") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "foo")),
          $sort(NonEmptyList(BsonField.Name("bar") -> SortDir.Ascending)),
          $project(
            reshape(sigil.Quasar -> $field("bar")),
            ExcludeId)))
    }

    "plan simple sort with wildcard" in {
      plan(sqlE"select * from zips order by pop") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $sort(NonEmptyList(BsonField.Name("pop") -> SortDir.Ascending))))
    }

    "plan sort with expression in key" in {
      plan(sqlE"select baz from foo order by bar/10") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "foo")),
          $project(
            reshape(
              "0" ->
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("bar")),
                    $lt($field("bar"), $literal(Bson.Text("")))),
                  divide($field("bar"), $literal(Bson.Int32(10))),
                  $literal(Bson.Undefined)),
              "src"    -> $$ROOT),
            ExcludeId),
          $sort(NonEmptyList(BsonField.Name("0") -> SortDir.Ascending)),
          $project(
            reshape("baz" -> $field("src", "baz")),
            ExcludeId)))
    }

    "plan select with wildcard and field" in {
      plan(sqlE"select *, pop from zips") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              underSigil(SpliceObjects(List(
                ident("x"),
                obj(
                  "pop" -> Select(ident("x"), "pop")))))))),
            ListMap())))
    }

    "plan select with wildcard and two fields" in {
      plan(sqlE"select *, city as city2, pop as pop2 from zips") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              obj(sigil.Quasar -> SpliceObjects(List(
                ident("x"),
                obj(
                  "city2" -> Select(ident("x"), "city")),
                obj(
                  "pop2"  -> Select(ident("x"), "pop")))))))),
            ListMap())))
    }

    "plan select with wildcard and two constants" in {
      plan(sqlE"""select *, "1", "2" from zips""") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              obj(sigil.Quasar -> SpliceObjects(List(
                ident("x"),
                obj(
                  "1" -> jscore.Literal(Js.Str("1"))),
                obj(
                  "2" -> jscore.Literal(Js.Str("2"))))))))),
            ListMap())))
    }

    "plan select with multiple wildcards and fields" in {
      plan(sqlE"select state as state2, *, city as city2, *, pop as pop2 from zips where pop < 1000") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.And(
            isNumeric(BsonField.Name("pop")),
            Selector.Doc(
              BsonField.Name("pop") -> Selector.Lt(Bson.Int32(1000))))),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              obj(sigil.Quasar -> SpliceObjects(List(
                  obj(
                    "state2" -> Select(ident("x"), "state")),
                  ident("x"),
                  obj(
                    "city2" -> Select(ident("x"), "city")),
                  ident("x"),
                  obj(
                    "pop2" -> Select(ident("x"), "pop")))))))),
            ListMap())))
    }

    "plan simple sort with field not in projections" in {
      plan(sqlE"select city from zips order by pop") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $sort(NonEmptyList(BsonField.Name("pop") -> SortDir.Ascending)),
          $project(
            reshape("city" -> $field("city")),
            ExcludeId)))
    }

    "plan sort with filter" in {
      plan(sqlE"select city, pop from zips where pop <= 1000 order by pop desc, city") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.And(
            isNumeric(BsonField.Name("pop")),
            Selector.Doc(
              BsonField.Name("pop") -> Selector.Lte(Bson.Int32(1000))))),
          $project(
            reshape(
              "city" -> $field("city"),
              "pop"  -> $field("pop")),
            ExcludeId),
          $sort(NonEmptyList(
            BsonField.Name("pop") -> SortDir.Descending,
            BsonField.Name("city") -> SortDir.Ascending))))
    }

    "plan multiple column sort with wildcard" in {
      plan(sqlE"select * from zips order by pop, city desc") must
       beWorkflow0(chain[Workflow](
         $read(collection("db", "zips")),
         $sort(NonEmptyList(
           BsonField.Name("pop") -> SortDir.Ascending,
           BsonField.Name("city") -> SortDir.Descending))))
    }

    "plan many sort columns" in {
      plan(sqlE"select * from zips order by pop, state, city, a4, a5, a6") must
       beWorkflow0(chain[Workflow](
         $read(collection("db", "zips")),
         $sort(NonEmptyList(
           BsonField.Name("pop") -> SortDir.Ascending,
           BsonField.Name("state") -> SortDir.Ascending,
           BsonField.Name("city") -> SortDir.Ascending,
           BsonField.Name("a4") -> SortDir.Ascending,
           BsonField.Name("a5") -> SortDir.Ascending,
           BsonField.Name("a6") -> SortDir.Ascending))))
    }

    "plan efficient count and field ref" in {
      plan(sqlE"SELECT city, COUNT(*) AS cnt FROM zips ORDER BY cnt DESC") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "city" -> $push($field("city")),
                "cnt"  -> $sum($literal(Bson.Int32(1)))),
              \/-($literal(Bson.Null))),
            $unwind(DocField("city")),
            $sort(NonEmptyList(BsonField.Name("cnt") -> SortDir.Descending)))
        }
    }.pendingWithActual(notOnPar, testFile("plan efficient count and field ref"))

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
    }.pendingUntilFixed

    "plan trivial group by" in {
      plan(sqlE"select city from zips group by city") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          grouped(),
          -\/(reshape("0" -> $field("city")))),
        $project(
          reshape(sigil.Quasar -> $field("_id", "0")),
          ExcludeId)))
    }

    "plan useless group by expression" in {
      plan(sqlE"select city from zips group by lower(city)") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(sigil.Quasar -> $field("city")),
          ExcludeId)))
    }

    "plan trivial group by with wildcard" in {
      plan(sqlE"select * from zips group by city") must
        beWorkflow($read(collection("db", "zips")))
    }

    "plan count grouped by single field" in {
      plan(sqlE"select count(*) from zips group by state") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped("f0" -> $sum($literal(Bson.Int32(1)))),
              -\/(reshape("0" -> $field("state")))),
            $project(
              reshape(
                sigil.Quasar -> $field("f0")),
              ExcludeId))
        }
    }

    "plan sum grouped by single field with filter" in {
      plan(sqlE"""select sum(pop) as sm from zips where state="CO" group by city""") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $match(Selector.Doc(
              BsonField.Name("state") -> Selector.Eq(Bson.Text("CO")))),
            $group(
              grouped("f0" ->
                $sum(
                  $cond(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $field("pop"),
                    $literal(Bson.Undefined)))),
              -\/(reshape("0" -> $field("city")))),
            $project(reshape("sm" -> $field("f0")), ExcludeId))
        }
    }

    "plan count and field when grouped" in {
      plan(sqlE"select count(*) as cnt, city from zips group by city") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "f0"  -> $sum($literal(Bson.Int32(1)))),
              -\/(reshape("0" -> $field("city")))),
            $project(
              reshape(
                "cnt" -> $field("f0"),
                "city" -> $field("_id", "0")),
              ExcludeId))
        }
    }

    "plan aggregation on grouped field" in {
      plan(sqlE"select city, count(city) from zips group by city") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "f1"  -> $sum($literal(Bson.Int32(1)))),
              -\/(reshape("0" -> $field("city")))),
            $project(
              reshape(
                "city" -> $field("_id", "0"),
                "1"    -> $field("f1")),
              ExcludeId))
        }
    }

    "plan simple having filter" in {
      val actual = plan(sqlE"select city from zips group by city having count(*) > 10")
      val expected = (chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          grouped(
            "__tmp1" -> $sum($literal(Bson.Int32(1)))),
          -\/(reshape("0" -> $field("city")))),
        $match(Selector.Doc(
          BsonField.Name("__tmp1") -> Selector.Gt(Bson.Int32(10)))),
        $project(
          reshape(sigil.Quasar -> $field("_id", "0")),
          ExcludeId)))

      skipped("#3021")
    }

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

    "plan array project with concat (3.0-)" in {
      plan3_0(sqlE"select city, loc[0] from zips") must
        beWorkflow {
          chain[Workflow](
            $read(collection("db", "zips")),
            $simpleMap(
              NonEmptyList(
                MapExpr(JsFn(Name("x"), obj(
                  "city" -> Select(ident("x"), "city"),
                  "1" ->
                    If(Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "loc"))),
                      Access(Select(ident("x"), "loc"), jscore.Literal(Js.Num(0, false))),
                      ident("undefined")))))),
              ListMap()),
            $project(
              reshape(
                "city" -> $include(),
                "1"    -> $include()),
              ExcludeId))
        }
    }

    "plan array project with concat (3.2+)" in {
      plan3_2(sqlE"select city, loc[0] from zips") must
        beWorkflow {
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "city" -> $field("city"),
                "1"    ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Arr(List())), $field("loc")),
                      $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                    $arrayElemAt($field("loc"), $literal(Bson.Int32(0))),
                    $literal(Bson.Undefined))),
              ExcludeId))
        }
    }

    "plan limit with offset" in {
      plan(sqlE"SELECT * FROM zips OFFSET 100 LIMIT 5") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $limit(105),
          $skip(100)))
    }

    "plan sort and limit" in {
      plan(sqlE"SELECT city, pop FROM zips ORDER BY pop DESC LIMIT 5") must
        beWorkflow {
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "city" -> $field("city"),
                "pop"  -> $field("pop")),
              ExcludeId),
            $sort(NonEmptyList(BsonField.Name("pop") -> SortDir.Descending)),
            $limit(5))
        }
    }

    "plan simple single field selection and limit" in {
      plan(sqlE"SELECT city FROM zips LIMIT 5") must
        beWorkflow {
          chain[Workflow](
            $read(collection("db", "zips")),
            $limit(5),
            $project(
              reshape(sigil.Quasar -> $field("city")),
              ExcludeId))
        }
    }

    "plan filter and expressions with IS NULL" in {
      plan(sqlE"select foo is null from zips where foo is null") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.Doc(
            BsonField.Name("foo") -> Selector.Eq(Bson.Null))),
          $project(
            reshape(sigil.Quasar -> $eq($field("foo"), $literal(Bson.Null))),
            ExcludeId)))
    }

    "plan simple distinct" in {
      plan(sqlE"select distinct city, state from zips") must
      beWorkflow0(
        chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            grouped(),
            -\/(reshape(
              "0" -> objectLit("city" -> $field("city"), "state" -> $field("state"))))),
          $project(
            reshape(
              sigil.Quasar -> $field("_id", "0")),
            ExcludeId)))
    }

    "plan distinct of wildcard" in {
      plan(sqlE"select distinct * from zips") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
            Call(ident("remove"),
              List(ident("x"), jscore.Literal(Js.Str("_id"))))))),
            ListMap()),
          $group(
            grouped(),
            -\/(reshape("0" -> $$ROOT))),
          $project(
            reshape(sigil.Quasar -> $field("_id", "0")),
            ExcludeId)))
    }

    "plan distinct of wildcard as expression" in {
      plan(sqlE"select count(distinct *) from zips") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
            Call(ident("remove"),
              List(ident("x"), jscore.Literal(Js.Str("_id")))) ))),
            ListMap()),
          $group(
            grouped(),
            -\/(reshape("0" -> $$ROOT))),
          $group(
            grouped("f0" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null))),
          $project(
            reshape(sigil.Quasar -> $field("f0")),
            ExcludeId)))
    }

    "plan distinct with simple order by" in {
      plan(sqlE"select distinct city from zips order by city") must
        beWorkflow0(
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(),
              -\/(reshape("0" -> $field("city")))),
            $project(
              reshape("f0" -> $field("_id", "0")),
              IgnoreId),
            $sort(NonEmptyList(BsonField.Name("f0") -> SortDir.Ascending)),
            $project(
              reshape(sigil.Quasar-> $field("f0")),
              ExcludeId)))
    }

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
    }.pendingUntilFixed

    "plan simple sort on map-reduce with mapBeforeSort" in {
      plan3_2(sqlE"select length(city) from zips order by city") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), Arr(List(
              obj("0" -> If(
                Call(ident("isString"), List(Select(ident("x"), "city"))),
                Call(ident("NumberLong"),
                  List(Select(Select(ident("x"), "city"), "length"))),
                ident("undefined"))),
              ident("x")))))),
            ListMap()),
          $project(
            reshape(
              "0" -> $let(ListMap(
                DocVar.Name("el") -> $arrayElemAt($$ROOT, $literal(Bson.Int32(1)))),
                $field("$el", "city")),
              "src" -> $$ROOT),
            ExcludeId),
          $sort(NonEmptyList(BsonField.Name("0") -> SortDir.Ascending)),
          $project(
            reshape(sigil.Quasar -> $arrayElemAt($field("src"), $literal(Bson.Int32(0)))),
            ExcludeId)))
    }

    "plan order by JS expr with filter" in {
      plan3_2(sqlE"select city, pop from zips where pop > 1000 order by length(city)") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.And(
            isNumeric(BsonField.Name("pop")),
            Selector.Doc(
              BsonField.Name("pop") -> Selector.Gt(Bson.Int32(1000))))),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
            "0" ->
              If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                Call(ident("NumberLong"),
                  List(Select(Select(ident("x"), "city"), "length"))),
                ident("undefined")),
            "src"   -> ident("x"))))),
            ListMap()),
          $sort(NonEmptyList(BsonField.Name("0") -> SortDir.Ascending)),
          $project(
            reshape(
              "city" -> $field("src", "city"),
              "pop"  -> $field("src", "pop")),
            ExcludeId)))
    }

    "plan select length()" in {
      plan3_2(sqlE"select length(city) from zips") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
            underSigil(If(Call(ident("isString"),
              List(Select(ident("x"), "city"))),
              Call(ident("NumberLong"), List(Select(Select(ident("x"), "city"), "length"))),
              ident("undefined")))))),
          ListMap())))
    }

    "plan select length() and simple field" in {
      plan(sqlE"select city, length(city) from zips") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "city" -> $field("city"),
            "1" ->
              $cond($and(
                $lte($literal(Bson.Text("")), $field("city")),
                $lt($field("city"), $literal(Bson.Doc()))),
                $strLenCP($field("city")),
                $literal(Bson.Undefined))),
          ExcludeId)))
    }

    "plan select length() and simple field 3.2" in {
      plan3_2(sqlE"select city, length(city) from zips") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "city" -> Select(ident("x"), "city"),
          "1" ->
            If(Call(ident("isString"), List(Select(ident("x"), "city"))),
              Call(ident("NumberLong"),
                List(Select(Select(ident("x"), "city"), "length"))),
              ident("undefined")))))),
          ListMap()),
        $project(
          reshape(
            "city" -> $include(),
            "1"    -> $include()),
          ExcludeId)))
    }

    "plan combination of two distinct sets" in {
      plan(sqlE"SELECT (DISTINCT foo.bar) + (DISTINCT foo.baz) FROM foo") must
        beWorkflow0(
          $read(collection("db", "zips")))
    }.pendingWithActual(notOnPar, testFile("plan combination of two distinct sets"))

    "plan filter with timestamp and interval" in {
      val date0 = Bson.Date.fromInstant(Instant.parse("2014-11-17T00:00:00Z")).get
      val date22 = Bson.Date.fromInstant(Instant.parse("2014-11-17T22:00:00Z")).get

      plan(sqlE"""select * from days where date < timestamp("2014-11-17T22:00:00Z") and date - interval("PT12H") > timestamp("2014-11-17T00:00:00Z")""") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "days")),
          $match(Selector.And(
            Selector.Or(
              Selector.Doc(BsonField.Name("date") -> Selector.Type(BsonType.Int32)),
              Selector.Doc(BsonField.Name("date") -> Selector.Type(BsonType.Int64)),
              Selector.Doc(BsonField.Name("date") -> Selector.Type(BsonType.Dec)),
              Selector.Doc(BsonField.Name("date") -> Selector.Type(BsonType.Date))),
            Selector.Or(
              Selector.Doc(BsonField.Name("date") -> Selector.Type(BsonType.Int32)),
              Selector.Doc(BsonField.Name("date") -> Selector.Type(BsonType.Int64)),
              Selector.Doc(BsonField.Name("date") -> Selector.Type(BsonType.Dec)),
              Selector.Doc(BsonField.Name("date") -> Selector.Type(BsonType.Text)),
              Selector.Doc(BsonField.Name("date") -> Selector.Type(BsonType.Date)),
              Selector.Doc(BsonField.Name("date") -> Selector.Type(BsonType.Bool))))),
          $project(
            reshape(
              "0" -> $field("date"),
              "1" -> $subtract($field("date"), $literal(Bson.Dec(12*60*60*1000))),
              "src" -> $$ROOT),
            ExcludeId),
          $match(Selector.And(
            Selector.Doc(BsonField.Name("0") -> Selector.Lt(date22)),
            Selector.Doc(BsonField.Name("1") -> Selector.Gt(date0)))),
          $project(
            reshape(sigil.Quasar -> $field("src")),
            ExcludeId)))
    }

    "plan time_of_day (pipeline)" in {
      import FormatSpecifier._

      plan3_0(sqlE"select time_of_day(ts) from days") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "days")),
          $project(
            reshape(sigil.Quasar ->
              $cond(
                $and(
                  $lte($literal(Check.minDate), $field("ts")),
                  $lt($field("ts"), $literal(Bson.Regex("", "")))),
                $dateToString(Hour :: ":" :: Minute :: ":" :: Second :: "." :: Millisecond :: FormatString.empty, $field("ts")),
                $literal(Bson.Undefined))),
            ExcludeId)))
    }

    "plan filter on date" in {
      val date23 = Bson.Date.fromInstant(Instant.parse("2015-01-23T00:00:00Z")).get
      val date25 = Bson.Date.fromInstant(Instant.parse("2015-01-25T00:00:00Z")).get
      val date26 = Bson.Date.fromInstant(Instant.parse("2015-01-26T00:00:00Z")).get
      val date28 = Bson.Date.fromInstant(Instant.parse("2015-01-28T00:00:00Z")).get
      val date29 = Bson.Date.fromInstant(Instant.parse("2015-01-29T00:00:00Z")).get
      val date30 = Bson.Date.fromInstant(Instant.parse("2015-01-30T00:00:00Z")).get

      // Note: both of these boundaries require comparing with the start of the *next* day.
      plan(sqlE"""select * from logs where ((ts > date("2015-01-22") and ts <= date("2015-01-27")) and ts != date("2015-01-25")) or ts = date("2015-01-29")""") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "logs")),
          $match(Selector.Or(
            // TODO: Eliminate duplicates
            Selector.And(
              isNumeric(BsonField.Name("ts")),
              Selector.And(
                isNumeric(BsonField.Name("ts")),
                Selector.And(
                  Selector.And(
                    Selector.Doc(
                      BsonField.Name("ts") -> Selector.Gte(date23)),
                    Selector.Doc(
                      BsonField.Name("ts") -> Selector.Lt(date28))),
                  Selector.Or(
                    Selector.Doc(
                      BsonField.Name("ts") -> Selector.Lt(date25)),
                    Selector.Doc(
                      BsonField.Name("ts") -> Selector.Gte(date26)))))),
            Selector.And(
              Selector.Doc(
                BsonField.Name("ts") -> Selector.Gte(date29)),
              Selector.Doc(
                BsonField.Name("ts") -> Selector.Lt(date30)))))))
    }.pendingWithActual(notOnPar, testFile("plan filter on date"))

    "plan js and filter with id" in {
      Bson.ObjectId.fromString("0123456789abcdef01234567").fold[Result](
        failure("Couldn’t create ObjectId."))(
        oid => plan3_2(sqlE"""select length(city), foo = oid("0123456789abcdef01234567") from days where `_id` = oid("0123456789abcdef01234567")""") must
          beWorkflow(chain[Workflow](
            $read(collection("db", "days")),
            $match(Selector.Doc(
              BsonField.Name("_id") -> Selector.Eq(oid))),
            $simpleMap(
              NonEmptyList(MapExpr(JsFn(Name("x"),
                obj(
                  "0" ->
                    If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                      Call(ident("NumberLong"),
                        List(Select(Select(ident("x"), "city"), "length"))),
                      ident("undefined")),
                  "1" ->
                    BinOp(jscore.Eq,
                      Select(ident("x"), "foo"),
                      Call(ident("ObjectId"), List(jscore.Literal(Js.Str("0123456789abcdef01234567"))))))))),
              ListMap()),
            $project(
              reshape(
                "0" -> $include(),
                "1" -> $include()),
              ExcludeId))))
    }

    "plan convert to timestamp" in {
      plan(sqlE"select to_timestamp(epoch) from foo") must beWorkflow {
        chain[Workflow](
          $read(collection("db", "foo")),
          $project(
            reshape(
              sigil.Quasar ->
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("epoch")),
                    $lt($field("epoch"), $literal(Bson.Text("")))),
                  $add($literal(Bson.Date(0)), $field("epoch")),
                  $literal(Bson.Undefined))),
            ExcludeId))
      }
    }

    "plan convert to timestamp in map-reduce" in {
      plan3_2(sqlE"select length(name), to_timestamp(epoch) from foo") must beWorkflow {
        chain[Workflow](
          $read(collection("db", "foo")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"), obj(
              "0" ->
                If(Call(ident("isString"), List(Select(ident("x"), "name"))),
                  Call(ident("NumberLong"),
                    List(Select(Select(ident("x"), "name"), "length"))),
                  ident("undefined")),
              "1" ->
                If(
                  BinOp(jscore.Or,
                    Call(ident("isNumber"), List(Select(ident("x"), "epoch"))),
                    BinOp(jscore.Or,
                      BinOp(Instance, Select(ident("x"), "epoch"), ident("NumberInt")),
                      BinOp(Instance, Select(ident("x"), "epoch"), ident("NumberLong")))),
                  New(Name("Date"), List(Select(ident("x"), "epoch"))),
                  ident("undefined")))))),
            ListMap()),
          $project(
            reshape(
              "0" -> $include(),
              "1" -> $include()),
            ExcludeId))
      }
    }

    "plan simple join (map-reduce)" in {
      plan2_6(sqlE"select smallZips.city from zips join smallZips on zips.`_id` = smallZips.`_id`") must
        beWorkflow0(
          joinStructure(
            $read(collection("db", "zips")), "0", $$ROOT,
            $read(collection("db", "smallZips")),
            reshape("0" -> $field("_id")),
            Obj(ListMap(Name("0") -> Select(ident("value"), "_id"))).right,
            chain[Workflow](_,
              $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
                JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
              $unwind(DocField(JoinHandler.RightName)),
              $unwind(DocField(JoinHandler.LeftName)),
              $project(
                reshape(sigil.Quasar -> $field(JoinDir.Right.name, "city")),
                ExcludeId)),
            false).op)
    }

    "plan simple join with sharded inputs" in {
      // NB: cannot use $lookup, so fall back to the old approach
      val query = sqlE"select smallZips.city from zips join smallZips on zips.`_id` = smallZips.`_id`"
      plan3_4(query,
        c => Map(
          collection("db", "zips") -> CollectionStatistics(10, 100, true),
          collection("db", "zips2") -> CollectionStatistics(15, 150, true)).get(c),
        defaultIndexes,
        emptyDoc) must_==
        plan2_6(query)
    }

    "plan simple join with sources in different DBs" in {
      // NB: cannot use $lookup, so fall back to the old approach
      val query = sqlE"select smallZips.city from `db1/zips` join `db2/smallZips` on zips.`_id` = smallZips.`_id`"
      plan0(query, rootDir[Sandboxed], MongoQueryModel.`3.4`, defaultStats, defaultIndexes, emptyDoc) must_==
        plan0(query, rootDir[Sandboxed], MongoQueryModel.`2.6`, defaultStats, defaultIndexes, emptyDoc)
    }

    "plan simple join with no index" in {
      // NB: cannot use $lookup, so fall back to the old approach
      val query = sqlE"select smallZips.city from zips join smallZips on zips.`_id` = smallZips.`_id`"
      plan3_4(query, defaultStats, κ(None), emptyDoc) must_== plan2_6(query)
    }
  }
}
