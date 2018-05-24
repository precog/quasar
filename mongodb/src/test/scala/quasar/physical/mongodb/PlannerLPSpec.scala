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
import quasar._
import quasar.common.{Map => _, _}
import quasar.contrib.specs2.PendingWithActualTracking
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP}
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.selector.Selector
import quasar.physical.mongodb.workflow._
import quasar.std._

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

class PlannerLPSpec extends
    PlannerHelpers with
    PendingWithActualTracking {

  import StdLib.{set => s, _}
  import structural._
  import Grouped.grouped
  import Reshape.reshape
  import CollectionUtil._

  import fixExprOp._
  import PlannerHelpers._

  //TODO the LP's should be translated to QScript
  //Then these tests can move to PlannerQScriptSpec
  "plan from LogicalPlan" should {
    import StdLib._

    "plan simple Sort" in {
      val lp =
        lpf.let(
          'tmp0, read("db/foo"),
          lpf.let(
            'tmp1, makeObj("bar" -> lpf.invoke2(MapProject, lpf.free('tmp0), lpf.constant(Data.Str("bar")))),
            lpf.let('tmp2,
              lpf.sort(
                lpf.free('tmp1),
                (lpf.invoke2(MapProject, lpf.free('tmp1), lpf.constant(Data.Str("bar"))), SortDir.asc).wrapNel),
              lpf.free('tmp2))))

      planLP(lp) must beWorkflow(chain[Workflow](
        $read(collection("db", "foo")),
        $sort(NonEmptyList(BsonField.Name("bar") -> SortDir.Ascending)),
        $project(
          reshape("bar" -> $field("bar")),
          ExcludeId)))
    }.pendingUntilFixed

    "plan Sort with expression" in {
      val lp =
        lpf.let(
          'tmp0, read("db/foo"),
          lpf.sort(
            lpf.free('tmp0),
            (math.Divide[Fix[LP]](
              lpf.invoke2(MapProject, lpf.free('tmp0), lpf.constant(Data.Str("bar"))),
              lpf.constant(Data.Dec(10.0))).embed, SortDir.asc).wrapNel))

      planLP(lp) must beWorkflow(chain[Workflow](
        $read(collection("db", "foo")),
        $project(
          reshape(
            "0" -> $divide($field("bar"), $literal(Bson.Dec(10.0))),
            "src" -> $$ROOT),
          ExcludeId),
        $sort(NonEmptyList(BsonField.Name("0") -> SortDir.Ascending)),
        $project(
          reshape(sigil.Quasar -> $field("src")),
          ExcludeId)))
    }

    "plan Sort with expression and earlier pipeline op" in {
      val lp =
        lpf.let(
          'tmp0, read("db/foo"),
          lpf.let(
            'tmp1,
            lpf.invoke2(s.Filter,
              lpf.free('tmp0),
              lpf.invoke2(relations.Eq,
                lpf.invoke2(MapProject, lpf.free('tmp0), lpf.constant(Data.Str("baz"))),
                lpf.constant(Data.Int(0)))),
            lpf.sort(
              lpf.free('tmp1),
              (lpf.invoke2(MapProject, lpf.free('tmp1), lpf.constant(Data.Str("bar"))), SortDir.asc).wrapNel)))

      planLP(lp) must beWorkflow(chain[Workflow](
        $read(collection("db", "foo")),
        $match(Selector.Doc(
          BsonField.Name("baz") -> Selector.Eq(Bson.Int32(0)))),
        $sort(NonEmptyList(BsonField.Name("bar") -> SortDir.Ascending))))
    }

    "plan Sort expression (and extra project)" in {
      val lp =
        lpf.let(
          'tmp0, read("db/foo"),
          lpf.let(
            'tmp9,
            makeObj(
              "bar" -> lpf.invoke2(MapProject, lpf.free('tmp0), lpf.constant(Data.Str("bar")))),
            lpf.sort(
              lpf.free('tmp9),
              (math.Divide[Fix[LP]](
                lpf.invoke2(MapProject, lpf.free('tmp9), lpf.constant(Data.Str("bar"))),
                lpf.constant(Data.Dec(10.0))).embed, SortDir.asc).wrapNel)))

      planLP(lp) must beWorkflow0(chain[Workflow](
        $read(collection("db", "foo")),
        $project(
          reshape(
            "bar"    -> $field("bar"),
            "__tmp0" -> divide($field("bar"), $literal(Bson.Dec(10.0)))),
          IgnoreId),
        $sort(NonEmptyList(BsonField.Name("__tmp0") -> SortDir.Ascending)),
        $project(
          reshape("bar" -> $field("bar")),
          ExcludeId)))
    }.pendingUntilFixed

    "plan with extra squash and flattening" in {
      // NB: this case occurs when a view's LP is embedded in a larger query
      //     (See SD-1403), because type-checks are inserted into the inner and
      //     outer queries separately.

      val lp =
        lpf.let(
          'tmp0,
          lpf.let(
            'check0,
            lpf.invoke1(identity.Squash, read("db/zips")),
            lpf.typecheck(
              lpf.free('check0),
              Type.Obj(Map(), Some(Type.Top)),
              lpf.free('check0),
              lpf.constant(Data.NA))),
          lpf.invoke1(identity.Squash,
            makeObj(
              "city" ->
                lpf.invoke2(MapProject,
                  lpf.invoke2(s.Filter,
                    lpf.free('tmp0),
                    lpf.invoke3(string.Search,
                      lpf.invoke1(FlattenArray,
                        lpf.let(
                          'check1,
                          lpf.invoke2(MapProject, lpf.free('tmp0), lpf.constant(Data.Str("loc"))),
                          lpf.typecheck(
                            lpf.free('check1),
                            Type.FlexArr(0, None, Type.Str),
                            lpf.free('check1),
                            lpf.constant(Data.Arr(List(Data.NA)))))),
                      lpf.constant(Data.Str("^.*MONT.*$")),
                      lpf.constant(Data.Bool(false)))),
                  lpf.constant(Data.Str("city"))))))

      planLP(lp) must beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "__tmp4" -> $cond(
              $and(
                $lte($literal(Bson.Doc()), $$ROOT),
                $lt($$ROOT, $literal(Bson.Arr(List())))),
              $$ROOT,
              $literal(Bson.Undefined)),
            "__tmp5" -> $$ROOT),
          IgnoreId),
        $project(
          reshape(
            "__tmp6" -> $cond(
              $and(
                $lte($literal(Bson.Arr(List())), $field("__tmp4", "loc")),
                $lt($field("__tmp4", "loc"), $literal(Bson.Binary.fromArray(Array[Byte]())))),
              $field("__tmp4", "loc"),
              $literal(Bson.Arr(List(Bson.Undefined)))),
            "__tmp7" -> $field("__tmp5")),
          IgnoreId),
        $unwind(DocField("__tmp6"), None, None),
        $match(
          Selector.Doc(
            BsonField.Name("__tmp6") -> Selector.Regex("^.*MONT.*$", false, true, false, false))),
        $project(
          reshape(
            "__tmp8" -> $cond(
              $and(
                $lte($literal(Bson.Doc()), $field("__tmp7")),
                $lt($field("__tmp7"), $literal(Bson.Arr(List())))),
              $field("__tmp7"),
              $literal(Bson.Undefined))),
          IgnoreId),
        $project(
          reshape("city" -> $field("__tmp16", "city")),
          IgnoreId),
        $group(
          grouped(),
          -\/(reshape("0" -> $field("city")))),
        $project(
          reshape("city" -> $field("_id", "0")),
          IgnoreId)))
    }.pendingUntilFixed
  }
}
