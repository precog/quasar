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

package quasar.qscript

import slamdata.Predef.{ Eq => _, _ }
import quasar.{Data, TreeMatchers, Type}
import quasar.common.{JoinType, SortDir}
import quasar.contrib.pathy.AFile
import quasar.fp._
import quasar.fp.ski._
import quasar.frontend.{logicalplan => lp}
import quasar.qscript.MapFuncsCore._
import quasar.sql.{CompilerHelpers, JoinDir, SqlStringContext}
import quasar.std.StdLib, StdLib._

import scala.Predef.implicitly
import scala.collection.immutable.{Map => ScalaMap}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._

class QScriptSpec
    extends quasar.Qspec
    with CompilerHelpers
    with QScriptHelpers
    with TreeMatchers {
  // TODO instead of calling `.toOption` on the `\/`
  // write an `Equal[PlannerError]` and test for specific errors too
  "replan" should {
    "convert a constant boolean" in {
       // "select true"
       convert(lc.some, lpf.constant(Data.Bool(true))) must
         beSome(beTreeEqual(chain(
           UnreferencedR,
           QC.inj(Map((), BoolLit(true))))))
    }

    "fail to convert a constant set" in {
      // "select {\"a\": 1, \"b\": 2, \"c\": 3, \"d\": 4, \"e\": 5}{*} limit 3 offset 1"
      convert(
        lc.some,
        lpf.constant(Data.Set(List(
          Data.Obj(ListMap("0" -> Data.Int(2))),
          Data.Obj(ListMap("0" -> Data.Int(3))))))) must beNone
    }

    "convert a simple read" in {
      convert(lc.some, lpRead("/foo/bar")) must
      beSome(beTreeEqual(chain(
        ReadR(rootDir </> dir("foo") </> file("bar")),
        QC.inj(LeftShift((), HoleF, ExcludeId, RightSideF)))))
    }

    // FIXME: This can be simplified to a Union of the Reads - the LeftShift
    //        cancels out the MakeMaps.
    "convert a directory read" in {
      convert(lc.some, lpRead("/foo")) must
      beSome(beTreeEqual(chain(
        ReadR(rootDir </> dir("foo")),
        QC.inj(LeftShift((), HoleF, ExcludeId, RightSideF)))))
    }

    "convert a squashed read" in {
      // "select * from foo"
      convert(lc.some, identity.Squash(lpRead("/foo/bar")).embed) must
      beSome(beTreeEqual(chain(
        ReadR(rootDir </> dir("foo") </> file("bar")),
        QC.inj(LeftShift((), HoleF, ExcludeId, RightSideF)))))
    }

    "convert a basic select with type checking" in {
      val lp = fullCompileExp(sqlE"select foo as foo from bar")
      val qs = convert(lc.some, lp)
      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("bar")),
        QC.inj(LeftShift((),
          HoleF,
          ExcludeId,
          Free.roll(MakeMap(
            StrLit("foo"),
            Free.roll(Guard(
              RightSideF,
              Type.Obj(ScalaMap(),Some(Type.Top)),
              ProjectFieldR(RightSideF, StrLit("foo")),
              Free.roll(Undefined()))))))))))
    }

    // TODO: This would benefit from better normalization around Sort (#1545)
    "convert a basic order by" in {
      val lp = fullCompileExp(sqlE"select * from zips order by city")
      val qs = convert(lc.some, lp)
      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((),
          HoleF,
          IncludeId,
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(
              Free.roll(Guard(
                ProjectIndexR(RightSideF, IntLit(1)),
                Type.Obj(scala.Predef.Map(), Type.Top.some),
                ProjectIndexR(RightSideF, IntLit(1)),
                Free.roll(Undefined()))))),
            Free.roll(MakeArray(
              Free.roll(Guard(
                ProjectIndexR(RightSideF, IntLit(1)),
                Type.Obj(scala.Predef.Map(), Type.Top.some),
                ProjectFieldR(
                  ProjectIndexR(RightSideF, IntLit(1)),
                  StrLit("city")),
                Free.roll(Undefined()))))))))),
        QC.inj(Sort((),
          Nil,
          (ProjectIndexR(HoleF, IntLit(1)), SortDir.asc).wrapNel)),
        QC.inj(Map((), ProjectIndexR(HoleF, IntLit(0)))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert a basic reduction" in {
      val lp = fullCompileExp(sqlE"select sum(pop) as pop from bar")
      val qs = convert(lc.some, lp)
      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("bar")),
        QC.inj(LeftShift((), HoleF, ExcludeId, RightSideF)),
        QC.inj(Reduce((),
          Nil,
          List(ReduceFuncs.Sum(
            Free.roll(Guard(
              HoleF, Type.Obj(ScalaMap(), Type.Top.some),
              Free.roll(Guard(
                ProjectFieldR(HoleF, StrLit("pop")),
                Type.Coproduct(Type.Coproduct(Type.Int, Type.Dec), Type.Interval),
                ProjectFieldR(HoleF, StrLit("pop")),
                Free.roll(Undefined()))),
              Free.roll(Undefined()))))),
          Free.roll(MakeMap(StrLit("pop"), ReduceIndexF(0.right))))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert a simple wildcard take" in {
      val lp = fullCompileExp(sqlE"select * from bar limit 10")
      val qs = convert(lc.some, lp)
      qs must beSome(beTreeEqual(
        QC.inj(Subset(QC.inj(Unreferenced[Fix, Fix[QS]]()).embed,
          Free.roll(QCT.inj(LeftShift(Free.roll(RTF.inj(Const[Read[AFile], FreeQS](Read(rootDir </> file("bar"))))), HoleF, ExcludeId, RightSideF))),
          Take,
          Free.roll(QCT.inj(Map(Free.roll(QCT.inj(Unreferenced())), IntLit(10)))))).embed))
    }

    "convert a simple take through a path" in {
      convert(lc.some, StdLib.set.Take(lpRead("/foo/bar"), lpf.constant(Data.Int(10))).embed) must
        beSome(beTreeEqual(
          QC.inj(Subset(
            QC.inj(Unreferenced[Fix, Fix[QS]]()).embed,
            Free.roll(QCT.inj(LeftShift(Free.roll(RTF.inj(Const[Read[AFile], FreeQS](Read(rootDir </> dir("foo") </> file("bar"))))), HoleF, ExcludeId, RightSideF))),
            Take,
            Free.roll(QCT.inj(Map(Free.roll(QCT.inj(Unreferenced())), IntLit(10)))))).embed))
    }

    "convert a multi-field select" in {
      val lp = fullCompileExp(sqlE"select city, state from bar")
      val qs = convert(lc.some, lp)
      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("bar")),
        QC.inj(LeftShift((),
          HoleF,
          ExcludeId,
          Free.roll(ConcatMaps(
            Free.roll(MakeMap(
              StrLit("city"),
              Free.roll(Guard(
                RightSideF,
                Type.Obj(ScalaMap(),Some(Type.Top)),
                ProjectFieldR(RightSideF, StrLit("city")),
                Free.roll(Undefined()))))),
            Free.roll(MakeMap(
              StrLit("state"),
              Free.roll(Guard(
                RightSideF,
                Type.Obj(ScalaMap(),Some(Type.Top)),
                ProjectFieldR(RightSideF, StrLit("state")),
                Free.roll(Undefined()))))))))))))
    }

    "convert a simple read with path projects" in {
      convert(lc.some, lpRead("/some/bar/car")) must
      beSome(beTreeEqual(chain(
        ReadR(rootDir </> dir("some") </> file("bar")),
        QC.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("car")),
          ExcludeId,
          RightSideF)))))
    }

    "convert a basic invoke" in {
      convert(None, math.Add(lpRead("/foo"), lpRead("/bar")).embed) must
      beSome(beTreeEqual(chain(
        RootR,
        TJ.inj(ThetaJoin((),
          Free.roll(QCT.inj(LeftShift(
            Free.roll(QCT.inj(Map(
              Free.point(SrcHole),
              ProjectFieldR(HoleF, StrLit("foo"))))),
            HoleF,
            IncludeId,
            Free.roll(MakeArray(RightSideF))))),
          Free.roll(QCT.inj(LeftShift(
            Free.roll(QCT.inj(Map(
              Free.point(SrcHole),
              ProjectFieldR(HoleF, StrLit("bar"))))),
            HoleF,
            IncludeId,
            Free.roll(MakeArray(RightSideF))))),
          BoolLit(true),
          JoinType.Inner,
          Free.roll(Add(
            ProjectIndexR(ProjectIndexR(LeftSideF, IntLit(0)), IntLit(1)),
            ProjectIndexR(ProjectIndexR(RightSideF, IntLit(0)), IntLit(1)))))))))
    }

    "convert project object and make object" in {
      convert(
        None,
        identity.Squash(
          makeObj(
            "name" -> structural.ObjectProject(
              lpRead("/city"),
              lpf.constant(Data.Str("name"))).embed)).embed) must
      beSome(beTreeEqual(chain(
        RootR,
        QC.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("city")),
          ExcludeId,
          Free.roll(MakeMap[Fix, JoinFunc](
            StrLit[Fix, JoinSide]("name"),
            ProjectFieldR(RightSideF, StrLit("name")))))))))
    }

    "convert a basic reduction" in {
      convert(
        lc.some,
        agg.Sum(lpRead("/person")).embed) must
      beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("person")),
        QC.inj(LeftShift((), HoleF, ExcludeId, RightSideF)),
        QC.inj(Reduce((),
          Nil,
          List(ReduceFuncs.Sum[FreeMap](HoleF)),
          ReduceIndexF(0.right))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert a basic reduction wrapped in an object" in {
      // "select sum(height) from person"
      convert(
        None,
        makeObj(
          "0" ->
            agg.Sum(structural.ObjectProject(lpRead("/person"), lpf.constant(Data.Str("height"))).embed).embed)) must
      beSome(beTreeEqual(chain(
        RootR,
        QC.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("person")),
          ExcludeId,
          RightSideF)),
        QC.inj(Reduce((),
          Nil,
          List(ReduceFuncs.Sum[FreeMap](ProjectFieldR(HoleF, StrLit("height")))),
          Free.roll(MakeMap(StrLit("0"), Free.point(ReduceIndex(0.right)))))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert a flatten array" in {
      // "select loc[:*] from zips",
      convert(
        None,
        makeObj(
          "loc" ->
            structural.FlattenArray(
              structural.ObjectProject(lpRead("/zips"), lpf.constant(Data.Str("loc"))).embed).embed)) must
      beSome(beTreeEqual(chain(
        RootR,
        QC.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("zips")),
          ExcludeId,
          ProjectFieldR(RightSideF, StrLit("loc")))),
        QC.inj(LeftShift((),
          HoleF,
          ExcludeId,
          Free.roll(MakeMap(StrLit("loc"), RightSideF)))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert a constant shift array of size one" in {
      // this query never makes it to LP->QS transform because it's a constant value
      // "foo := (7); select * from foo"
      convert(
        None,
        identity.Squash(
          structural.ShiftArray(
            structural.MakeArrayN(lpf.constant(Data.Int(7))).embed).embed).embed) must
      beSome(beTreeEqual(chain(
        UnreferencedR,
        QC.inj(LeftShift((),
          Free.roll(Constant(ejsonArr(ejsonInt(7)))),
          ExcludeId,
          RightSideF)))))
    }

    "convert a constant shift array of size two" in {
      // this query never makes it to LP->QS transform because it's a constant value
      // "foo := (7,8); select * from foo"
      convert(
        None,
        identity.Squash(
          structural.ShiftArray(
            structural.ArrayConcat(
              structural.MakeArrayN(lpf.constant(Data.Int(7))).embed,
              structural.MakeArrayN(lpf.constant(Data.Int(8))).embed).embed).embed).embed) must
      beSome(beTreeEqual(chain(
        QC.inj(Unreferenced[Fix, Fix[QS]]()),
        QC.inj(LeftShift(
          (),
          Free.roll(Constant(ejsonArr(ejsonInt(7), ejsonInt(8)))),
          ExcludeId,
          RightSideF)))))
    }

    "convert a constant shift array of size three" in {
      // this query never makes it to LP->QS transform because it's a constant value
      // "foo := (7,8,9); select * from foo"
      convert(
        None,
        identity.Squash(
          structural.ShiftArray(
            structural.ArrayConcat(
              structural.ArrayConcat(
                structural.MakeArrayN(lpf.constant(Data.Int(7))).embed,
                structural.MakeArrayN(lpf.constant(Data.Int(8))).embed).embed,
              structural.MakeArrayN(lpf.constant(Data.Int(9))).embed).embed).embed).embed) must
      beSome(beTreeEqual(chain(
        QC.inj(Unreferenced[Fix, Fix[QS]]()),
        QC.inj(LeftShift((),
          Free.roll(Constant(ejsonArr(ejsonInt(7), ejsonInt(8), ejsonInt(9)))),
          ExcludeId,
          RightSideF)))))
    }

    "convert a read shift array" in {
      // select (baz || quux || ducks)[*] from `/foo/bar`
      convert(
        None,
        lp.let('x, lpRead("/foo/bar"),
          structural.ShiftArray(
            structural.ArrayConcat(
              structural.ArrayConcat(
                structural.ObjectProject(lpf.free('x), lpf.constant(Data.Str("baz"))).embed,
                structural.ObjectProject(lpf.free('x), lpf.constant(Data.Str("quux"))).embed).embed,
              structural.ObjectProject(lpf.free('x), lpf.constant(Data.Str("ducks"))).embed).embed).embed).embed) must
      beSome(beTreeEqual(chain(
        RootR,
        QC.inj(LeftShift((),
          ProjectFieldR(ProjectFieldR(HoleF, StrLit("foo")), StrLit("bar")),
          ExcludeId,
          Free.roll(ConcatArrays[Fix, JoinFunc](
            Free.roll(ConcatArrays[Fix, JoinFunc](
              ProjectFieldR(RightSideF, StrLit("baz")),
              ProjectFieldR(RightSideF, StrLit("quux")))),
            ProjectFieldR(RightSideF, StrLit("ducks")))))),
        QC.inj(LeftShift((), HoleF, ExcludeId, RightSideF)))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert a shift/unshift array" in {
      // "select [loc[_:] * 10 ...] from zips",
      convert(
        lc.some,
        makeObj(
          "0" ->
            structural.UnshiftArray(
              math.Multiply(
                structural.ShiftArrayIndices(
                  structural.ObjectProject(lpRead("/zips"), lpf.constant(Data.Str("loc"))).embed).embed,
                lpf.constant(Data.Int(10))).embed).embed)) must
      beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((),
          HoleF,
          IncludeId,
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(LeftSideF)),
            Free.roll(MakeArray(RightSideF)))))),
        QC.inj(LeftShift((),
          ProjectFieldR(
            ProjectIndexR(ProjectIndexR(HoleF, IntLit(1)), IntLit(1)),
            StrLit("loc")),
          IdOnly,
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(LeftSideF)),
            Free.roll(MakeArray(RightSideF)))))),
        QC.inj(Reduce((),
          List(
            ProjectIndexR(
              ProjectIndexR(ProjectIndexR(HoleF, IntLit(0)), IntLit(1)),
              IntLit(0))),
          List(
            ReduceFuncs.UnshiftArray(
              Free.roll(Multiply(
                ProjectIndexR(HoleF, IntLit(1)),
                IntLit(10))))),
          Free.roll(MakeMap[Fix, FreeMapA[ReduceIndex]](
            StrLit[Fix, ReduceIndex]("0"),
            ReduceIndexF(0.right))))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert a filter" in {
      // "select * from foo where baz between 1 and 10"
      convert(
        lc.some,
        StdLib.set.Filter(
          lpRead("/bar"),
          relations.Between(
            structural.ObjectProject(lpRead("/bar"), lpf.constant(Data.Str("baz"))).embed,
            lpf.constant(Data.Int(1)),
            lpf.constant(Data.Int(10))).embed).embed) must
      beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("bar")),
        QC.inj(LeftShift((),
          HoleF,
          IncludeId,
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(ProjectIndexR(RightSideF, IntLit(1)))),
            Free.roll(MakeArray(
              Free.roll(Between(
                ProjectFieldR(
                  ProjectIndexR(RightSideF, IntLit(1)),
                  StrLit("baz")),
                IntLit(1),
                IntLit(10))))))))),
        QC.inj(Filter((), ProjectIndexR(HoleF, IntLit(1)))),
        QC.inj(Map((), ProjectIndexR(HoleF, IntLit(0)))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    // an example of how logical plan expects magical "left" and "right" fields to exist
    "convert magical query" in {
      // "select * from person, car",
      convert(
        lc.some,
        lp.let('__tmp0,
          lp.join(
            lpRead("/person"),
            lpRead("/car"),
            JoinType.Inner,
            lp.JoinCondition('__leftJoin5, '__rightJoin6, lpf.constant(Data.Bool(true)))).embed,
          identity.Squash(
            structural.ObjectConcat(
              JoinDir.Left.projectFrom(lpf.free('__tmp0)),
              JoinDir.Right.projectFrom(lpf.free('__tmp0))).embed).embed).embed) must
      beSome(beTreeEqual(chain(
        QC.inj(Unreferenced[Fix, Fix[QS]]()),
        TJ.inj(ThetaJoin((),
          Free.roll(QCT.inj(LeftShift(
            Free.roll(RTF.inj(Const(Read(rootDir </> file("person"))))),
            HoleF,
            IncludeId,
            Free.roll(MakeArray(RightSideF))))),
          Free.roll(QCT.inj(LeftShift(
            Free.roll(RTF.inj(Const(Read(rootDir </> file("car"))))),
            HoleF,
            IncludeId,
            Free.roll(MakeArray(RightSideF))))),
          BoolLit(true),
          JoinType.Inner,
          Free.roll(ConcatMaps(
            ProjectIndexR(ProjectIndexR(LeftSideF, IntLit(0)), IntLit(1)),
            ProjectIndexR(ProjectIndexR(RightSideF, IntLit(0)), IntLit(1)))))))))
    }

    "convert basic join with explicit join condition" in {
      //"select foo.name, bar.address from foo join bar on foo.id = bar.foo_id",

      val query =
        lpf.let('__tmp0,
          lpf.join(
            lpRead("/foo"),
            lpRead("/bar"),
            JoinType.Inner,
            lp.JoinCondition('__leftJoin9, '__rightJoin10,
              relations.Eq(
                structural.ObjectProject(lpf.joinSideName('__leftJoin9), lpf.constant(Data.Str("id"))).embed,
                structural.ObjectProject(lpf.joinSideName('__rightJoin10), lpf.constant(Data.Str("foo_id"))).embed).embed)),
          makeObj(
            "name" ->
              structural.ObjectProject(
                JoinDir.Left.projectFrom(lpf.free('__tmp0)),
                lpf.constant(Data.Str("name"))).embed,
            "address" ->
              structural.ObjectProject(
                JoinDir.Right.projectFrom(lpf.free('__tmp0)),
                lpf.constant(Data.Str("address"))).embed))

      convert(None, query) must
        beSome(beTreeEqual(chain(
          RootR,
          TJ.inj(ThetaJoin((),
            Free.roll(QCT.inj(LeftShift(
              Free.roll(QCT.inj(Map(HoleQS, ProjectFieldR(HoleF, StrLit("foo"))))),
              HoleF,
              IncludeId,
              MakeArrayR(RightSideF)))),
            Free.roll(QCT.inj(LeftShift(
              Free.roll(QCT.inj(Map(HoleQS, ProjectFieldR(HoleF, StrLit("bar"))))),
              HoleF,
              IncludeId,
              MakeArrayR(RightSideF)))),
            Free.roll(Eq(
              ProjectFieldR(
                ProjectIndexR(
                  ProjectIndexR(LeftSideF, IntLit(0)),
                  IntLit(1)),
                StrLit("id")),
              ProjectFieldR(
                ProjectIndexR(
                  ProjectIndexR(RightSideF, IntLit(0)),
                  IntLit(1)),
                StrLit("foo_id")))),
            JoinType.Inner,
            ConcatMapsR(
              MakeMapR(
                StrLit("name"),
                ProjectFieldR(
                  ProjectIndexR(
                    ProjectIndexR(LeftSideF, IntLit(0)),
                    IntLit(1)),
                  StrLit("name"))),
              MakeMapR(
                StrLit("address"),
                ProjectFieldR(
                  ProjectIndexR(
                    ProjectIndexR(RightSideF, IntLit(0)),
                    IntLit(1)),
                  StrLit("address")))))))))
    }

    "convert union" in {
      val lp = fullCompileExp(sqlE"select * from city union select * from person")
      val qs = convert(lc.some, lp)
      qs must beSome(beTreeEqual(chain(
        QC.inj(Unreferenced[Fix, Fix[QS]]()),
        QC.inj(Union((),
          Free.roll(QCT.inj(LeftShift(
            Free.roll(RTF.inj(Const(Read(rootDir </> file("city"))))),
            HoleF,
            ExcludeId,
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(
                Free.roll(MakeArray(
                  ProjectIndexR(ProjectIndexR(RightSideF, IntLit(1)), IntLit(0)))))),
              Free.roll(MakeArray(RightSideF))))))),
          Free.roll(QCT.inj(LeftShift(
            Free.roll(RTF.inj(Const(Read(rootDir </> file("person"))))),
            HoleF,
            ExcludeId,
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(
                Free.roll(MakeArray(
                  ProjectIndexR(ProjectIndexR(RightSideF, IntLit(1)), IntLit(0)))))),
              Free.roll(MakeArray(RightSideF))))))))),
        QC.inj(Reduce((),
          List(ProjectIndexR(HoleF, IntLit(1))),
          // FIXME: This should be fetched from the bucket
          List(ReduceFuncs.Arbitrary(ProjectIndexR(HoleF, IntLit(1)))),
          ReduceIndexF(0.right))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert distinct by" in {
      val lp = fullCompileExp(sqlE"select distinct(city) from zips order by pop")
      val qs = convert(lc.some, lp)
      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((),
          HoleF,
          IncludeId,
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(
              Free.roll(ConcatMaps(
                Free.roll(MakeMap(
                  StrLit("city"),
                  Free.roll(Guard(
                    ProjectIndexR(RightSideF, IntLit(1)),
                    Type.Obj(ScalaMap(),Some(Type.Top)),
                    ProjectFieldR(
                      ProjectIndexR(RightSideF, IntLit(1)),
                      StrLit("city")),
                    Free.roll(Undefined()))))),
                Free.roll(MakeMap(
                  StrLit("__sd__0"),
                  Free.roll(Guard(
                    ProjectIndexR(RightSideF, IntLit(1)),
                    Type.Obj(ScalaMap(),Some(Type.Top)),
                    ProjectFieldR(
                      ProjectIndexR(RightSideF, IntLit(1)),
                      StrLit("pop")),
                    Free.roll(Undefined()))))))))),
            Free.roll(MakeArray(
              Free.roll(Guard(
                ProjectIndexR(RightSideF, IntLit(1)),
                Type.Obj(ScalaMap(),Some(Type.Top)),
                ProjectFieldR(
                  ProjectIndexR(RightSideF, IntLit(1)),
                  StrLit("pop")),
                Free.roll(Undefined()))))))))),
        // FIXME #2034
        // this `Sort` should sort by the representation of the synthetic field "__sd0__"
        QC.inj(Sort((),
          Nil,
          (ProjectIndexR(HoleF, IntLit(1)) -> SortDir.asc).wrapNel)),
        QC.inj(Reduce((),
          List(Free.roll(DeleteField(ProjectIndexR(HoleF, IntLit(0)), StrLit("__sd__0")))),
          List(ReduceFuncs.First(ProjectIndexR(HoleF, IntLit(0)))),
          Free.roll(ConcatArrays(
            Free.roll(ConcatArrays(
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(
                  Free.roll(MakeArray(ReduceIndexF(0.left))))),
                Free.roll(MakeArray(
                  Free.roll(MakeArray(ReduceIndexF(0.left))))))),
              Free.roll(MakeArray(ReduceIndexF(0.right))))),
            Free.roll(MakeArray(
              ProjectFieldR(ReduceIndexF(0.right), StrLit("__sd__0")))))))),
        QC.inj(Sort((),
          Nil,
          (ProjectIndexR(HoleF, IntLit(3)) -> SortDir.asc).wrapNel)),
        QC.inj(Map((),
          Free.roll(DeleteField(ProjectIndexR(HoleF, IntLit(2)), StrLit("__sd__0"))))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert a multi-field reduce" in {
      val lp = fullCompileExp(sqlE"select max(pop), min(city) from zips")
      val qs = convert(lc.some, lp)
      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((), HoleF, ExcludeId, RightSideF)),
        QC.inj(Reduce((),
          Nil,
          List(
            ReduceFuncs.Max(
              Free.roll(Guard(
                Free.roll(Guard(
                  HoleF,
                  Type.Obj(ScalaMap(),Some(Type.Top)),
                  ProjectFieldR(HoleF, StrLit("pop")),
                  Free.roll(Undefined()))),
                Type.Coproduct(Type.Int, Type.Coproduct(Type.Dec, Type.Coproduct(Type.Interval, Type.Coproduct(Type.Str, Type.Coproduct(Type.Timestamp, Type.Coproduct(Type.Date, Type.Coproduct(Type.Time, Type.Bool))))))),
                Free.roll(Guard(
                  HoleF,
                  Type.Obj(ScalaMap(),Some(Type.Top)),
                  ProjectFieldR(HoleF, StrLit("pop")),
                  Free.roll(Undefined()))),
                Free.roll(Undefined())))),
            ReduceFuncs.Min(
              Free.roll(Guard(
                Free.roll(Guard(
                  HoleF,
                  Type.Obj(ScalaMap(),Some(Type.Top)),
                  ProjectFieldR(HoleF, StrLit("city")),
                  Free.roll(Undefined()))),
                Type.Coproduct(Type.Int, Type.Coproduct(Type.Dec, Type.Coproduct(Type.Interval, Type.Coproduct(Type.Str, Type.Coproduct(Type.Timestamp, Type.Coproduct(Type.Date, Type.Coproduct(Type.Time, Type.Bool))))))),
                Free.roll(Guard(
                  HoleF,
                  Type.Obj(ScalaMap(),Some(Type.Top)),
                  ProjectFieldR(HoleF, StrLit("city")),
                  Free.roll(Undefined()))),
                Free.roll(Undefined()))))),
          Free.roll(ConcatMaps(
            Free.roll(MakeMap(StrLit("0"), ReduceIndexF(0.right))),
            Free.roll(MakeMap(StrLit("1"), ReduceIndexF(1.right))))))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert a filter followed by a reduce" in {
      val lp = fullCompileExp(sqlE"select count(*) from zips where pop > 1000")
      val qs = convert(lc.some, lp)

      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((),
          HoleF,
          IncludeId,
           Free.roll(ConcatArrays(
             Free.roll(MakeArray(
               Free.roll(Guard(
                 ProjectIndexR(RightSideF, IntLit(1)),
                 Type.Obj(ScalaMap(),Some(Type.Top)),
                 ProjectIndexR(RightSideF, IntLit(1)),
                 Free.roll(Undefined()))))),
             Free.roll(MakeArray(
               Free.roll(Guard(
                 Free.roll(Guard(
                   ProjectIndexR(RightSideF, IntLit(1)),
                   Type.Obj(ScalaMap(),Some(Type.Top)),
                   Free.roll(ProjectField(
                     ProjectIndexR(RightSideF, IntLit(1)),
                     StrLit("pop"))),
                   Free.roll(Undefined()))),
                 Type.Coproduct(Type.Int, Type.Coproduct(Type.Dec, Type.Coproduct(Type.Interval, Type.Coproduct(Type.Str, Type.Coproduct(Type.Timestamp, Type.Coproduct(Type.Date, Type.Coproduct(Type.Time, Type.Bool))))))),
                 Free.roll(Guard(
                   ProjectIndexR(RightSideF, IntLit(1)),
                   Type.Obj(ScalaMap(),Some(Type.Top)),
                   Free.roll(Gt(
                     Free.roll(ProjectField(
                       ProjectIndexR(RightSideF, IntLit(1)),
                       StrLit("pop"))),
                     IntLit(1000))),
                   Free.roll(Undefined()))),
                 Free.roll(Undefined()))))))))),
        QC.inj(Filter((),
          Free.roll(ProjectIndex(HoleF, IntLit(1))))),
        QC.inj(Reduce((),
          Nil,
          List(ReduceFuncs.Count[FreeMap](ProjectIndexR(HoleF, IntLit(0)))),
          ReduceIndexF(0.right))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert a non-static array projection" in {
      val lp = fullCompileExp(sqlE"select (loc || [7, 8])[0] from zips")
      val qs = convert(lc.some, lp)

      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((),
          HoleF,
          ExcludeId,
          Free.roll(Guard(
            RightSideF,
            Type.Obj(ScalaMap(),Some(Type.Top)),
            Free.roll(Guard(
              ProjectFieldR(RightSideF, StrLit("loc")),
              Type.FlexArr(0, None, Type.Top),
              ProjectIndexR(
                ConcatArraysR(
                  ProjectFieldR(RightSideF, StrLit("loc")),
                  Free.roll(Constant(ejsonArr(ejsonInt(7), ejsonInt(8))))),
                IntLit(0)),
              Free.roll(Undefined()))),
            Free.roll(Undefined()))))))))
    }

    "convert a static array projection prefix" in {
      val lp = fullCompileExp(sqlE"select ([7, 8] || loc)[1] from zips")
      val qs = convert(lc.some, lp)

      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((),
          HoleF,
          ExcludeId,
          Free.roll(Guard(
            RightSideF,
            Type.Obj(ScalaMap(),Some(Type.Top)),
            Free.roll(Guard(
              ProjectFieldR(RightSideF, StrLit("loc")),
              Type.FlexArr(0, None, Type.Top),
              IntLit(8),
              Free.roll(Undefined()))),
            Free.roll(Undefined()))))))))
    }

    "convert a group by with reduction" in {
      val lp = fullCompileExp(sqlE"select (loc[0] > -78.0) as l, count(*) as c from zips group by (loc[0] > -78.0)")
      val qs = convert(lc.some, lp)

      val inner: FreeMap =
        Free.roll(Guard(
          Free.roll(Guard(
            HoleF,
            Type.Obj(ScalaMap(),Some(Type.Top)),
            ProjectFieldR(HoleF, StrLit("loc")),
            Free.roll(Undefined()))),
          Type.FlexArr(0, None, Type.Top),
          Free.roll(Guard(
            Free.roll(Guard(
              HoleF,
              Type.Obj(ScalaMap(),Some(Type.Top)),
              ProjectIndexR(ProjectFieldR(HoleF, StrLit("loc")), IntLit(0)),
              Free.roll(Undefined()))),
            Type.Coproduct(Type.Int, Type.Coproduct(Type.Dec, Type.Coproduct(Type.Interval, Type.Coproduct(Type.Str, Type.Coproduct(Type.Timestamp, Type.Coproduct(Type.Date, Type.Coproduct(Type.Time, Type.Bool))))))),
            Free.roll(Guard(
              HoleF,
              Type.Obj(ScalaMap(),Some(Type.Top)),
              Free.roll(Gt(
                ProjectIndexR(ProjectFieldR(HoleF, StrLit("loc")), IntLit(0)),
                DecLit(-78.0))),
              Free.roll(Undefined()))),
            Free.roll(Undefined()))),
          Free.roll(Undefined())))

      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((), HoleF, ExcludeId, RightSideF)),
        QC.inj(Reduce((),
          List(Free.roll(MakeArray(inner))),
          List(
            ReduceFuncs.Arbitrary[FreeMap](inner),
            ReduceFuncs.Count[FreeMap](Free.roll(Guard(
              HoleF,
              Type.Obj(ScalaMap(),Some(Type.Top)),
              HoleF,
              Free.roll(Undefined()))))),
          Free.roll(ConcatMaps(
            Free.roll(MakeMap(StrLit("l"), ReduceIndexF(0.right))),
            Free.roll(MakeMap(StrLit("c"), ReduceIndexF(1.right))))))))(
        implicitly, Corecursive[Fix[QS], QS])))

    }

    "convert an ordered filtered distinct" in {
      val lp = fullCompileExp(sqlE"select distinct city from zips where pop <= 10 order by pop")
      val qs = convert(lc.some, lp)

      def guard(jf: JoinFunc => JoinFunc): JoinFunc =
        Free.roll(Guard(
          ProjectIndexR(RightSideF, IntLit(1)),
          Type.Obj(ScalaMap(), Some(Type.Top)),
          jf(ProjectIndexR(RightSideF, IntLit(1))),
          Free.roll(Undefined())))

      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((),
          HoleF,
          IncludeId,
          ConcatArraysR(
            MakeArrayR(guard(ι)),
            MakeArrayR(Free.roll(Guard(
              guard(ProjectFieldR(_, StrLit("pop"))),
              Type.Coproduct(Type.Int, Type.Coproduct(Type.Dec, Type.Coproduct(Type.Interval, Type.Coproduct(Type.Str, Type.Coproduct(Type.Timestamp, Type.Coproduct(Type.Date, Type.Coproduct(Type.Time, Type.Bool))))))),
              guard(jf => Free.roll(Lte(ProjectFieldR(jf, StrLit("pop")), IntLit(10)))),
              Free.roll(Undefined()))))))),
        QC.inj(Filter((),
          ProjectIndexR(HoleF, IntLit(1)))),
        // FIXME #2034
        // this `Sort` should sort by the representation of the synthetic field "__sd0__"
        QC.inj(Sort((),
          Nil,
          (ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("pop")), SortDir.asc).wrapNel)),
        QC.inj(Reduce((),
          List(
            MakeMapR(
              StrLit("city"),
              ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("city")))),
          List(
            ReduceFuncs.First(ConcatMapsR(
              MakeMapR(StrLit("city"), ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("city"))),
              MakeMapR(StrLit("__sd__0"), ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("pop")))))),
          ConcatArraysR(
            ConcatArraysR(
              ConcatArraysR(
                MakeArrayR(MakeArrayR(ReduceIndexF(0.left))),
                MakeArrayR(MakeArrayR(ReduceIndexF(0.left)))),
              MakeArrayR(ReduceIndexF(0.right))),
            MakeArrayR(ProjectFieldR(ReduceIndexF(0.right), StrLit("__sd__0")))))),
        // FIXME #2034
        // this `Sort` should sort by the representation of the synthetic field "__sd0__"
        QC.inj(Sort((),
          Nil,
          (ProjectIndexR(HoleF, IntLit(3)), SortDir.asc).wrapNel)),
        QC.inj(Map((), DeleteFieldR(ProjectIndexR(HoleF, IntLit(2)), StrLit("__sd__0")))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert an ordered filtered distinct with cases" in {
      val lp = fullCompileExp(sqlE"""select distinct case state when "MA" then "Massachusetts" else "unknown" end as name from zips where pop <= 10 order by pop""")
      val qs = convert(lc.some, lp)

      def guard(jf: JoinFunc => JoinFunc): JoinFunc =
        Free.roll(Guard(
          ProjectIndexR(RightSideF, IntLit(1)),
          Type.Obj(ScalaMap(), Some(Type.Top)),
          jf(ProjectIndexR(RightSideF, IntLit(1))),
          Free.roll(Undefined())))

      val cond: FreeMap =
        Free.roll(Cond(
          Free.roll(Eq(ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("state")), StrLit("MA"))),
          StrLit("Massachusetts"),
          StrLit("unknown")))

      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((),
          HoleF,
          IncludeId,
          ConcatArraysR(
            MakeArrayR(guard(ι)),
            MakeArrayR(Free.roll(Guard(
              guard(ProjectFieldR(_, StrLit("pop"))),
              Type.Coproduct(Type.Int, Type.Coproduct(Type.Dec, Type.Coproduct(Type.Interval, Type.Coproduct(Type.Str, Type.Coproduct(Type.Timestamp, Type.Coproduct(Type.Date, Type.Coproduct(Type.Time, Type.Bool))))))),
              guard(jf => Free.roll(Lte(ProjectFieldR(jf, StrLit("pop")), IntLit(10)))),
              Free.roll(Undefined()))))))),
        QC.inj(Filter((),
          ProjectIndexR(HoleF, IntLit(1)))),
        // FIXME #2034
        // this `Sort` should sort by the representation of the synthetic field "__sd0__"
        QC.inj(Sort((),
          Nil,
          (ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("pop")), SortDir.asc).wrapNel)),
        QC.inj(Reduce((),
          List(MakeMapR(StrLit("name"), cond)),
          List(
            ReduceFuncs.First(ConcatMapsR(
              MakeMapR(StrLit("name"), cond),
              MakeMapR(StrLit("__sd__0"), ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("pop")))))),
          ConcatArraysR(
            ConcatArraysR(
              ConcatArraysR(
                MakeArrayR(MakeArrayR(ReduceIndexF(0.left))),
                MakeArrayR(MakeArrayR(ReduceIndexF(0.left)))),
              MakeArrayR(ReduceIndexF(0.right))),
            MakeArrayR(ProjectFieldR(ReduceIndexF(0.right), StrLit("__sd__0")))))),
        // FIXME #2034
        // this `Sort` should sort by the representation of the synthetic field "__sd0__"
        QC.inj(Sort((),
          Nil,
          (ProjectIndexR(HoleF, IntLit(3)), SortDir.asc).wrapNel)),
        QC.inj(Map((),
          DeleteFieldR(ProjectIndexR(HoleF, IntLit(2)), StrLit("__sd__0")))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert a flattened array" in {
      val lp = fullCompileExp(sqlE"select city, loc[*] from zips")
      val qs = convert(lc.some, lp)

      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((),
          HoleF,
          IncludeId,
          ConcatArraysR(
            MakeArrayR(ConcatArraysR(
              ConcatArraysR(
                ConcatArraysR(
                  MakeArrayR(MakeArrayR(ProjectIndexR(RightSideF, IntLit(0)))),
                  MakeArrayR(MakeArrayR(ProjectIndexR(RightSideF, IntLit(0))))),
                MakeArrayR(ProjectIndexR(RightSideF, IntLit(1)))),
              MakeArrayR(ProjectIndexR(RightSideF, IntLit(1))))),
            MakeArrayR(ConcatArraysR(
              ConcatArraysR(
                ConcatArraysR(
                  MakeArrayR(MakeArrayR(ProjectIndexR(RightSideF, IntLit(0)))),
                  MakeArrayR(MakeArrayR(ProjectIndexR(RightSideF, IntLit(0))))),
                MakeArrayR(
                  Free.roll(Guard(
                    ProjectIndexR(RightSideF, IntLit(1)),
                    Type.Obj(ScalaMap(), Some(Type.Top)),
                    ProjectFieldR(
                      ProjectIndexR(RightSideF, IntLit(1)),
                      StrLit("loc")),
                    Free.roll(Undefined()))))),
              MakeArrayR(
                Free.roll(Guard(
                  ProjectIndexR(RightSideF, IntLit(1)),
                  Type.Obj(ScalaMap(), Some(Type.Top)),
                  ProjectFieldR(
                    ProjectIndexR(RightSideF, IntLit(1)),
                    StrLit("loc")),
                  Free.roll(Undefined()))))))))),
        QC.inj(LeftShift((),
          Free.roll(Guard(
            ProjectIndexR(ProjectIndexR(HoleF, IntLit(1)), IntLit(2)),
            Type.FlexArr(0, None, Type.Top),
            ProjectIndexR(ProjectIndexR(HoleF, IntLit(1)), IntLit(3)),
            Free.roll(Undefined()))),
          ExcludeId,
          ConcatMapsR(
            MakeMapR(
              StrLit("city"),
              Free.roll(Guard(
                ProjectIndexR(ProjectIndexR(LeftSideF, IntLit(0)), IntLit(2)),
                Type.Obj(ScalaMap(), Some(Type.Top)),
                ProjectFieldR(
                  ProjectIndexR(ProjectIndexR(LeftSideF, IntLit(0)), IntLit(3)),
                  StrLit("city")),
                Free.roll(Undefined())))),
            MakeMapR(StrLit("loc"), RightSideF)))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }
  }
}
