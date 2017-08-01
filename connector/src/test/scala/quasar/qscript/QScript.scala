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
          Free.roll(MFC(MakeMap(
            StrLit("foo"),
            Free.roll(MFC(Guard(
              RightSideF,
              Type.Obj(ScalaMap(),Some(Type.Top)),
              ProjectFieldR(RightSideF, StrLit("foo")),
              Free.roll(MFC(Undefined())))))))))))))
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
          Free.roll(MFC(ConcatArrays(
            Free.roll(MFC(MakeArray(
              Free.roll(MFC(Guard(
                ProjectIndexR(RightSideF, IntLit(1)),
                Type.Obj(scala.Predef.Map(), Type.Top.some),
                ProjectIndexR(RightSideF, IntLit(1)),
                Free.roll(MFC(Undefined())))))))),
            Free.roll(MFC(MakeArray(
              ProjectFieldR(
                Free.roll(MFC(Guard(
                  ProjectIndexR(RightSideF, IntLit(1)),
                  Type.Obj(scala.Predef.Map(), Type.Top.some),
                  ProjectIndexR(RightSideF, IntLit(1)),
                  Free.roll(MFC(Undefined()))))),
                StrLit("city")))))))))),
        QC.inj(Sort((),
          NullLit(),
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
          NullLit(),
          List(ReduceFuncs.Sum(
            Free.roll(MFC(Guard(
              HoleF, Type.Obj(ScalaMap(), Type.Top.some),
              Free.roll(MFC(Guard(
                ProjectFieldR(HoleF, StrLit("pop")),
                Type.Coproduct(Type.Coproduct(Type.Int, Type.Dec), Type.Interval),
                ProjectFieldR(HoleF, StrLit("pop")),
                Free.roll(MFC(Undefined()))))),
              Free.roll(MFC(Undefined()))))))),
          Free.roll(MFC(MakeMap(StrLit("pop"), ReduceIndexF(0.some)))))))(
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
          Free.roll(MFC(ConcatMaps(
            Free.roll(MFC(MakeMap(
              StrLit("city"),
              ProjectFieldR(
                Free.roll(MFC(Guard(
                  RightSideF,
                  Type.Obj(ScalaMap(),Some(Type.Top)),
                  RightSideF,
                  Free.roll(MFC(Undefined()))))),
                StrLit("city"))))),
            Free.roll(MFC(MakeMap(
              StrLit("state"),
              ProjectFieldR(
                Free.roll(MFC(Guard(
                  RightSideF,
                  Type.Obj(ScalaMap(),Some(Type.Top)),
                  RightSideF,
                  Free.roll(MFC(Undefined()))))),
                StrLit("state")))))))))))))
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
            Free.roll(MFC(MakeArray(RightSideF)))))),
          Free.roll(QCT.inj(LeftShift(
            Free.roll(QCT.inj(Map(
              Free.point(SrcHole),
              ProjectFieldR(HoleF, StrLit("bar"))))),
            HoleF,
            IncludeId,
            Free.roll(MFC(MakeArray(RightSideF)))))),
          BoolLit(true),
          JoinType.Inner,
          Free.roll(MFC(Add(
            ProjectIndexR(ProjectIndexR(LeftSideF, IntLit(0)), IntLit(1)),
            ProjectIndexR(ProjectIndexR(RightSideF, IntLit(0)), IntLit(1))))))))))
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
          Free.roll(MFC(MakeMap[Fix, JoinFunc](
            StrLit[Fix, JoinSide]("name"),
            ProjectFieldR(RightSideF, StrLit("name"))))))))))
    }

    "convert a basic reduction" in {
      convert(
        lc.some,
        agg.Sum(lpRead("/person")).embed) must
      beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("person")),
        QC.inj(LeftShift((), HoleF, ExcludeId, RightSideF)),
        QC.inj(Reduce((),
          NullLit(), // reduce on a constant bucket, which is normalized to Null
          List(ReduceFuncs.Sum[FreeMap](HoleF)),
          ReduceIndexF(0.some))))(
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
          NullLit(), // reduce on a constant bucket, which is normalized to Null
          List(ReduceFuncs.Sum[FreeMap](ProjectFieldR(HoleF, StrLit("height")))),
          Free.roll(MFC(MakeMap(StrLit("0"), Free.point(ReduceIndex(0.some))))))))(
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
          Free.roll(MFC(MakeMap(StrLit("loc"), RightSideF))))))(
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
          Free.roll(MFC(Constant(ejsonArr(ejsonInt(7))))),
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
          Free.roll(MFC(Constant(ejsonArr(ejsonInt(7), ejsonInt(8))))),
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
          Free.roll(MFC(Constant(ejsonArr(ejsonInt(7), ejsonInt(8), ejsonInt(9))))),
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
          Free.roll(MFC(ConcatArrays[Fix, JoinFunc](
            Free.roll(MFC(ConcatArrays[Fix, JoinFunc](
              ProjectFieldR(RightSideF, StrLit("baz")),
              ProjectFieldR(RightSideF, StrLit("quux"))))),
            ProjectFieldR(RightSideF, StrLit("ducks"))))))),
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
          Free.roll(MFC(ConcatArrays(
            Free.roll(MFC(MakeArray(LeftSideF))),
            Free.roll(MFC(MakeArray(RightSideF)))))))),
        QC.inj(LeftShift((),
          ProjectFieldR(
            ProjectIndexR(ProjectIndexR(HoleF, IntLit(1)), IntLit(1)),
            StrLit("loc")),
          IdOnly,
          Free.roll(MFC(ConcatArrays(
            Free.roll(MFC(MakeArray(LeftSideF))),
            Free.roll(MFC(MakeArray(RightSideF)))))))),
        QC.inj(Reduce((),
          Free.roll(MFC(MakeArray(
            ProjectIndexR(
              ProjectIndexR(ProjectIndexR(HoleF, IntLit(0)), IntLit(1)),
              IntLit(0))))),
          List(
            ReduceFuncs.UnshiftArray(
              Free.roll(MFC(Multiply(
                ProjectIndexR(HoleF, IntLit(1)),
                IntLit(10)))))),
          Free.roll(MFC(MakeMap[Fix, FreeMapA[ReduceIndex]](
            StrLit[Fix, ReduceIndex]("0"),
            ReduceIndexF(0.some)))))))(
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
          Free.roll(MFC(ConcatArrays(
            Free.roll(MFC(MakeArray(ProjectIndexR(RightSideF, IntLit(1))))),
            Free.roll(MFC(MakeArray(
              Free.roll(MFC(Between(
                ProjectFieldR(
                  ProjectIndexR(RightSideF, IntLit(1)),
                  StrLit("baz")),
                IntLit(1),
                IntLit(10)))))))))))),
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
            Free.roll(MFC(MakeArray(RightSideF)))))),
          Free.roll(QCT.inj(LeftShift(
            Free.roll(RTF.inj(Const(Read(rootDir </> file("car"))))),
            HoleF,
            IncludeId,
            Free.roll(MFC(MakeArray(RightSideF)))))),
          BoolLit(true),
          JoinType.Inner,
          Free.roll(MFC(ConcatMaps(
            ProjectIndexR(ProjectIndexR(LeftSideF, IntLit(0)), IntLit(1)),
            ProjectIndexR(ProjectIndexR(RightSideF, IntLit(0)), IntLit(1))))))))))
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
            Free.roll(MFC(Eq(
              ProjectFieldR(
                ProjectIndexR(
                  ProjectIndexR(LeftSideF, IntLit(0)),
                  IntLit(1)),
                StrLit("id")),
              ProjectFieldR(
                ProjectIndexR(
                  ProjectIndexR(RightSideF, IntLit(0)),
                  IntLit(1)),
                StrLit("foo_id"))))),
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
            Free.roll(MFC(ConcatArrays(
              Free.roll(MFC(MakeArray(
                Free.roll(MFC(MakeArray(
                  ProjectIndexR(ProjectIndexR(RightSideF, IntLit(1)), IntLit(0)))))))),
              Free.roll(MFC(MakeArray(RightSideF))))))))),
          Free.roll(QCT.inj(LeftShift(
            Free.roll(RTF.inj(Const(Read(rootDir </> file("person"))))),
            HoleF,
            ExcludeId,
            Free.roll(MFC(ConcatArrays(
              Free.roll(MFC(MakeArray(
                Free.roll(MFC(MakeArray(
                  ProjectIndexR(ProjectIndexR(RightSideF, IntLit(1)), IntLit(0)))))))),
              Free.roll(MFC(MakeArray(RightSideF))))))))))),
        QC.inj(Reduce((),
          Free.roll(MFC(MakeArray(
            ProjectIndexR(HoleF, IntLit(1))))),
          Nil,
          ProjectIndexR(ReduceIndexF(None), IntLit(0)))))(
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
          Free.roll(MFC(ConcatArrays(
            Free.roll(MFC(MakeArray(
              Free.roll(MFC(ConcatMaps(
                Free.roll(MFC(MakeMap(
                  StrLit("city"),
                  ProjectFieldR(
                    Free.roll(MFC(Guard(
                      ProjectIndexR(RightSideF, IntLit(1)),
                      Type.Obj(ScalaMap(),Some(Type.Top)),
                      ProjectIndexR(RightSideF, IntLit(1)),
                      Free.roll(MFC(Undefined()))))),
                    StrLit("city"))))),
                Free.roll(MFC(MakeMap(
                  StrLit("__sd__0"),
                  ProjectFieldR(
                    Free.roll(MFC(Guard(
                      ProjectIndexR(RightSideF, IntLit(1)),
                      Type.Obj(ScalaMap(),Some(Type.Top)),
                      ProjectIndexR(RightSideF, IntLit(1)),
                      Free.roll(MFC(Undefined()))))),
                    StrLit("pop"))))))))))),
            Free.roll(MFC(MakeArray(
              ProjectFieldR(
                Free.roll(MFC(Guard(
                  ProjectIndexR(RightSideF, IntLit(1)),
                  Type.Obj(ScalaMap(),Some(Type.Top)),
                  ProjectIndexR(RightSideF, IntLit(1)),
                  Free.roll(MFC(Undefined()))))),
                StrLit("pop")))))))))),
        // FIXME #2034
        // this `Sort` should sort by the representation of the synthetic field "__sd0__"
        QC.inj(Sort((),
          NullLit(),
          (ProjectIndexR(HoleF, IntLit(1)) -> SortDir.asc).wrapNel)),
        QC.inj(Reduce((),
          Free.roll(MFC(MakeArray(
            Free.roll(MFC(DeleteField(ProjectIndexR(HoleF, IntLit(0)), StrLit("__sd__0"))))))),
          List(ReduceFuncs.First(ProjectIndexR(HoleF, IntLit(0)))),
          Free.roll(MFC(DeleteField(ReduceIndexF(0.some), StrLit("__sd__0")))))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert a multi-field reduce" in {
      val lp = fullCompileExp(sqlE"select max(pop), min(city) from zips")
      val qs = convert(lc.some, lp)
      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((), HoleF, ExcludeId, RightSideF)),
        QC.inj(Reduce((),
          NullLit(),
          List(
            ReduceFuncs.Max(
              Free.roll(MFC(Guard(
                ProjectFieldR(
                  Free.roll(MFC(Guard(
                    HoleF,
                    Type.Obj(ScalaMap(),Some(Type.Top)),
                    HoleF,
                    Free.roll(MFC(Undefined()))))),
                  StrLit("pop")),
                Type.Coproduct(Type.Int, Type.Coproduct(Type.Dec, Type.Coproduct(Type.Interval, Type.Coproduct(Type.Str, Type.Coproduct(Type.Timestamp, Type.Coproduct(Type.Date, Type.Coproduct(Type.Time, Type.Bool))))))),
                ProjectFieldR(
                  Free.roll(MFC(Guard(
                    HoleF,
                    Type.Obj(ScalaMap(),Some(Type.Top)),
                    HoleF,
                    Free.roll(MFC(Undefined()))))),
                  StrLit("pop")),
                Free.roll(MFC(Undefined())))))),
            ReduceFuncs.Min(
              Free.roll(MFC(Guard(
                ProjectFieldR(
                  Free.roll(MFC(Guard(
                    HoleF,
                    Type.Obj(ScalaMap(),Some(Type.Top)),
                    HoleF,
                    Free.roll(MFC(Undefined()))))),
                  StrLit("city")),
                Type.Coproduct(Type.Int, Type.Coproduct(Type.Dec, Type.Coproduct(Type.Interval, Type.Coproduct(Type.Str, Type.Coproduct(Type.Timestamp, Type.Coproduct(Type.Date, Type.Coproduct(Type.Time, Type.Bool))))))),
                ProjectFieldR(
                  Free.roll(MFC(Guard(
                    HoleF,
                    Type.Obj(ScalaMap(),Some(Type.Top)),
                    HoleF,
                    Free.roll(MFC(Undefined()))))),
                  StrLit("city")),
                Free.roll(MFC(Undefined()))))))),
          Free.roll(MFC(ConcatMaps(
            Free.roll(MFC(MakeMap(StrLit("0"), ReduceIndexF(0.some)))),
            Free.roll(MFC(MakeMap(StrLit("1"), ReduceIndexF(1.some))))))))))(
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
           Free.roll(MFC(ConcatArrays(
             Free.roll(MFC(MakeArray(
               Free.roll(MFC(Guard(
                 ProjectIndexR(RightSideF, IntLit(1)),
                 Type.Obj(ScalaMap(),Some(Type.Top)),
                 ProjectIndexR(RightSideF, IntLit(1)),
                 Free.roll(MFC(Undefined())))))))),
             Free.roll(MFC(MakeArray(
               Free.roll(MFC(Guard(
                 Free.roll(MFC(ProjectField(
                   Free.roll(MFC(Guard(
                     ProjectIndexR(RightSideF, IntLit(1)),
                     Type.Obj(ScalaMap(),Some(Type.Top)),
                     ProjectIndexR(RightSideF, IntLit(1)),
                     Free.roll(MFC(Undefined()))))),
                   StrLit("pop")))),
                 Type.Coproduct(Type.Int, Type.Coproduct(Type.Dec, Type.Coproduct(Type.Interval, Type.Coproduct(Type.Str, Type.Coproduct(Type.Timestamp, Type.Coproduct(Type.Date, Type.Coproduct(Type.Time, Type.Bool))))))),
                 Free.roll(MFC(Gt(
                   Free.roll(MFC(ProjectField(
                     Free.roll(MFC(Guard(
                     ProjectIndexR(RightSideF, IntLit(1)),
                     Type.Obj(ScalaMap(),Some(Type.Top)),
                     ProjectIndexR(RightSideF, IntLit(1)),
                     Free.roll(MFC(Undefined()))))),
                   StrLit("pop")))),
                 IntLit(1000)))),
               Free.roll(MFC(Undefined()))))))))))))),
        QC.inj(Filter((),
          Free.roll(MFC(ProjectIndex(HoleF, IntLit(1)))))),
        QC.inj(Reduce((),
          NullLit(),
          List(ReduceFuncs.Count[FreeMap](ProjectIndexR(HoleF, IntLit(0)))),
          ReduceIndexF(0.some))))(
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
          Free.roll(MFC(Guard(
            RightSideF,
            Type.Obj(ScalaMap(),Some(Type.Top)),
            Free.roll(MFC(Guard(
              ProjectFieldR(RightSideF, StrLit("loc")),
              Type.FlexArr(0, None, Type.Top),
              ProjectIndexR(
                ConcatArraysR(
                  ProjectFieldR(RightSideF, StrLit("loc")),
                  Free.roll(MFC(Constant(ejsonArr(ejsonInt(7), ejsonInt(8)))))),
                IntLit(0)),
              Free.roll(MFC(Undefined()))))),
            Free.roll(MFC(Undefined()))))))))))
    }

    "convert a static array projection prefix" in {
      val lp = fullCompileExp(sqlE"select ([7, 8] || loc)[1] from zips")
      val qs = convert(lc.some, lp)

      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((),
          HoleF,
          ExcludeId,
          Free.roll(MFC(Guard(
            RightSideF,
            Type.Obj(ScalaMap(),Some(Type.Top)),
            Free.roll(MFC(Guard(
              ProjectFieldR(RightSideF, StrLit("loc")),
              Type.FlexArr(0, None, Type.Top),
              IntLit(8),
              Free.roll(MFC(Undefined()))))),
            Free.roll(MFC(Undefined()))))))))))
    }

    "convert a group by with reduction" in {
      val lp = fullCompileExp(sqlE"select (loc[0] > -78.0) as l, count(*) as c from zips group by (loc[0] > -78.0)")
      val qs = convert(lc.some, lp)

      val inner: FreeMap =
        Free.roll(MFC(Guard(
          ProjectFieldR(
            Free.roll(MFC(Guard(HoleF, Type.Obj(ScalaMap(),Some(Type.Top)), HoleF, Free.roll(MFC(Undefined()))))),
            StrLit("loc")),
          Type.FlexArr(0, None, Type.Top),
          Free.roll(MFC(Guard(
            ProjectIndexR(
              ProjectFieldR(
                Free.roll(MFC(Guard(HoleF, Type.Obj(ScalaMap(),Some(Type.Top)), HoleF, Free.roll(MFC(Undefined()))))),
                StrLit("loc")),
              IntLit(0)),
            Type.Coproduct(Type.Int, Type.Coproduct(Type.Dec, Type.Coproduct(Type.Interval, Type.Coproduct(Type.Str, Type.Coproduct(Type.Timestamp, Type.Coproduct(Type.Date, Type.Coproduct(Type.Time, Type.Bool))))))),
            Free.roll(MFC(Gt(
              ProjectIndexR(
                ProjectFieldR(
                  Free.roll(MFC(Guard(HoleF, Type.Obj(ScalaMap(),Some(Type.Top)), HoleF, Free.roll(MFC(Undefined()))))),
                  StrLit("loc")),
                IntLit(0)),
              DecLit(-78.0)))),
            Free.roll(MFC(Undefined()))))),
          Free.roll(MFC(Undefined())))))

      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((), HoleF, ExcludeId, RightSideF)),
        QC.inj(Reduce((),
          Free.roll(MFC(MakeArray(
            Free.roll(MFC(MakeArray(inner)))))),
          List(
            ReduceFuncs.Arbitrary[FreeMap](inner),
            ReduceFuncs.Count[FreeMap](Free.roll(MFC(Guard(
              HoleF,
              Type.Obj(ScalaMap(),Some(Type.Top)),
              HoleF,
              Free.roll(MFC(Undefined()))))))),
          Free.roll(MFC(ConcatMaps(
            Free.roll(MFC(MakeMap(StrLit("l"), ReduceIndexF(0.some)))),
            Free.roll(MFC(MakeMap(StrLit("c"), ReduceIndexF(1.some))))))))))(
        implicitly, Corecursive[Fix[QS], QS])))

    }

    "convert an ordered filtered distinct" in {
      val lp = fullCompileExp(sqlE"select distinct city from zips where pop <= 10 order by pop")
      val qs = convert(lc.some, lp)

      val guard: JoinFunc =
        Free.roll(MFC(Guard(
          ProjectIndexR(RightSideF, IntLit(1)),
          Type.Obj(ScalaMap(), Some(Type.Top)),
          ProjectIndexR(RightSideF, IntLit(1)),
          Free.roll(MFC(Undefined())))))

      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((),
          HoleF,
          IncludeId,
          ConcatArraysR(
            MakeArrayR(guard),
            MakeArrayR(Free.roll(MFC(Guard(
              ProjectFieldR(guard, StrLit("pop")),
              Type.Coproduct(Type.Int, Type.Coproduct(Type.Dec, Type.Coproduct(Type.Interval, Type.Coproduct(Type.Str, Type.Coproduct(Type.Timestamp, Type.Coproduct(Type.Date, Type.Coproduct(Type.Time, Type.Bool))))))),
              Free.roll(MFC(Lte(ProjectFieldR(guard, StrLit("pop")), IntLit(10)))),
              Free.roll(MFC(Undefined()))))))))),
        QC.inj(Filter((),
          ProjectIndexR(HoleF, IntLit(1)))),
        // FIXME #2034
        // this `Sort` should sort by the representation of the synthetic field "__sd0__"
        QC.inj(Sort((),
          NullLit(),
          (ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("pop")), SortDir.asc).wrapNel)),
        QC.inj(Reduce((),
          MakeArrayR(DeleteFieldR(
            ConcatMapsR(
              MakeMapR(StrLit("city"), ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("city"))),
              MakeMapR(StrLit("__sd__0"), ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("pop")))),
            StrLit("__sd__0"))),
          List(ReduceFuncs.First(ConcatMapsR(
            MakeMapR(StrLit("city"), ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("city"))),
            MakeMapR(StrLit("__sd__0"), ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("pop")))))),
          DeleteFieldR(ReduceIndexF(0.some), StrLit("__sd__0")))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }

    "convert an ordered filtered distinct with cases" in {
      val lp = fullCompileExp(sqlE"""select distinct case state when "MA" then "Massachusetts" else "unknown" end as name from zips where pop <= 10 order by pop""")
      val qs = convert(lc.some, lp)

      val guard: JoinFunc =
        Free.roll(MFC(Guard(
          ProjectIndexR(RightSideF, IntLit(1)),
          Type.Obj(ScalaMap(), Some(Type.Top)),
          ProjectIndexR(RightSideF, IntLit(1)),
          Free.roll(MFC(Undefined())))))

      val cond: FreeMap =
        Free.roll(MFC(Cond(
          Free.roll(MFC(Eq(ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("state")), StrLit("MA")))),
          StrLit("Massachusetts"),
          StrLit("unknown"))))

      qs must beSome(beTreeEqual(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((),
          HoleF,
          IncludeId,
          ConcatArraysR(
            MakeArrayR(guard),
            MakeArrayR(Free.roll(MFC(Guard(
              ProjectFieldR(guard, StrLit("pop")),
              Type.Coproduct(Type.Int, Type.Coproduct(Type.Dec, Type.Coproduct(Type.Interval, Type.Coproduct(Type.Str, Type.Coproduct(Type.Timestamp, Type.Coproduct(Type.Date, Type.Coproduct(Type.Time, Type.Bool))))))),
              Free.roll(MFC(Lte(ProjectFieldR(guard, StrLit("pop")), IntLit(10)))),
              Free.roll(MFC(Undefined()))))))))),
        QC.inj(Filter((),
          ProjectIndexR(HoleF, IntLit(1)))),
        // FIXME #2034
        // this `Sort` should sort by the representation of the synthetic field "__sd0__"
        QC.inj(Sort((),
          NullLit(),
          (ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("pop")), SortDir.asc).wrapNel)),
        QC.inj(Reduce((),
          MakeArrayR(DeleteFieldR(
            ConcatMapsR(
              MakeMapR(StrLit("name"), cond),
              MakeMapR(StrLit("__sd__0"), ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("pop")))),
            StrLit("__sd__0"))),
          List(ReduceFuncs.First(ConcatMapsR(
            MakeMapR(StrLit("name"), cond),
            MakeMapR(StrLit("__sd__0"), ProjectFieldR(ProjectIndexR(HoleF, IntLit(0)), StrLit("pop")))))),
          DeleteFieldR(ReduceIndexF(0.some), StrLit("__sd__0")))))(
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
                MakeArrayR(ProjectFieldR(
                  Free.roll(MFC(Guard(
                    ProjectIndexR(RightSideF, IntLit(1)),
                    Type.Obj(ScalaMap(), Some(Type.Top)),
                    ProjectIndexR(RightSideF, IntLit(1)),
                    Free.roll(MFC(Undefined()))))),
                  StrLit("loc")))),
              MakeArrayR(ProjectFieldR(
                Free.roll(MFC(Guard(
                  ProjectIndexR(RightSideF, IntLit(1)),
                  Type.Obj(ScalaMap(), Some(Type.Top)),
                  ProjectIndexR(RightSideF, IntLit(1)),
                  Free.roll(MFC(Undefined()))))),
                StrLit("loc")))))))),
        QC.inj(LeftShift((),
          Free.roll(MFC(Guard(
            ProjectIndexR(ProjectIndexR(HoleF, IntLit(1)), IntLit(2)),
            Type.FlexArr(0, None, Type.Top),
            ProjectIndexR(ProjectIndexR(HoleF, IntLit(1)), IntLit(3)),
            Free.roll(MFC(Undefined()))))),
          ExcludeId,
          ConcatMapsR(
            MakeMapR(
              StrLit("city"),
              ProjectFieldR(
                Free.roll(MFC(Guard(
                  ProjectIndexR(ProjectIndexR(LeftSideF, IntLit(0)), IntLit(2)),
                  Type.Obj(ScalaMap(), Some(Type.Top)),
                  ProjectIndexR(ProjectIndexR(LeftSideF, IntLit(0)), IntLit(3)),
                  Free.roll(MFC(Undefined()))))),
                StrLit("city"))),
            MakeMapR(StrLit("loc"), RightSideF)))))(
        implicitly, Corecursive[Fix[QS], QS])))
    }
  }
}
