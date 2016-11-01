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

package quasar.qscript

import quasar.Predef._
import quasar.{Data, Type}
import quasar.fp._
import quasar.{logicalplan => lp}
import quasar.qscript.MapFuncs._
import quasar.sql.{CompilerHelpers, JoinDir}
import quasar.std.StdLib, StdLib._

import scala.collection.immutable.{Map => ScalaMap}

import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

class QScriptSpec extends quasar.Qspec with CompilerHelpers with QScriptHelpers {
  import quasar.frontend.fixpoint.lpf

  // TODO instead of calling `.toOption` on the `\/`
  // write an `Equal[PlannerError]` and test for specific errors too
  "replan" should {
    "convert a constant boolean" in {
       // "select true"
       convert(listContents.some, lpf.constant(Data.Bool(true))) must
         equal(chain(
           UnreferencedR,
           QC.inj(Map((), BoolLit(true)))).some)
    }

    "fail to convert a constant set" in {
      // "select {\"a\": 1, \"b\": 2, \"c\": 3, \"d\": 4, \"e\": 5}{*} limit 3 offset 1"
      convert(
        listContents.some,
        lpf.constant(Data.Set(List(
          Data.Obj(ListMap("0" -> Data.Int(2))),
          Data.Obj(ListMap("0" -> Data.Int(3))))))) must
      equal(None)
    }

    "convert a simple read" in {
      convert(listContents.some, lpRead("/foo/bar")) must
      equal(chain(
        ReadR(rootDir </> dir("foo") </> file("bar")),
        QC.inj(LeftShift((), HoleF, RightSideF))).some)
    }

    // FIXME: This can be simplified to a Union of the Reads - the LeftShift
    //        cancels out the MakeMaps.
    "convert a directory read" in {
      convert(listContents.some, lpRead("/foo")) must
      equal(chain(
        UnreferencedR,
        QC.inj(Union((),
          Free.roll(QCT.inj(Map(Free.roll(RT.inj(Const[Read, FreeQS](Read(rootDir </> dir("foo") </> file("bar"))))), Free.roll(MakeMap(StrLit("bar"), HoleF))))),
          Free.roll(QCT.inj(Union(Free.roll(QCT.inj(Unreferenced[Fix, FreeQS]())),
            Free.roll(QCT.inj(Map(Free.roll(RT.inj(Const[Read, FreeQS](Read(rootDir </> dir("foo") </> file("car"))))), Free.roll(MakeMap(StrLit("car"), HoleF))))),
            Free.roll(QCT.inj(Union(Free.roll(QCT.inj(Unreferenced[Fix, FreeQS]())),
              Free.roll(QCT.inj(Map(Free.roll(RT.inj(Const[Read, FreeQS](Read(rootDir </> dir("foo") </> file("city"))))), Free.roll(MakeMap(StrLit("city"), HoleF))))),
              Free.roll(QCT.inj(Union(Free.roll(QCT.inj(Unreferenced[Fix, FreeQS]())),
                Free.roll(QCT.inj(Map(Free.roll(RT.inj(Const[Read, FreeQS](Read(rootDir </> dir("foo") </> file("person"))))), Free.roll(MakeMap(StrLit("person"), HoleF))))),
                Free.roll(QCT.inj(Map(Free.roll(RT.inj(Const[Read, FreeQS](Read(rootDir </> dir("foo") </> file("zips"))))), Free.roll(MakeMap(StrLit("zips"), HoleF)))))))))))))))),
        QC.inj(LeftShift((), HoleF, RightSideF))).some)
    }

    "convert a squashed read" in {
      // "select * from foo"
      convert(listContents.some, identity.Squash(lpRead("/foo/bar")).embed) must
      equal(chain(
        ReadR(rootDir </> dir("foo") </> file("bar")),
        QC.inj(LeftShift((), HoleF, RightSideF))).some)
    }

    "convert a basic select with type checking" in {
      val lp = fullCompileExp("select foo from bar")
      val qs = convert(listContents.some, lp)
      qs must equal(chain(
        ReadR(rootDir </> file("bar")),
        QC.inj(LeftShift((),
          HoleF,
          Free.roll(MakeMap(
            StrLit("foo"),
            Free.roll(Guard(
              RightSideF,
              Type.Obj(ScalaMap(),Some(Type.Top)),
              ProjectFieldR(RightSideF, StrLit("foo")),
              Free.roll(Undefined())))))))).some)
    }

    // TODO: This would benefit from better normalization around Sort (#1545)
    "convert a basic order by" in {
      val lp = fullCompileExp("select * from zips order by city")
      val qs = convert(listContents.some, lp)
      qs must equal(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((),
          Free.roll(ZipMapKeys(HoleF)),
          Free.roll(ConcatArrays(
            Free.roll(ConcatArrays(
              // FIXME: #1622
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(
                  ProjectIndexR(
                    ProjectIndexR(ProjectIndexR(RightSideF, IntLit(0)), IntLit(0)),
                    IntLit(0)))),
                Free.roll(MakeArray(
                  ProjectIndexR(
                    ProjectIndexR(
                      ProjectIndexR(ProjectIndexR(RightSideF, IntLit(0)), IntLit(0)),
                      IntLit(0)),
                    IntLit(0)))))),
              Free.roll(MakeArray(
                Free.roll(Guard(
                  ProjectIndexR(RightSideF, IntLit(1)),
                  Type.Obj(scala.Predef.Map(), Type.Top.some),
                  ProjectIndexR(RightSideF, IntLit(1)),
                  Free.roll(Undefined()))))))),
            Free.roll(MakeArray(
              Free.roll(MakeArray(
                ProjectFieldR(
                  Free.roll(Guard(
                    ProjectIndexR(RightSideF, IntLit(1)),
                    Type.Obj(scala.Predef.Map(), Type.Top.some),
                    ProjectIndexR(RightSideF, IntLit(1)),
                    Free.roll(Undefined()))),
                  StrLit("city")))))))))),
        QC.inj(Sort((),
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(ProjectIndexR(ProjectIndexR(HoleF, IntLit(0)), IntLit(0)))),
            Free.roll(MakeArray(ProjectIndexR(ProjectIndexR(HoleF, IntLit(1)), IntLit(0)))))),
          List((ProjectIndexR(HoleF, IntLit(3)), SortDir.Ascending)))),
        QC.inj(Map((), ProjectIndexR(HoleF, IntLit(2))))).some)
    }

    "convert a basic reduction" in {
      val lp = fullCompileExp("select sum(pop) from bar")
      val qs = convert(listContents.some, lp)
      qs must equal(chain(
        ReadR(rootDir </> file("bar")),
        QC.inj(LeftShift((), HoleF, RightSideF)),
        QC.inj(Reduce((),
          NullLit(),
          List(ReduceFuncs.Sum(
            Free.roll(Guard(
              HoleF, Type.Obj(ScalaMap(), Type.Top.some),
              Free.roll(Guard(
                ProjectFieldR(HoleF, StrLit("pop")),
                Type.Coproduct(Type.Coproduct(Type.Int, Type.Dec), Type.Interval),
                ProjectFieldR(HoleF, StrLit("pop")),
                Free.roll(Undefined()))),
              Free.roll(Undefined()))))),
          Free.roll(MakeMap(StrLit("0"), ReduceIndexF(0)))))).some)
    }

    "convert a simple wildcard take" in {
      val lp = fullCompileExp("select * from bar limit 10")
      val qs = convert(listContents.some, lp)
      qs must equal(
        QC.inj(Subset(QC.inj(Unreferenced[Fix, Fix[QS]]()).embed,
          Free.roll(QCT.inj(LeftShift(Free.roll(RT.inj(Const[Read, FreeQS](Read(rootDir </> file("bar"))))), HoleF, RightSideF))),
          Take,
          Free.roll(QCT.inj(Map(Free.roll(QCT.inj(Unreferenced())), IntLit(10)))))).embed.some)
    }

    "convert a simple take through a path" in {
      convert(listContents.some, StdLib.set.Take(lpRead("/foo/bar"), lpf.constant(Data.Int(10))).embed) must
        equal(
          QC.inj(Subset(
            QC.inj(Unreferenced[Fix, Fix[QS]]()).embed,
            Free.roll(QCT.inj(LeftShift(Free.roll(RT.inj(Const[Read, FreeQS](Read(rootDir </> dir("foo") </> file("bar"))))), HoleF, RightSideF))),
            Take,
            Free.roll(QCT.inj(Map(Free.roll(QCT.inj(Unreferenced())), IntLit(10)))))).embed.some)
    }

    "convert a multi-field select" in {
      val lp = fullCompileExp("select city, state from bar")
      val qs = convert(listContents.some, lp)
      qs must equal(chain(
        ReadR(rootDir </> file("bar")),
        QC.inj(LeftShift((),
          HoleF,
          Free.roll(ConcatMaps(
            Free.roll(MakeMap(
              StrLit("city"),
              ProjectFieldR(
                Free.roll(Guard(
                  RightSideF,
                  Type.Obj(ScalaMap(),Some(Type.Top)),
                  RightSideF,
                  Free.roll(Undefined()))),
                StrLit("city")))),
            Free.roll(MakeMap(
              StrLit("state"),
              ProjectFieldR(
                Free.roll(Guard(
                  RightSideF,
                  Type.Obj(ScalaMap(),Some(Type.Top)),
                  RightSideF,
                  Free.roll(Undefined()))),
                StrLit("state"))))))))).some)
    }

    "convert a simple read with path projects" in {
      convert(listContents.some, lpRead("/some/bar/car")) must
      equal(chain(
        ReadR(rootDir </> dir("some") </> file("bar")),
        QC.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("car")),
          RightSideF))).some)
    }

    "convert a basic invoke" in {
      convert(None, math.Add(lpRead("/foo"), lpRead("/bar")).embed) must
      equal(chain(
        RootR,
        TJ.inj(ThetaJoin((),
          chain[Free[?[_], Hole], QScriptTotal](
            QCT.inj(Map(Free.point(SrcHole),
              ProjectFieldR(HoleF, StrLit("foo")))),
            QCT.inj(LeftShift((),
              Free.roll(ZipMapKeys(HoleF)),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(LeftSideF)),
                Free.roll(MakeArray(RightSideF))))))),
          chain[Free[?[_], Hole], QScriptTotal](
            QCT.inj(Map(Free.point(SrcHole),
              ProjectFieldR(HoleF, StrLit("bar")))),
            QCT.inj(LeftShift((),
              Free.roll(ZipMapKeys(HoleF)),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(LeftSideF)),
                Free.roll(MakeArray(RightSideF))))))),
          BoolLit(true),
          Inner,
          Free.roll(Add(
            ProjectIndexR(ProjectIndexR(LeftSideF, IntLit(1)), IntLit(1)),
            ProjectIndexR(ProjectIndexR(RightSideF, IntLit(1)), IntLit(1))))))).some)
    }

    "convert project object and make object" in {
      convert(
        None,
        identity.Squash(
          makeObj(
            "name" -> structural.ObjectProject(
              lpRead("/city"),
              lpf.constant(Data.Str("name")))))) must
      equal(chain(
        RootR,
        QC.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("city")),
          Free.roll(MakeMap[Fix, JoinFunc](
            StrLit[Fix, JoinSide]("name"),
            ProjectFieldR(RightSideF, StrLit("name"))))))).some)
    }

    "convert a basic reduction" in {
      convert(
        listContents.some,
        agg.Sum(lpRead("/person")).embed) must
      equal(chain(
        ReadR(rootDir </> file("person")),
        QC.inj(LeftShift((), HoleF, RightSideF)),
        QC.inj(Reduce((),
          NullLit(), // reduce on a constant bucket, which is normalized to Null
          List(ReduceFuncs.Sum[FreeMap](HoleF)),
          ReduceIndexF(0)))).some)
    }

    // FIXME: Should be able to get rid of ZipMapKeys
    "convert a basic reduction wrapped in an object" in {
      // "select sum(height) from person"
      convert(
        None,
        makeObj(
          "0" ->
            agg.Sum(structural.ObjectProject(lpRead("/person"), lpf.constant(Data.Str("height"))).embed))) must
      equal(chain(
        RootR,
        QC.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("person")),
          RightSideF)),
        QC.inj(Reduce((),
          NullLit(), // reduce on a constant bucket, which is normalized to Null
          List(ReduceFuncs.Sum[FreeMap](ProjectFieldR(HoleF, StrLit("height")))),
          Free.roll(MakeMap(StrLit("0"), Free.point(ReduceIndex(0))))))).some)
    }

    "convert a flatten array" in {
      // "select loc[:*] from zips",
      convert(
        None,
        makeObj(
          "loc" ->
            structural.FlattenArray(
              structural.ObjectProject(lpRead("/zips"), lpf.constant(Data.Str("loc"))).embed))) must
      equal(chain(
        RootR,
        QC.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("zips")),
          ProjectFieldR(RightSideF, StrLit("loc")))),
        QC.inj(LeftShift((),
          HoleF,
          Free.roll(MakeMap(StrLit("loc"), RightSideF))))).some)
    }

    "convert a constant shift array of size one" in {
      // this query never makes it to LP->QS transform because it's a constant value
      // "foo := (7); select * from foo"
      convert(
        None,
        identity.Squash(
          structural.ShiftArray(
            structural.MakeArrayN[Fix](lpf.constant(Data.Int(7))).embed).embed).embed) must
      equal(chain(
        UnreferencedR,
        QC.inj(LeftShift((),
          Free.roll(Constant(ejsonArr(ejsonInt(7)))),
          RightSideF))).some)
    }

    "convert a constant shift array of size two" in {
      // this query never makes it to LP->QS transform because it's a constant value
      // "foo := (7,8); select * from foo"
      convert(
        None,
        identity.Squash(
          structural.ShiftArray(
            structural.ArrayConcat(
              structural.MakeArrayN[Fix](lpf.constant(Data.Int(7))).embed,
              structural.MakeArrayN[Fix](lpf.constant(Data.Int(8))).embed).embed).embed).embed) must
      equal(chain(
        QC.inj(Unreferenced[Fix, Fix[QS]]()),
        QC.inj(LeftShift(
          (),
          Free.roll(Constant(ejsonArr(ejsonInt(7), ejsonInt(8)))),
          RightSideF))).some)
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
                structural.MakeArrayN[Fix](lpf.constant(Data.Int(7))).embed,
                structural.MakeArrayN[Fix](lpf.constant(Data.Int(8))).embed).embed,
              structural.MakeArrayN[Fix](lpf.constant(Data.Int(9))).embed).embed).embed).embed) must
      equal(chain(
        QC.inj(Unreferenced[Fix, Fix[QS]]()),
        QC.inj(LeftShift((),
          Free.roll(Constant(ejsonArr(ejsonInt(7), ejsonInt(8), ejsonInt(9)))),
          RightSideF))).some)
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
              structural.ObjectProject(lpf.free('x), lpf.constant(Data.Str("ducks"))).embed).embed).embed)) must
      equal(chain(
        RootR,
        QC.inj(LeftShift((),
          ProjectFieldR(ProjectFieldR(HoleF, StrLit("foo")), StrLit("bar")),
          Free.roll(ConcatArrays[Fix, JoinFunc](
            Free.roll(ConcatArrays[Fix, JoinFunc](
              ProjectFieldR(RightSideF, StrLit("baz")),
              ProjectFieldR(RightSideF, StrLit("quux")))),
            ProjectFieldR(RightSideF, StrLit("ducks")))))),
        QC.inj(LeftShift((), HoleF, RightSideF))).some)
    }

    "convert a shift/unshift array" in {
      // "select [loc[_:] * 10 ...] from zips",
      convert(
        listContents.some,
        makeObj(
          "0" ->
            structural.UnshiftArray(
              math.Multiply(
                structural.ShiftArrayIndices(
                  structural.ObjectProject(lpRead("/zips"), lpf.constant(Data.Str("loc"))).embed).embed,
                lpf.constant(Data.Int(10))).embed))) must
      equal(chain(
        ReadR(rootDir </> file("zips")),
        QC.inj(LeftShift((),
          Free.roll(ZipMapKeys(HoleF)),
          Free.roll(ConcatArrays(
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(ProjectIndexR(RightSideF, IntLit(0)))),
              Free.roll(MakeArray(ProjectIndexR(RightSideF, IntLit(1)))))),
            Free.roll(Constant(ejsonArr(ejsonStr("loc")))))))),
        QC.inj(LeftShift((),
          Free.roll(DupArrayIndices(
            ProjectFieldR(
              ProjectIndexR(HoleF, IntLit(1)),
              ProjectIndexR(HoleF, IntLit(2))))),
          Free.roll(ConcatArrays(
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(
                Free.roll(ConcatArrays(
                  Free.roll(MakeArray(RightSideF)),
                  Free.roll(MakeArray(
                    ProjectIndexR(
                      ProjectIndexR(LeftSideF, IntLit(0)),
                      IntLit(0)))))))),
              Free.roll(MakeArray(RightSideF)))),
            Free.roll(Constant(ejsonArr(ejsonInt(10)))))))),
        QC.inj(Reduce((),
          ProjectIndexR(ProjectIndexR(HoleF, IntLit(0)), IntLit(1)),
          List(
            ReduceFuncs.UnshiftArray(
              Free.roll(Multiply(
                ProjectIndexR(HoleF, IntLit(1)),
                ProjectIndexR(HoleF, IntLit(2)))))),
          Free.roll(MakeMap[Fix, FreeMapA[ReduceIndex]](
            StrLit[Fix, ReduceIndex]("0"),
            ReduceIndexF(0)))))).some)
    }

    "convert a filter" in {
      // "select * from foo where bar between 1 and 10"
      convert(
        listContents.some,
        StdLib.set.Filter(
          lpRead("/bar"),
          relations.Between(
            structural.ObjectProject(lpRead("/bar"), lpf.constant(Data.Str("baz"))).embed,
            lpf.constant(Data.Int(1)),
            lpf.constant(Data.Int(10))).embed).embed) must
      equal(chain(
        ReadR(rootDir </> file("bar")),
        QC.inj(LeftShift((),
          Free.roll(ZipMapKeys(HoleF)),
          Free.roll(ConcatArrays(
            Free.roll(ConcatArrays(
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(ProjectIndexR(RightSideF, IntLit(0)))),
                // FIXME: #1622
                Free.roll(MakeArray(ProjectIndexR(ProjectIndexR(ProjectIndexR(ProjectIndexR(RightSideF, IntLit(0)), IntLit(0)), IntLit(0)), IntLit(0)))))),
              Free.roll(MakeArray(ProjectIndexR(RightSideF, IntLit(1)))))),
            Free.roll(MakeArray(
              Free.roll(Between(
                ProjectFieldR(
                  ProjectIndexR(RightSideF, IntLit(1)),
                  StrLit("baz")),
                IntLit(1),
                IntLit(10))))))))),
        QC.inj(Filter((), ProjectIndexR(HoleF, IntLit(3)))),
        QC.inj(Map((), ProjectIndexR(HoleF, IntLit(2))))).some)
    }

    // an example of how logical plan expects magical "left" and "right" fields to exist
    "convert magical query" in {
      // "select * from person, car",
      convert(
        listContents.some,
        lp.let('__tmp0,
          StdLib.set.InnerJoin(lpRead("/person"), lpRead("/car"), lpf.constant(Data.Bool(true))).embed,
          identity.Squash(
            structural.ObjectConcat(
              JoinDir.Left.projectFrom(lpf.free('__tmp0)),
              JoinDir.Right.projectFrom(lpf.free('__tmp0))).embed).embed)) must
      equal(chain(
        QC.inj(Unreferenced[Fix, Fix[QS]]()),
        TJ.inj(ThetaJoin((),
          Free.roll(QCT.inj(LeftShift(
            Free.roll(RT.inj(Const(Read(rootDir </> file("person"))))),
            Free.roll(ZipMapKeys(HoleF)),
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(LeftSideF)),
              Free.roll(MakeArray(RightSideF))))))),
          Free.roll(QCT.inj(LeftShift(
            Free.roll(RT.inj(Const(Read(rootDir </> file("car"))))),
            Free.roll(ZipMapKeys(HoleF)),
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(LeftSideF)),
              Free.roll(MakeArray(RightSideF))))))),
          BoolLit(true),
          Inner,
          Free.roll(ConcatMaps(
            ProjectIndexR(ProjectIndexR(LeftSideF, IntLit(1)), IntLit(1)),
            ProjectIndexR(ProjectIndexR(RightSideF, IntLit(1)), IntLit(1))))))).some)
    }

    "convert basic join with explicit join condition" in {
      //"select foo.name, bar.address from foo join bar on foo.id = bar.foo_id",

      val lp = lpf.let('__tmp0, lpRead("/foo"),
        lpf.let('__tmp1, lpRead("/bar"),
          lpf.let('__tmp2,
            StdLib.set.InnerJoin(lpf.free('__tmp0), lpf.free('__tmp1),
              relations.Eq(
                structural.ObjectProject(lpf.free('__tmp0), lpf.constant(Data.Str("id"))).embed,
                structural.ObjectProject(lpf.free('__tmp1), lpf.constant(Data.Str("foo_id"))).embed).embed).embed,
            makeObj(
              "name" ->
                structural.ObjectProject(
                  JoinDir.Left.projectFrom(lpf.free('__tmp2)),
                  lpf.constant(Data.Str("name"))),
              "address" ->
                structural.ObjectProject(
                  JoinDir.Right.projectFrom(lpf.free('__tmp2)),
                  lpf.constant(Data.Str("address")))))))
      convert(None, lp) must
        equal(chain(
          RootR,
          QC.inj(Map((), ProjectFieldR(HoleF, StrLit("foo"))))).some)
    }.pendingUntilFixed
  }

  "convert union" in {
    val lp = fullCompileExp("select * from city union select * from person")
    val qs = convert(listContents.some, lp)
    qs must equal(chain(
      QC.inj(Unreferenced[Fix, Fix[QS]]()),
      QC.inj(Union((),
        Free.roll(QCT.inj(LeftShift(
          Free.roll(RT.inj(Const(Read(rootDir </> file("city"))))),
          HoleF,
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(
              ProjectIndexR(ProjectIndexR(RightSideF, IntLit(1)), IntLit(0)))),
            Free.roll(MakeArray(RightSideF))))))),
        Free.roll(QCT.inj(LeftShift(
          Free.roll(RT.inj(Const(Read(rootDir </> file("person"))))),
          HoleF,
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(
              ProjectIndexR(ProjectIndexR(RightSideF, IntLit(1)), IntLit(0)))),
            Free.roll(MakeArray(RightSideF))))))))),
      QC.inj(Reduce((),
        ProjectIndexR(HoleF, IntLit(1)),
        List(ReduceFuncs.Arbitrary[FreeMap](ProjectIndexR(HoleF, IntLit(1)))),
        ReduceIndexF(0)))).some)
  }

  "convert distinct by" in {
    val lp = fullCompileExp("select distinct(city) from zips order by pop")
    val qs = convert(listContents.some, lp)
    qs must equal(chain(
      ReadR(rootDir </> file("zips")),
      QC.inj(LeftShift((),
        Free.roll(ZipMapKeys(HoleF)),
        Free.roll(ConcatArrays(
          Free.roll(ConcatArrays(
            // FIXME: #1622
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(
                ProjectIndexR(ProjectIndexR(ProjectIndexR(ProjectIndexR(ProjectIndexR(ProjectIndexR(RightSideF, IntLit(0)), IntLit(0)), IntLit(0)), IntLit(0)), IntLit(0)), IntLit(0)))),
              Free.roll(MakeArray(
                ProjectIndexR(ProjectIndexR(ProjectIndexR(ProjectIndexR(ProjectIndexR(ProjectIndexR(ProjectIndexR(RightSideF, IntLit(0)), IntLit(0)), IntLit(0)), IntLit(0)), IntLit(0)), IntLit(0)), IntLit(0)))))),
            Free.roll(MakeArray(
              Free.roll(ConcatMaps(
                Free.roll(MakeMap(
                  StrLit("city"),
                  ProjectFieldR(
                    Free.roll(Guard(
                      ProjectIndexR(RightSideF, IntLit(1)),
                      Type.Obj(ScalaMap(),Some(Type.Top)),
                      ProjectIndexR(RightSideF, IntLit(1)),
                      Free.roll(Undefined()))),
                    StrLit("city")))),
                Free.roll(MakeMap(
                  StrLit("__sd__0"),
                  ProjectFieldR(
                    Free.roll(Guard(
                      ProjectIndexR(RightSideF, IntLit(1)),
                      Type.Obj(ScalaMap(),Some(Type.Top)),
                      ProjectIndexR(RightSideF, IntLit(1)),
                      Free.roll(Undefined()))),
                    StrLit("pop")))))))))),
          Free.roll(MakeArray(
            Free.roll(MakeArray(
              ProjectFieldR(
                Free.roll(Guard(
                  ProjectIndexR(RightSideF, IntLit(1)),
                  Type.Obj(ScalaMap(),Some(Type.Top)),
                  ProjectIndexR(RightSideF, IntLit(1)),
                  Free.roll(Undefined()))),
                StrLit("pop")))))))))),
      QC.inj(Sort((),
        Free.roll(ConcatArrays(
          Free.roll(MakeArray(ProjectIndexR(ProjectIndexR(HoleF, IntLit(0)), IntLit(0)))),
          Free.roll(MakeArray(ProjectIndexR(ProjectIndexR(HoleF, IntLit(1)), IntLit(0)))))),
        List(ProjectIndexR(HoleF, IntLit(3)) -> SortDir.Ascending))),
      QC.inj(Reduce((),
        Free.roll(DeleteField(ProjectIndexR(HoleF, IntLit(2)), StrLit("__sd__0"))),
        List(ReduceFuncs.Arbitrary(ProjectIndexR(HoleF, IntLit(2)))),
        Free.roll(DeleteField(ReduceIndexF(0), StrLit("__sd__0")))))).some)
  }

  "convert a multi-field reduce" in {
    val lp = fullCompileExp("select max(pop), min(city) from zips")
    val qs = convert(listContents.some, lp)
    qs must equal(chain(
      ReadR(rootDir </> file("zips")),
      QC.inj(LeftShift((), HoleF, RightSideF)),
      QC.inj(Reduce((),
        NullLit(),
        List(
          ReduceFuncs.Max(
            Free.roll(Guard(
              ProjectFieldR(
                Free.roll(Guard(
                  HoleF,
                  Type.Obj(ScalaMap(),Some(Type.Top)),
                  HoleF,
                  Free.roll(Undefined()))),
                StrLit("pop")),
              Type.Coproduct(Type.Int, Type.Coproduct(Type.Dec, Type.Coproduct(Type.Interval, Type.Coproduct(Type.Str, Type.Coproduct(Type.Timestamp, Type.Coproduct(Type.Date, Type.Coproduct(Type.Time, Type.Bool))))))),
              ProjectFieldR(
                Free.roll(Guard(
                  HoleF,
                  Type.Obj(ScalaMap(),Some(Type.Top)),
                  HoleF,
                  Free.roll(Undefined()))),
                StrLit("pop")),
              Free.roll(Undefined())))),
          ReduceFuncs.Min(
            Free.roll(Guard(
              ProjectFieldR(
                Free.roll(Guard(
                  HoleF,
                  Type.Obj(ScalaMap(),Some(Type.Top)),
                  HoleF,
                  Free.roll(Undefined()))),
                StrLit("city")),
              Type.Coproduct(Type.Int, Type.Coproduct(Type.Dec, Type.Coproduct(Type.Interval, Type.Coproduct(Type.Str, Type.Coproduct(Type.Timestamp, Type.Coproduct(Type.Date, Type.Coproduct(Type.Time, Type.Bool))))))),
              ProjectFieldR(
                Free.roll(Guard(
                  HoleF,
                  Type.Obj(ScalaMap(),Some(Type.Top)),
                  HoleF,
                  Free.roll(Undefined()))),
                StrLit("city")),
              Free.roll(Undefined()))))),
        Free.roll(ConcatMaps(
          Free.roll(MakeMap(StrLit("0"), ReduceIndexF(0))),
          Free.roll(MakeMap(StrLit("1"), ReduceIndexF(1)))))))).some)
  }
}
