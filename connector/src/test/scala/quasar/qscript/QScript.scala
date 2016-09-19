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
import quasar.{Data, LogicalPlan => LP}
import quasar.ejson, ejson.EJson
import quasar.fp._
import quasar.qscript.MapFuncs._
import quasar.sql.CompilerHelpers
import quasar.std.StdLib, StdLib._

import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

class QScriptSpec extends quasar.Qspec with CompilerHelpers with QScriptHelpers {
  // TODO instead of calling `.toOption` on the `\/`
  // write an `Equal[PlannerError]` and test for specific errors too
  "replan" should {
    "convert a constant boolean" in {
       // "select true"
       convert(listContents.some, LP.Constant(Data.Bool(true))) must
         equal(chain(
           UnreferencedR,
           QC.inj(Map((), BoolLit(true)))).some)
    }

    "fail to convert a constant set" in {
      // "select {\"a\": 1, \"b\": 2, \"c\": 3, \"d\": 4, \"e\": 5}{*} limit 3 offset 1"
      convert(
        listContents.some,
        LP.Constant(Data.Set(List(
          Data.Obj(ListMap("0" -> Data.Int(2))),
          Data.Obj(ListMap("0" -> Data.Int(3))))))) must
      equal(None)
    }

    "convert a simple read" in {
      convert(listContents.some, lpRead("/foo/bar")) must
      equal(chain(
        ReadR(rootDir </> dir("foo") </> file("bar")),
        QC.inj(LeftShift((),
          HoleF,
          Free.point(RightSide)))).some)
    }

    // FIXME: This can be simplified to a Union of the Reads - the LeftShift
    //        cancels out the MakeMaps.
    "convert a directory read" in {
      convert(listContents.some, lpRead("/foo")) must
      equal(chain(
        UnreferencedR,
        QC.inj(Union((),
          Free.roll(QC.inj(Map(Free.roll(R.inj(Const[Read, FreeQS[Fix]](Read(rootDir </> dir("foo") </> file("bar"))))), Free.roll(MakeMap(StrLit("bar"), HoleF))))),
          Free.roll(QC.inj(Union(Free.roll(QC.inj(Unreferenced[Fix, FreeQS[Fix]]())),
            Free.roll(QC.inj(Map(Free.roll(R.inj(Const[Read, FreeQS[Fix]](Read(rootDir </> dir("foo") </> file("car"))))), Free.roll(MakeMap(StrLit("car"), HoleF))))),
            Free.roll(QC.inj(Union(Free.roll(QC.inj(Unreferenced[Fix, FreeQS[Fix]]())),
              Free.roll(QC.inj(Map(Free.roll(R.inj(Const[Read, FreeQS[Fix]](Read(rootDir </> dir("foo") </> file("city"))))), Free.roll(MakeMap(StrLit("city"), HoleF))))),
              Free.roll(QC.inj(Union(Free.roll(QC.inj(Unreferenced[Fix, FreeQS[Fix]]())),
                Free.roll(QC.inj(Map(Free.roll(R.inj(Const[Read, FreeQS[Fix]](Read(rootDir </> dir("foo") </> file("person"))))), Free.roll(MakeMap(StrLit("person"), HoleF))))),
                Free.roll(QC.inj(Map(Free.roll(R.inj(Const[Read, FreeQS[Fix]](Read(rootDir </> dir("foo") </> file("zips"))))), Free.roll(MakeMap(StrLit("zips"), HoleF)))))))))))))))),

        QC.inj(LeftShift((),
          HoleF,
          Free.point(RightSide)))).some)
    }

    "convert a squashed read" in {
      // "select * from foo"
      convert(listContents.some, identity.Squash(lpRead("/foo/bar")).embed) must
      equal(chain(
        ReadR(rootDir </> dir("foo") </> file("bar")),
        QC.inj(LeftShift((),
          HoleF,
          Free.point(RightSide)))).some)
    }

    "convert a basic select with type checking" in {
      val lp = fullCompileExp("select foo from bar")
      val qs = convert(listContents.some, lp)
      qs must equal(chain(RootR).some) // TODO incorrect expectation
    }.pendingUntilFixed

    "convert a simple take" in pending {
      convert(listContents.some, StdLib.set.Take(lpRead("/foo/bar"), LP.Constant(Data.Int(10))).embed) must
      equal(
        chain(
          ReadR(rootDir </> dir("foo") </> file("bar")),
          QC.inj(LeftShift((), HoleF, Free.point(RightSide))),
          QC.inj(Take((),
            Free.point(SrcHole),
            Free.roll(QC.inj(Map(
              Free.roll(DE.inj(Const[DeadEnd, FreeQS[Fix]](Root))),
              IntLit[Fix, Hole](10))))))).some)
    }

    "convert a simple read with path projects" in {
      convert(listContents.some, lpRead("/some/bar/car")) must
      equal(chain(
        ReadR(rootDir </> dir("some") </> file("bar")),
        QC.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("car")),
          Free.point(RightSide)))).some)
    }

    "convert a basic invoke" in {
      convert(None, math.Add(lpRead("/foo"), lpRead("/bar")).embed) must
      equal(chain(
        RootR,
        TJ.inj(ThetaJoin((),
          chain[Free[?[_], Hole], QS](
            QC.inj(Map(Free.point(SrcHole),
              ProjectFieldR(HoleF, StrLit("foo")))),
            QC.inj(LeftShift((),
              Free.roll(ZipMapKeys(HoleF)),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(Free.point(LeftSide))),
                Free.roll(MakeArray(Free.point(RightSide)))))))),
          chain[Free[?[_], Hole], QS](
            QC.inj(Map(Free.point(SrcHole),
              ProjectFieldR(HoleF, StrLit("bar")))),
            QC.inj(LeftShift((),
              Free.roll(ZipMapKeys(HoleF)),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(Free.point(LeftSide))),
                Free.roll(MakeArray(Free.point(RightSide)))))))),
          BoolLit(true),
          Inner,
          Free.roll(Add(
            Free.roll(ProjectIndex(
              Free.roll(ProjectIndex(Free.point(LeftSide), IntLit(1))),
              IntLit(1))),
            Free.roll(ProjectIndex(
              Free.roll(ProjectIndex(Free.point(RightSide), IntLit(1))),
              IntLit(1)))))))).some)
    }

    "convert project object and make object" in {
      convert(
        None,
        identity.Squash(
          makeObj(
            "name" -> structural.ObjectProject(
              lpRead("/city"),
              LP.Constant(Data.Str("name")))))) must
      equal(chain(
        RootR,
        QC.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("city")),
          Free.roll(MakeMap[Fix, JoinFunc[Fix]](
            StrLit[Fix, JoinSide]("name"),
            ProjectFieldR(
              Free.point[MapFunc[Fix, ?], JoinSide](RightSide),
              StrLit[Fix, JoinSide]("name"))))))).some)
    }

    "convert a basic reduction" in {
      convert(
        listContents.some,
        agg.Sum(lpRead("/person")).embed) must
      equal(chain(
        ReadR(rootDir </> file("person")),
        QC.inj(LeftShift((),
          Free.roll(ZipMapKeys(HoleF)),
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(Free.point(LeftSide))),
            Free.roll(MakeArray(Free.point(RightSide))))))),
        QC.inj(Reduce((),
          NullLit(), // reduce on a constant bucket, which is normalized to Null
          List(ReduceFuncs.Sum[FreeMap[Fix]](
            Free.roll(ProjectIndex(
              Free.roll(ProjectIndex(HoleF, IntLit(1))),
              IntLit(1))))),
          Free.point(ReduceIndex(0))))).some)
    }

    "convert a basic reduction wrapped in an object" in {
      // "select sum(height) from person"
      convert(
        None,
        makeObj(
          "0" ->
            agg.Sum(structural.ObjectProject(lpRead("/person"), LP.Constant(Data.Str("height"))).embed))) must
      equal(chain(
        RootR,
        QC.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("person")),
          ProjectFieldR(
            Free.point(RightSide),
            StrLit("height")))),
        QC.inj(Reduce((),
          NullLit(), // reduce on a constant bucket, which is normalized to Null
          List(ReduceFuncs.Sum[FreeMap[Fix]](HoleF)),
          Free.roll(MakeMap(StrLit("0"), Free.point(ReduceIndex(0))))))).some)
    }

    "convert a flatten array" in {
      // "select loc[:*] from zips",
      convert(
        None,
        makeObj(
          "loc" ->
            structural.FlattenArray(
              structural.ObjectProject(lpRead("/zips"), LP.Constant(Data.Str("loc"))).embed))) must
      equal(chain(
        RootR,
        QC.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("zips")),
          ProjectFieldR(
            Free.point(RightSide),
            StrLit("loc")))),
        QC.inj(LeftShift((),
          HoleF,
          Free.roll(MakeMap(StrLit("loc"), Free.point(RightSide)))))).some)
    }

    "convert a constant shift array of size one" in {
      // this query never makes it to LP->QS transform because it's a constant value
      // "foo := (7); select * from foo"
      convert(
        None,
        identity.Squash(
          structural.ShiftArray(
            structural.MakeArrayN[Fix](LP.Constant(Data.Int(7))).embed).embed).embed) must
      equal(chain(
        UnreferencedR,
        QC.inj(LeftShift(
          (),
          Free.roll(Constant(
            EJson.fromCommon[Fix].apply(ejson.Arr[Fix[ejson.EJson]](List(
              EJson.fromExt[Fix].apply(ejson.Int[Fix[ejson.EJson]](7))))))),
          Free.point(RightSide)))).some)
    }

    "convert a constant shift array of size two" in {
      // this query never makes it to LP->QS transform because it's a constant value
      // "foo := (7,8); select * from foo"
      convert(
        None,
        identity.Squash(
          structural.ShiftArray(
            structural.ArrayConcat(
              structural.MakeArrayN[Fix](LP.Constant(Data.Int(7))).embed,
              structural.MakeArrayN[Fix](LP.Constant(Data.Int(8))).embed).embed).embed).embed) must
      equal(chain(
        UnreferencedR,
        QC.inj(LeftShift(
          (),
          Free.roll(Constant(
            EJson.fromCommon[Fix].apply(ejson.Arr(List(
              EJson.fromExt[Fix].apply(ejson.Int[Fix[ejson.EJson]](7)),
              EJson.fromExt[Fix].apply(ejson.Int[Fix[ejson.EJson]](8))))))),
          Free.point(RightSide)))).some)
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
                structural.MakeArrayN[Fix](LP.Constant(Data.Int(7))).embed,
                structural.MakeArrayN[Fix](LP.Constant(Data.Int(8))).embed).embed,
              structural.MakeArrayN[Fix](LP.Constant(Data.Int(9))).embed).embed).embed).embed) must
      equal(chain(
        UnreferencedR,
        QC.inj(LeftShift(
          (),
          Free.roll(Constant(
            EJson.fromCommon[Fix].apply(ejson.Arr[Fix[ejson.EJson]](List(
              EJson.fromExt[Fix].apply(ejson.Int[Fix[ejson.EJson]](7)),
              EJson.fromExt[Fix].apply(ejson.Int[Fix[ejson.EJson]](8)),
              EJson.fromExt[Fix].apply(ejson.Int[Fix[ejson.EJson]](9))))))),
          Free.point(RightSide)))).some)
    }

    "convert a read shift array" in {
      // select (baz || quux || ducks)[*] from `/foo/bar`
      convert(
        None,
        LP.Let('x, lpRead("/foo/bar"),
          structural.ShiftArray(
            structural.ArrayConcat(
              structural.ArrayConcat(
                structural.ObjectProject(LP.Free('x), LP.Constant(Data.Str("baz"))).embed,
                structural.ObjectProject(LP.Free('x), LP.Constant(Data.Str("quux"))).embed).embed,
              structural.ObjectProject(LP.Free('x), LP.Constant(Data.Str("ducks"))).embed).embed).embed)) must
      equal(chain(RootR).some) // TODO incorrect expectation
    }.pendingUntilFixed

    "convert a shift/unshift array" in {
      // "select [loc[_:] * 10 ...] from zips",
      convert(
        None,
        makeObj(
          "0" ->
            structural.UnshiftArray(
              math.Multiply(
                structural.ShiftArrayIndices(
                  structural.ObjectProject(lpRead("/zips"), LP.Constant(Data.Str("loc"))).embed).embed,
                LP.Constant(Data.Int(10))).embed))) must
      equal(chain(
        RootR,
        QC.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("zips")),
          Free.point[MapFunc[Fix, ?], JoinSide](RightSide))),
        QC.inj(LeftShift((),
          Free.roll(DupArrayIndices(
            ProjectFieldR(HoleF, StrLit("loc")))),
          Free.roll(Multiply(Free.point(RightSide), IntLit(10))))),
        QC.inj(Reduce((),
          HoleF, // FIXME provenance needs to be here
          List(ReduceFuncs.UnshiftArray(HoleF[Fix])),
          Free.roll(MakeMap[Fix, Free[MapFunc[Fix, ?], ReduceIndex]](
            StrLit[Fix, ReduceIndex]("0"),
            Free.point(ReduceIndex(0))))))).some)
    }.pendingUntilFixed

    "convert a filter" in { // takes 1 min 17 sec to run
      // "select * from foo where bar between 1 and 10"
      convert(
        listContents.some,
        StdLib.set.Filter(
          lpRead("/bar"),
          relations.Between(
            structural.ObjectProject(lpRead("/bar"), LP.Constant(Data.Str("baz"))).embed,
            LP.Constant(Data.Int(1)),
            LP.Constant(Data.Int(10))).embed).embed) must
      equal(chain(
        ReadR(rootDir </> file("bar")),
        QC.inj(LeftShift((),
          HoleF,
          Free.point(RightSide))),
        QC.inj(Filter((),
          Free.roll(Between(
            ProjectFieldR(HoleF, StrLit("baz")),
            IntLit(1),
            IntLit(10)))))).some)
    }.pendingUntilFixed

    // an example of how logical plan expects magical "left" and "right" fields to exist
    "convert magical query" in {
      // "select * from person, car",
      convert(
        None,
        LP.Let('__tmp0,
          StdLib.set.InnerJoin(lpRead("/person"), lpRead("/car"), LP.Constant(Data.Bool(true))).embed,
          identity.Squash(
            structural.ObjectConcat(
              structural.ObjectProject(LP.Free('__tmp0), LP.Constant(Data.Str("left"))).embed,
              structural.ObjectProject(LP.Free('__tmp0), LP.Constant(Data.Str("right"))).embed).embed).embed)) must
      equal(chain(RootR).some) // TODO incorrect expectation
    }.pendingUntilFixed

    "convert basic join with explicit join condition" in {
      //"select foo.name, bar.address from foo join bar on foo.id = bar.foo_id",

      val lp = LP.Let('__tmp0, lpRead("/foo"),
        LP.Let('__tmp1, lpRead("/bar"),
          LP.Let('__tmp2,
            StdLib.set.InnerJoin(LP.Free('__tmp0), LP.Free('__tmp1),
              relations.Eq(
                structural.ObjectProject(LP.Free('__tmp0), LP.Constant(Data.Str("id"))).embed,
                structural.ObjectProject(LP.Free('__tmp1), LP.Constant(Data.Str("foo_id"))).embed).embed).embed,
            makeObj(
              "name" ->
                structural.ObjectProject(
                  structural.ObjectProject(LP.Free('__tmp2), LP.Constant(Data.Str("left"))).embed,
                  LP.Constant(Data.Str("name"))),
              "address" ->
                structural.ObjectProject(
                  structural.ObjectProject(LP.Free('__tmp2), LP.Constant(Data.Str("right"))).embed,
                  LP.Constant(Data.Str("address")))))))
      convert(None, lp) must
      equal(chain(
        RootR,
        QC.inj(Map((), ProjectFieldR(HoleF, StrLit("foo"))))).some)
    }.pendingUntilFixed
  }
}
