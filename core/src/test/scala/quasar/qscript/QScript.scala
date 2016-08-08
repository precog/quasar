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
import quasar.{LogicalPlan => LP, Data, CompilerHelpers}
import quasar.ejson
import quasar.fp._
import quasar.fs._
import quasar.qscript.MapFuncs._
import quasar.std.StdLib._

import scala.Predef.implicitly

import matryoshka._
import org.specs2.scalaz._
import pathy.Path._
import scalaz._, Scalaz._

class QScriptSpec extends CompilerHelpers with ScalazMatchers {
  val transform = new Transform[Fix, QScriptTotal[Fix, ?]]

  // TODO: Narrow this to QScriptPure
  type QS[A] = QScriptTotal[Fix, A]
  val DE = implicitly[Const[DeadEnd, ?] :<: QS]
  val QC = implicitly[QScriptCore[Fix, ?] :<: QS]
  val SP = implicitly[SourcedPathable[Fix, ?] :<: QS]
  val TJ = implicitly[ThetaJoin[Fix, ?] :<: QS]

  def RootR: Fix[QS] = CorecursiveOps[Fix, QS](DE.inj(Const[DeadEnd, Fix[QS]](Root))).embed

  def ProjectFieldR[A](src: FreeMap[Fix], field: FreeMap[Fix]): FreeMap[Fix] =
    Free.roll(ProjectField(src, field))

  def lpRead(path: String): Fix[LP] =
    LP.Read(sandboxAbs(posixCodec.parseAbsFile(path).get))

  // TODO instead of calling `.toOption` on the `\/`
  // write an `Equal[PlannerError]` and test for specific errors too
  "replan" should {
    "convert a constant boolean" in {
       // "select true"
       QueryFile.convertToQScript(LP.Constant(Data.Bool(true))).toOption must
       equal(
         QC.inj(Map(RootR, BoolLit(true))).embed.some)
    }

    "fail to convert a constant set" in {
      // "select {\"a\": 1, \"b\": 2, \"c\": 3, \"d\": 4, \"e\": 5}{*} limit 3 offset 1"
      QueryFile.convertToQScript(
        LP.Constant(Data.Set(List(
          Data.Obj(ListMap("0" -> Data.Int(2))),
          Data.Obj(ListMap("0" -> Data.Int(3))))))).toOption must
      equal(None)
    }

    "convert a simple read" in {
      QueryFile.convertToQScript(lpRead("/foo")).toOption must
      equal(
        SP.inj(LeftShift(
          RootR,
          ProjectFieldR(HoleF, StrLit("foo")),
          Free.point(RightSide))).embed.some)
    }

    "convert a squashed read" in {
      // "select * from foo"
      QueryFile.convertToQScript(
        identity.Squash(lpRead("/foo"))).toOption must
      equal(
        SP.inj(LeftShift(
          RootR,
          ProjectFieldR(HoleF, StrLit("foo")),
          Free.point(RightSide))).embed.some)
    }

    "convert a simple read with path projects" in {
      QueryFile.convertToQScript(
        lpRead("/some/foo/bar")).toOption must
      equal(
        SP.inj(LeftShift(
          RootR,
          ProjectFieldR(
            ProjectFieldR(
              ProjectFieldR(HoleF, StrLit("some")),
              StrLit("foo")),
            StrLit("bar")),
          Free.point(RightSide))).embed.some)
    }

    "convert a basic invoke" in {
      QueryFile.convertToQScript(
        math.Add(lpRead("/foo"), lpRead("/bar")).embed).toOption must
      equal(
        TJ.inj(ThetaJoin(
          RootR,
          Free.roll(SP.inj(LeftShift(
            Free.roll(QC.inj(Map(
              Free.point(SrcHole),
              ProjectFieldR(HoleF, StrLit("foo"))))),
            Free.roll(ZipMapKeys(HoleF)),
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(Free.point(LeftSide))),
              Free.roll(MakeArray(Free.point(RightSide)))))))),
          Free.roll(SP.inj(LeftShift(
            Free.roll(QC.inj(Map(
              Free.point(SrcHole),
              ProjectFieldR(HoleF, StrLit("bar"))))),
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
              IntLit(1))))))).embed.some)
    }

    "convert project object and make object" in {
      QueryFile.convertToQScript(
        identity.Squash(
          makeObj(
            "name" -> structural.ObjectProject(
              lpRead("/city"),
              LP.Constant(Data.Str("name")))))).toOption must
      equal(
        SP.inj(LeftShift(
          RootR,
          ProjectFieldR(HoleF, StrLit("city")),
          Free.roll(MakeMap[Fix, JoinFunc[Fix]](
            StrLit[Fix, JoinSide]("name"),
            Free.roll(ProjectField(
              Free.point[MapFunc[Fix, ?], JoinSide](RightSide),
              StrLit[Fix, JoinSide]("name"))))))).embed.some)
    }

    "convert a basic reduction" in {
      QueryFile.convertToQScript(
        agg.Sum[FLP](lpRead("/person"))).toOption must
      equal(
        QC.inj(Reduce(
          SP.inj(LeftShift(
            QC.inj(Map(
              RootR,
              Free.roll(ProjectField(HoleF, StrLit("person"))))).embed,
            Free.roll(ZipMapKeys(HoleF)),
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(Free.point(LeftSide))),
              Free.roll(MakeArray(Free.point(RightSide))))))).embed,
          Free.roll(MakeArray(Free.roll(MakeMap(StrLit("f"), StrLit("person"))))),
          List(ReduceFuncs.Sum[FreeMap[Fix]](
            Free.roll(ProjectIndex(
              Free.roll(ProjectIndex(HoleF, IntLit(1))),
              IntLit(1))))),
          Free.point(ReduceIndex(0)))).embed.some)
    }

    "convert a basic reduction wrapped in an object" in {
      // "select sum(height) from person"
      QueryFile.convertToQScript(
        makeObj(
          "0" ->
            agg.Sum[FLP](structural.ObjectProject(lpRead("/person"), LP.Constant(Data.Str("height")))))).toOption must
      equal(
        QC.inj(Reduce(
          SP.inj(LeftShift(
            RootR,
            Free.roll(ProjectField(HoleF, StrLit("person"))),
            Free.roll(ProjectField(
              Free.point(RightSide),
              StrLit("height"))))).embed,
          Free.roll(MakeArray(
            Free.roll(MakeMap(
              StrLit("j"),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(Free.roll(MakeMap(StrLit("f"), StrLit("person"))))),
                Free.roll(MakeArray(NullLit())))))))),
          List(ReduceFuncs.Sum[FreeMap[Fix]](HoleF)),
          Free.roll(MakeMap(StrLit("0"), Free.point(ReduceIndex(0)))))).embed.some)
    }

    "convert a flatten array" in {
      // "select loc[:*] from zips",
      QueryFile.convertToQScript(
        makeObj(
          "loc" ->
            structural.FlattenArray[FLP](
              structural.ObjectProject(lpRead("/zips"), LP.Constant(Data.Str("loc")))))).toOption must
      equal(
        SP.inj(LeftShift(
          SP.inj(LeftShift(
            RootR,
            Free.roll(ProjectField(HoleF, StrLit("zips"))),
            Free.roll(ProjectField(
              Free.point(RightSide),
              StrLit("loc"))))).embed,
          HoleF,
          Free.roll(MakeMap(StrLit("loc"), Free.point(RightSide))))).embed.some)
    }

    "convert a constant shift array" in pending {
      // this query never makes it to LP->QS transform because it's a constant value
      // "foo := (1,2,3); select * from foo"
      QueryFile.convertToQScript(
        LP.Let('x, lpRead("/foo/bar"),
          structural.ShiftArray[FLP](
            structural.ArrayConcat[FLP](
              structural.ArrayConcat[FLP](
                structural.ObjectProject[FLP](LP.Free('x), LP.Constant(Data.Str("baz"))),
                structural.ObjectProject[FLP](LP.Free('x), LP.Constant(Data.Str("quux")))),
              structural.ObjectProject[FLP](LP.Free('x), LP.Constant(Data.Str("ducks"))))))).toOption must
      equal(
        SP.inj(LeftShift(
          RootR,
          Free.roll(Nullary(
            CommonEJson.inj(ejson.Arr(List(
              ExtEJson.inj(ejson.Int[Fix[ejson.EJson]](1)).embed,
              ExtEJson.inj(ejson.Int[Fix[ejson.EJson]](2)).embed,
              ExtEJson.inj(ejson.Int[Fix[ejson.EJson]](3)).embed))).embed)),
          Free.point(RightSide))).embed.some)
    }

    "convert a shift/unshift array" in pending {
      // "select [loc[_:] * 10 ...] from zips",
      QueryFile.convertToQScript(
        makeObj(
          "0" ->
            structural.UnshiftArray[FLP](
              math.Multiply[FLP](
                structural.ShiftArrayIndices[FLP](
                  structural.ObjectProject(lpRead("/zips"), LP.Constant(Data.Str("loc")))),
                LP.Constant(Data.Int(10)))))).toOption must
      equal(
        QC.inj(Reduce(
          SP.inj(LeftShift(
            RootR,
            Free.roll(DupArrayIndices(
              Free.roll(ProjectField(
                Free.roll(ProjectField(HoleF, StrLit("zips"))),
                StrLit("loc"))))),
            Free.roll(Multiply(Free.point(RightSide), IntLit(10))))).embed,
          HoleF,
          List(ReduceFuncs.UnshiftArray(HoleF[Fix])),
          Free.roll(MakeMap[Fix, Free[MapFunc[Fix, ?], ReduceIndex]](
            StrLit[Fix, ReduceIndex]("0"),
            Free.point(ReduceIndex(0)))))).embed.some)
    }

    "convert a filter" in pending {
      // "select * from foo where bar between 1 and 10"
      QueryFile.convertToQScript(
        set.Filter[FLP](
          lpRead("/foo"),
          relations.Between[FLP](
            structural.ObjectProject(lpRead("/foo"), LP.Constant(Data.Str("bar"))),
            LP.Constant(Data.Int(1)),
            LP.Constant(Data.Int(10))))).toOption must
      equal(
        QC.inj(Filter(
          SP.inj(LeftShift(
            RootR,
            Free.roll(ProjectField(HoleF, StrLit("foo"))),
            Free.point(RightSide))).embed,
          Free.roll(Between(
            Free.roll(ProjectField(HoleF, StrLit("bar"))),
            IntLit(1),
            IntLit(10))))).embed.some)
    }

    // an example of how logical plan expects magical "left" and "right" fields to exist
    "convert magical query" in pending {
      // "select * from person, car",
      QueryFile.convertToQScript(
        LP.Let('__tmp0,
          set.InnerJoin(lpRead("/person"), lpRead("/car"), LP.Constant(Data.Bool(true))),
          identity.Squash[FLP](
            structural.ObjectConcat[FLP](
              structural.ObjectProject(LP.Free('__tmp0), LP.Constant(Data.Str("left"))),
              structural.ObjectProject(LP.Free('__tmp0), LP.Constant(Data.Str("right"))))))).toOption must
      equal(RootR.some) // TODO incorrect expectation
    }

    "convert basic join with explicit join condition" in pending {
      //"select foo.name, bar.address from foo join bar on foo.id = bar.foo_id",

      val lp = LP.Let('__tmp0, lpRead("/foo"),
        LP.Let('__tmp1, lpRead("/bar"),
          LP.Let('__tmp2,
            set.InnerJoin[FLP](LP.Free('__tmp0), LP.Free('__tmp1),
              relations.Eq[FLP](
                structural.ObjectProject(LP.Free('__tmp0), LP.Constant(Data.Str("id"))),
                structural.ObjectProject(LP.Free('__tmp1), LP.Constant(Data.Str("foo_id"))))),
            makeObj(
              "name" ->
                structural.ObjectProject[FLP](
                  structural.ObjectProject(LP.Free('__tmp2), LP.Constant(Data.Str("left"))),
                  LP.Constant(Data.Str("name"))),
              "address" ->
                structural.ObjectProject[FLP](
                  structural.ObjectProject(LP.Free('__tmp2), LP.Constant(Data.Str("right"))),
                  LP.Constant(Data.Str("address")))))))
      QueryFile.convertToQScript(lp).toOption must equal(
        QC.inj(Map(RootR, ProjectFieldR(HoleF, StrLit("foo")))).embed.some)
    }
  }
}
