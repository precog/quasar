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

package quasar.qsu

import slamdata.Predef._

import quasar.{Data, DataGenerators, Type, Qspec}
import quasar.fs.Planner.PlannerError
import quasar.common.{JoinType, SortDir}
import quasar.contrib.scalaz.{NonEmptyListE => NELE}
import quasar.frontend.logicalplan.{JoinCondition, LogicalPlan, LogicalPlanHelpers}
import quasar.qscript.{
  Center,
  Drop,
  LeftSide,
  LeftSide3,
  MapFuncsCore,
  MapFuncsDerived,
  ReduceFuncs,
  RightSide,
  RightSide3,
  Sample,
  SrcHole,
  Take
}
import quasar.qsu.{QScriptUniform => QSU}
import quasar.std.{
  AggLib,
  IdentityLib,
  MathLib,
  RelationsLib,
  SetLib,
  StringLib,
  StructuralLib
}
import quasar.time.TemporalPart

import matryoshka.data.Fix
import org.specs2.matcher.{Expectable, Matcher, MatchResult}
import scalaz.{\/, EitherT, Need, NonEmptyList => NEL, StateT}
import scalaz.syntax.bifunctor._
import scalaz.syntax.show._
import pathy.Path, Path.{file, Sandboxed}
import iotaz.CopK

object ReadLPSpec extends Qspec with LogicalPlanHelpers with DataGenerators with QSUTTypes[Fix] {
  import QSUGraph.Extractors._

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]

  val reader = ReadLP[Fix, F] _
  val root = Path.rootDir[Sandboxed]

  val IC = CopK.Inject[MapFuncCore, MapFunc]
  val ID = CopK.Inject[MapFuncDerived, MapFunc]

  "reading lp into qsu" should {
    "convert Read nodes" in {
      read("foobar") must readQsuAs {
        case Transpose(Read(path), QSU.Retain.Values, QSU.Rotation.ShiftMap) =>
          path mustEqual (root </> file("foobar"))
      }
    }

    // we can't do this as a property test because Data and EJson don't round-trip! :-(
    "convert constant nodes" >> {
      "Int" >> {
        val data = Data.Int(42)

        lpf.constant(data) must readQsuAs {
          case DataConstant(`data`) => ok
        }
      }

      "String" >> {
        val data = Data.Str("foo")

        lpf.constant(data) must readQsuAs {
          case DataConstant(`data`) => ok
        }
      }
    }

    "convert FlattenMap" in {
      lpf.invoke1(StructuralLib.FlattenMap, read("foo")) must readQsuAs {
        case Transpose(TRead(_), QSU.Retain.Values, QSU.Rotation.FlattenMap) => ok
      }
    }

    "convert FlattenMapKeys" in {
      lpf.invoke1(StructuralLib.FlattenMapKeys, read("foo")) must readQsuAs {
        case Transpose(TRead(_), QSU.Retain.Identities, QSU.Rotation.FlattenMap) => ok
      }
    }

    "convert FlattenArray" in {
      lpf.invoke1(StructuralLib.FlattenArray, read("foo")) must readQsuAs {
        case Transpose(TRead(_), QSU.Retain.Values, QSU.Rotation.FlattenArray) => ok
      }
    }

    "convert FlattenArrayIndices" in {
      lpf.invoke1(StructuralLib.FlattenArrayIndices, read("foo")) must readQsuAs {
        case Transpose(TRead(_), QSU.Retain.Identities, QSU.Rotation.FlattenArray) => ok
      }
    }

    "convert ShiftMap" in {
      lpf.invoke1(StructuralLib.ShiftMap, read("foo")) must readQsuAs {
        case Transpose(TRead(_), QSU.Retain.Values, QSU.Rotation.ShiftMap) => ok
      }
    }

    "convert ShiftMapKeys" in {
      lpf.invoke1(StructuralLib.ShiftMapKeys, read("foo")) must readQsuAs {
        case Transpose(TRead(_), QSU.Retain.Identities, QSU.Rotation.ShiftMap) => ok
      }
    }

    "convert ShiftArray" in {
      lpf.invoke1(StructuralLib.ShiftArray, read("foo")) must readQsuAs {
        case Transpose(TRead(_), QSU.Retain.Values, QSU.Rotation.ShiftArray) => ok
      }
    }

    "convert ShiftArrayIndices" in {
      lpf.invoke1(StructuralLib.ShiftArrayIndices, read("foo")) must readQsuAs {
        case Transpose(TRead(_), QSU.Retain.Identities, QSU.Rotation.ShiftArray) => ok
      }
    }

    "convert GroupBy" in {
      lpf.invoke2(SetLib.GroupBy, read("foo"), read("bar")) must readQsuAs {
        case GroupBy(TRead("foo"), TRead("bar")) => ok
      }
    }

    "convert Squash" in {
      lpf.invoke1(IdentityLib.Squash, read("foo")) must readQsuAs {
        case DimEdit(TRead("foo"), QSU.DTrans.Squash()) => ok
      }
    }

    "convert Filter" in {
      lpf.invoke2(SetLib.Filter, read("foo"), read("bar")) must readQsuAs {
        case LPFilter(TRead("foo"), TRead("bar")) => ok
      }
    }

    "convert Sample" in {
      lpf.invoke2(SetLib.Sample, read("foo"), read("bar")) must readQsuAs {
        case Subset(TRead("foo"), Sample, TRead("bar")) => ok
      }
    }

    "convert Take" in {
      lpf.invoke2(SetLib.Take, read("foo"), read("bar")) must readQsuAs {
        case Subset(TRead("foo"), Take, TRead("bar")) => ok
      }
    }

    "convert Drop" in {
      lpf.invoke2(SetLib.Drop, read("foo"), read("bar")) must readQsuAs {
        case Subset(TRead("foo"), Drop, TRead("bar")) => ok
      }
    }

    "convert Union" in {
      lpf.invoke2(SetLib.Union, read("foo"), read("bar")) must readQsuAs {
        case Union(TRead("foo"), TRead("bar")) => ok
      }
    }

    "convert reductions" in {
      lpf.invoke1(AggLib.Count, read("foo")) must readQsuAs {
        case LPReduce(TRead("foo"), ReduceFuncs.Count(())) => ok
      }
    }

    "convert unary mapping function" in {
      lpf.invoke1(MathLib.Negate, read("foo")) must readQsuAs {
        case Unary(TRead("foo"), IC(MapFuncsCore.Negate(SrcHole))) => ok
      }
    }

    "convert binary mapping function" in {
      lpf.invoke2(MathLib.Add, read("foo"), read("bar")) must readQsuAs {
        case AutoJoin2C(
          TRead("foo"),
          TRead("bar"),
          MapFuncsCore.Add(LeftSide, RightSide)) => ok
      }
    }

    "convert ternary mapping function" in {
      lpf.invoke3(RelationsLib.Cond, read("foo"), read("bar"), read("baz")) must readQsuAs {
        case AutoJoin3C(
          TRead("foo"),
          TRead("bar"),
          TRead("baz"),
          MapFuncsCore.Cond(LeftSide3, Center, RightSide3)) => ok
      }
    }

    "convert TemporalTrunc" in {
      lpf.temporalTrunc(TemporalPart.Decade, read("foo")) must readQsuAs {
        case Unary(
          TRead("foo"),
          IC(MapFuncsCore.TemporalTrunc(TemporalPart.Decade, SrcHole))) => ok
      }
    }

    "convert Typecheck" in {
      lpf.typecheck(read("foo"), Type.AnyObject, read("bar"), read("baz")) must readQsuAs {
        case AutoJoin3C(
          TRead("foo"),
          TRead("bar"),
          TRead("baz"),
          MapFuncsCore.Guard(LeftSide3, Type.AnyObject, Center, RightSide3)) => ok
      }
    }

    "convert join side" in {
      lpf.joinSideName('heythere) must readQsuAs {
        case JoinSideRef('heythere) => ok
      }
    }

    "convert real join" in {
      lpf.join(
        read("foo"),
        read("bar"),
        JoinType.LeftOuter,
        JoinCondition(
          'left,
          'right,
          read("baz"))) must readQsuAs {
        case LPJoin(
          TRead("foo"),
          TRead("bar"),
          TRead("baz"),
          JoinType.LeftOuter,
          'left,
          'right) => ok
      }
    }

    "import let bindings" in {
      lpf.let(
        'tmp0,
        read("foo"),
        lpf.invoke2(
          SetLib.Filter,
          lpf.free('tmp0),
          read("bar"))) must readQsuAs {
        case LPFilter(TRead("foo"), TRead("bar")) => ok
      }
    }

    "convert a sort" in {
      lpf.sort(
        read("foo"),
        NEL(
          read("bar") -> SortDir.Ascending,
          read("baz") -> SortDir.Descending)) must readQsuAs {
        case LPSort(
          TRead("foo"),
          NELE(
            (TRead("bar"), SortDir.Ascending),
            (TRead("baz"), SortDir.Descending))) => ok
      }
    }

    "compress redundant first- and second-order nodes" in {
      val qgraphM = reader(lpf.invoke2(SetLib.Filter, read("foo"), read("foo")))
      val result = evaluate(qgraphM).toOption

      result must beSome
      result.get.vertices must haveSize(3)
    }

    "manage a straightforward query" in {
      // select city from zips where city ~ "OULD.{0,2} CIT"
      val input =
        lpf.let(
          '__tmp0,
          lpf.let(
            '__tmp1,
            read("zips"),
            lpf.typecheck(
              lpf.free('__tmp1),
              Type.AnyObject,
              lpf.free('__tmp1),
              lpf.constant(Data.NA))),
          lpf.invoke2(
            SetLib.Take,
            lpf.invoke1(
              IdentityLib.Squash,
              lpf.invoke2(
                StructuralLib.MapProject,
                lpf.invoke2(
                  SetLib.Filter,
                  lpf.free('__tmp0),
                  lpf.let(
                    '__tmp2,
                    lpf.invoke2(
                      StructuralLib.MapProject,
                      lpf.free('__tmp0),
                      lpf.constant(Data.Str("city"))),
                    lpf.typecheck(
                      lpf.free('__tmp2),
                      Type.Str,
                      lpf.invoke3(
                        StringLib.Search,
                        lpf.free('__tmp2),
                        lpf.constant(Data.Str("OULD.{0,2} CIT")),
                        lpf.constant(Data.Bool(false))),
                      lpf.constant(Data.NA)))),
                lpf.constant(Data.Str("city")))),
            lpf.constant(Data.Int(11))))

      input must readQsuAs {
        case Subset(
          DimEdit(
            AutoJoin2C(
              LPFilter(
                Unary(   // '__tmp0
                  Transpose(    // '__tmp1
                    Read(_),
                    QSU.Retain.Values,
                    QSU.Rotation.ShiftMap),
                  ID(MapFuncsDerived.Typecheck(SrcHole, Type.AnyObject))),
                AutoJoin3C(
                  AutoJoin2C(   // '__tmp2
                    Unary(   // '__tmp0
                      Transpose(    // '__tmp1
                        Read(_),
                        QSU.Retain.Values,
                        QSU.Rotation.ShiftMap),
                      ID(MapFuncsDerived.Typecheck(SrcHole, Type.AnyObject))),
                    DataConstant(Data.Str("city")),
                    MapFuncsCore.ProjectKey(LeftSide, RightSide)),
                  AutoJoin3C(
                    AutoJoin2C(   // '__tmp2
                      Unary(   // '__tmp0
                        Transpose(    // '__tmp1
                          Read(_),
                          QSU.Retain.Values,
                          QSU.Rotation.ShiftMap),
                        ID(MapFuncsDerived.Typecheck(SrcHole, Type.AnyObject))),
                      DataConstant(Data.Str("city")),
                      MapFuncsCore.ProjectKey(LeftSide, RightSide)),
                    DataConstant(Data.Str("OULD.{0,2} CIT")),
                    DataConstant(Data.Bool(false)),
                    MapFuncsCore.Search(LeftSide3, Center, RightSide3)),
                  _,
                  MapFuncsCore.Guard(LeftSide3, Type.Str, Center, RightSide3))),
              DataConstant(Data.Str("city")),
              MapFuncsCore.ProjectKey(LeftSide, RightSide)),
            QSU.DTrans.Squash()),
          Take,
          DataConstant(Data.Int(subsetTakeI))) =>

        subsetTakeI mustEqual 11
      }
    }
  }

  def readQsuAs(pf: PartialFunction[QSUGraph, MatchResult[_]]): Matcher[Fix[LogicalPlan]] = {
    new Matcher[Fix[LogicalPlan]] {
      def apply[S <: Fix[LogicalPlan]](s: Expectable[S]): MatchResult[S] = {
        val resulted = evaluate(reader(s.value)) leftMap { err =>
          failure(s"reading produced planner error: ${err.shows}", s)
        }

        val continued = resulted rightMap { qgraph =>
          val mapped = pf.lift(qgraph) map { r =>
            result(
              r.isSuccess,
              s.description + " is correct: " + r.message,
              s.description + " is incorrect: " + r.message,
              s)
          }

          // TODO Show[QSUGraph[Fix]]
          mapped.getOrElse(
            failure(s"${qgraph.shows} did not match expected pattern", s))
        }

        continued.merge
      }
    }
  }

  def evaluate[A](fa: F[A]): PlannerError \/ A = fa.run.eval(0L).value
}
