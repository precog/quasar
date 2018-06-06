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

package quasar.qscript.analysis

import slamdata.Predef.{Map => _, _}
import quasar.common.{JoinType, JoinTypeArbitrary, SortDir}
import quasar.contrib.matryoshka._
import quasar.contrib.matryoshka.arbitrary._
import quasar.fp._, Helpers._
import quasar.contrib.iota._
import quasar.fp.ski.κ
import quasar.ejson
import quasar.ejson.{CommonEJson, EJson, EJsonArbitrary, ExtEJson}
import quasar.ejson.implicits._
import quasar.qscript.RecFreeS._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.data.Fix
import matryoshka.data.free._
import matryoshka.implicits._
import org.specs2.scalacheck._
import scalaz._, Scalaz._

final class OutlineSpec extends quasar.Qspec with QScriptHelpers {
  import EJsonArbitrary._, FigureArbitrary._, JoinTypeArbitrary._
  import MapFuncCore.StaticMap
  import Outline.{Figure, Shape, freeEJsonEqual, undefinedF, unknownF, arrF, mapF}

  implicit val params = Parameters(maxSize = 10)

  import qstdsl._

  val toFree = convertToFree[EJson, Figure](_: Fix[EJson])
  val rollS = Free.roll[EJson, Figure] _

  val joinFunc =
    func.StaticMapS(
      "left" -> func.LeftSide,
      "right" -> func.RightSide)

  val modSides: (Shape, Shape) => Shape =
    (l, r) => rollS(ExtEJson(ejson.Map(List(
      rollS(CommonEJson(ejson.Str("left"))) -> l,
      rollS(CommonEJson(ejson.Str("right"))) -> r))))

  "Outline FreeMap" >> {
    val outlineFM = Outline.outlineF(_: FreeMap)(κ(unknownF))
    val av = func.ConcatArrays(func.Hole, func.Hole)
    val mv = func.ConcatMaps(func.Hole, func.Hole)
    val uv = func.ProjectIndexI(func.Hole, 42)

    "unknown when no static structure" >> {
      outlineFM(uv) must_= unknownF
    }

    "constants are lifted into Free" >> prop { ejs: Fix[EJson] =>
      outlineFM(func.Constant(ejs)).transCataM[Option, Fix[EJson], EJson](_.run.toOption) must_= Some(ejs)
    }

    "map construction with constant keys result in maps" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson]) => (k1 =/= k2) ==> {

      val fm = func.ConcatMaps(func.MakeMap(func.Constant(k1), av), func.MakeMap(func.Constant(k2), mv))

      val ss =
        rollS(ExtEJson(ejson.Map(List(
          toFree(k1) -> arrF,
          toFree(k2) -> mapF))))

      outlineFM(fm) must_= ss
    }}

    "map construction with at least one non-constant key is not static" >> prop { k1: Fix[EJson] =>
      val k2 = func.ProjectKeyS(func.Hole, "keyName")
      val fm = func.ConcatMaps(func.MakeMap(func.Constant(k1), uv), func.MakeMap(k2, uv))

      outlineFM(fm) must_= mapF
    }

    "concatenation of maps is right-biased" >> prop { k: Fix[EJson] =>
      val fm = func.ConcatMaps(func.MakeMap(func.Constant(k), mv), func.MakeMap(func.Constant(k), av))
      val ss = rollS(ExtEJson(ejson.Map(List(toFree(k) -> arrF))))

      outlineFM(fm) must_= ss
    }

    "concatenation of maps preserves key order" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson], k3: Fix[EJson]) => (k1 =/= k2 && k1 =/= k3 && k2 =/= k3) ==> {

      val l =
        StaticMap[Fix, Hole](List(k1 -> func.Hole, k2 -> func.Hole, k3 -> func.Hole))

      val r =
        StaticMap[Fix, Hole](List(k2 -> av))

      val ss = rollS(ExtEJson(ejson.Map(List(
        toFree(k1) -> unknownF,
        toFree(k2) -> arrF,
        toFree(k3) -> unknownF))))

      outlineFM(func.ConcatMaps(l, r)) must_= ss
    }}

    "key projection on static map results in key value" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson]) => (k1 =/= k2) ==> {

      val fm =
        func.ProjectKey(
          func.ConcatMaps(func.MakeMap(func.Constant(k1), mv), func.MakeMap(func.Constant(k2), av)),
          func.Constant(k1))

      outlineFM(fm) must_= mapF
    }}

    "key projection on static map without key results in undefined" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson]) => (k1 =/= k2) ==> {

      val fm = func.ProjectKey(func.MakeMap(func.Constant(k1), av), func.Constant(k2))

      outlineFM(fm) must_= undefinedF
    }}

    "key deletion on static map results in static key deleted" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson]) => (k1 =/= k2) ==> {

      val fm =
        func.DeleteKey(
          func.ConcatMaps(func.MakeMap(func.Constant(k2), av), func.MakeMap(func.Constant(k1), mv)),
          func.Constant(k1))

      val ss =
        rollS(ExtEJson(ejson.Map(List(toFree(k2) -> arrF))))

      outlineFM(fm) must_= ss
    }}

    "key deletion of nonexistent on static map is identity" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson]) => (k1 =/= k2) ==> {

      val fm = func.DeleteKey(func.MakeMap(func.Constant(k1), av), func.Constant(k2))

      val ss = rollS(ExtEJson(ejson.Map(List(toFree(k1) -> arrF))))

      outlineFM(fm) must_= ss
    }}

    "array construction results in arrays" >> {
      val fm =
        func.ConcatArrays(
          func.MakeArray(uv),
          func.ConcatArrays(
            func.ConcatArrays(
              func.MakeArray(mv),
              func.MakeArray(av)),
            func.MakeArray(uv)))

      val ss =
        rollS(CommonEJson(ejson.Arr(List(unknownF, mapF, arrF, unknownF))))

      outlineFM(fm) must_= ss
    }

    "index projection on static array results in index value" >> {
      val fm =
        func.ProjectIndexI(
          func.ConcatArrays(func.ConcatArrays(func.MakeArray(av), func.MakeArray(mv)), func.MakeArray(uv)), 1)

      outlineFM(fm) must_= mapF
    }

    "index projection on static array with invalid index results in undefined" >> {
      outlineFM(func.ProjectIndexI(func.MakeArray(mv), -5)) must_= undefinedF
    }

    "index projection on static array with nonexistent index results in undefined" >> {
      outlineFM(func.ProjectIndexI(func.MakeArray(mv), 7)) must_= undefinedF
    }

    "complex shape" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson]) => (k1 =/= k2) ==> {

      val fm =
        func.ConcatArrays(
          func.MakeArray(uv),
          func.MakeArray(func.ConcatMaps(
            func.MakeMap(func.Constant(k1), func.MakeArray(mv)),
            func.MakeMap(func.Constant(k2), func.MakeMap(func.Constant(k1), av)))))

      val ss =
        rollS(CommonEJson(ejson.Arr(List(
          unknownF,
          rollS(ExtEJson(ejson.Map(List(
            toFree(k1) -> rollS(CommonEJson(ejson.Arr(List(mapF)))),
            toFree(k2) -> rollS(ExtEJson(ejson.Map(List(toFree(k1) -> arrF))))))))))))

      outlineFM(fm) must_= ss
    }}
  }

  "Outline QScriptCore" >> {
    val outlineQC: QScriptCore[Shape] => Shape =
      Outline[QScriptCore].outlineƒ

    val fun =
      func.ConcatArrays(
        func.MakeArray(func.Hole),
        func.MakeArray(func.Constant(Fix(ExtEJson(ejson.Int(75))))))

    val recFun =
      recFunc.ConcatArrays(
        recFunc.MakeArray(recFunc.Hole),
        recFunc.MakeArray(recFunc.Constant(Fix(ExtEJson(ejson.Int(75))))))

    val modSrc: Shape => Shape =
      s => rollS(CommonEJson(ejson.Arr(List(s, rollS(ExtEJson(ejson.Int(75)))))))

    "Map is shape of function applied to source" >> prop { srcShape: Shape =>
      outlineQC(Map(srcShape, recFun)) must_= modSrc(srcShape)
    }

    "LeftShift(IncludeId) results in shape of repair applied to static array" >> prop { srcShape: Shape =>
      val r = rollS(CommonEJson(ejson.Arr(List(unknownF, unknownF))))
      outlineQC(LeftShift(srcShape, recFunc.Hole, IncludeId, ShiftType.Array, OnUndefined.Omit, joinFunc)) must_= modSides(srcShape, r)
    }

    "RightSide of repair is unknown when not IncludeId" >> prop { srcShape: Shape =>
      outlineQC(LeftShift(srcShape, recFunc.Hole, ExcludeId, ShiftType.Array, OnUndefined.Omit, joinFunc)) must_= modSides(srcShape, unknownF)
    }

    "Reduce tracks static input shape through buckets" >> prop { srcShape: Shape =>
      val rfunc = joinFunc map {
        case LeftSide => ReduceIndex(0.left)
        case RightSide => ReduceIndex(0.right)
      }

      val outShape = modSides(modSrc(srcShape), unknownF)

      outlineQC(Reduce(srcShape, List(fun), List(ReduceFuncs.Count(func.Hole)), rfunc)) must_= outShape
    }

    "Reduce tracks static input shape through parametric reducers" >> prop { srcShape: Shape =>
      val rfunc = joinFunc map {
        case LeftSide => ReduceIndex(0.right)
        case RightSide => ReduceIndex(1.right)
      }

      val reducers = List[ReduceFunc[FreeMap]](
        ReduceFuncs.Count(func.Hole),
        ReduceFuncs.Last(fun))

      val outShape = modSides(unknownF, modSrc(srcShape))

      outlineQC(Reduce(srcShape, Nil, reducers, rfunc)) must_= outShape
    }

    "Shape of composite-typed reducers is composite type" >> prop { srcShape: Shape =>
      val rfunc = joinFunc map {
        case LeftSide => ReduceIndex(0.right)
        case RightSide => ReduceIndex(1.right)
      }

      val reducers = List[ReduceFunc[FreeMap]](
        ReduceFuncs.UnshiftArray(func.Hole),
        ReduceFuncs.UnshiftMap(func.Hole, func.Hole))

      val outShape = modSides(arrF, mapF)

      outlineQC(Reduce(srcShape, Nil, reducers, rfunc)) must_= outShape
    }

    "Subset is the shape of `from` applied to the source shape" >> prop { srcShape: Shape =>
      val from: FreeQS =
        free.Sort(
          free.Map(free.Hole, recFun),
          Nil,
          NonEmptyList((func.Hole, SortDir.Ascending)))

      val count: FreeQS =
        free.Map(free.Hole, recFunc.Constant(json.int(2)))

      outlineQC(Subset(srcShape, from, Take, count)) must_= modSrc(srcShape)
    }

    "Sort does not affect shape" >> prop { srcShape: Shape =>
      outlineQC(Sort(srcShape, Nil, NonEmptyList((fun, SortDir.Descending)))) must_= srcShape
    }

    "Filter does not affect shape" >> prop { srcShape: Shape =>
      outlineQC(Filter(srcShape, fun.asRec)) must_= srcShape
    }

    "Unreferenced has undefined shape" >> {
      outlineQC(Unreferenced()) must_= undefinedF
    }
  }

  val lfn: RecFreeMap = MapFuncCore.RecStaticArray(List(recFunc.Hole, recFunc.Constant(json.int(53))))
  val l: FreeQS = free.Map(free.Hole, lfn)
  def lShape(srcShape: Shape): Shape =
    rollS(CommonEJson(ejson.Arr(List(srcShape, rollS(ExtEJson(ejson.Int(53)))))))

  val rfn: RecFreeMap = MapFuncCore.RecStaticArray(List(recFunc.Constant(json.int(78)), recFunc.Hole))
  val r: FreeQS = free.Map(free.Hole, rfn)
  def rShape(srcShape: Shape): Shape =
    rollS(CommonEJson(ejson.Arr(List(rollS(ExtEJson(ejson.Int(78))), srcShape))))

  def joinShape(srcShape: Shape) =
    modSides(lShape(srcShape), rShape(srcShape))

  "Outline ThetaJoin" >> {
    "results from applying combine to branch shapes" >> prop { (srcShape: Shape, jtype: JoinType) =>
      val on = func.Constant[JoinSide](json.nul())
      Outline[ThetaJoin].outlineƒ(ThetaJoin(srcShape, l, r, on, jtype, joinFunc)) must_= joinShape(srcShape)
    }
  }

  "Outline EquiJoin" >> {
    "results from applying combine to branch shapes" >> prop { (srcShape: Shape, jtype: JoinType) =>
      Outline[EquiJoin].outlineƒ(EquiJoin(srcShape, l, r, Nil, jtype, joinFunc)) must_= joinShape(srcShape)
    }
  }
}
