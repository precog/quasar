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

package quasar.qscript.rewrites

import slamdata.Predef._
import quasar.common.{JoinType, SortDir}
import quasar.contrib.matryoshka._
import quasar.contrib.matryoshka.arbitrary._
import quasar.contrib.pathy._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski.κ
import quasar.ejson.{EJson, EJsonArbitrary}
import quasar.ejson.implicits._
import quasar.qscript._
import quasar.qscript.RecFreeS._
import quasar.qscript.analysis.Outline

import matryoshka.{delayEqual, delayShow}
import matryoshka.data.Fix
import matryoshka.data.free._
import org.specs2.scalacheck._
import pathy.Path._

import scalaz._
import Scalaz._

final class PreferProjectionSpec extends quasar.Qspec with QScriptHelpers {
  import EJsonArbitrary._
  import PreferProjection.{projectComplement, preferProjection}

  implicit val params = Parameters(maxSize = 10)

  import qsdsl._

  "projectComplement" >> {
    "replace key deletion of statically known structure with projection of complement" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson], k3: Fix[EJson]) => (k1 =/= k2 && k2 =/= k3 && k1 =/= k3) ==> {

      val m =
        func.ConcatMaps(
          func.ConcatMaps(
            func.MakeMap(func.Constant(k1), func.Hole),
            func.MakeMap(func.Constant(k2), func.Hole)),
          func.MakeMap(func.Constant(k3), func.Hole))

      val in =
        func.DeleteKey(m, func.Constant(k2))

      val out =
        func.ConcatMaps(
          func.MakeMap(func.Constant(k1), func.Hole),
          func.MakeMap(func.Constant(k3), func.Hole))

      projectComplement(in)(κ(Outline.unknownF)) must_= out
    }}

    "preserve key deletion when structure isn't statically known" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson], k3: Fix[EJson]) => (k1 =/= k2 && k2 =/= k3 && k1 =/= k3) ==> {

      val m =
        func.ConcatMaps(
          func.ConcatMaps(
            func.MakeMap(func.Constant(k1), func.Hole),
            func.MakeMap(func.Constant(k2), func.Hole)),
          func.Hole)

      val in =
        func.DeleteKey(m, func.Constant(k2))

      projectComplement(in)(κ(Outline.unknownF)) must_= in
    }}

    "preserve key deletion when any key not statically known" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson], v1: Fix[EJson]) => (k1 =/= k2) ==> {

      val m =
        func.ConcatMaps(
          func.ConcatMaps(
            func.MakeMap(func.Constant(k1), func.Hole),
            func.MakeMap(func.Constant(k2), func.Hole)),
          func.MakeMap(func.ProjectIndex(func.Hole, func.Constant(json.int(1))), func.Constant(v1)))

      val in =
        func.DeleteKey(m, func.Constant(k2))

      projectComplement(in)(κ(Outline.unknownF)) must_= in
    }}
  }

  "preferProjection" >> {
    val base: Fix[QS] =
      fix.LeftShift(
        fix.Read[AFile](rootDir </> dir("foo") </> file("bar")),
        recFunc.Hole,
        ExcludeId,
        ShiftType.Array,
        OnUndefined.Omit,
        MapFuncCore.StaticMap(List(
          json.str("a") -> func.ProjectIndex(func.RightSide, func.Constant(json.int(0))),
          json.str("b") -> func.ProjectIndex(func.RightSide, func.Constant(json.int(2))),
          json.str("c") -> func.ProjectIndex(func.RightSide, func.Constant(json.int(5))))))

    def prjFrom[A](src: FreeMapA[A], key: String, keys: String*): FreeMapA[A] =
      MapFuncCore.StaticMap((key :: keys.toList) map { name =>
        json.str(name) -> func.ProjectKeyS(src, name)
      })

    def recPrjFrom[A](src: RecFreeMapA[A], key: String, keys: String*): RecFreeMapA[A] =
      MapFuncCore.RecStaticMap((key :: keys.toList) map { name =>
        json.str(name) -> recFunc.ProjectKeyS(src, name)
      })

    "Map" >> {
      val q =
        fix.Map(
          base,
          recFunc.DeleteKeyS(recFunc.Hole, "b"))

      val e =
        fix.Map(
          base,
          recPrjFrom(recFunc.Hole, "a", "c"))

      preferProjection[QS](q) must_= e
    }

    "LeftShift" >> {
      val q =
        fix.LeftShift(
          base,
          recFunc.DeleteKeyS(recFunc.Hole, "c"),
          IncludeId,
          ShiftType.Array,
          OnUndefined.Omit,
          MapFuncCore.StaticArray(List(
            func.DeleteKeyS(func.DeleteKeyS(func.LeftSide, "a"), "b"),
            func.RightSide)))

      val e =
        fix.LeftShift(
          base,
          recPrjFrom(recFunc.Hole, "a", "b"),
          IncludeId,
          ShiftType.Array,
          OnUndefined.Omit,
          MapFuncCore.StaticArray(List(
            prjFrom(func.LeftSide, "c"),
            func.RightSide)))

      preferProjection[QS](q) must_= e
    }

    "Reduce" >> {
      import ReduceFuncs._

      val q =
        fix.Reduce(
          base,
          List(func.DeleteKeyS(func.Hole, "a")),
          List(Count(func.Hole), First(func.DeleteKeyS(func.Hole, "b"))),
          MapFuncCore.StaticArray(List(
            func.ReduceIndex(0.right),
            func.DeleteKeyS(func.ReduceIndex(1.right), "a"),
            func.DeleteKeyS(func.ReduceIndex(0.left), "c"))))

      val e =
        fix.Reduce(
          base,
          List(prjFrom(func.Hole, "b", "c")),
          List(Count(func.Hole), First(prjFrom(func.Hole, "a", "c"))),
          MapFuncCore.StaticArray(List(
            func.ReduceIndex(0.right),
            prjFrom(func.ReduceIndex(1.right), "c"),
            prjFrom(func.ReduceIndex(0.left), "b"))))

      preferProjection[QS](q) must_= e
    }

    "Sort" >> {
      val q =
        fix.Sort(
          base,
          List(func.DeleteKeyS(func.DeleteKeyS(func.Hole, "a"), "c")),
          NonEmptyList((func.DeleteKeyS(func.Hole, "b"), SortDir.Ascending)))

      val e =
        fix.Sort(
          base,
          List(prjFrom(func.Hole, "b")),
          NonEmptyList((prjFrom(func.Hole, "a", "c"), SortDir.Ascending)))

      preferProjection[QS](q) must_= e
    }

    "Filter" >> {
      val q =
        fix.Filter(
          base,
          recFunc.Eq(recFunc.DeleteKeyS(recFunc.Hole, "a"), recFunc.Hole))

      val e =
        fix.Filter(
          base,
          recFunc.Eq(prjFrom(func.Hole, "b", "c").asRec, recFunc.Hole))

      preferProjection[QS](q) must_= e
    }

    "Subset" >> {
      val q =
        fix.Subset(
          base,
          free.Map(
            free.Hole,
            recFunc.DeleteKeyS(recFunc.Hole, "c")),
          Take,
          free.Reduce(
            free.Hole,
            List(),
            List(ReduceFuncs.First(func.DeleteKeyS(func.Hole, "a"))),
            func.DeleteKeyS(func.ReduceIndex(0.right), "b")))

      val e =
        fix.Subset(
          base,
          free.Map(
            free.Hole,
            recPrjFrom(recFunc.Hole, "a", "b")),
          Take,
          free.Reduce(
            free.Hole,
            List(),
            List(ReduceFuncs.First(prjFrom(func.Hole, "b", "c"))),
            prjFrom(func.ReduceIndex(0.right), "c")))

      preferProjection[QS](q) must_= e
    }

    "ThetaJoin" >> {
      val q =
        fix.ThetaJoin(
          base,
          free.Map(
            free.Hole,
            recFunc.DeleteKeyS(recFunc.Hole, "a")),
          free.Map(
            free.Hole,
            recFunc.DeleteKeyS(recFunc.Hole, "b")),
          func.Eq(
            func.DeleteKeyS(func.LeftSide, "b"),
            func.DeleteKeyS(func.RightSide, "a")),
          JoinType.Inner,
          MapFuncCore.StaticMap(List(
            json.str("left") -> func.DeleteKeyS(func.LeftSide, "c"),
            json.str("right") -> func.DeleteKeyS(func.RightSide, "c"))))

      val e =
        fix.ThetaJoin(
          base,
          free.Map(
            free.Hole,
            recPrjFrom(recFunc.Hole, "b", "c")),
          free.Map(
            free.Hole,
            recPrjFrom(recFunc.Hole, "a", "c")),
          func.Eq(
            prjFrom(func.LeftSide, "c"),
            prjFrom(func.RightSide, "c")),
          JoinType.Inner,
          MapFuncCore.StaticMap(List(
            json.str("left") -> prjFrom(func.LeftSide, "b"),
            json.str("right") -> prjFrom(func.RightSide, "a"))))

      preferProjection[QS](q) must_= e
    }
  }
}
