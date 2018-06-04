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

import scala.Predef.implicitly

import quasar.common.{JoinType, SortDir}
import quasar.qscript._
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript.MapFuncsCore._

import matryoshka.Algebra
import matryoshka.data._
import pathy.Path._
import scalaz._, Scalaz._

final class DeepShapeSpec extends quasar.Qspec with QScriptHelpers with TTypes[Fix] {
  import DeepShape._

  import qstdsl._

  "DeepShape" >> {

    val shape: FreeShape[Fix] =
      func.ProjectKeyS(freeShape[Fix](RootShape()), "quxx")

    "QScriptCore" >> {

      val deepShapeQS: Algebra[QScriptCore, FreeShape[Fix]] =
        implicitly[DeepShape[Fix, QScriptCore]].deepShapeƒ

      "Map" >> {
        val fun: RecFreeMap = recFunc.ProjectIndexI(recFunc.Hole, 3)
        val qs = Map(shape, fun)

        deepShapeQS(qs) must equal(fun.linearize >> shape)
      }

      "LeftShift" >> {
        val struct = recFunc.ProjectIndexI(recFunc.Hole, 3)

        val repair: JoinFunc =
          func.ConcatArrays(
            func.MakeArray(func.Add(func.LeftSide, IntLit(9))),
            func.MakeArray(func.Subtract(func.RightSide, IntLit(10))))

        val qs = LeftShift(shape, struct, IdOnly, ShiftType.Array, OnUndefined.Omit, repair)

        val expected: FreeShape[Fix] =
          func.ConcatArrays(
            func.MakeArray(func.Add(shape, IntLit(9))),
            func.MakeArray(func.Subtract(freeShape(Shifting(IdOnly, struct.linearize >> shape)), IntLit(10))))

        deepShapeQS(qs) must equal(expected)
      }

      "Sort" >> {
        import qsdsl.func
        val bucket: FreeMap = func.ProjectIndexI(func.Hole, 3)
        val order: FreeMap = func.ProjectIndexI(func.Hole, 5)

        val qs = Sort(shape, List(bucket), NonEmptyList[(FreeMap, SortDir)]((order, SortDir.Ascending)))

        deepShapeQS(qs) must equal(freeShape[Fix](UnknownShape()))
      }

      "Filter" >> {
        val fun: RecFreeMap = recFunc.ProjectIndexI(recFunc.Hole, 3)
        val qs = Filter(shape, fun)

        deepShapeQS(qs) must equal(freeShape[Fix](UnknownShape()))
      }

      "Subset" >> {
        import qstdsl._
        val from: FreeQS =
          free.Map(
            free.Hole,
            recFunc.ProjectKeyS(recFunc.Hole, "foo"))

        val count: FreeQS =
          free.Map(
            free.Hole,
            recFunc.ProjectKeyS(recFunc.Hole, "bar"))

        val qs = Subset(shape, from, Take, count)

        deepShapeQS(qs) must equal(freeShape[Fix](UnknownShape()))
      }

      "Union" >> {
        val lBranch: FreeQS =
          free.Map(
            free.Hole,
            recFunc.ProjectKeyS(recFunc.Hole, "foo"))

        val rBranch: FreeQS =
          free.Map(
            free.Hole,
            recFunc.ProjectKeyS(recFunc.Hole, "bar"))

        val qs = Union(shape, lBranch, rBranch)

        deepShapeQS(qs) must equal(freeShape[Fix](UnknownShape()))
      }

      "Reduce" >> {
        val bucket: List[FreeMap] = List(func.ProjectIndexI(func.Hole, 3))

        val reducers: List[ReduceFunc[FreeMap]] =
          List(ReduceFuncs.Sum(func.ProjectKeyS(func.Hole, "foobar")))

        val repair: FreeMapA[ReduceIndex] =
          func.ConcatArrays(
            func.MakeArray(func.Add(func.ReduceIndex(0.left), func.Constant(json.int(9)))),
            func.MakeArray(func.Subtract(func.ReduceIndex(0.right), func.Constant(json.int(10)))))

        val qs = Reduce(shape, bucket, reducers, repair)

        val expected: FreeShape[Fix] =
          func.ConcatArrays(
            func.MakeArray(func.Add(bucket(0) >> shape, func.Constant(json.int(9)))),
            func.MakeArray(func.Subtract(freeShape(Reducing(reducers(0).map(_ >> shape))), func.Constant(json.int(10)))))

        deepShapeQS(qs) must equal(expected)
      }

      "Unreferenced" >> {
        deepShapeQS(Unreferenced()) must equal(freeShape[Fix](RootShape()))
      }
    }

    "Root" >> {
      "Read" >> {
        def deepShapeRead[A]: Algebra[Const[Read[A], ?], FreeShape[Fix]] =
          implicitly[DeepShape[Fix, Const[Read[A], ?]]].deepShapeƒ

        val qs = Read(rootDir[Sandboxed] </> dir("foo"))

        deepShapeRead(Const(qs)) must equal(freeShape[Fix](RootShape()))
      }

      "ShiftedRead" >> {
        def deepShapeSR[A]: Algebra[Const[ShiftedRead[A], ?], FreeShape[Fix]] =
          implicitly[DeepShape[Fix, Const[ShiftedRead[A], ?]]].deepShapeƒ

        val qs = ShiftedRead(rootDir[Sandboxed] </> dir("foo"), IdOnly)

        deepShapeSR(Const(qs)) must equal(freeShape[Fix](RootShape()))
      }

      "DeadEnd" >> {
        def deepShapeDE: Algebra[Const[DeadEnd, ?], FreeShape[Fix]] =
          implicitly[DeepShape[Fix, Const[DeadEnd, ?]]].deepShapeƒ

        deepShapeDE(Const(Root)) must equal(freeShape[Fix](RootShape()))
      }
    }

    "ProjectBucket" >> {

      val deepShapePB: Algebra[ProjectBucket, FreeShape[Fix]] =
        implicitly[DeepShape[Fix, ProjectBucket]].deepShapeƒ

      val value: FreeMap = func.ProjectIndexI(func.Hole, 7)
      val access: FreeMap = func.ProjectIndexI(func.Hole, 5)

      "BucketKey" >> {
        val qs = BucketKey(shape, value, access)
        val expected = func.ProjectKey(value >> shape, access >> shape)

        deepShapePB(qs) must equal(expected)
      }

      "BucketIndex" >> {
        val qs = BucketIndex(shape, value, access)
        val expected = func.ProjectIndex(value >> shape, access >> shape)

        deepShapePB(qs) must equal(expected)
      }
    }

    "Joins" >> {
      val lBranch: FreeQS =
        free.LeftShift(
          free.Map(
            free.Hole,
            recFunc.ProjectKeyS(recFunc.Hole, "foo")),
          recFunc.Hole,
          IncludeId,
          ShiftType.Array,
          OnUndefined.Omit,
          func.RightSide)

      val rBranch: FreeQS =
        free.LeftShift(
          free.Map(
            free.Hole,
            recFunc.ProjectKeyS(recFunc.Hole, "bar")),
          recFunc.Hole,
          IncludeId,
          ShiftType.Array,
          OnUndefined.Omit,
          func.LeftSide)

      val combine: JoinFunc =
        func.Add(
          func.ProjectIndexI(func.LeftSide, 1),
          func.ProjectIndexI(func.RightSide, 2))

      "ThetaJoin" >> {

        val deepShapeTJ: Algebra[ThetaJoin, FreeShape[Fix]] =
          implicitly[DeepShape[Fix, ThetaJoin]].deepShapeƒ

        val qs = ThetaJoin(
          shape,
          lBranch,
          rBranch,
          func.Constant(json.bool(true)),
          JoinType.Inner,
          combine)

        val expected: FreeShape[Fix] =
          func.Add(
            func.ProjectIndexI(freeShape(Shifting(IncludeId, func.ProjectKeyS(shape, "foo"))), 1),
            func.ProjectIndexI(func.ProjectKeyS(shape, "bar"), 2))

        deepShapeTJ(qs) must equal(expected)
      }

      "EquiJoin" >> {

        val deepShapeEJ: Algebra[EquiJoin, FreeShape[Fix]] =
          implicitly[DeepShape[Fix, EquiJoin]].deepShapeƒ

        val qs = EquiJoin(
          shape,
          lBranch,
          rBranch,
          List((func.Constant[Hole](json.bool(true)), func.Constant[Hole](json.bool(true)))),
          JoinType.Inner,
          combine)

        val expected: FreeShape[Fix] =
          func.Add(
            func.ProjectIndexI(freeShape(Shifting(IncludeId, func.ProjectKeyS(shape, "foo"))), 1),
            func.ProjectIndexI(func.ProjectKeyS(shape, "bar"), 2))

        deepShapeEJ(qs) must equal(expected)
      }
    }
  }
}
