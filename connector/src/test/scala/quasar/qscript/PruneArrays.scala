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

import slamdata.Predef._
import quasar.common.{JoinType, SortDir}
import quasar.contrib.pathy.AFile
import quasar.fp._
import quasar.qscript.MapFuncsCore._
import quasar.sql.CompilerHelpers

import matryoshka.data.Fix
import pathy.Path._
import scalaz._, Scalaz._

class PruneArraysSpec extends quasar.Qspec with CompilerHelpers with QScriptHelpers {
  import qstdsl._

  private def makeLeftShift3[A](src: A, dsl: construction.Dsl[Fix, QST, A]): A =
    dsl.LeftShift(
      src,
      func.Hole,
      ExcludeId,
      func.ConcatArrays(
        func.ConcatArrays(
          func.MakeArray(func.Constant(json.int(6))),
          func.MakeArray(func.Constant(json.int(7)))),
        func.MakeArray(func.Constant(json.int(8)))))

  private val arrayBranch3: FreeQS =
    makeLeftShift3[FreeQS](free.Hole, free)

  private val array3: Fix[QST] =
    makeLeftShift3[Fix[QST]](fix.Unreferenced, fix)

  "prune arrays" should {
    "rewrite map-filter with unused array elements" in {
      def initial(src: Fix[QST]): Fix[QST] =
        fix.Map(
          fix.Filter(
            fix.LeftShift(
              src,
              func.Hole,
              ExcludeId,
              func.ConcatArrays(
                func.MakeArray(func.Constant(json.int(6))),
                func.MakeArray(func.Constant(json.bool(true))))),
            func.ProjectIndexI(func.Hole, 1)),
          func.ProjectIndexI(func.Hole, 1))

      def expected(src: Fix[QST]): Fix[QST] =
        fix.Map(
          fix.Filter(
            fix.LeftShift(
              src,
              func.Hole,
              ExcludeId,
              func.Constant(json.bool(true))),
            func.Hole),
          func.Hole)

      initial(fix.Unreferenced).pruneArraysF must equal(expected(fix.Unreferenced))
      initial(fix.Root).pruneArraysF must equal(expected(fix.Root))

      val data = rootDir </> file("zips")
      initial(fix.Read[AFile](data)).pruneArraysF must equal(expected(fix.Read[AFile](data)))
    }

    "not rewrite map-filter with no unused array elements" in {
      val initial: Fix[QST] =
        fix.Map(
          fix.Filter(
            fix.LeftShift(
              fix.Unreferenced,
              func.Hole,
              ExcludeId,
              func.ConcatArrays(
                func.MakeArray(func.Constant(json.int(6))),
                func.MakeArray(func.Constant(json.bool(true))))),
            func.ProjectIndexI(func.Hole, 1)),
          func.ProjectIndexI(func.Hole, 0))

      initial.pruneArraysF must equal(initial)
    }

    "rewrite map-sort with unused array elements" in {
      val initialArray: JoinFunc =
        func.ConcatArrays(
          func.ConcatArrays(
            func.ConcatArrays(
              func.MakeArray(func.Constant(json.int(6))),
              func.MakeArray(func.Constant(json.int(7)))),
            func.MakeArray(func.Constant(json.int(8)))),
          func.MakeArray(func.Constant(json.int(9))))

      def initial(src: Fix[QST]): Fix[QST] =
        fix.Map(
          fix.Sort(
            fix.LeftShift(
              src,
              func.Hole,
              ExcludeId,
              initialArray),
            List(func.ProjectIndexI(func.Hole, 0)),
            NonEmptyList(
              (func.ProjectIndexI(func.Hole, 1), SortDir.Ascending),
              (func.ProjectIndexI(func.Hole, 3), SortDir.Ascending))),
          func.ProjectIndexI(func.Hole, 1))

      val expectedArray: JoinFunc =
        func.ConcatArrays(
          func.ConcatArrays(
            func.MakeArray(func.Constant(json.int(6))),
            func.MakeArray(func.Constant(json.int(7)))),
          func.MakeArray(func.Constant(json.int(9))))

      def expected(src: Fix[QST]): Fix[QST] =
        fix.Map(
          fix.Sort(
            fix.LeftShift(
              src,
              func.Hole,
              ExcludeId,
              expectedArray),
            List(func.ProjectIndexI(func.Hole, 0)),
            NonEmptyList(
              (func.ProjectIndexI(func.Hole, 1), SortDir.Ascending),
              (func.ProjectIndexI(func.Hole, 2), SortDir.Ascending))),
          func.ProjectIndexI(func.Hole, 1))

      initial(fix.Unreferenced).pruneArraysF must equal(expected(fix.Unreferenced))
      initial(fix.Root).pruneArraysF must equal(expected(fix.Root))

      val data = rootDir </> file("zips")
      initial(fix.Read[AFile](data)).pruneArraysF must equal(expected(fix.Read[AFile](data)))
    }

    "not rewrite map-sort with no unused array elements" in {
      val initialArray: JoinFunc =
        func.ConcatArrays(
          func.ConcatArrays(
            func.ConcatArrays(
              func.MakeArray(func.Constant(json.int(6))),
              func.MakeArray(func.Constant(json.int(7)))),
            func.MakeArray(func.Constant(json.int(8)))),
          func.MakeArray(func.Constant(json.int(9))))

      def initial: Fix[QST] =
        fix.Map(
          fix.Sort(
            fix.LeftShift(
              fix.Unreferenced,
              func.Hole,
              ExcludeId,
              initialArray),
            List(func.ProjectIndexI(func.Hole, 0)),
            NonEmptyList(
              (func.ProjectIndexI(func.Hole, 1), SortDir.Ascending),
              (func.ProjectIndexI(func.Hole, 3), SortDir.Ascending))),
          func.ProjectIndexI(func.Hole, 2))

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite filter with unused array elements" in {
      val initial: Fix[QST] =
        fix.Filter(
          fix.LeftShift(
            fix.Unreferenced,
            func.Hole,
            ExcludeId,
            func.ConcatArrays(
              func.MakeArray(func.Constant(json.int(6))),
              func.MakeArray(func.Constant(json.bool(true))))),
          func.ProjectIndexI(func.Hole, 1))

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite sort with unused array elements" in {
      val initial: Fix[QST] =
        fix.Sort(
          fix.LeftShift(
            fix.Unreferenced,
            func.Hole,
            ExcludeId,
            func.ConcatArrays(
              func.MakeArray(func.Constant(json.int(6))),
              func.MakeArray(func.Constant(json.int(7))))),
          List(func.ProjectIndexI(func.Hole, 1)),
          NonEmptyList(
            (func.ProjectIndexI(func.Hole, 1), SortDir.Ascending),
            (func.ProjectIndexI(func.Hole, 1), SortDir.Descending)))

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite map with entire array referenced" in {
      val initial: Fix[QST] =
        fix.Map(
          array3,
          func.Hole)

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite map with entire array and specific index referenced" in {
      val initial: Fix[QST] =
        fix.Map(
          array3,
          func.ConcatArrays(
            func.MakeArray(func.ProjectIndexI(func.Hole, 0)),
            func.MakeArray(func.Hole)))

      initial.pruneArraysF must equal(initial)
    }

    "rewrite map with unused array elements 1,2" in {
      val initial: Fix[QST] =
        fix.Map(
          array3,
          func.ProjectIndexI(func.Hole, 0))

      val expected: Fix[QST] =
        fix.Map(
          fix.LeftShift(
            fix.Unreferenced,
            func.Hole,
            ExcludeId,
            func.Constant(json.int(6))),
          func.Hole)

      initial.pruneArraysF must equal(expected)
    }

    "rewrite map with unused array elements 0,2" in {
      val initial: Fix[QST] =
        fix.Map(
          array3,
          func.ProjectIndexI(func.Hole, 1))

      val expected: Fix[QST] =
        fix.Map(
          fix.LeftShift(
            fix.Unreferenced,
            func.Hole,
            ExcludeId,
            func.Constant(json.int(7))),
          func.Hole)

      initial.pruneArraysF must equal(expected)
    }

    "rewrite map with unused array elements 0,1" in {
      val initial: Fix[QST] =
        fix.Map(
          array3,
          func.ProjectIndexI(func.Hole, 2))

      val expected: Fix[QST] =
        fix.Map(
          fix.LeftShift(
            fix.Unreferenced,
            func.Hole,
            ExcludeId,
            func.Constant(json.int(8))),
          func.Hole)

      initial.pruneArraysF must equal(expected)
    }

    "rewrite map with unused array elements in a binary map func" in {
      val initial: Fix[QST] =
        fix.Map(
          array3,
          func.Add(
            func.ProjectIndexI(func.Hole, 2),
            func.ProjectIndexI(func.Hole, 0)))

      val expected: Fix[QST] =
        fix.Map(
          fix.LeftShift(
            fix.Unreferenced,
            func.Hole,
            ExcludeId,
            func.ConcatArrays(
              func.MakeArray(func.Constant(json.int(6))),
              func.MakeArray(func.Constant(json.int(8))))),
          func.Add(
            func.ProjectIndexI(func.Hole, 1),
            func.ProjectIndexI(func.Hole, 0)))

      initial.pruneArraysF must equal(expected)
    }

    "not rewrite leftshift with nonstatic array dereference" in {
      val initial: Fix[QST] =
        fix.Map(
          array3,
          func.Add(
            func.ProjectIndexI(func.Hole, 2),
            func.ProjectIndex(func.Hole, func.Add(func.Constant(json.int(0)), func.Constant(json.int(1))))))

      initial.pruneArraysF must equal(initial)
    }

    "rewrite two leftshift arrays" in {
      val innerInitial: Fix[QST] =
        fix.LeftShift(
          fix.Unreferenced,
          func.Hole,
          ExcludeId,
          func.ConcatArrays(      // [6, [7], 8]
            func.ConcatArrays(
              func.MakeArray(func.Constant(json.int(6))),
              func.MakeArray(func.MakeArray(func.Constant(json.int(7))))),
            func.MakeArray(func.Constant(json.int(8)))))

      val initial: Fix[QST] =
        fix.Map(
          fix.LeftShift(
            fix.Map(
              innerInitial,
              func.ProjectIndexI(func.Hole, 1)),
            func.Hole,
            ExcludeId,
            func.ConcatArrays(func.MakeArray(func.LeftSide), func.MakeArray(func.RightSide))),
          func.ProjectIndexI(func.Hole, 0))

      val innerExpected: Fix[QST] =
        fix.LeftShift(
          fix.Unreferenced,
          func.Hole,
          ExcludeId,
          func.MakeArray(func.Constant(json.int(7))))

      val expected: Fix[QST] =
        fix.Map(
          fix.LeftShift(
            fix.Map(
              innerExpected,
              func.Hole),
            func.Hole,
            ExcludeId,
            func.LeftSide),
          func.Hole)

      initial.pruneArraysF must equal(expected)
    }

    "rewrite filter-map-filter-leftshift" in {
      val innerArray: JoinFunc =
        func.ConcatArrays(     // [7, 8, 9]
          func.ConcatArrays(
            func.MakeArray(func.Constant(json.int(7))),
            func.MakeArray(func.Constant(json.int(8)))),
          func.MakeArray(func.Constant(json.int(9))))

      val srcInitial: Fix[QST] =
        fix.LeftShift(
          fix.Unreferenced,
          func.Hole,
          ExcludeId,
          func.ConcatArrays(      // ["a", [7, 8, 9], true]
            func.ConcatArrays(
              func.MakeArray(func.Constant(json.str("a"))),
              func.MakeArray(innerArray)),
            func.MakeArray(func.Constant(json.bool(true)))))

      val initial: Fix[QST] =
        fix.Filter(
          fix.Map(
            fix.Filter(
              srcInitial,
              func.ProjectIndexI(func.Hole, 2)),
            func.ProjectIndexI(func.Hole, 1)),
          func.ProjectIndexI(func.Hole, 0))

      val srcExpected: Fix[QST] =
        fix.LeftShift(
          fix.Unreferenced,
          func.Hole,
          ExcludeId,
          func.ConcatArrays(      // [[7, 8, 9], true]
            func.MakeArray(innerArray),
            func.MakeArray(func.Constant(json.bool(true)))))

      val expected: Fix[QST] =
        fix.Filter(
          fix.Map(
            fix.Filter(
              srcExpected,
              func.ProjectIndexI(func.Hole, 1)),
            func.ProjectIndexI(func.Hole, 0)),
          func.ProjectIndexI(func.Hole, 0))

      initial.pruneArraysF must equal(expected)
    }

    "rewrite reduce-filter-leftshift" in {
      val srcInitial: Fix[QST] =
        fix.LeftShift(
          fix.Unreferenced,
          func.Hole,
          ExcludeId,
          func.ConcatArrays(
            func.ConcatArrays(
              func.ConcatArrays(
                func.MakeArray(func.Constant(json.str("a"))),
                func.MakeArray(func.Constant(json.str("b")))),
              func.MakeArray(func.Constant(json.str("c")))),
            func.MakeArray(func.Constant(json.str("d")))))

      val initial: Fix[QST] =
        fix.Reduce(
          fix.Filter(
            srcInitial,
            func.ProjectIndexI(func.Hole, 3)),
          Nil,
          List(ReduceFuncs.Count(func.ProjectIndexI(func.Hole, 2))),
          func.MakeMap(func.Constant(json.int(0)), func.ReduceIndex(0.right)))

      val srcExpected: Fix[QST] =
        fix.LeftShift(
          fix.Unreferenced,
          func.Hole,
          ExcludeId,
          func.ConcatArrays(
            func.MakeArray(func.Constant(json.str("c"))),
            func.MakeArray(func.Constant(json.str("d")))))

      val expected: Fix[QST] =
        fix.Reduce(
          fix.Filter(
            srcExpected,
            func.ProjectIndexI(func.Hole, 1)),
          Nil,
          List(ReduceFuncs.Count(func.ProjectIndexI(func.Hole, 0))),
          func.MakeMap(func.Constant(json.int(0)), func.ReduceIndex(0.right)))

      initial.pruneArraysF must equal(expected)
    }

    "rewrite bucket key with unused array elements" in {
      val initial: Fix[QST] =
        fix.BucketKey(
          fix.LeftShift(
            fix.Unreferenced,
            func.Hole,
            ExcludeId,
            func.ConcatArrays(
              func.ConcatArrays(
                func.MakeArray(func.Constant(json.int(6))),
                func.MakeArray(func.Constant(json.int(7)))),
              func.MakeArray(func.Constant(json.str("foo"))))),
          func.ProjectIndexI(func.Hole, 2),
          func.ProjectIndexI(func.Hole, 0))

      val expected: Fix[QST] =
        fix.BucketKey(
          fix.LeftShift(
            fix.Unreferenced,
            func.Hole,
            ExcludeId,
            func.ConcatArrays(
              func.MakeArray(func.Constant(json.int(6))),
              func.MakeArray(func.Constant(json.str("foo"))))),
          func.ProjectIndexI(func.Hole, 1),
          func.ProjectIndexI(func.Hole, 0))

      initial.pruneArraysF must equal(expected)
    }

    "rewrite bucket index with unused array elements" in {
      val initial: Fix[QST] =
        fix.BucketIndex(
          array3,
          func.ProjectIndexI(func.Hole, 2),
          func.ProjectIndexI(func.Hole, 0))

      val expected: Fix[QST] =
        fix.BucketIndex(
          fix.LeftShift(
            fix.Unreferenced,
            func.Hole,
            ExcludeId,
            func.ConcatArrays(
              func.MakeArray(func.Constant(json.int(6))),
              func.MakeArray(func.Constant(json.int(8))))),
          func.ProjectIndexI(func.Hole, 1),
          func.ProjectIndexI(func.Hole, 0))

      initial.pruneArraysF must equal(expected)
    }

    "rewrite array used in from of subset" in {
      val initial: Fix[QST] =
        fix.Reduce(
          fix.Subset(
            fix.Unreferenced,
            free.LeftShift(
              free.Read[AFile](rootDir </> file("zips")),
              func.Hole,
              IncludeId,
              func.ConcatArrays(func.MakeArray(func.LeftSide), func.MakeArray(func.RightSide))),
            Drop,
            free.Map(free.Unreferenced, func.Constant(json.int(10)))),
          Nil,
          List(ReduceFuncs.Count(func.ProjectIndexI(func.ProjectIndexI(func.Hole, 1), 1))),
          func.ReduceIndex(0.right))

      val expected: Fix[QST] =
        fix.Reduce(
          fix.Subset(
            fix.Unreferenced,
            free.LeftShift(
              free.Read[AFile](rootDir </> file("zips")),
              func.Hole,
              IncludeId,
              func.RightSide),
            Drop,
            free.Map(free.Unreferenced, func.Constant(json.int(10)))),
          Nil,
          List(ReduceFuncs.Count(func.ProjectIndexI(func.Hole, 1))),
          func.ReduceIndex(0.right))

      initial.pruneArraysF must equal(expected)
    }

    // FIXME: this can be rewritten - we just don't support that yet
    "not rewrite subset with unused array elements" in {
      val initial: Fix[QST] =
        fix.Subset(
          array3,
          free.Map(
            free.Hole,
            func.ProjectIndexI(func.Hole, 2)),
          Drop,
          free.Map(
            free.Hole,
            func.ProjectIndexI(func.Hole, 0)))

      initial.pruneArraysF must equal(initial)
    }

    // FIXME: this can be rewritten - we just don't support that yet
    "not rewrite union with unused array elements" in {
      val initial: Fix[QST] =
        fix.Union(
          array3,
          free.Map(
            free.Hole,
            func.ProjectIndexI(func.Hole, 2)),
          free.Map(
            free.Hole,
            func.ProjectIndexI(func.Hole, 1)))

      initial.pruneArraysF must equal(initial)
    }

    // FIXME: this can be rewritten - we just don't support that yet
    "not rewrite theta join with unused array elements in source" in {
      val initial: Fix[QST] =
        fix.ThetaJoin(
          array3,
          free.Map(
            free.Hole,
            func.ProjectIndexI(func.Hole, 2)),
          free.Hole,
          BoolLit[Fix, JoinSide](true),
          JoinType.Inner,
          func.MakeMapS("xyz", func.LeftSide))

      initial.pruneArraysF must equal(initial)
    }

    // FIXME: this can be rewritten - we just don't support that yet
    "not rewrite equi join with unused array elements in source" in {
      val initial: Fix[QST] =
        fix.EquiJoin(
          array3,
          free.Map(
            free.Hole,
            func.ProjectIndexI(func.Hole, 2)),
          free.Hole,
          List((func.Hole, func.Hole)),
          JoinType.Inner,
          func.MakeMapS("xyz", func.LeftSide))

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite equi join with entire array branch referenced in key" in {
      val initial: Fix[QST] =
        fix.EquiJoin(
          fix.Unreferenced,
          arrayBranch3,
          free.Hole,
          List((func.Hole, func.Hole)),
          JoinType.Inner,
          func.MakeMapS(
            "xyz",
            func.ProjectIndexI(func.LeftSide, 2)))

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite theta join with entire array branch referenced in condition" in {
      val initial: Fix[QST] =
        fix.ThetaJoin(
          fix.Unreferenced,
          arrayBranch3,
          free.Hole,
          func.Eq(
            func.Add(func.LeftSide, func.Constant(json.int(2))),  // reference entire left branch
            func.RightSide),
          JoinType.Inner,
          func.MakeMapS(
            "xyz",
            func.ProjectIndexI(func.LeftSide, 2)))

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite equi join with entire array branch referenced in combine" in {
      val initial: Fix[QST] =
        fix.EquiJoin(
          fix.Unreferenced,
          arrayBranch3,
          free.Hole,
          List((func.ProjectIndexI(func.Hole, 2), func.Hole)),
          JoinType.Inner,
          func.MakeMapS(
            "xyz",
            func.LeftSide))  // reference entire left branch

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite theta join with entire array branch referenced in combine" in {
      val initial: Fix[QST] =
        fix.ThetaJoin(
          fix.Unreferenced,
          arrayBranch3,
          free.Hole,
          func.ProjectIndexI(func.LeftSide, 2),
          JoinType.Inner,
          func.MakeMapS(
            "xyz",
            func.LeftSide))  // reference entire left branch

      initial.pruneArraysF must equal(initial)
    }

    // FIXME: we'd like to be able to rewrite this
    // but it might require more work in addition to array pruning
    "not rewrite equi join with array branch referenced outside of join" in {
      val initial: Fix[QST] =
        fix.Map(
          fix.EquiJoin(
            fix.Unreferenced,
            arrayBranch3,
            free.Hole,
            List((func.ProjectIndexI(func.Hole, 2), func.Hole)),
            JoinType.Inner,
            func.ConcatArrays(func.MakeArray(func.LeftSide), func.MakeArray(func.RightSide))),
          func.ProjectIndexI(func.ProjectIndexI(func.Hole, 0), 2))

      initial.pruneArraysF must equal(initial)
    }

    // FIXME: this can be rewritten - we just don't support that yet
    "not rewrite theta join with filtered left shift as branch" in {
      val initial: Fix[QST] =
        fix.ThetaJoin(
          fix.Unreferenced,
          free.Filter(arrayBranch3, func.ProjectIndexI(func.Hole, 1)),
          free.Hole,
          func.Eq(
            func.ProjectIndexI(func.LeftSide, 2),
            func.Constant(json.str("foo"))),
          JoinType.Inner,
          func.MakeMapS(
            "bar",
            func.ProjectIndexI(func.LeftSide, 1)))

      initial.pruneArraysF must equal(initial)
    }

    // FIXME: this can be rewritten - we just don't support that yet
    "not rewrite equi join with filtered left shift as branch" in {
      val initial: Fix[QST] =
        fix.EquiJoin(
          fix.Unreferenced,
          free.Filter(arrayBranch3, func.ProjectIndexI(func.Hole, 1)),
          free.Hole,
          List(
            (func.Eq(
              func.ProjectIndexI(func.Hole, 2),
              func.Constant(json.str("foo"))),
              func.Hole)),
          JoinType.Inner,
          func.MakeMapS(
            "bar",
            func.ProjectIndexI(func.LeftSide, 1)))

      initial.pruneArraysF must equal(initial)
    }

    "rewrite theta join with unused array elements in both branches" in {
      val rBranch: FreeQS =
        free.LeftShift(
          free.Hole,
          func.Hole,
          ExcludeId,
          func.ConcatArrays(
            func.ConcatArrays(
              func.MakeArray(func.Constant(json.int(1))),
              func.MakeArray(func.Constant(json.int(2)))),
            func.MakeArray(func.Constant(json.str("xyz")))))

      val initial: Fix[QST] =
        fix.ThetaJoin(
          fix.Unreferenced,
          arrayBranch3,
          rBranch,
          func.Eq(
            func.ProjectIndexI(func.LeftSide, 2),
            func.ProjectIndexI(func.RightSide, 0)),
          JoinType.Inner,
          func.MakeMap(
            func.ProjectIndexI(func.LeftSide, 2),
            func.ProjectIndexI(func.RightSide, 2)))

      val lBranchExpected: FreeQS =
        free.LeftShift(
          free.Hole,
          func.Hole,
          ExcludeId,
          func.Constant(json.int(8)))

      val rBranchExpected: FreeQS =
        free.LeftShift(
          free.Hole,
          func.Hole,
          ExcludeId,
          func.ConcatArrays(
            func.MakeArray(func.Constant(json.int(1))),
            func.MakeArray(func.Constant(json.str("xyz")))))


      val expected: Fix[QST] =
        fix.ThetaJoin(
          fix.Unreferenced,
          lBranchExpected,
          rBranchExpected,
          func.Eq(
            func.LeftSide,
            func.ProjectIndexI(func.RightSide, 0)),
          JoinType.Inner,
          func.MakeMap(
            func.LeftSide,
            func.ProjectIndexI(func.RightSide, 1)))

      initial.pruneArraysF must equal(expected)
    }

    "rewrite equi join with unused array elements in both branches" in {
      val rBranch: FreeQS =
        free.LeftShift(
          free.Hole,
          func.Hole,
          ExcludeId,
          func.ConcatArrays(
            func.ConcatArrays(
              func.MakeArray(func.Constant(json.int(1))),
              func.MakeArray(func.Constant(json.int(2)))),
            func.MakeArray(func.Constant(json.str("xyz")))))

      val initial: Fix[QST] =
        fix.EquiJoin(
          fix.Unreferenced,
          arrayBranch3,
          rBranch,
          List(
            (func.ProjectIndexI(func.Hole, 2),
              func.ProjectIndexI(func.Hole, 0))),
          JoinType.Inner,
          func.MakeMap(
            func.ProjectIndexI(func.LeftSide, 2),
            func.ProjectIndexI(func.RightSide, 2)))

      val lBranchExpected: FreeQS =
        free.LeftShift(
          free.Hole,
          func.Hole,
          ExcludeId,
          func.Constant(json.int(8)))

      val rBranchExpected: FreeQS =
        free.LeftShift(
          free.Hole,
          func.Hole,
          ExcludeId,
          func.ConcatArrays(
            func.MakeArray(func.Constant(json.int(1))),
            func.MakeArray(func.Constant(json.str("xyz")))))


      val expected: Fix[QST] =
        fix.EquiJoin(
          fix.Unreferenced,
          lBranchExpected,
          rBranchExpected,
          List((func.Hole, func.ProjectIndexI(func.Hole, 0))),
          JoinType.Inner,
          func.MakeMap(
            func.LeftSide,
            func.ProjectIndexI(func.RightSide, 1)))

      initial.pruneArraysF must equal(expected)
    }

    "rewrite left shift with array referenced through struct" in {
      val initial: Fix[QST] =
        fix.LeftShift(
          array3,
          func.ProjectIndexI(func.Hole, 2),
          ExcludeId,
          func.MakeMapS("xyz", func.RightSide))

      val expectedSrc: Fix[QST] =
        fix.LeftShift(
          fix.Unreferenced,
          func.Hole,
          ExcludeId,
          func.Constant(json.int(8)))

      val expected: Fix[QST] =
        fix.LeftShift(
          expectedSrc,
          func.Hole,
          ExcludeId,
          func.MakeMapS("xyz", func.RightSide))

      initial.pruneArraysF must equal(expected)
    }

    "rewrite left shift when struct projects an index pruned from repair" in {
      val initial: Fix[QST] =
        fix.Map(
          fix.Filter(
            fix.LeftShift(
              fix.Read[AFile](rootDir </> file("data")),
              func.ProjectIndexI(func.ProjectIndexI(func.Hole, 2), 1),
              ExcludeId,
              func.ConcatArrays(
                func.ConcatArrays(
                  func.MakeArray(func.Constant(json.int(3))),
                  func.MakeArray(func.Constant(json.int(6)))),
                func.MakeArray(func.Constant(json.bool(true))))),
            func.ProjectIndexI(func.Hole, 2)),
        func.ProjectIndexI(func.Hole, 1))

      val expected: Fix[QST] =
        fix.Map(
          fix.Filter(
            fix.LeftShift(
              fix.Read[AFile](rootDir </> file("data")),
              func.ProjectIndexI(func.ProjectIndexI(func.Hole, 2), 1),
              ExcludeId,
              func.ConcatArrays(
                func.MakeArray(func.Constant(json.int(6))),
                func.MakeArray(func.Constant(json.bool(true))))),
            func.ProjectIndexI(func.Hole, 1)),
        func.ProjectIndexI(func.Hole, 0))

      initial.pruneArraysF must equal(expected)
    }

    "not rewrite left shift with entire array referenced through left side" in {
      val initial: Fix[QST] =
        fix.LeftShift(
          array3,
          func.ProjectIndexI(func.Hole, 2),
          ExcludeId,
          func.MakeMapS("xyz", func.LeftSide))

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite left shift with array referenced non-statically through struct" in {
      val initial: Fix[QST] =
        fix.LeftShift(
          array3,
          func.ProjectIndex(func.Hole, func.Add(func.Constant(json.int(0)), func.Constant(json.int(1)))),
          ExcludeId,
          func.MakeMapS("xyz", func.LeftSide))

      initial.pruneArraysF must equal(initial)
    }

    "rewrite left shift with array referenced through left side and struct" in {
      val initial: Fix[QST] =
        fix.LeftShift(
          array3,
          func.ProjectIndexI(func.Hole, 2),
          ExcludeId,
          func.ProjectIndexI(func.LeftSide, 1))

      val expectedSrc: Fix[QST] =
        fix.LeftShift(
          fix.Unreferenced,
          func.Hole,
          ExcludeId,
          func.ConcatArrays(
            func.MakeArray(func.Constant(json.int(7))),
            func.MakeArray(func.Constant(json.int(8)))))

      val expected: Fix[QST] =
        fix.LeftShift(
          expectedSrc,
          func.ProjectIndexI(func.Hole, 1),
          ExcludeId,
          func.ProjectIndexI(func.LeftSide, 0))

      initial.pruneArraysF must equal(expected)
    }

    "rewrite left shift with array referenced through struct with a right side reference" in {
      val initialSrc: Fix[QST] =
        fix.LeftShift(
          fix.Unreferenced,
          func.Hole,
          ExcludeId,
          func.ConcatArrays(
            func.ConcatArrays(
              func.MakeArray(func.Constant(json.int(6))),
              func.MakeArray(func.Constant(json.int(7)))),
            func.MakeArray(func.ConcatArrays(func.MakeArray(func.Constant(json.int(8))), func.MakeArray(func.Constant(json.int(9)))))))

      val initial: Fix[QST] =
        fix.LeftShift(
          initialSrc,
          func.ProjectIndexI(func.Hole, 2),
          ExcludeId,
          func.ProjectIndexI(func.RightSide, 1))

      val expectedSrc: Fix[QST] =
        fix.LeftShift(
          fix.Unreferenced,
          func.Hole,
          ExcludeId,
          func.ConcatArrays(func.MakeArray(func.Constant(json.int(8))), func.MakeArray(func.Constant(json.int(9)))))

      val expected: Fix[QST] =
        fix.LeftShift(
          expectedSrc,
          func.Hole,
          ExcludeId,
          func.ProjectIndexI(func.RightSide, 1))

      initial.pruneArraysF must equal(expected)
    }

    "rewrite left shift with entire array unreferenced" in {
      val initial: Fix[QST] =
        fix.LeftShift(
          array3,
          func.Constant(json.int(2)),
          ExcludeId,
          func.Add(func.Constant(json.int(2)), func.Constant(json.int(3))))

      val expectedSrc: Fix[QST] =
        fix.LeftShift(
          fix.Unreferenced,
          func.Hole,
          ExcludeId,
          func.Constant(json.arr(Nil)))

      val expected: Fix[QST] =
        fix.LeftShift(
          expectedSrc,
          func.Constant(json.int(2)),
          ExcludeId,
          func.Add(func.Constant(json.int(2)), func.Constant(json.int(3))))

      initial.pruneArraysF must equal(expected)
    }

    "not rewrite left shift with entire array referenced by the right side" in {
      val initial: Fix[QST] =
        fix.LeftShift(
          array3,
          func.Hole,
          ExcludeId,
          func.RightSide)

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite left shift with entire array referenced by the left side" in {
      val initial: Fix[QST] =
        fix.LeftShift(
          array3,
          func.Hole,
          ExcludeId,
          func.LeftSide)

      initial.pruneArraysF must equal(initial)
    }
  }

  "prune arrays branches" should {
    val rBranch: FreeQS =
      free.LeftShift(
        free.Hole,
        func.Hole,
        ExcludeId,
        func.ConcatArrays(
          func.ConcatArrays(
            func.MakeArray(func.Constant(json.int(1))),
            func.MakeArray(func.Constant(json.int(2)))),
          func.MakeArray(func.Constant(json.str("xyz")))))

    val innerInitial: FreeQS =
      free.ThetaJoin(
        free.Unreferenced,
        arrayBranch3,
        rBranch,
        func.Eq(
          func.ProjectIndexI(func.LeftSide, 2),
          func.ProjectIndexI(func.RightSide, 0)),
        JoinType.Inner,
        func.MakeMap(
          func.ProjectIndexI(func.LeftSide, 2),
          func.ProjectIndexI(func.RightSide, 2)))

    val lBranchExpected: FreeQS =
      free.LeftShift(
        free.Hole,
        func.Hole,
        ExcludeId,
        func.Constant(json.int(8)))

    val rBranchExpected: FreeQS =
      free.LeftShift(
        free.Hole,
        func.Hole,
        ExcludeId,
        func.ConcatArrays(
          func.MakeArray(func.Constant(json.int(1))),
          func.MakeArray(func.Constant(json.str("xyz")))))

    val innerExpected: FreeQS =
      free.ThetaJoin(
        free.Unreferenced,
        lBranchExpected,
        rBranchExpected,
        func.Eq(
          func.LeftSide,
          func.ProjectIndexI(func.RightSide, 0)),
        JoinType.Inner,
        func.MakeMap(
          func.LeftSide,
          func.ProjectIndexI(func.RightSide, 1)))

    "rewrite left branch of theta join" in {
      def outer(branch: FreeQS): Fix[QST] =
        fix.ThetaJoin(
          fix.Unreferenced,
          branch,
          free.Hole,
          func.Eq(func.LeftSide, func.RightSide),
          JoinType.Inner,
          func.MakeMap(func.LeftSide, func.RightSide))

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }

    "rewrite right branch of theta join" in {
      def outer(branch: FreeQS): Fix[QST] =
        fix.ThetaJoin(
          fix.Unreferenced,
          free.Hole,
          branch,
          func.Eq(func.LeftSide, func.RightSide),
          JoinType.Inner,
          func.MakeMap(func.LeftSide, func.RightSide))

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }

    "rewrite left branch of equi join" in {
      def outer(branch: FreeQS): Fix[QST] =
        fix.EquiJoin(
          fix.Unreferenced,
          branch,
          free.Hole,
          List((func.Hole, func.Hole)),
          JoinType.Inner,
          func.MakeMap(func.LeftSide, func.RightSide))

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }

    "rewrite right branch of equi join" in {
      def outer(branch: FreeQS): Fix[QST] =
        fix.EquiJoin(
          fix.Unreferenced,
          free.Hole,
          branch,
          List((func.Hole, func.Hole)),
          JoinType.Inner,
          func.MakeMap(func.LeftSide, func.RightSide))

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }

    "rewrite left branch of union" in {
      def outer(branch: FreeQS): Fix[QST] =
        fix.Union(
          fix.Unreferenced,
          branch,
          free.Hole)

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }

    "rewrite right branch of union" in {
      def outer(branch: FreeQS): Fix[QST] =
        fix.Union(
          fix.Unreferenced,
          free.Hole,
          branch)

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }

    "rewrite from branch of subset" in {
      def outer(branch: FreeQS): Fix[QST] =
        fix.Subset(
          fix.Unreferenced,
          branch,
          Drop,
          free.Hole)

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }

    "rewrite count branch of subset" in {
      def outer(branch: FreeQS): Fix[QST] =
        fix.Subset(
          fix.Unreferenced,
          free.Hole,
          Drop,
          branch)

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }
  }
}
