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

import slamdata.Predef.List
import quasar.common.{JoinType, SortDir}
import quasar.contrib.pathy.AFile
import quasar.fp._
import quasar.qscript.MapFuncsCore._
import quasar.sql.CompilerHelpers

import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._

class PruneArraysSpec extends quasar.Qspec with CompilerHelpers with QScriptHelpers {
  private def makeLeftShift3[A](src: A): QST[A] =
    QCT.inj(LeftShift(
      src,
      HoleF,
      ExcludeId,
      ConcatArraysR(
        ConcatArraysR(
          MakeArrayR(IntLit(6)),
          MakeArrayR(IntLit(7))),
        MakeArrayR(IntLit(8)))))

  private val arrayBranch3: FreeQS =
    Free.roll(makeLeftShift3[FreeQS](HoleQS))

  private val array3: Fix[QST] =
    makeLeftShift3[Fix[QST]](UnreferencedRT.embed).embed

  "prune arrays" should {
    "rewrite map-filter with unused array elements" in {
      def initial(src: QST[Fix[QST]]): Fix[QST] =
        QCT.inj(Map(
          QCT.inj(Filter(
            QCT.inj(LeftShift(
              src.embed,
              HoleF,
              ExcludeId,
              ConcatArraysR(
                MakeArrayR(IntLit(6)),
                MakeArrayR(BoolLit(true))))).embed,
            ProjectIndexR(HoleF, IntLit(1)))).embed,
          ProjectIndexR(HoleF, IntLit(1)))).embed

      def expected(src: QST[Fix[QST]]): Fix[QST] =
        QCT.inj(Map(
          QCT.inj(Filter(
            QCT.inj(LeftShift(
              src.embed,
              HoleF,
              ExcludeId,
              MakeArrayR(BoolLit(true)))).embed,
            ProjectIndexR(HoleF, IntLit(0)))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      initial(UnreferencedRT).pruneArraysF must equal(expected(UnreferencedRT))
      initial(RootRT).pruneArraysF must equal(expected(RootRT))

      val data = rootDir </> file("zips")
      initial(ReadRT(data)).pruneArraysF must equal(expected(ReadRT(data)))
    }

    "not rewrite map-filter with no unused array elements" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(Filter(
            QCT.inj(LeftShift(
              UnreferencedRT.embed,
              HoleF,
              ExcludeId,
              ConcatArraysR(
                MakeArrayR(IntLit(6)),
                MakeArrayR(BoolLit(true))))).embed,
            ProjectIndexR(HoleF, IntLit(1)))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      initial.pruneArraysF must equal(initial)
    }

    "rewrite map-sort with unused array elements" in {
      val initialArray: JoinFunc =
        ConcatArraysR(
          ConcatArraysR(
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(IntLit(7))),
            MakeArrayR(IntLit(8))),
          MakeArrayR(IntLit(9)))

      def initial(src: QST[Fix[QST]]): Fix[QST] =
        QCT.inj(Map(
          QCT.inj(Sort(
            QCT.inj(LeftShift(
              src.embed,
              HoleF,
              ExcludeId,
              initialArray)).embed,
            ProjectIndexR(HoleF, IntLit(0)),
            NonEmptyList(
              (ProjectIndexR(HoleF, IntLit(1)), SortDir.Ascending),
              (ProjectIndexR(HoleF, IntLit(3)), SortDir.Ascending)))).embed,
          ProjectIndexR(HoleF, IntLit(1)))).embed

      val expectedArray: JoinFunc =
        ConcatArraysR(
          ConcatArraysR(
            MakeArrayR(IntLit(6)),
            MakeArrayR(IntLit(7))),
          MakeArrayR(IntLit(9)))

      def expected(src: QST[Fix[QST]]): Fix[QST] =
        QCT.inj(Map(
          QCT.inj(Sort(
            QCT.inj(LeftShift(
              src.embed,
              HoleF,
              ExcludeId,
              expectedArray)).embed,
            ProjectIndexR(HoleF, IntLit(0)),
            NonEmptyList(
              (ProjectIndexR(HoleF, IntLit(1)), SortDir.Ascending),
              (ProjectIndexR(HoleF, IntLit(2)), SortDir.Ascending)))).embed,
          ProjectIndexR(HoleF, IntLit(1)))).embed

      initial(UnreferencedRT).pruneArraysF must equal(expected(UnreferencedRT))
      initial(RootRT).pruneArraysF must equal(expected(RootRT))

      val data = rootDir </> file("zips")
      initial(ReadRT(data)).pruneArraysF must equal(expected(ReadRT(data)))
    }

    "not rewrite map-sort with no unused array elements" in {
      val initialArray: JoinFunc =
        ConcatArraysR(
          ConcatArraysR(
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(IntLit(7))),
            MakeArrayR(IntLit(8))),
          MakeArrayR(IntLit(9)))

      def initial: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(Sort(
            QCT.inj(LeftShift(
              UnreferencedRT.embed,
              HoleF,
              ExcludeId,
              initialArray)).embed,
            ProjectIndexR(HoleF, IntLit(0)),
            NonEmptyList(
              (ProjectIndexR(HoleF, IntLit(1)), SortDir.Ascending),
              (ProjectIndexR(HoleF, IntLit(3)), SortDir.Ascending)))).embed,
          ProjectIndexR(HoleF, IntLit(2)))).embed

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite filter with unused array elements" in {
      val initial: Fix[QST] =
        QCT.inj(Filter(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(BoolLit(true))))).embed,
          ProjectIndexR(HoleF, IntLit(1)))).embed

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite sort with unused array elements" in {
      val initial: Fix[QST] =
        QCT.inj(Sort(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(IntLit(7))))).embed,
          ProjectIndexR(HoleF, IntLit(1)),
          NonEmptyList(
            (ProjectIndexR(HoleF, IntLit(1)), SortDir.Ascending),
            (ProjectIndexR(HoleF, IntLit(1)), SortDir.Descending)))).embed

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite map with entire array referenced" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          array3,
          HoleF)).embed

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite map with entire array and specific index referenced" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          array3,
          ConcatArraysR(
            MakeArrayR(ProjectIndexR(HoleF, IntLit(0))),
            MakeArrayR(HoleF)))).embed

      initial.pruneArraysF must equal(initial)
    }

    "rewrite map with unused array elements 1,2" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          array3,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      val expected: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            MakeArrayR(IntLit(6)))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      initial.pruneArraysF must equal(expected)
    }

    "rewrite map with unused array elements 0,2" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          array3,
          ProjectIndexR(HoleF, IntLit(1)))).embed

      val expected: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            MakeArrayR(IntLit(7)))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      initial.pruneArraysF must equal(expected)
    }

    "rewrite map with unused array elements 0,1" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          array3,
          ProjectIndexR(HoleF, IntLit(2)))).embed

      val expected: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            MakeArrayR(IntLit(8)))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      initial.pruneArraysF must equal(expected)
    }

    "rewrite map with unused array elements in a binary map func" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          array3,
          AddR(
            ProjectIndexR(HoleF, IntLit(2)),
            ProjectIndexR(HoleF, IntLit(0))))).embed

      val expected: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(IntLit(8))))).embed,
          AddR(
            ProjectIndexR(HoleF, IntLit(1)),
            ProjectIndexR(HoleF, IntLit(0))))).embed

      initial.pruneArraysF must equal(expected)
    }

    "not rewrite leftshift with nonstatic array dereference" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          array3,
          AddR(
            ProjectIndexR(HoleF, IntLit(2)),
            ProjectIndexR(HoleF, AddR(IntLit(0), IntLit(1)))))).embed

      initial.pruneArraysF must equal(initial)
    }

    "rewrite two leftshift arrays" in {
      val innerInitial: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(      // [6, [7], 8]
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(MakeArrayR(IntLit(7)))),
            MakeArrayR(IntLit(8))))).embed

      val initial: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            QCT.inj(Map(
              innerInitial,
              ProjectIndexR(HoleF, IntLit(1)))).embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(LeftSideF, RightSideF))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      val innerExpected: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          MakeArrayR(MakeArrayR(IntLit(7))))).embed

      val expected: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            QCT.inj(Map(
              innerExpected,
              ProjectIndexR(HoleF, IntLit(0)))).embed,
            HoleF,
            ExcludeId,
            LeftSideF[Fix])).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      initial.pruneArraysF must equal(expected)
    }

    "rewrite filter-map-filter-leftshift" in {
      val innerArray: JoinFunc =
        ConcatArraysR(     // [7, 8, 9]
          ConcatArraysR(
            MakeArrayR(IntLit(7)),
            MakeArrayR(IntLit(8))),
          MakeArrayR(IntLit(9)))

      val srcInitial: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(      // ["a", [7, 8, 9], true]
            ConcatArraysR(
              MakeArrayR(StrLit("a")),
              MakeArrayR(innerArray)),
            MakeArrayR(BoolLit(true))))).embed

      val initial: Fix[QST] =
        QCT.inj(Filter(
          QCT.inj(Map(
            QCT.inj(Filter(
              srcInitial,
              ProjectIndexR(HoleF, IntLit(2)))).embed,
            ProjectIndexR(HoleF, IntLit(1)))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      val srcExpected: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(      // [[7, 8, 9], true]
            MakeArrayR(innerArray),
            MakeArrayR(BoolLit(true))))).embed

      val expected: Fix[QST] =
        QCT.inj(Filter(
          QCT.inj(Map(
            QCT.inj(Filter(
              srcExpected,
              ProjectIndexR(HoleF, IntLit(1)))).embed,
            ProjectIndexR(HoleF, IntLit(0)))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      initial.pruneArraysF must equal(expected)
    }

    "rewrite reduce-filter-leftshift" in {
      val srcInitial: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(
            ConcatArraysR(
              ConcatArraysR(
                MakeArrayR(StrLit("a")),
                MakeArrayR(StrLit("b"))),
              MakeArrayR(StrLit("c"))),
            MakeArrayR(StrLit("d"))))).embed

      val initial: QST[Fix[QST]] =
        QCT.inj(Reduce(
          QCT.inj(Filter(
            srcInitial,
            ProjectIndexR(HoleF, IntLit(3)))).embed,
          NullLit(),
          List(ReduceFuncs.Count(ProjectIndexR(HoleF, IntLit(2)))),
          MakeMapR(IntLit(0), ReduceIndexF(0.some))))

      val srcExpected: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(
            MakeArrayR(StrLit("c")),
            MakeArrayR(StrLit("d"))))).embed

      val expected: QST[Fix[QST]] =
        QCT.inj(Reduce(
          QCT.inj(Filter(
            srcExpected,
            ProjectIndexR(HoleF, IntLit(1)))).embed,
          NullLit(),
          List(ReduceFuncs.Count(ProjectIndexR(HoleF, IntLit(0)))),
          MakeMapR(IntLit(0), ReduceIndexF(0.some))))

      initial.embed.pruneArraysF must equal(expected.embed)
    }

    "rewrite bucket field with unused array elements" in {
      val initial: Fix[QST] =
        PBT.inj(BucketField(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(
              ConcatArraysR(
                MakeArrayR(IntLit(6)),
                MakeArrayR(IntLit(7))),
              MakeArrayR(StrLit("foo"))))).embed,
          ProjectIndexR(HoleF, IntLit(2)),
          ProjectIndexR(HoleF, IntLit(0)))).embed

      val expected: Fix[QST] =
        PBT.inj(BucketField(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(StrLit("foo"))))).embed,
          ProjectIndexR(HoleF, IntLit(1)),
          ProjectIndexR(HoleF, IntLit(0)))).embed

      initial.pruneArraysF must equal(expected)
    }

    "rewrite bucket index with unused array elements" in {
      val initial: Fix[QST] =
        PBT.inj(BucketIndex(
          array3,
          ProjectIndexR(HoleF, IntLit(2)),
          ProjectIndexR(HoleF, IntLit(0)))).embed

      val expected: Fix[QST] =
        PBT.inj(BucketIndex(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(IntLit(8))))).embed,
          ProjectIndexR(HoleF, IntLit(1)),
          ProjectIndexR(HoleF, IntLit(0)))).embed

      initial.pruneArraysF must equal(expected)
    }

    "rewrite array used in from of subset" in {
      val initial: Fix[QST] =
        QCT.inj(Reduce(
          QCT.inj(Subset(
            UnreferencedRT.embed,
            Free.roll(QCT.inj(LeftShift(
              Free.roll(RTF.inj(Const[Read[AFile], FreeQS](Read(rootDir </> file("zips"))))),
              HoleF,
              IncludeId,
              ConcatArraysR(MakeArrayR(LeftSideF), MakeArrayR(RightSideF))))),
            Drop,
            Free.roll(QCT.inj(Map(Free.roll(QCT.inj(Unreferenced())), IntLit[Fix, Hole](10)))))).embed,
          NullLit(),
          List(ReduceFuncs.Count(ProjectIndexR(ProjectIndexR(HoleF, IntLit(1)), IntLit(1)))),
          ReduceIndexF(0.some))).embed

      val expected: Fix[QST] =
        QCT.inj(Reduce(
          QCT.inj(Subset(
            UnreferencedRT.embed,
            Free.roll(QCT.inj(LeftShift(
              Free.roll(RTF.inj(Const[Read[AFile], FreeQS](Read(rootDir </> file("zips"))))),
              HoleF,
              IncludeId,
              MakeArrayR(RightSideF)))),
            Drop,
            Free.roll(QCT.inj(Map(Free.roll(QCT.inj(Unreferenced())), IntLit[Fix, Hole](10)))))).embed,
          NullLit(),
          List(ReduceFuncs.Count(ProjectIndexR(ProjectIndexR(HoleF, IntLit(0)), IntLit(1)))),
          ReduceIndexF(0.some))).embed

      initial.pruneArraysF must equal(expected)
    }

    // FIXME: this can be rewritten - we just don't support that yet
    "not rewrite subset with unused array elements" in {
      val initial: Fix[QST] =
        QCT.inj(Subset(
          array3,
          Free.roll(QCT.inj(Map(
            HoleQS,
            ProjectIndexR(HoleF, IntLit[Fix, Hole](2))))),
          Drop,
          Free.roll(QCT.inj(Map(
            HoleQS,
            ProjectIndexR(HoleF, IntLit[Fix, Hole](0))))))).embed

      initial.pruneArraysF must equal(initial)
    }

    // FIXME: this can be rewritten - we just don't support that yet
    "not rewrite union with unused array elements" in {
      val initial: Fix[QST] =
        QCT.inj(Union(
          array3,
          Free.roll(QCT.inj(Map(
            HoleQS,
            ProjectIndexR(HoleF, IntLit[Fix, Hole](2))))),
          Free.roll(QCT.inj(Map(
            HoleQS,
            ProjectIndexR(HoleF, IntLit[Fix, Hole](1))))))).embed

      initial.pruneArraysF must equal(initial)
    }

    // FIXME: this can be rewritten - we just don't support that yet
    "not rewrite theta join with unused array elements in source" in {
      val initial: Fix[QST] =
        TJT.inj(ThetaJoin(
          array3,
          Free.roll(QCT.inj(Map(
            HoleQS,
            ProjectIndexR(HoleF, IntLit[Fix, Hole](2))))),
          HoleQS,
          BoolLit[Fix, JoinSide](true),
          JoinType.Inner,
          MakeMapR(StrLit("xyz"), LeftSideF))).embed

      initial.pruneArraysF must equal(initial)
    }

    // FIXME: this can be rewritten - we just don't support that yet
    "not rewrite equi join with unused array elements in source" in {
      val initial: Fix[QST] =
        EJT.inj(EquiJoin(
          array3,
          Free.roll(QCT.inj(Map(
            HoleQS,
            ProjectIndexR(HoleF, IntLit[Fix, Hole](2))))),
          HoleQS,
          HoleF,
          HoleF,
          JoinType.Inner,
          MakeMapR(StrLit("xyz"), LeftSideF))).embed

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite equi join with entire array branch referenced in key" in {
      val initial: Fix[QST] =
        EJT.inj(EquiJoin(
          UnreferencedRT.embed,
          arrayBranch3,
          HoleQS,
          HoleF,  // reference entire left branch
          HoleF,
          JoinType.Inner,
          MakeMapR(
            StrLit("xyz"),
            ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](2))))).embed

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite theta join with entire array branch referenced in condition" in {
      val initial: Fix[QST] =
        TJT.inj(ThetaJoin(
          UnreferencedRT.embed,
          arrayBranch3,
          HoleQS,
          EqR(
            AddR(LeftSideF, IntLit[Fix, JoinSide](2)),  // reference entire left branch
            RightSideF),
          JoinType.Inner,
          MakeMapR(
            StrLit("xyz"),
            ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](2))))).embed

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite equi join with entire array branch referenced in combine" in {
      val initial: Fix[QST] =
        EJT.inj(EquiJoin(
          UnreferencedRT.embed,
          arrayBranch3,
          HoleQS,
          ProjectIndexR(HoleF, IntLit[Fix, Hole](2)),
          HoleF,
          JoinType.Inner,
          MakeMapR(
            StrLit("xyz"),
            LeftSideF))).embed  // reference entire left branch

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite theta join with entire array branch referenced in combine" in {
      val initial: Fix[QST] =
        TJT.inj(ThetaJoin(
          UnreferencedRT.embed,
          arrayBranch3,
          HoleQS,
          ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](2)),
          JoinType.Inner,
          MakeMapR(
            StrLit("xyz"),
            LeftSideF))).embed  // reference entire left branch

      initial.pruneArraysF must equal(initial)
    }

    // FIXME: we'd like to be able to rewrite this
    // but it might require more work in addition to array pruning
    "not rewrite equi join with array branch referenced outside of join" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          EJT.inj(EquiJoin(
            UnreferencedRT.embed,
            arrayBranch3,
            HoleQS,
            ProjectIndexR(HoleF, IntLit[Fix, Hole](2)),
            HoleF,
            JoinType.Inner,
            ConcatArraysR(MakeArrayR(LeftSideF), MakeArrayR(RightSideF)))).embed,
          ProjectIndexR(
            ProjectIndexR(HoleF, IntLit[Fix, Hole](0)),
            IntLit[Fix, Hole](2)))).embed

      initial.pruneArraysF must equal(initial)
    }

    // FIXME: this can be rewritten - we just don't support that yet
    "not rewrite theta join with filtered left shift as branch" in {
      val initial: Fix[QST] =
        TJT.inj(ThetaJoin(
          UnreferencedRT.embed,
          Free.roll(QCT.inj(Filter(arrayBranch3, ProjectIndexR(HoleF, IntLit[Fix, Hole](1))))),
          HoleQS,
          EqR(
            ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](2)),
            StrLit[Fix, JoinSide]("foo")),
          JoinType.Inner,
          MakeMapR(
            StrLit[Fix, JoinSide]("bar"),
            ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](1))))).embed

      initial.pruneArraysF must equal(initial)
    }

    // FIXME: this can be rewritten - we just don't support that yet
    "not rewrite equi join with filtered left shift as branch" in {
      val initial: Fix[QST] =
        EJT.inj(EquiJoin(
          UnreferencedRT.embed,
          Free.roll(QCT.inj(Filter(arrayBranch3, ProjectIndexR(HoleF, IntLit[Fix, Hole](1))))),
          HoleQS,
          EqR(
            ProjectIndexR(HoleF, IntLit[Fix, Hole](2)),
            StrLit[Fix, Hole]("foo")),
          HoleF,
          JoinType.Inner,
          MakeMapR(
            StrLit[Fix, JoinSide]("bar"),
            ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](1))))).embed

      initial.pruneArraysF must equal(initial)
    }

    "rewrite theta join with unused array elements in both branches" in {
      val rBranch: FreeQS =
        Free.roll(QCT.inj(LeftShift(
          HoleQS,
          HoleF,
          ExcludeId,
          ConcatArraysR(
            ConcatArraysR(
              MakeArrayR(IntLit(1)),
              MakeArrayR(IntLit(2))),
            MakeArrayR(StrLit("xyz"))))))

      val initial: Fix[QST] =
        TJT.inj(ThetaJoin(
          UnreferencedRT.embed,
          arrayBranch3,
          rBranch,
          EqR(
            ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](2)),
            ProjectIndexR(RightSideF, IntLit[Fix, JoinSide](0))),
          JoinType.Inner,
          MakeMapR(
            ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](2)),
            ProjectIndexR(RightSideF, IntLit[Fix, JoinSide](2))))).embed

      val lBranchExpected: FreeQS =
        Free.roll(QCT.inj(LeftShift(
          HoleQS,
          HoleF,
          ExcludeId,
          MakeArrayR(IntLit(8)))))

      val rBranchExpected: FreeQS =
        Free.roll(QCT.inj(LeftShift(
          HoleQS,
          HoleF,
          ExcludeId,
          ConcatArraysR(
            MakeArrayR(IntLit(1)),
            MakeArrayR(StrLit("xyz"))))))


      val expected: Fix[QST] =
        TJT.inj(ThetaJoin(
          UnreferencedRT.embed,
          lBranchExpected,
          rBranchExpected,
          EqR(
            ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](0)),
            ProjectIndexR(RightSideF, IntLit[Fix, JoinSide](0))),
          JoinType.Inner,
          MakeMapR(
            ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](0)),
            ProjectIndexR(RightSideF, IntLit[Fix, JoinSide](1))))).embed

      initial.pruneArraysF must equal(expected)
    }

    "rewrite equi join with unused array elements in both branches" in {
      val rBranch: FreeQS =
        Free.roll(QCT.inj(LeftShift(
          HoleQS,
          HoleF,
          ExcludeId,
          ConcatArraysR(
            ConcatArraysR(
              MakeArrayR(IntLit(1)),
              MakeArrayR(IntLit(2))),
            MakeArrayR(StrLit("xyz"))))))

      val initial: Fix[QST] =
        EJT.inj(EquiJoin(
          UnreferencedRT.embed,
          arrayBranch3,
          rBranch,
          ProjectIndexR(HoleF, IntLit[Fix, Hole](2)),
          ProjectIndexR(HoleF, IntLit[Fix, Hole](0)),
          JoinType.Inner,
          MakeMapR(
            ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](2)),
            ProjectIndexR(RightSideF, IntLit[Fix, JoinSide](2))))).embed

      val lBranchExpected: FreeQS =
        Free.roll(QCT.inj(LeftShift(
          HoleQS,
          HoleF,
          ExcludeId,
          MakeArrayR(IntLit(8)))))

      val rBranchExpected: FreeQS =
        Free.roll(QCT.inj(LeftShift(
          HoleQS,
          HoleF,
          ExcludeId,
          ConcatArraysR(
            MakeArrayR(IntLit(1)),
            MakeArrayR(StrLit("xyz"))))))


      val expected: Fix[QST] =
        EJT.inj(EquiJoin(
          UnreferencedRT.embed,
          lBranchExpected,
          rBranchExpected,
          ProjectIndexR(HoleF, IntLit[Fix, Hole](0)),
          ProjectIndexR(HoleF, IntLit[Fix, Hole](0)),
          JoinType.Inner,
          MakeMapR(
            ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](0)),
            ProjectIndexR(RightSideF, IntLit[Fix, JoinSide](1))))).embed

      initial.pruneArraysF must equal(expected)
    }

    "rewrite left shift with array referenced through struct" in {
      val initial: Fix[QST] =
        QCT.inj(LeftShift(
          array3,
          ProjectIndexR(HoleF, IntLit[Fix, Hole](2)),
          ExcludeId,
          MakeMapR(StrLit("xyz"), RightSideF))).embed

      val expectedSrc: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          MakeArrayR(IntLit(8)))).embed

      val expected: Fix[QST] =
        QCT.inj(LeftShift(
          expectedSrc,
          ProjectIndexR(HoleF, IntLit[Fix, Hole](0)),
          ExcludeId,
          MakeMapR(StrLit("xyz"), RightSideF))).embed

      initial.pruneArraysF must equal(expected)
    }

    "not rewrite left shift with entire array referenced through left side" in {
      val initial: Fix[QST] =
        QCT.inj(LeftShift(
          array3,
          ProjectIndexR(HoleF, IntLit[Fix, Hole](2)),
          ExcludeId,
          MakeMapR(StrLit("xyz"), LeftSideF))).embed

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite left shift with array referenced non-statically through struct" in {
      val initial: Fix[QST] =
        QCT.inj(LeftShift(
          array3,
          ProjectIndexR(HoleF, AddR(IntLit(0), IntLit(1))),
          ExcludeId,
          MakeMapR(StrLit("xyz"), LeftSideF))).embed

      initial.pruneArraysF must equal(initial)
    }

    "rewrite left shift with array referenced through left side and struct" in {
      val initial: Fix[QST] =
        QCT.inj(LeftShift(
          array3,
          ProjectIndexR(HoleF, IntLit[Fix, Hole](2)),
          ExcludeId,
          ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](1)))).embed

      val expectedSrc: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(
            MakeArrayR(IntLit(7)),
            MakeArrayR(IntLit(8))))).embed

      val expected: Fix[QST] =
        QCT.inj(LeftShift(
          expectedSrc,
          ProjectIndexR(HoleF, IntLit[Fix, Hole](1)),
          ExcludeId,
          ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](0)))).embed

      initial.pruneArraysF must equal(expected)
    }

    "rewrite left shift with array referenced through struct with a right side reference" in {
      val initialSrc: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(IntLit(7))),
            MakeArrayR(ConcatArraysR(MakeArrayR(IntLit(8)), MakeArrayR(IntLit(9))))))).embed

      val initial: Fix[QST] =
        QCT.inj(LeftShift(
          initialSrc,
          ProjectIndexR(HoleF, IntLit[Fix, Hole](2)),
          ExcludeId,
          ProjectIndexR(RightSideF, IntLit[Fix, JoinSide](1)))).embed

      val expectedSrc: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          MakeArrayR(ConcatArraysR(MakeArrayR(IntLit(8)), MakeArrayR(IntLit(9)))))).embed

      // TODO this can be rewritten further so that `struct` is just `HoleF`
      val expected: Fix[QST] =
        QCT.inj(LeftShift(
          expectedSrc,
          ProjectIndexR(HoleF, IntLit[Fix, Hole](0)),
          ExcludeId,
          ProjectIndexR(RightSideF, IntLit[Fix, JoinSide](1)))).embed

      initial.pruneArraysF must equal(expected)
    }

    "rewrite left shift with entire array unreferenced" in {
      val initial: Fix[QST] =
        QCT.inj(LeftShift(
          array3,
          IntLit(2),
          ExcludeId,
          AddR(IntLit[Fix, JoinSide](2), IntLit[Fix, JoinSide](3)))).embed

      val expectedSrc: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          Free.roll(MFC(Constant(ejsonArr()))))).embed

      val expected: Fix[QST] =
        QCT.inj(LeftShift(
          expectedSrc,
          IntLit(2),
          ExcludeId,
          AddR(IntLit[Fix, JoinSide](2), IntLit[Fix, JoinSide](3)))).embed

      initial.pruneArraysF must equal(expected)
    }

    "not rewrite left shift with entire array referenced by the right side" in {
      val initial: Fix[QST] =
        QCT.inj(LeftShift(
          array3,
          HoleF,
          ExcludeId,
          RightSideF)).embed

      initial.pruneArraysF must equal(initial)
    }

    "not rewrite left shift with entire array referenced by the left side" in {
      val initial: Fix[QST] =
        QCT.inj(LeftShift(
          array3,
          HoleF,
          ExcludeId,
          LeftSideF)).embed

      initial.pruneArraysF must equal(initial)
    }
  }

  "prune arrays branches" should {
    val rBranch: FreeQS =
      Free.roll(QCT.inj(LeftShift(
        HoleQS,
        HoleF,
        ExcludeId,
        ConcatArraysR(
          ConcatArraysR(
            MakeArrayR(IntLit(1)),
            MakeArrayR(IntLit(2))),
          MakeArrayR(StrLit("xyz"))))))

    val innerInitial: FreeQS =
      Free.roll(TJT.inj(ThetaJoin(
        Free.roll(QCT.inj(Unreferenced())),
        arrayBranch3,
        rBranch,
        EqR(
          ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](2)),
          ProjectIndexR(RightSideF, IntLit[Fix, JoinSide](0))),
        JoinType.Inner,
        MakeMapR(
          ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](2)),
          ProjectIndexR(RightSideF, IntLit[Fix, JoinSide](2))))))

    val lBranchExpected: FreeQS =
      Free.roll(QCT.inj(LeftShift(
        HoleQS,
        HoleF,
        ExcludeId,
        MakeArrayR(IntLit(8)))))

    val rBranchExpected: FreeQS =
      Free.roll(QCT.inj(LeftShift(
        HoleQS,
        HoleF,
        ExcludeId,
        ConcatArraysR(
          MakeArrayR(IntLit(1)),
          MakeArrayR(StrLit("xyz"))))))

    val innerExpected: FreeQS =
      Free.roll(TJT.inj(ThetaJoin(
        Free.roll(QCT.inj(Unreferenced())),
        lBranchExpected,
        rBranchExpected,
        EqR(
          ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](0)),
          ProjectIndexR(RightSideF, IntLit[Fix, JoinSide](0))),
        JoinType.Inner,
        MakeMapR(
          ProjectIndexR(LeftSideF, IntLit[Fix, JoinSide](0)),
          ProjectIndexR(RightSideF, IntLit[Fix, JoinSide](1))))))

    "rewrite left branch of theta join" in {
      def outer(branch: FreeQS): Fix[QST] =
        TJT.inj(ThetaJoin(
          UnreferencedRT.embed,
          branch,
          HoleQS,
          EqR(LeftSideF, RightSideF),
          JoinType.Inner,
          MakeMapR(LeftSideF, RightSideF))).embed

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }

    "rewrite right branch of theta join" in {
      def outer(branch: FreeQS): Fix[QST] =
        TJT.inj(ThetaJoin(
          UnreferencedRT.embed,
          HoleQS,
          branch,
          EqR(LeftSideF, RightSideF),
          JoinType.Inner,
          MakeMapR(LeftSideF, RightSideF))).embed

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }

    "rewrite left branch of equi join" in {
      def outer(branch: FreeQS): Fix[QST] =
        EJT.inj(EquiJoin(
          UnreferencedRT.embed,
          branch,
          HoleQS,
          HoleF,
          HoleF,
          JoinType.Inner,
          MakeMapR(LeftSideF, RightSideF))).embed

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }

    "rewrite right branch of equi join" in {
      def outer(branch: FreeQS): Fix[QST] =
        EJT.inj(EquiJoin(
          UnreferencedRT.embed,
          HoleQS,
          branch,
          HoleF,
          HoleF,
          JoinType.Inner,
          MakeMapR(LeftSideF, RightSideF))).embed

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }

    "rewrite left branch of union" in {
      def outer(branch: FreeQS): Fix[QST] =
        QCT.inj(Union(
          UnreferencedRT.embed,
          branch,
          HoleQS)).embed

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }

    "rewrite right branch of union" in {
      def outer(branch: FreeQS): Fix[QST] =
        QCT.inj(Union(
          UnreferencedRT.embed,
          HoleQS,
          branch)).embed

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }

    "rewrite from branch of subset" in {
      def outer(branch: FreeQS): Fix[QST] =
        QCT.inj(Subset(
          UnreferencedRT.embed,
          branch,
          Drop,
          HoleQS)).embed

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }

    "rewrite count branch of subset" in {
      def outer(branch: FreeQS): Fix[QST] =
        QCT.inj(Subset(
          UnreferencedRT.embed,
          HoleQS,
          Drop,
          branch)).embed

      outer(innerInitial).pruneArraysF must equal(outer(innerExpected))
    }
  }
}
