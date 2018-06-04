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
import quasar.Qspec
import quasar.fs.Planner.{InternalError, PlannerError}
import quasar.common.{JoinType, SortDir}
import quasar.contrib.pathy.AFile
import quasar.ejson.{EJson, Fixed}
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript.{
  construction,
  Hole,
  HoleF,
  IncludeId,
  JoinSide,
  OnUndefined,
  ReduceFunc,
  ReduceFuncs,
  ReduceIndex,
  ReduceIndexF,
  ShiftType,
  SrcHole,
  Take,
  qScriptReadToQscriptTotal
}
import quasar.qscript.MapFuncsCore.{IntLit, RecIntLit}
import quasar.qsu.ApplyProvenance.AuthenticatedQSU
import matryoshka.EqualT
import matryoshka.data.Fix
import Fix._
import org.specs2.matcher.{Expectable, MatchResult, Matcher}
import pathy.Path
import Path.{Sandboxed, file}

import scalaz.{EitherT, Free, Need, StateT, \/, \/-, NonEmptyList => NEL}
import scalaz.Scalaz._

object GraduateSpec extends Qspec with QSUTTypes[Fix] {
  import QScriptUniform.Rotation

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]

  type QSU[A] = QScriptUniform[A]
  type QSE[A] = QScriptEducated[A]

  val researched = ReifyIdentities[Fix, F] _
  val grad = Graduate[Fix, F] _

  val qsu = QScriptUniform.DslT[Fix]

  val defaults = construction.mkDefaults[Fix, QSE]
  val func = defaults.func
  val recFunc = defaults.recFunc
  val fqse = defaults.free
  val qse = defaults.fix

  val root = Path.rootDir[Sandboxed]
  val afile: AFile = root </> file("foobar")

  "graduating QSU to QScript" should {

    "convert the QScript-ish nodes" >> {
      "convert Read" in {
        val qgraph: Fix[QSU] = qsu.read(afile)
        val qscript: Fix[QSE] = qse.Read[AFile](afile)

        qgraph must graduateAs(qscript)
      }

      "convert Map" in {
        val fm: RecFreeMap = recFunc.Add(recFunc.Hole, RecIntLit(17))

        val qgraph: Fix[QSU] = qsu.map(qsu.read(afile), fm)
        val qscript: Fix[QSE] = qse.Map(qse.Read[AFile](afile), fm)

        qgraph must graduateAs(qscript)
      }

      "convert QSFilter" in {
        val fm: RecFreeMap = recFunc.Add(recFunc.Hole, RecIntLit(17))

        val qgraph: Fix[QSU] = qsu.qsFilter(qsu.read(afile), fm)
        val qscript: Fix[QSE] = qse.Filter(qse.Read[AFile](afile), fm)

        qgraph must graduateAs(qscript)
      }

      "convert QSReduce" in {
        val buckets: List[FreeMap] = List(func.Add(HoleF, IntLit(17)))
        val abuckets: List[FreeAccess[Hole]] = buckets.map(_.map(Access.value[Fix[EJson], Hole](_)))
        val reducers: List[ReduceFunc[FreeMap]] = List(ReduceFuncs.Count(HoleF))
        val repair: FreeMapA[ReduceIndex] = ReduceIndexF(\/-(0))

        val qgraph: Fix[QSU] = qsu.qsReduce(qsu.read(afile), abuckets, reducers, repair)
        val qscript: Fix[QSE] = qse.Reduce(qse.Read[AFile](afile), buckets, reducers, repair)

        qgraph must graduateAs(qscript)
      }

      "convert LeftShift" in {
        val struct: RecFreeMap = recFunc.Add(recFunc.Hole, recFunc.Constant(Fixed[Fix[EJson]].int(17)))

        val arepair: FreeMapA[QScriptUniform.ShiftTarget[Fix]] = func.ConcatArrays(
          func.MakeArray(AccessLeftTarget[Fix](Access.valueHole(_))),
          func.ConcatArrays(
            LeftTarget[Fix],
            func.MakeArray(RightTarget[Fix])))
        val repair: JoinFunc = func.ConcatArrays(
          func.MakeArray(func.LeftSide),
          func.ConcatArrays(
            func.LeftSide,
            func.MakeArray(func.RightSide)))

        val qgraph: Fix[QSU] = qsu.leftShift(qsu.read(afile), struct, IncludeId, OnUndefined.Omit, arepair, Rotation.ShiftArray)
        val qscript: Fix[QSE] = qse.LeftShift(qse.Read[AFile](afile), struct, IncludeId, ShiftType.Array, OnUndefined.Omit, repair)

        qgraph must graduateAs(qscript)
      }

      "convert QSSort" in {
        val buckets: List[FreeMap] = List(func.Add(HoleF, IntLit(17)))
        val abuckets: List[FreeAccess[Hole]] = buckets.map(_.map(Access.value[Fix[EJson], Hole](_)))
        val order: NEL[(FreeMap, SortDir)] = NEL(HoleF -> SortDir.Descending)

        val qgraph: Fix[QSU] = qsu.qsSort(qsu.read(afile), abuckets, order)
        val qscript: Fix[QSE] = qse.Sort(qse.Read[AFile](afile), buckets, order)

        qgraph must graduateAs(qscript)
      }

      "convert Distinct" in {
        val qgraph: Fix[QSU] = qsu.distinct(qsu.read(afile))

        val qscript: Fix[QSE] =
          qse.Reduce(
            qse.Read[AFile](afile),
            List(HoleF),
            List(ReduceFuncs.Arbitrary(HoleF)),
            ReduceIndexF(\/-(0)))

        qgraph must graduateAs(qscript)
      }

      "convert Unreferenced" in {
        val qgraph: Fix[QSU] = qsu.unreferenced()
        val qscript: Fix[QSE] = qse.Unreferenced

        qgraph must graduateAs(qscript)
      }
    }

    "fail to convert the LP-ish nodes" >> {
      "not convert LPFilter" in {
        val qgraph: Fix[QSU] = qsu.lpFilter(qsu.read(afile), qsu.read(afile))

        qgraph must notGraduate
      }
    }

    "graduate naive `select * from zips`" in {
      val aconcatArr =
        func.ConcatArrays(
          func.MakeArray(AccessLeftTarget[Fix](Access.valueHole(_))),
          func.MakeArray(RightTarget[Fix]))
      val concatArr =
        func.ConcatArrays(
          func.MakeArray(func.LeftSide),
          func.MakeArray(func.RightSide))

      val projectIdx = func.ProjectIndex(func.LeftSide, func.RightSide)

      val qgraph =
        qsu.subset(
          qsu.thetaJoin(
            qsu.leftShift(
              qsu.read(root </> file("zips")),
              recFunc.Hole,
              IncludeId,
              OnUndefined.Omit,
              aconcatArr,
              Rotation.FlattenArray),
            qsu.cint(1),
            func.Constant[JoinSide](Fixed[Fix[EJson]].bool(true)),
            JoinType.Inner,
            projectIdx),
          Take,
          qsu.cint(11))

      val lhs: Free[QSE, Hole] =
        fqse.LeftShift(
          fqse.Read(root </> file("zips")),
          recFunc.Hole,
          IncludeId,
          ShiftType.Array,
          OnUndefined.Omit,
          concatArr)

      val rhs: Free[QSE, Hole] =
        fqse.Map(fqse.Unreferenced, recFunc.Constant(Fixed[Fix[EJson]].int(1)))

      val qscript =
        qse.Subset(
          qse.Unreferenced,
          fqse.ThetaJoin(
            fqse.Unreferenced,
            lhs,
            rhs,
            func.Constant(Fixed[Fix[EJson]].bool(true)),
            JoinType.Inner,
            projectIdx),
          Take,
          fqse.Map(
            Free.pure[QSE, Hole](SrcHole),
            recFunc.Constant(Fixed[Fix[EJson]].int(11))))

      qgraph must graduateAs(qscript)
    }
  }

  def graduateAs(expected: Fix[QSE]): Matcher[Fix[QSU]] = {
    new Matcher[Fix[QSU]] {
      def apply[S <: Fix[QSU]](s: Expectable[S]): MatchResult[S] = {
        val authd = AuthenticatedQSU[Fix](QSUGraph.fromTree[Fix](s.value), QAuth.empty)
        val actual: PlannerError \/ Fix[QSE] = evaluate(researched(authd) >>= grad)

        actual.bimap[MatchResult[S], MatchResult[S]](
        { err =>
          failure(s"graduating produced unexpected planner error: ${err.shows}", s)
        },
        { qscript =>
          result(
            EqualT[Fix].equal[QSE](qscript, expected),
            s"received expected qscript:\n${qscript.shows}",
            s"received unexpected qscript:\n${qscript.shows}\nexpected:\n${expected.shows}",
            s)
        }).merge
      }
    }
  }

  def notGraduate: Matcher[Fix[QSU]] = {
    new Matcher[Fix[QSU]] {
      def apply[S <: Fix[QSU]](s: Expectable[S]): MatchResult[S] = {
        val authd = AuthenticatedQSU[Fix](QSUGraph.fromTree[Fix](s.value), QAuth.empty)
        val actual: PlannerError \/ Fix[QSE] = evaluate(researched(authd) >>= grad)

        // TODO better equality checking for PlannerError
        actual.bimap[MatchResult[S], MatchResult[S]](
        {
          case err @ InternalError(_, None) =>
            success(s"received expected InternalError: ${(err: PlannerError).shows}", s)
          case err =>
            failure(s"expected an InternalError without a cause, received: ${err.shows}", s)
        },
        { qscript =>
          failure(s"expected an error but found qscript:\n${qscript.shows}", s)
        }).merge
      }
    }
  }

  def evaluate[A](fa: F[A]): PlannerError \/ A = fa.run.eval(0L).value
}
