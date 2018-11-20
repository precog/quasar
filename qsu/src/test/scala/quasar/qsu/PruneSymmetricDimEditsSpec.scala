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
import quasar.IdStatus.ExcludeId
import quasar.common.SortDir
import quasar.qscript.{construction, MapFuncsCore, PlannerError}
import quasar.qsu.{QScriptUniform => QSU}

import matryoshka.data.Fix

import pathy.Path, Path._

import scalaz.{EitherT, StateT, Need, NonEmptyList => NEL}

object PruneSymmetricDimEditsSpec extends Qspec with QSUTTypes[Fix] {
  import QSUGraph.Extractors._

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]

  val qsu = QScriptUniform.DslT[Fix]
  val func = construction.Func[Fix]
  val recFunc = construction.RecFunc[Fix]

  val afile = Path.rootDir[Sandboxed] </> Path.file("afile")

  "pruning of symmetric DimEdit roots" should {
    "remove trivial left/right roots" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.dimEdit((
            qsu.read(afile, ExcludeId),
            QSU.DTrans.Squash())),
          qsu.dimEdit((
            qsu.distinct(qsu.read(afile, ExcludeId)),
            QSU.DTrans.Squash())),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case AutoJoin2(Read(_, ExcludeId), Distinct(Read(_, ExcludeId)), _) => ok
      }
    }

    "remove edits various dimensional no-ops" >> {
      "distinct" >> {
        val qgraph = QSUGraph.fromTree[Fix](
          qsu.autojoin2((
            qsu.distinct(
              qsu.dimEdit((
                qsu.read(afile, ExcludeId),
                QSU.DTrans.Squash()))),
            qsu.dimEdit((
              qsu.distinct(qsu.read(afile, ExcludeId)),
              QSU.DTrans.Squash())),
            _(MapFuncsCore.Add(_, _)))))

        runOn(qgraph) must beLike {
          case AutoJoin2(Distinct(Read(_, ExcludeId)), Distinct(Read(_, ExcludeId)), _) => ok
        }
      }

      "filter" >> {
        val qgraph = QSUGraph.fromTree[Fix](
          qsu.autojoin2((
            qsu.qsFilter((
              qsu.dimEdit((
                qsu.read(afile, ExcludeId),
                QSU.DTrans.Squash())),
              recFunc.Hole)),
            qsu.dimEdit((
              qsu.distinct(qsu.read(afile, ExcludeId)),
              QSU.DTrans.Squash())),
            _(MapFuncsCore.Add(_, _)))))

        runOn(qgraph) must beLike {
          case AutoJoin2(QSFilter(Read(_, ExcludeId), _), Distinct(Read(_, ExcludeId)), _) => ok
        }
      }

      "sort" >> {
        val qgraph = QSUGraph.fromTree[Fix](
          qsu.autojoin2((
            qsu.qsSort((
              qsu.dimEdit((
                qsu.read(afile, ExcludeId),
                QSU.DTrans.Squash())),
              Nil,
              NEL((func.Hole, SortDir.Ascending)))),
            qsu.dimEdit((
              qsu.distinct(qsu.read(afile, ExcludeId)),
              QSU.DTrans.Squash())),
            _(MapFuncsCore.Add(_, _)))))

        runOn(qgraph) must beLike {
          case AutoJoin2(QSSort(Read(_, ExcludeId), _, _), Distinct(Read(_, ExcludeId)), _) => ok
        }
      }

      "map" >> {
        val qgraph = QSUGraph.fromTree[Fix](
          qsu.autojoin2((
            qsu.map((
              qsu.dimEdit((
                qsu.read(afile, ExcludeId),
                QSU.DTrans.Squash())),
              recFunc.Hole)),
            qsu.dimEdit((
              qsu.distinct(qsu.read(afile, ExcludeId)),
              QSU.DTrans.Squash())),
            _(MapFuncsCore.Add(_, _)))))

        runOn(qgraph) must beLike {
          case AutoJoin2(Map(Read(_, ExcludeId), _), Distinct(Read(_, ExcludeId)), _) => ok
        }
      }
    }

    "remove trivial left root when right unreferenced" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.dimEdit((
            qsu.read(afile, ExcludeId),
            QSU.DTrans.Squash())),
          qsu.unreferenced(),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case AutoJoin2(Read(_, ExcludeId), Unreferenced(), _) => ok
      }
    }

    "retain pointers to vertices with multiple inbound edges" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.autojoin2((
            qsu.dimEdit((
              qsu.map(
                qsu.read(afile, ExcludeId),
                recFunc.ProjectKeyS(recFunc.Hole, "bar")),
              QSU.DTrans.Squash())),
            qsu.dimEdit((
              qsu.map(
                qsu.read(afile, ExcludeId),
                recFunc.ProjectKeyS(recFunc.Hole, "baz")),
              QSU.DTrans.Squash())),
            _(MapFuncsCore.Add(_, _)))),
          qsu.dimEdit((
            qsu.map(
              qsu.read(afile, ExcludeId),
              recFunc.ProjectKeyS(recFunc.Hole, "bar")),
            QSU.DTrans.Squash())),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must not(throwA[Exception])
    }
  }

  private def runOn(g: QSUGraph): QSUGraph = {
    val resultsF = PruneSymmetricDimEdits[Fix, F](g)

    val results = resultsF.run.eval(0L).value.toEither
    results must beRight

    results.right.get
  }
}
