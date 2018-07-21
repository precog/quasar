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

import quasar.{Qspec, TreeMatchers}
import quasar.ejson.{EJson, Fixed}
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript.{construction, MapFuncsCore, PlannerError}
import quasar.qscript.provenance.JoinKeys

import matryoshka.data.Fix
import matryoshka.data.freeEqual
import matryoshka.delayEqual
import pathy.Path
import pathy.Path.Sandboxed
import scalaz.{EitherT, INil, Need, StateT}

object ReifyAutoJoinSpecs extends Qspec with TreeMatchers with QSUTTypes[Fix] {
  import QSUGraph.Extractors._
  import ApplyProvenance.AuthenticatedQSU

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]

  val qsu = QScriptUniform.DslT[Fix]
  val func = construction.Func[Fix]
  val recFunc = construction.RecFunc[Fix]

  val J = Fixed[Fix[EJson]]

  val afile1 = Path.rootDir[Sandboxed] </> Path.file("afile")
  val afile2 = Path.rootDir[Sandboxed] </> Path.file("afile2")
  val afile3 = Path.rootDir[Sandboxed] </> Path.file("afile3")

  "autojoin reification" >> {
    "reify an autojoin2" >> {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.map(
            qsu.read(afile1),
            recFunc.ProjectKeyS(recFunc.Hole, "foo")),
          qsu.map(
            qsu.read(afile2),
            recFunc.ProjectKeyS(recFunc.Hole, "bar")),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case QSAutoJoin(
          Map(Read(`afile1`), fmL),
          Map(Read(`afile2`), fmR),
          JoinKeys(INil()),
          fmCombiner) =>

          fmL.linearize must beTreeEqual(
            func.ProjectKeyS(func.Hole, "foo"))

          fmR.linearize must beTreeEqual(
            func.ProjectKeyS(func.Hole, "bar"))

          fmCombiner must beTreeEqual(
            func.Add(func.LeftSide, func.RightSide))
      }
    }

    "reify an autojoin3" >> {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu._autojoin3((
          qsu.map(
            qsu.read(afile1),
            recFunc.ProjectKeyS(recFunc.Hole, "foo")),
          qsu.map(
            qsu.read(afile2),
            recFunc.ProjectKeyS(recFunc.Hole, "bar")),
          qsu.map(
            qsu.read(afile3),
            recFunc.ProjectKeyS(recFunc.Hole, "baz")),
          func.Subtract(func.Add(func.LeftSide3, func.RightSide3), func.Center))))

      runOn(qgraph) must beLike {
        case QSAutoJoin(
          QSAutoJoin(
            Map(Read(`afile1`), fmL),
            Map(Read(`afile2`), fmC),
            JoinKeys(INil()),
            fmInner),
          Map(Read(`afile3`), fmR),
          JoinKeys(INil()),
          fmOuter) =>

          fmL.linearize must beTreeEqual(
           func.ProjectKeyS(func.Hole, "foo"))

          fmC.linearize must beTreeEqual(
           func.ProjectKeyS(func.Hole, "bar"))

          fmR.linearize must beTreeEqual(
           func.ProjectKeyS(func.Hole, "baz"))

          fmInner must beTreeEqual(
           func.ConcatMaps(
             func.MakeMap(func.Constant(J.str("leftAccess1")), func.LeftSide),
             func.MakeMap(func.Constant(J.str("centerAccess2")), func.RightSide)))

          fmOuter must beTreeEqual(
            func.Subtract(
              func.Add(
                func.ProjectKey(func.LeftSide, func.Constant(J.str("leftAccess1"))),
                func.RightSide),
              func.ProjectKey(func.LeftSide, func.Constant(J.str("centerAccess2")))))
      }
    }
  }

  def runOn(qgraph: QSUGraph): QSUGraph =
    runOn_(qgraph).graph

  def runOn_(qgraph: QSUGraph): AuthenticatedQSU[Fix] = {
    val resultsF = for {
      prov <- ApplyProvenance[Fix, F](qgraph)
      back <- ReifyAutoJoins[Fix, F](prov)
    } yield back

    val results = resultsF.run.eval(0L).value.toEither
    results must beRight

    results.right.get
  }
}
