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

import quasar.{Data, Qspec, RenderTree}
import RenderTree.ops._
import quasar.fs.Planner.PlannerError
import quasar.contrib.matryoshka._
import quasar.ejson.{EJson, Fixed}
import quasar.fp._
import quasar.contrib.iota._
import quasar.frontend.logicalplan.{LogicalPlan, LogicalPlanHelpers}
import quasar.qscript.construction
import quasar.qscript.{ExcludeId, HoleF, OnUndefined, ReduceFuncs, ReduceIndex, RightSideF, ShiftType, qScriptReadToQscriptTotal}
import quasar.std.{AggLib, IdentityLib}
import slamdata.Predef._
import matryoshka._
import matryoshka.data.Fix
import org.specs2.matcher.Matcher
import org.specs2.matcher.MatchersImplicits._
import pathy.Path
import Path.{Sandboxed, file}

import scalaz.{-\/, EitherT, Equal, Free, Need, StateT, \/-}
import scalaz.syntax.show._

object LPtoQSSpec extends Qspec with LogicalPlanHelpers with QSUTTypes[Fix] {
  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]

  val qsu = LPtoQS[Fix]

  val defaults = construction.mkDefaults[Fix, QScriptEducated]

  val func = defaults.func
  val qs = defaults.fix
  val recFunc = defaults.recFunc
  val json = Fixed[Fix[EJson]]

  val root = Path.rootDir[Sandboxed]
  val afoo = root </> file("foo")

  "logicalplan -> qscript" >> {
    "constant value" >> {
      val lp = lpf.constant(Data.Bool(true))

      val expected = qs.Map(
        qs.Unreferenced,
        recFunc.Constant(json.bool(true)))

      lp must compileTo(expected)
    }

    "select * from foo" >> {
      val lp = read("foo")

      val expected = qs.LeftShift(
        qs.Read(afoo),
        recFunc.Hole,
        ExcludeId,
        ShiftType.Map,
        OnUndefined.Omit,
        RightSideF[Fix])

      lp must compileTo(expected)
    }

    "select count(*) from foo" >> {
      val lp = lpf.invoke1(
        IdentityLib.Squash,
        lpf.invoke1(
          AggLib.Count,
          read("foo")))

      val expected = qs.Reduce(
        qs.LeftShift(
          qs.Read(afoo),
          recFunc.Hole,
          ExcludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          RightSideF[Fix]),
        Nil,
        List(ReduceFuncs.Count(HoleF[Fix])),
        Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0))))

      lp must compileTo(expected)
    }
  }

  def compileTo(qs: Fix[QScriptEducated]): Matcher[Fix[LogicalPlan]] = { lp: Fix[LogicalPlan] =>
    val result = qsu[F](lp).run.eval(0L).value

    result match {
      case -\/(errs) =>
        (false, s"${lp.render.shows}\n produced errors ${errs.shows}", "")

      case \/-(compiled) =>
        (
          Equal[Fix[QScriptEducated]].equal(compiled, qs),
          s"\n${lp.render.shows}\n\n compiled to:\n\n ${compiled.render.shows}",
          s"\n${lp.render.shows}\n\n compiled to:\n\n ${compiled.render.shows}\n\n expected:\n\n ${qs.render.shows}")
    }
  }
}
