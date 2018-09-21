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

import quasar.{Qspec, RenderTree}, RenderTree.ops._
import quasar.common.PhaseResults
import quasar.common.data.Data
import quasar.contrib.matryoshka._
import quasar.ejson.{EJson, Fixed}
import quasar.fp._
import quasar.contrib.iota._
import quasar.frontend.logicalplan.{LogicalPlan, LogicalPlanHelpers}
import quasar.qscript.construction
import quasar.qscript.{qScriptReadToQscriptTotal, ExcludeId, HoleF, LeftShift, OnUndefined, PlannerError, ReduceFuncs, ReduceIndex, RightSideF, ShiftType}
import quasar.std.{AggLib, IdentityLib, StructuralLib}

import iotaz.CopK

import matryoshka._
import matryoshka.implicits._
import matryoshka.data.Fix

import org.specs2.matcher.Matcher
import org.specs2.matcher.MatchersImplicits._

import pathy.Path
import Path.{Sandboxed, file}

import scalaz.{\/-, -\/, EitherT, Equal, Free, Need, StateT, WriterT}
import scalaz.syntax.show._

object LPtoQSSpec extends Qspec with LogicalPlanHelpers with QSUTTypes[Fix] {
  type F[A] = WriterT[EitherT[StateT[Need, Long, ?], PlannerError, ?], PhaseResults, A]

  val qsu = LPtoQS[Fix]

  val defaults = construction.mkDefaults[Fix, QScriptEducated]

  val func = defaults.func
  val qs = defaults.fix
  val recFunc = defaults.recFunc
  val json = Fixed[Fix[EJson]]

  val root = Path.rootDir[Sandboxed]
  val afoo = root </> file("foo")

  val QC = CopK.Inject[QScriptCore, QScriptEducated]

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

    "select a[*], a[*] from foo" >> {
       val lp = lpf.let(
         '__tmp0,
         lpf.read(afoo),
         lpf.invoke1(
           IdentityLib.Squash,
           lpf.invoke2(
             StructuralLib.MapConcat,
             lpf.invoke2(
               StructuralLib.MakeMap,
               lpf.constant(Data.Str("a")),
               lpf.invoke1(
                 StructuralLib.FlattenArray,
                 lpf.let(
                   '__tmp2,
                   lpf.invoke2(
                     StructuralLib.MapProject,
                     lpf.free('__tmp0),
                     lpf.constant(Data.Str("a"))),
                   lpf.free('__tmp2)))),
             lpf.invoke2(
               StructuralLib.MakeMap,
               lpf.constant(Data.Str("a0")),
               lpf.invoke1(
                 StructuralLib.FlattenArray,
                 lpf.let(
                   '__tmp3,
                   lpf.invoke2(
                     StructuralLib.MapProject,
                     lpf.free('__tmp0),
                     lpf.constant(Data.Str("a"))),
                   lpf.free('__tmp3)))))))

      lp must compileToMatch { qs =>
        val count = qs foldMap { fix =>
          fix.project match {
            case QC(LeftShift(_, _, _, _, _, _)) => 1
            case _ => 0
          }
        }

        count mustEqual 2   // shifted read and the *one* collapsed shift
      }
    }
  }

  def compileTo(qs: Fix[QScriptEducated]): Matcher[Fix[LogicalPlan]] =
    compileToMatch(Equal[Fix[QScriptEducated]].equal(_, qs))

  def compileToMatch(pred: Fix[QScriptEducated] => Boolean): Matcher[Fix[LogicalPlan]] = { lp: Fix[LogicalPlan] =>
    val result = qsu[F](lp).run.map(_._2).run.eval(0L).value

    result match {
      case -\/(errs) =>
        (false, s"${lp.render.shows}\n produced errors ${errs.shows}", "")

      case \/-(compiled) =>
        (
          pred(compiled),
          s"\n${lp.render.shows}\n\n compiled to:\n\n ${compiled.render.shows}",
          s"\n${lp.render.shows}\n\n compiled to:\n\n ${compiled.render.shows}")
    }
  }
}
