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

import slamdata.Predef.{Map => SMap, _}

import quasar.{ParseType, Qspec}
import quasar.IdStatus.{ExcludeId, IdOnly, IncludeId}
import quasar.ParseInstruction.{Ids, Mask, Pivot, Project, Wrap}
import quasar.api.resource.ResourcePath
import quasar.common.CPath
import quasar.contrib.iota._
import quasar.ejson.{EJson, Fixed}
import quasar.fp._
import quasar.qscript._

import iotaz.{CopK, TNilK}
import iotaz.TListK.:::

import matryoshka.data.Fix
import matryoshka.implicits._

import pathy.Path._

import scalaz.Const

object FocusedPushdownSpec extends Qspec {

  type QSBase =
    QScriptCore[Fix, ?] :::
    EquiJoin[Fix, ?] :::
    Const[Read[ResourcePath], ?] :::
    TNilK

  // initial QScript
  type QS[A] = CopK[QSBase, A]

  implicit val QS: Injectable[QS, QScriptTotal[Fix, ?]] =
    SubInject[QS, QScriptTotal[Fix, ?]]

  val qsDsl = construction.mkDefaults[Fix, QS]
  val fix = qsDsl.fix
  val recFunc = qsDsl.recFunc

  // initial QScript with InterpretedRead
  type QSExtra[A] = CopK[Const[InterpretedRead[ResourcePath], ?] ::: QSBase, A]

  implicit val QSExtra: Injectable[QSExtra, QScriptTotal[Fix, ?]] =
    SubInject[QSExtra, QScriptTotal[Fix, ?]]

  val qsExtraDsl = construction.mkDefaults[Fix, QSExtra]
  val fixE = qsExtraDsl.fix
  val recFuncE = qsExtraDsl.recFunc
  val funcE = qsExtraDsl.func

  val ejs = Fixed[Fix[EJson]]

  val path = ResourcePath.leaf(rootDir </> file("foo"))

  def focusedPushdown(expr: Fix[QSExtra]): Fix[QSExtra] =
    expr.transCata[Fix[QSExtra]](FocusedPushdown[Fix, QSExtra, QSExtra, ResourcePath])

  "pivot" >> {
    "when the read is shifted at the top-level and only the shifted values are referenced" >> {
      "with IncludeId" >> {
        val initial: Fix[QSExtra] =
          fixE.LeftShift(
            fixE.Read[ResourcePath](path, ExcludeId),
            recFuncE.Hole,
            IncludeId, // IncludeId
            ShiftType.Map,
            OnUndefined.Omit,
            funcE.ConcatMaps(
              funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
              funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

        val expected: Fix[QSExtra] =
          fixE.Map(
            fixE.InterpretedRead[ResourcePath](
              path,
              List(
                Mask(SMap(CPath.Identity -> Set(ParseType.Object))),
                Pivot(CPath.Identity, IncludeId, ParseType.Object),
                Mask(SMap((CPath.Identity \ 0) -> ParseType.Top, (CPath.Identity \ 1) -> ParseType.Top)))),
            recFuncE.ConcatMaps(
              recFuncE.MakeMapS("k1", recFuncE.ProjectIndexI(recFuncE.Hole, 0)),
              recFuncE.MakeMapS("v1", recFuncE.ProjectIndexI(recFuncE.Hole, 1))))

        focusedPushdown(initial) must equal(expected)
      }

      "with IdOnly" >> {
        val initial: Fix[QSExtra] =
          fixE.LeftShift(
            fixE.Read[ResourcePath](path, ExcludeId),
            recFuncE.Hole,
            IdOnly, // IdOnly
            ShiftType.Map,
            OnUndefined.Omit,
            funcE.MakeMapS("k1", funcE.RightSide))

        val expected: Fix[QSExtra] =
          fixE.InterpretedRead[ResourcePath](
            path,
            List(
              Mask(SMap(CPath.Identity -> Set(ParseType.Object))),
              Pivot(CPath.Identity, IdOnly, ParseType.Object),
              Wrap(CPath.Identity, "k1")))

        focusedPushdown(initial) must equal(expected)
      }

      "with ExcludeId" >> {
        val initial: Fix[QSExtra] =
          fixE.LeftShift(
            fixE.Read[ResourcePath](path, ExcludeId),
            recFuncE.Hole,
            ExcludeId, // ExcludeId
            ShiftType.Map,
            OnUndefined.Omit,
            funcE.MakeMapS("v1", funcE.RightSide))

        val expected: Fix[QSExtra] =
          fixE.InterpretedRead[ResourcePath](
            path,
            List(
              Mask(SMap(CPath.Identity -> Set(ParseType.Object))),
              Pivot(CPath.Identity, ExcludeId, ParseType.Object),
              Wrap(CPath.Identity, "v1")))

        focusedPushdown(initial) must equal(expected)
      }
    }

    "when the target LeftShift is the source of another node" >> {
      val initial: Fix[QSExtra] =
        fixE.Filter(
          fixE.LeftShift(
            fixE.Read[ResourcePath](path, ExcludeId),
            recFuncE.Hole,
            ExcludeId,
            ShiftType.Map,
            OnUndefined.Omit,
            funcE.MakeMapS("v1", funcE.RightSide)),
          recFuncE.Constant(ejs.bool(true)))

      val expected: Fix[QSExtra] =
        fixE.Filter(
          fixE.InterpretedRead[ResourcePath](
            path,
            List(
              Mask(SMap(CPath.Identity -> Set(ParseType.Object))),
              Pivot(CPath.Identity, ExcludeId, ParseType.Object),
              Wrap(CPath.Identity, "v1"))),
          recFuncE.Constant(ejs.bool(true)))

      focusedPushdown(initial) must equal(expected)
    }

    "when the shift source is a single object projection" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.Read[ResourcePath](path, ExcludeId),
          recFuncE.ProjectKeyS(recFunc.Hole, "xyz"), // shift source is a single projection
          IncludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

        val expected: Fix[QSExtra] =
          fixE.Map(
            fixE.InterpretedRead[ResourcePath](
              path,
              List(
                Project(CPath.Identity \ "xyz"),
                Mask(SMap(CPath.Identity -> Set(ParseType.Object))),
                Pivot(CPath.Identity, IncludeId, ParseType.Object),
                Mask(SMap((CPath.Identity \ 0) -> ParseType.Top, (CPath.Identity \ 1) -> ParseType.Top)))),
            recFuncE.ConcatMaps(
              recFuncE.MakeMapS("k1", recFuncE.ProjectIndexI(recFuncE.Hole, 0)),
              recFuncE.MakeMapS("v1", recFuncE.ProjectIndexI(recFuncE.Hole, 1))))

      focusedPushdown(initial) must equal(expected)
    }

    "when the shift source is three object projections" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.Read[ResourcePath](path, ExcludeId),
          recFuncE.ProjectKeyS( // shift source is three projections
            recFuncE.ProjectKeyS(
              recFuncE.ProjectKeyS(recFunc.Hole,
                "aaa"),
              "bbb"),
            "ccc"),
          IncludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

        val expected: Fix[QSExtra] =
          fixE.Map(
            fixE.InterpretedRead[ResourcePath](
              path,
              List(
                Project(CPath.parse(".aaa.bbb.ccc")),
                Mask(SMap(CPath.Identity -> Set(ParseType.Object))),
                Pivot(CPath.Identity, IncludeId, ParseType.Object),
                Mask(SMap((CPath.Identity \ 0) -> ParseType.Top, (CPath.Identity \ 1) -> ParseType.Top)))),
            recFuncE.ConcatMaps(
              recFuncE.MakeMapS("k1", recFuncE.ProjectIndexI(recFuncE.Hole, 0)),
              recFuncE.MakeMapS("v1", recFuncE.ProjectIndexI(recFuncE.Hole, 1))))

      focusedPushdown(initial) must equal(expected)
    }

    // 🦖  rawr!
    "when the shift struct contains static array and object projections" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.Read[ResourcePath](path, ExcludeId),
          recFuncE.ProjectKeyS(
            recFuncE.ProjectIndexI( // shift source contains an array projection
              recFuncE.ProjectKeyS(recFunc.Hole,
                "aaa"),
              42),
            "ccc"),
          IncludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

        val expected: Fix[QSExtra] =
          fixE.Map(
            fixE.InterpretedRead[ResourcePath](
              path,
              List(
                Project(CPath.parse(".aaa[42].ccc")),
                Mask(SMap(CPath.Identity -> Set(ParseType.Object))),
                Pivot(CPath.Identity, IncludeId, ParseType.Object),
                Mask(SMap((CPath.Identity \ 0) -> ParseType.Top, (CPath.Identity \ 1) -> ParseType.Top)))),
            recFuncE.ConcatMaps(
              recFuncE.MakeMapS("k1", recFuncE.ProjectIndexI(recFuncE.Hole, 0)),
              recFuncE.MakeMapS("v1", recFuncE.ProjectIndexI(recFuncE.Hole, 1))))

      focusedPushdown(initial) must equal(expected)
    }

    "when the shift struct contains two static array projections" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.Read[ResourcePath](path, ExcludeId),
          recFuncE.ProjectKeyS(
            recFuncE.ProjectIndexI( // shift source contains an array projection
              recFuncE.ProjectIndexI(recFunc.Hole,
                17),
              42),
            "ccc"),
          IncludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

        val expected: Fix[QSExtra] =
          fixE.Map(
            fixE.InterpretedRead[ResourcePath](
              path,
              List(
                Project(CPath.parse(".[17][42].ccc")),
                Mask(SMap(CPath.Identity -> Set(ParseType.Object))),
                Pivot(CPath.Identity, IncludeId, ParseType.Object),
                Mask(SMap((CPath.Identity \ 0) -> ParseType.Top, (CPath.Identity \ 1) -> ParseType.Top)))),
            recFuncE.ConcatMaps(
              recFuncE.MakeMapS("k1", recFuncE.ProjectIndexI(recFuncE.Hole, 0)),
              recFuncE.MakeMapS("v1", recFuncE.ProjectIndexI(recFuncE.Hole, 1))))

      focusedPushdown(initial) must equal(expected)
    }

    "when the Read has IncludeId" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.Read[ResourcePath](path, IncludeId),
          recFunc.Hole,
          IncludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          funcE.RightSide)

      val expected: Fix[QSExtra] =
        fixE.InterpretedRead[ResourcePath](
          path,
          List(
            Ids,
            Mask(SMap(CPath.Identity -> Set(ParseType.Object))),
            Pivot(CPath.Identity, IncludeId, ParseType.Object)))

      focusedPushdown(initial) must_= expected
    }

    "when the Read has IdOnly" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.Read[ResourcePath](path, IdOnly),
          recFunc.Hole,
          IncludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          funcE.RightSide)

      val expected: Fix[QSExtra] =
        fixE.InterpretedRead[ResourcePath](
          path,
          List(
            Ids,
            Project(CPath.Identity \ 0),
            Mask(SMap(CPath.Identity -> Set(ParseType.Object))),
            Pivot(CPath.Identity, IncludeId, ParseType.Object)))

      focusedPushdown(initial) must_= expected
    }

    // we will need to allow certain static mapfuncs as structs in the future.
    // 🦕  herbivore rawr!
    "not when the shift struct is not a projection and not Hole" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.Read[ResourcePath](path, ExcludeId),
          recFuncE.ProjectKeyS(
            recFuncE.MakeMapS( // shift source is not a projection
              "i am a key",
              recFuncE.Multiply(recFunc.Hole, recFunc.Hole)),
            "ccc"),
          IncludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

      focusedPushdown(initial) must_= initial
    }

    "not when the non-shifted data is referenced (via LeftSide)" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.Read[ResourcePath](path, ExcludeId),
          recFunc.Hole,
          IncludeId,
          ShiftType.Map,
          OnUndefined.Emit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.LeftSide))) // LeftSide is referenced

      focusedPushdown(initial) must equal(initial)
    }

    "not when the shift struct is a (nonsensical) constant" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.Read[ResourcePath](path, ExcludeId),
          recFuncE.Constant(ejs.str("string!")), // constant string
          IncludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

      focusedPushdown(initial) must equal(initial)
    }

    "consecutive focused shifts" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.LeftShift(
            fixE.Read[ResourcePath](path, ExcludeId),
            recFuncE.ProjectKeyS(
              recFuncE.ProjectKeyS(
                recFuncE.ProjectKeyS(recFunc.Hole,
                  "aaa"),
                "bbb"),
              "ccc"),
            IncludeId,
            ShiftType.Map,
            OnUndefined.Omit,
            funcE.RightSide),
          recFuncE.ProjectKeyS(
            recFuncE.ProjectIndexI(recFuncE.Hole, 1),
            "ddd"),
          ExcludeId,
          ShiftType.Array,
          OnUndefined.Omit,
          funcE.MakeMapS("result", funcE.RightSide))

      val expected: Fix[QSExtra] =
        fixE.InterpretedRead[ResourcePath](
          path,
          List(
            Project(CPath.parse(".aaa.bbb.ccc")),
            Mask(SMap(CPath.Identity -> Set(ParseType.Object))),
            Pivot(CPath.Identity, IncludeId, ParseType.Object),
            Project(CPath.parse(".[1].ddd")),
            Mask(SMap(CPath.Identity -> Set(ParseType.Array))),
            Pivot(CPath.Identity, ExcludeId, ParseType.Array),
            Wrap(CPath.Identity, "result")))

      focusedPushdown(initial) must_= expected
    }
  }

  "project" >> {
    "common prefix from multiple projections" >> {
      val initial: Fix[QSExtra] =
        fixE.Map(
          fixE.Read[ResourcePath](path, ExcludeId),
          recFuncE.Subtract(
            recFuncE.ProjectKeyS(
              recFuncE.ProjectKeyS(
                recFuncE.ProjectIndexI(recFuncE.Hole, 3),
                "order"),
              "tax"),
            recFuncE.ProjectKeyS(
              recFuncE.ProjectKeyS(
                recFuncE.ProjectIndexI(recFuncE.Hole, 3),
                "order"),
              "total")))

      val expected: Fix[QSExtra] =
        fixE.Map(
          fixE.InterpretedRead[ResourcePath](
            path,
            List(
              Project(CPath.parse(".[3].order")),
              Mask(SMap(
                CPath.parse(".tax") -> ParseType.Top,
                CPath.parse(".total") -> ParseType.Top)))),
          recFuncE.Subtract(
            recFuncE.ProjectKeyS(recFuncE.Hole, "tax"),
            recFuncE.ProjectKeyS(recFuncE.Hole, "total")))

      focusedPushdown(initial) must_= expected
    }

    "not when expression has 'outer' semantics" >> {
      val initial: Fix[QSExtra] =
        fixE.Map(
          fixE.Read[ResourcePath](path, ExcludeId),
          recFuncE.StaticMapS(
            "a" -> recFuncE.IfUndefined(
              recFuncE.ProjectKeyS(recFuncE.Hole, "x"),
              recFuncE.Now),
            "b" -> recFuncE.ProjectKeyS(
              recFuncE.ProjectKeyS(recFuncE.Hole, "x"),
              "foo")))

      focusedPushdown(initial) must_= initial
    }
  }

  "mask" >> {
    "multiple projections" >> {
      val initial: Fix[QSExtra] =
        fixE.Map(
          fixE.Read[ResourcePath](path, ExcludeId),
          recFuncE.Subtract(
            recFuncE.ProjectKeyS(
              recFuncE.ProjectKeyS(
                recFuncE.Hole,
                "orderA"),
              "tax"),
            recFuncE.ProjectKeyS(
              recFuncE.ProjectKeyS(
                recFuncE.Hole,
                "orderB"),
              "total")))

      val expected: Fix[QSExtra] =
        fixE.Map(
          fixE.InterpretedRead[ResourcePath](
            path,
            List(Mask(SMap(
              CPath.parse(".orderA.tax") -> ParseType.Top,
              CPath.parse(".orderB.total") -> ParseType.Top)))),
          recFuncE.Subtract(
            recFuncE.ProjectKeyS(
              recFuncE.ProjectKeyS(
                recFuncE.Hole,
                "orderA"),
              "tax"),
            recFuncE.ProjectKeyS(
              recFuncE.ProjectKeyS(
                recFuncE.Hole,
                "orderB"),
              "total")))

      focusedPushdown(initial) must_= expected
    }

    "projection from array, rewriting index" >> {
      val initial: Fix[QSExtra] =
        fixE.Map(
          fixE.Read[ResourcePath](path, ExcludeId),
          recFuncE.Add(
            recFuncE.ProjectKeyS(
              recFuncE.ProjectKeyS(
                recFuncE.Hole,
                "a"),
              "aa"),
            recFuncE.ProjectIndexI(
              recFuncE.ProjectKeyS(
                recFuncE.Hole,
                "b"),
              4)))

      val expected: Fix[QSExtra] =
        fixE.Map(
          fixE.InterpretedRead[ResourcePath](
            path,
            List(Mask(SMap(
              CPath.parse(".a.aa") -> ParseType.Top,
              CPath.parse(".b[4]") -> ParseType.Top)))),
          recFuncE.Add(
            recFuncE.ProjectKeyS(
              recFuncE.ProjectKeyS(
                recFuncE.Hole,
                "a"),
              "aa"),
            recFuncE.ProjectIndexI(
              recFuncE.ProjectKeyS(
                recFuncE.Hole,
                "b"),
              0)))

      focusedPushdown(initial) must_= expected
    }

    "multiple projections from same array, rewriting indicies" >> {
      val initial: Fix[QSExtra] =
        fixE.Map(
          fixE.Read[ResourcePath](path, ExcludeId),
          recFuncE.Add(
            recFuncE.Add(
              recFuncE.ProjectKeyS(
                recFuncE.ProjectIndexI(recFuncE.Hole, 3),
                "a"),
              recFuncE.ProjectIndexI(
                recFuncE.ProjectKeyS(
                  recFuncE.ProjectIndexI(recFuncE.Hole, 9),
                  "b"),
                6)),
            recFuncE.ProjectIndexI(
              recFuncE.ProjectKeyS(
                recFuncE.ProjectIndexI(recFuncE.Hole, 9),
                "b"),
              2)))

      val expected: Fix[QSExtra] =
        fixE.Map(
          fixE.InterpretedRead[ResourcePath](
            path,
            List(Mask(SMap(
              CPath.parse(".[3].a") -> ParseType.Top,
              CPath.parse(".[9].b[6]") -> ParseType.Top,
              CPath.parse(".[9].b[2]") -> ParseType.Top)))),
          recFuncE.Add(
            recFuncE.Add(
              recFuncE.ProjectKeyS(
                recFuncE.ProjectIndexI(recFuncE.Hole, 0),
                "a"),
              recFuncE.ProjectIndexI(
                recFuncE.ProjectKeyS(
                  recFuncE.ProjectIndexI(recFuncE.Hole, 1),
                  "b"),
                1)),
            recFuncE.ProjectIndexI(
              recFuncE.ProjectKeyS(
                recFuncE.ProjectIndexI(recFuncE.Hole, 1),
                "b"),
              0)))

      focusedPushdown(initial) must_= expected
    }

    "not when expression has 'outer' semantics" >> {
      val initial: Fix[QSExtra] =
        fixE.Map(
          fixE.Read[ResourcePath](path, ExcludeId),
          recFuncE.StaticMapS(
            "date" ->
              recFuncE.NowDate,

            "tax" ->
              recFuncE.ProjectKeyS(
                recFuncE.ProjectKeyS(
                  recFuncE.Hole,
                  "orderA"),
                "tax"),

            "total" ->
              recFuncE.ProjectKeyS(
                recFuncE.ProjectKeyS(
                  recFuncE.Hole,
                  "orderB"),
                "total")))

      focusedPushdown(initial) must_= initial
    }
  }
}
