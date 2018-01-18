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

package quasar.qscript.qsu

import slamdata.Predef._
import quasar.{Planner, Qspec, TreeMatchers, Type}, Planner.PlannerError
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.{
  construction,
  ExcludeId,
  IncludeId,
  Hole,
  LeftSide,
  MapFuncsCore,
  OnUndefined,
  ReduceFuncs,
  ReduceIndex,
  RightSide,
  SrcHole
}
import matryoshka._
import matryoshka.data.Fix
import matryoshka.data.free._
import pathy.Path
import Path.Sandboxed

import scalaz.{\/, \/-, EitherT, Equal, Free, IList, Need, StateT}
import scalaz.std.anyVal._
import scalaz.syntax.either._
// import scalaz.syntax.show._

object MinimizeAutoJoinsSpec extends Qspec with TreeMatchers with QSUTTypes[Fix] {
  import QSUGraph.Extractors._
  import ApplyProvenance.AuthenticatedQSU
  import QScriptUniform.{DTrans, Retain, Rotation}

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]

  val qsu = QScriptUniform.DslT[Fix]
  val func = construction.Func[Fix]
  val qprov = QProv[Fix]

  type J = Fix[EJson]
  val J = Fixed[Fix[EJson]]

  val afile = Path.rootDir[Sandboxed] </> Path.file("afile")
  val afile2 = Path.rootDir[Sandboxed] </> Path.file("afile2")

  implicit val eqP: Equal[qprov.P] =
    qprov.prov.provenanceEqual(Equal[qprov.D], Equal[QIdAccess])

  "autojoin minimization" should {
    "linearize .foo + .bar" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.map(
            qsu.read(afile),
            func.ProjectKeyS(func.Hole, "foo")),
          qsu.map(
            qsu.read(afile),
            func.ProjectKeyS(func.Hole, "bar")),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(Read(_), fm) =>
          // must_=== doesn't work
          fm must beTreeEqual(
            func.Add(
              func.ProjectKeyS(func.Hole, "foo"),
              func.ProjectKeyS(func.Hole, "bar")))
      }
    }

    "convert Typecheck to a Map(_, Guard)" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin3((
          qsu.read(afile),
          qsu.read(afile),
          qsu.undefined(),
          _(MapFuncsCore.Guard(_, Type.AnyObject, _, _)))))

      runOn(qgraph) must beLike {
        case Map(Read(_), fm) =>
          // must_=== doesn't work
          fm must beTreeEqual(func.Guard(func.Hole, Type.AnyObject, func.Hole, func.Undefined))
      }
    }

    "coalesce two summed reductions" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Count(func.Hole)),
            func.ReduceIndex(\/-(0))),
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Sum(func.Hole)),
            func.ReduceIndex(\/-(0))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            Read(_),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repair),
          fm) =>

          // must_=== doesn't work
          h1 must beTreeEqual(func.Hole)
          h2 must beTreeEqual(func.Hole)

          repair must beTreeEqual(
            func.StaticMapS(
              "0" ->
                func.ReduceIndex(\/-(0)),
              "1" ->
                func.ReduceIndex(\/-(1))))

          fm must beTreeEqual(
            func.Add(
              func.ProjectKeyS(func.Hole, "0"),
              func.ProjectKeyS(func.Hole, "1")))
      }
    }

    "coalesce three summed reductions" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.autojoin2((
            qsu.qsReduce(
              qsu.read(afile),
              Nil,
              List(ReduceFuncs.Count(func.Hole)),
              func.ReduceIndex(\/-(0))),
            qsu.qsReduce(
              qsu.read(afile),
              Nil,
              List(ReduceFuncs.Sum(func.Hole)),
              func.ReduceIndex(\/-(0))),
            _(MapFuncsCore.Add(_, _)))),
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Max(func.Hole)),
            func.ReduceIndex(\/-(0))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            Read(_),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2), ReduceFuncs.Max(h3)),
            repair),
          fm) =>

          // must_=== doesn't work
          h1 must beTreeEqual(func.Hole)
          h2 must beTreeEqual(func.Hole)
          h3 must beTreeEqual(func.Hole)

          repair must beTreeEqual(
            func.StaticMapS(
              "0" ->
                func.StaticMapS(
                  "0" ->
                    func.ReduceIndex(\/-(0)),
                  "1" ->
                    func.ReduceIndex(\/-(1))),
              "1" ->
                func.ReduceIndex(\/-(2))))

          fm must beTreeEqual(
            func.Add(
              func.Add(
                func.ProjectKeyS(
                  func.ProjectKeyS(func.Hole, "0"),
                  "0"),
                func.ProjectKeyS(
                  func.ProjectKeyS(func.Hole, "0"),
                  "1")),
              func.ProjectKeyS(func.Hole, "1")))
      }
    }

    "coalesce two summed reductions, one downstream of an autojoin" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.autojoin2((
              qsu.read(afile),
              qsu.map1((
                qsu.unreferenced(),
                MapFuncsCore.Constant[Fix, Hole](J.str("hey")))),
              _(MapFuncsCore.ConcatArrays(_, _)))),
            Nil,
            List(ReduceFuncs.Count(func.Hole)),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Sum(func.Hole)),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            Read(_),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repair),
          fm) =>

          // must_=== doesn't work
          h1 must beTreeEqual(
            func.ConcatArrays(
              func.Hole,
              func.Constant(J.str("hey"))))

          h2 must beTreeEqual(func.Hole)

          repair must beTreeEqual(
            func.StaticMapS(
              "0" ->
                func.ReduceIndex(\/-(0)),
              "1" ->
                func.ReduceIndex(\/-(1))))

          fm must beTreeEqual(
            func.Add(
              func.ProjectKeyS(func.Hole, "0"),
              func.ProjectKeyS(func.Hole, "1")))
      }
    }

    "rewrite filter into cond only to avoid join" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.read(afile),
          qsu.qsFilter(
            qsu.read(afile),
            func.Eq(func.Hole, func.Constant(J.str("foo")))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(Read(_), fm) =>
          fm must beTreeEqual(
            func.Add(
              func.Hole,
              func.Cond(
                func.Eq(func.Hole, func.Constant(J.str("foo"))),
                func.Hole,
                func.Undefined)))
      }
    }

    "not rewrite filter acting as upstream source" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsFilter(
            qsu.read(afile),
            func.Eq(func.Hole, func.Constant(J.str("foo")))),
          qsu.cint(42),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(QSFilter(_, _), fm) =>
          fm must beTreeEqual(
            func.Add(func.Hole, func.Constant(J.int(42))))
      }
    }

    "coalesce two summed bucketing reductions, inlining functions into the buckets" in {
      val readAndThings =
        qsu.map1((
          qsu.read(afile),
          MapFuncsCore.Negate(SrcHole)))

      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            readAndThings,
            List(func.AccessHole),
            List(ReduceFuncs.Count(func.Hole)),
            func.ReduceIndex(\/-(0))),
          qsu.qsReduce(
            readAndThings,
            List(func.AccessHole),
            List(ReduceFuncs.Sum(func.Hole)),
            func.ReduceIndex(\/-(0))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            Read(_),
            List(bucket),
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repair),
          fm) =>

          // must_=== doesn't work
          bucket must beTreeEqual(func.Negate(func.AccessHole))

          h1 must beTreeEqual(func.Negate(func.Hole))
          h2 must beTreeEqual(func.Negate(func.Hole))

          repair must beTreeEqual(
            func.StaticMapS(
              "0" ->
                func.ReduceIndex(\/-(0)),
              "1" ->
                func.ReduceIndex(\/-(1))))

          fm must beTreeEqual(
            func.Add(
              func.ProjectKeyS(func.Hole, "0"),
              func.ProjectKeyS(func.Hole, "1")))
      }
    }

    "remap coalesced bucket references in dimensions" in {
      val readAndThings =
        qsu.map((
          qsu.dimEdit((
            qsu.read(afile),
            DTrans.Group(func.ProjectKeyS(func.Hole, "label")))),
          func.Negate(func.ProjectKeyS(func.Hole, "metric"))))

      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2(((
          qsu.lpReduce((
            readAndThings,
            ReduceFuncs.Count(()))),
          qsu.lpReduce((
            readAndThings,
            ReduceFuncs.Sum(()))),
          _(MapFuncsCore.Add(_, _))))))

      val AuthenticatedQSU(agraph, auth) = runOn_(qgraph)

      agraph must beLike {
        case m @ Map(r @ QSReduce(_, _, _, _), _) =>
          val expDims =
            IList(qprov.prov.value(IdAccess.bucket(r.root, 0)))

          auth.dims(m.root) must_= expDims
      }
    }

    "leave uncoalesced reductions of different bucketing" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Sum(func.Hole)),
            func.ReduceIndex(\/-(0))),
          qsu.qsReduce(
            qsu.read(afile),
            List(func.ProjectKeyS(func.AccessHole, "state")),
            List(ReduceFuncs.Sum(func.Hole)),
            func.ReduceIndex(\/-(0))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case AutoJoin2C(
          QSReduce(Read(_), Nil, _, _),
          QSReduce(Read(_), List(_), _, _),
          MapFuncsCore.Add(LeftSide, RightSide)) => ok
      }
    }

    "minimize an autojoin after prior source failure" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.autojoin2((
            qsu.read(afile),
            qsu.read(afile2),
            _(MapFuncsCore.Subtract(_, _)))),
          qsu.cint(42),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          AutoJoin2C(
            Read(`afile`),
            Read(`afile2`),
            MapFuncsCore.Subtract(LeftSide, RightSide)),
          fm) =>

          fm must beTreeEqual(func.Add(func.Hole, func.Constant(J.int(42))))
      }
    }

    "halt minimization at a grouped vertex" in {
      val groupKey =
        func.Lower(func.ProjectKeyS(func.Hole, "city"))

      val groupedGuardedRead =
        qsu.dimEdit(
          qsu.autojoin3((
            qsu.read(afile),
            qsu.read(afile),
            qsu.undefined(),
            _(MapFuncsCore.Guard(_, Type.AnyObject, _, _)))),
          DTrans.Group(groupKey))

      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.autojoin2((
            qsu.cstr("city"),
            qsu.autojoin2((
              groupedGuardedRead,
              qsu.cstr("city"),
              _(MapFuncsCore.ProjectKey(_, _)))),
            _(MapFuncsCore.MakeMap(_, _)))),
          qsu.autojoin2((
            qsu.cstr("1"),
            qsu.lpReduce(
              qsu.autojoin2((
                groupedGuardedRead,
                qsu.cstr("pop"),
                _(MapFuncsCore.ProjectKey(_, _)))),
              ReduceFuncs.Sum(())),
            _(MapFuncsCore.MakeMap(_, _)))),
          _(MapFuncsCore.ConcatMaps(_, _)))))

      runOn(qgraph) must beLike {
        case AutoJoin2C(
          Map(
            Map(Read(_), guardL),
            minL),
          Map(
            QSReduce(
              Map(Read(_), guardR),
              bucket :: Nil,
              ReduceFuncs.Sum(prjPop) :: Nil,
              _),
            minR),
          MapFuncsCore.ConcatMaps(_, _)) =>

          guardL must beTreeEqual(guardR)

          minL must beTreeEqual(
            func.MakeMapS(
              "city",
              func.ProjectKeyS(func.Hole, "city")))

          minR must beTreeEqual(func.MakeMapS("1", func.Hole))

          prjPop must beTreeEqual(func.ProjectKeyS(func.Hole, "pop"))
      }
    }

    "coalesce an autojoin on a single leftshift on a shared source" in {
      val shiftedRead =
        qsu.leftShift(qsu.read(afile), func.Hole, ExcludeId, OnUndefined.Omit, func.RightTarget, Rotation.ShiftMap)

      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.leftShift(
            shiftedRead,
            func.Hole,
            ExcludeId,
            OnUndefined.Omit,
            func.RightTarget,
            Rotation.ShiftArray),
          shiftedRead,
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              LeftShift(Read(`afile`), _, _, _, _, _),
              struct,
              ExcludeId,
              _,
              repair,
              _),
            fm) =>

          struct must beTreeEqual(func.Hole)

          repair must beTreeEqual(
            func.StaticMapS(
              "0" -> func.RightTarget,
              "1" -> func.AccessLeftTarget(Access.valueHole(_))))

          fm must beTreeEqual(
            func.Add(
              func.ProjectKeyS(func.Hole, "0"),
              func.ProjectKeyS(func.Hole, "1")))
      }
    }

    "coalesce an autojoin on a single leftshift on a shared source (RTL)" in {
      val shiftedRead =
        qsu.leftShift(qsu.read(afile), func.Hole, ExcludeId, OnUndefined.Omit, func.RightTarget, Rotation.ShiftMap)

      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          shiftedRead,
          qsu.leftShift(
            shiftedRead,
            func.Hole,
            ExcludeId,
            OnUndefined.Omit,
            func.RightTarget,
            Rotation.ShiftArray),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              LeftShift(Read(`afile`), _, _, _, _, _),
              struct,
              ExcludeId,
              OnUndefined.Emit,
              repair,
              _),
            fm) =>

          struct must beTreeEqual(func.Hole)

          repair must beTreeEqual(
            func.StaticMapS(
              "1" -> func.RightTarget,
              "0" -> func.AccessLeftTarget(Access.valueHole(_))))

          fm must beTreeEqual(
            func.Add(
              func.ProjectKeyS(func.Hole, "0"),
              func.ProjectKeyS(func.Hole, "1")))
      }
    }

    "inductively coalesce reduces on coalesced shifts" in {
      val shiftedRead =
        qsu.leftShift(qsu.read(afile), func.Hole, ExcludeId, OnUndefined.Omit, func.RightTarget, Rotation.ShiftMap)

      // count(a[*]) + sum(a)
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.leftShift(
              shiftedRead,
              func.Hole,
              ExcludeId,
              OnUndefined.Omit,
              func.RightTarget,
              Rotation.ShiftArray),
            Nil,
            List(ReduceFuncs.Count(func.Hole)),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          qsu.qsReduce(
            shiftedRead,
            Nil,
            List(ReduceFuncs.Sum(func.Hole)),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            LeftShift(
              LeftShift(Read(`afile`), _, _, _, _, _),
              struct,
              ExcludeId,
              OnUndefined.Emit,
              repairInner,
              _),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repairOuter),
          fm) =>

        struct must beTreeEqual(func.Hole)

        repairInner must beTreeEqual(
          func.StaticMapS(
            "0" -> func.RightTarget,
            "1" -> func.AccessLeftTarget(Access.valueHole(_))))

        h1 must beTreeEqual(func.ProjectKeyS(func.Hole, "0"))
        h2 must beTreeEqual(func.ProjectKeyS(func.Hole, "1"))

        repairOuter must beTreeEqual(
          func.StaticMapS(
            "0" -> func.ReduceIndex(\/-(0)),
            "1" -> func.ReduceIndex(\/-(1))))

        fm must beTreeEqual(
          func.Add(
            func.ProjectKeyS(func.Hole, "0"),
            func.ProjectKeyS(func.Hole, "1")))
      }
    }

    "inductively coalesce reduces on coalesced shifts" in {
      val shiftedRead =
        qsu.leftShift(qsu.read(afile), func.Hole, ExcludeId, OnUndefined.Omit, func.RightTarget, Rotation.ShiftMap)

      // count(a[*]) + sum(a)
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.leftShift(
              shiftedRead,
              func.Hole,
              ExcludeId,
              OnUndefined.Omit,
              func.RightTarget,
              Rotation.ShiftArray),
            Nil,
            List(ReduceFuncs.Count(func.Hole)),
            func.ReduceIndex(\/-(0))),
          qsu.qsReduce(
            shiftedRead,
            Nil,
            List(ReduceFuncs.Sum(func.Hole)),
            func.ReduceIndex(\/-(0))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            LeftShift(
              LeftShift(Read(`afile`), _, _, _, _, _),
              struct,
              ExcludeId,
              OnUndefined.Emit,
              repairInner,
              _),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repairOuter),
          fm) =>

        struct must beTreeEqual(func.Hole)

        repairInner must beTreeEqual(
          func.StaticMapS(
            "0" -> func.RightTarget,
            "1" -> func.AccessLeftTarget(Access.valueHole(_))))

        h1 must beTreeEqual(func.ProjectKeyS(func.Hole, "0"))
        h2 must beTreeEqual(func.ProjectKeyS(func.Hole, "1"))

        repairOuter must beTreeEqual(
          func.StaticMapS(
            "0" -> func.ReduceIndex(\/-(0)),
            "1" -> func.ReduceIndex(\/-(1))))

        fm must beTreeEqual(
          func.Add(
            func.ProjectKeyS(func.Hole, "0"),
            func.ProjectKeyS(func.Hole, "1")))
      }
    }

    "coalesce an autojoin on two leftshifts on a shared source" in {
      val shiftedRead =
        qsu.leftShift(qsu.read(afile), func.Hole, ExcludeId, OnUndefined.Omit, func.RightTarget, Rotation.ShiftMap)

      // a[*][*] + a
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.leftShift(
            qsu.leftShift(
              shiftedRead,
              func.Hole,
              IncludeId,
              OnUndefined.Omit,
              func.RightTarget,
              Rotation.ShiftArray),
            func.Hole,
            ExcludeId,
            OnUndefined.Omit,
            func.RightTarget,
            Rotation.ShiftArray),
          shiftedRead,
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              LeftShift(
                LeftShift(Read(`afile`), _, _, _, _, _),
                structInner,
                IncludeId,
                OnUndefined.Emit,
                repairInner,
                _),
              structOuter,
              ExcludeId,
              OnUndefined.Emit,
              repairOuter,
              _),
            fm) =>

          structInner must beTreeEqual(func.Hole)

          repairInner must beTreeEqual(
            func.StaticMapS(
              "original" -> func.AccessLeftTarget(Access.valueHole(_)),
              "results" -> func.RightTarget))

          structOuter must beTreeEqual(
            func.ProjectKeyS(func.Hole, "results"))

          repairOuter must beTreeEqual(
            func.StaticMapS(
              "0" ->
                func.RightTarget,
              "1" ->
                func.ProjectKeyS(
                  func.AccessLeftTarget(Access.valueHole(_)),
                  "original")))

          fm must beTreeEqual(
            func.Add(
              func.ProjectKeyS(func.Hole, "0"),
              func.ProjectKeyS(func.Hole, "1")))
      }
    }

    "coalesce an autojoin on three leftshifts on a shared source" in {
      val shiftedRead =
        qsu.leftShift(qsu.read(afile), func.Hole, ExcludeId, OnUndefined.Omit, func.RightTarget, Rotation.ShiftMap)

      // a[*][*][*] + a
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.leftShift(
            qsu.leftShift(
              qsu.leftShift(
                shiftedRead,
                func.Hole,
                IncludeId,
                OnUndefined.Omit,
                func.RightTarget,
                Rotation.ShiftArray),
              func.Hole,
              ExcludeId,
              OnUndefined.Omit,
              func.RightTarget,
              Rotation.ShiftMap),
            func.Hole,
            ExcludeId,
            OnUndefined.Omit,
            func.RightTarget,
            Rotation.ShiftArray),
          shiftedRead,
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              LeftShift(
                LeftShift(
                  LeftShift(Read(`afile`), _, _, _, _, _),
                  structInnerInner,
                  IncludeId,
                  OnUndefined.Emit,
                  repairInnerInner,
                  Rotation.ShiftArray),
                structInner,
                ExcludeId,
                OnUndefined.Emit,
                repairInner,
                Rotation.ShiftMap),
              structOuter,
              ExcludeId,
              OnUndefined.Emit,
              repairOuter,
              Rotation.ShiftArray),
            fm) =>

          structInnerInner must beTreeEqual(func.Hole)

          repairInnerInner must beTreeEqual(
            func.StaticMapS(
              "original" -> func.AccessLeftTarget(Access.valueHole(_)),
              "results" -> func.RightTarget))

          structInner must beTreeEqual(func.ProjectKeyS(func.Hole, "results"))

          repairInner must beTreeEqual(
            func.StaticMapS(
              "original" ->
                func.ProjectKeyS(func.AccessLeftTarget(Access.valueHole(_)), "original"),
              "results" -> func.RightTarget))

          structOuter must beTreeEqual(func.ProjectKeyS(func.Hole, "results"))

          repairOuter must beTreeEqual(
            func.StaticMapS(
              "0" ->
                func.RightTarget,
              "1" ->
                func.ProjectKeyS(
                  func.AccessLeftTarget(Access.valueHole(_)),
                  "original")))

          fm must beTreeEqual(
            func.Add(
              func.ProjectKeyS(func.Hole, "0"),
              func.ProjectKeyS(func.Hole, "1")))
      }
    }

    // c[*] / d[*][*]
    "coalesce uneven shifts" in {
      val shiftedRead =
        qsu.leftShift(qsu.read(afile), func.Hole, ExcludeId, OnUndefined.Omit, func.RightTarget, Rotation.ShiftMap)

      val cdivd =
        qsu.autojoin2((
          qsu.leftShift(
            shiftedRead,
            func.ProjectKeyS(func.Hole, "c"),
            ExcludeId,
            OnUndefined.Omit,
            func.RightTarget,
            Rotation.ShiftArray),
          qsu.leftShift(
            qsu.leftShift(
              shiftedRead,
              func.ProjectKeyS(func.Hole, "d"),
              ExcludeId,
              OnUndefined.Omit,
              func.RightTarget,
              Rotation.ShiftArray),
            func.Hole,
            ExcludeId,
            OnUndefined.Omit,
            func.RightTarget,
            Rotation.ShiftArray),
          _(MapFuncsCore.Divide(_, _))))

      val qgraph = QSUGraph.fromTree[Fix](cdivd)

      runOn(qgraph) must beLike {
        case
          Map(
            Map(
              LeftShift(
                MultiLeftShift(
                  LeftShift(Read(_), _, _, _, _, _),
                  List(
                    (cstruct, _, _),
                    (dstruct, _, _)),
                  OnUndefined.Emit,
                  innerRepair),
                outerStruct,
                _,
                OnUndefined.Emit,
                outerRepair,
                _),
              innerFM),
            fm) =>

          cstruct must beTreeEqual(func.ProjectKeyS(func.Hole, "c"))
          dstruct must beTreeEqual(func.ProjectKeyS(func.Hole, "d"))

          innerRepair must beTreeEqual(
            func.StaticMapS(
              "left" ->
                func.MakeMapS("0", Free.pure[MapFunc, QAccess[Hole] \/ Int](0.right)),
              "right" ->
                Free.pure[MapFunc, QAccess[Hole] \/ Int](1.right)))

          outerStruct must beTreeEqual(
            func.ProjectKeyS(func.Hole, "right"))

          outerRepair must beTreeEqual(
            func.StaticMapS(
              "left" ->
                func.ProjectKeyS(func.AccessLeftTarget(Access.value(_)), "left"),
              "right" ->
                func.MakeMapS("1", func.RightTarget)))

          innerFM must beTreeEqual(
            func.StaticMapS(
              "0" ->
                func.ProjectKeyS(
                  func.ProjectKeyS(func.Hole, "left"),
                  "0"),
              "1" ->
                func.ProjectKeyS(
                  func.ProjectKeyS(func.Hole, "right"),
                  "1")))

          fm must beTreeEqual(
            func.Divide(
              func.ProjectKeyS(func.Hole, "0"),
              func.ProjectKeyS(func.Hole, "1")))
      }
    }

    // a[*][*][*] + b - c[*] / d[*][*]
    "coalesce a thing that looks a lot like the search card" in {
      val shiftedRead =
        qsu.leftShift(qsu.read(afile), func.Hole, ExcludeId, OnUndefined.Omit, func.RightTarget, Rotation.ShiftMap)

      // a[*][*][*] + b
      val aplusb =
        qsu.autojoin2((
          qsu.leftShift(
            qsu.leftShift(
              qsu.leftShift(
                shiftedRead,
                func.ProjectKeyS(func.Hole, "a"),
                ExcludeId,
                OnUndefined.Omit,
                func.RightTarget,
                Rotation.ShiftArray),
              func.Hole,
              ExcludeId,
              OnUndefined.Omit,
              func.RightTarget,
              Rotation.ShiftArray),
            func.Hole,
            ExcludeId,
            OnUndefined.Omit,
            func.RightTarget,
            Rotation.ShiftArray),
          qsu.map((shiftedRead, func.ProjectKeyS(func.Hole, "b"))),
          _(MapFuncsCore.Add(_, _))))

      // c[*] / d[*][*]
      val cdivd =
        qsu.autojoin2((
          qsu.leftShift(
            shiftedRead,
            func.ProjectKeyS(func.Hole, "c"),
            ExcludeId,
            OnUndefined.Omit,
            func.RightTarget,
            Rotation.ShiftArray),
          qsu.leftShift(
            qsu.leftShift(
              shiftedRead,
              func.ProjectKeyS(func.Hole, "d"),
              ExcludeId,
              OnUndefined.Omit,
              func.RightTarget,
              Rotation.ShiftArray),
            func.Hole,
            ExcludeId,
            OnUndefined.Omit,
            func.RightTarget,
            Rotation.ShiftArray),
          _(MapFuncsCore.Divide(_, _))))

      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((aplusb, cdivd, _(MapFuncsCore.Subtract(_, _)))))

      runOn(qgraph) must beLike {
        case
          Map(
            Map(
              LeftShift(
                MultiLeftShift(
                  MultiLeftShift(
                    LeftShift(Read(`afile`), _, _, _, _, _),
                    List(
                      (innerastruct, _, _),
                      (innercstruct, _, _),
                      (innerdstruct, _, _)),
                    OnUndefined.Emit,
                    innerMultiRepair),
                  List(
                    (outerastruct, _, _),
                    (outerdstruct, _, _)),
                  OnUndefined.Emit,
                  outerMultiRepair),
                singleStruct,
                _,
                OnUndefined.Emit,
                singleRepair,
                _),
              innerFM),
            fm) =>

        innerastruct must beTreeEqual(func.ProjectKeyS(func.Hole, "a"))
        innercstruct must beTreeEqual(func.ProjectKeyS(func.Hole, "c"))
        innerdstruct must beTreeEqual(func.ProjectKeyS(func.Hole, "d"))

        innerMultiRepair must beTreeEqual(
          func.StaticMapS(
            "left" ->
              func.StaticMapS(
                "original" ->
                  func.AccessHole.map(_.left[Int]),
                "results" ->
                  Free.pure[MapFunc, QAccess[Hole] \/ Int](0.right)),
            "right" ->
              func.StaticMapS(
                "left" ->
                  func.MakeMapS("0", Free.pure(1.right[QAccess[Hole]])),
                "right" ->
                  Free.pure[MapFunc, QAccess[Hole] \/ Int](2.right))))

        outerastruct must beTreeEqual(
          func.ProjectKeyS(
            func.ProjectKeyS(func.Hole, "left"),
            "results"))

        outerdstruct must beTreeEqual(
          func.ProjectKeyS(func.ProjectKeyS(func.Hole, "right"), "right"))

        outerMultiRepair must beTreeEqual(
          func.StaticMapS(
            "left" ->
              func.StaticMapS(
                "original" ->
                  func.ProjectKeyS(
                    func.ProjectKeyS(func.AccessHole.map(_.left[Int]), "left"),
                    "original"),
                "results" ->
                  Free.pure[MapFunc, QAccess[Hole] \/ Int](0.right)),
            "right" ->
              func.MakeMapS(
                "1",
                func.StaticMapS(
                  "left" ->
                    func.ProjectKeyS(
                      func.ProjectKeyS(func.AccessHole.map(_.left[Int]), "right"),
                      "left"),
                  "right" ->
                    func.MakeMapS("1", Free.pure[MapFunc, QAccess[Hole] \/ Int](1.right))))))

        singleStruct must beTreeEqual(
          func.ProjectKeyS(func.ProjectKeyS(func.Hole, "left"), "results"))

        singleRepair must beTreeEqual(
          func.StaticMapS(
            ("left",
              func.MakeMapS(
                "0",
                func.StaticMapS(
                  "0" -> func.RightTarget,
                  "1" ->
                    func.ProjectKeyS(
                      func.ProjectKeyS(
                        func.AccessLeftTarget(Access.value(_)),
                        "left"),
                      "original")))),
            "right" ->
              func.ProjectKeyS(func.AccessLeftTarget(Access.value(_)), "right")))

        innerFM must beTreeEqual(
          func.StaticMapS(
            "0" ->
              func.ProjectKeyS(
                func.ProjectKeyS(func.Hole, "left"),
                "0"),
            "1" ->
              func.ProjectKeyS(
                func.ProjectKeyS(func.Hole, "right"),
                "1")))

        fm must beTreeEqual(
          func.Subtract(
            func.Add(
              func.ProjectKeyS(func.ProjectKeyS(func.Hole, "0"), "0"),
              func.ProjectKeyS(
                func.ProjectKeyS(func.ProjectKeyS(func.Hole, "0"), "1"),
                "b")),
            func.Divide(
              func.ProjectKeyS(
                func.StaticMapS(
                  "0" ->
                    func.ProjectKeyS(
                      func.ProjectKeyS(
                        func.ProjectKeyS(func.Hole, "1"),
                        "left"),
                      "0"),
                  "1" ->
                    func.ProjectKeyS(
                      func.ProjectKeyS(
                        func.ProjectKeyS(func.Hole, "1"),
                        "right"),
                      "1")),
                "0"),
              func.ProjectKeyS(
                func.StaticMapS(
                  "0" ->
                    func.ProjectKeyS(
                      func.ProjectKeyS(
                        func.ProjectKeyS(func.Hole, "1"),
                        "left"),
                      "0"),
                  "1" ->
                    func.ProjectKeyS(
                      func.ProjectKeyS(
                        func.ProjectKeyS(func.Hole, "1"),
                        "right"),
                      "1")),
                "1"))))
      }
    }

    // [a, b[*][*]]]
    "coalesce with proper struct a contextual shift autojoin" in {
      val shiftedRead =
        qsu.leftShift(qsu.read(afile), func.Hole, ExcludeId, OnUndefined.Omit, func.RightTarget, Rotation.ShiftMap)

      val qgraph =
        QSUGraph.fromTree[Fix](
          qsu.autojoin2((
            qsu.map(
              shiftedRead,
              func.MakeArray(
                func.Guard(
                  func.Hole,
                  Type.AnyObject,
                  func.ProjectKeyS(func.Hole, "a"),
                  func.Undefined))),
            qsu.map(
              qsu.leftShift(
                qsu.leftShift(
                  qsu.map(
                    shiftedRead,
                    func.Guard(
                      func.Hole,
                      Type.AnyObject,
                      func.ProjectKeyS(func.Hole, "b"),
                      func.Undefined)),
                  func.Hole,
                  ExcludeId,
                  OnUndefined.Omit,
                  func.RightTarget,
                  Rotation.ShiftArray),
                func.Hole,
                ExcludeId,
                OnUndefined.Omit,
                func.RightTarget,
                Rotation.ShiftArray),
              func.MakeArray(func.Hole)),
            _(MapFuncsCore.ConcatArrays(_, _)))))

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              LeftShift(
                LeftShift(Read(_), _, _, _, _, _),
                innerStruct,
                _,
                OnUndefined.Emit,
                innerRepair,
                _),
              outerStruct,
              _,
              OnUndefined.Emit,
              outerRepair,
              _),
            fm) =>

          innerStruct must beTreeEqual(
            func.Guard(
              func.Hole,
              Type.AnyObject,
              func.ProjectKeyS(func.Hole, "b"),
              func.Undefined))

          innerRepair must beTreeEqual(
            func.StaticMapS(
              "original" ->
                func.AccessLeftTarget(Access.value(_)),
              "results" ->
                func.RightTarget))

          outerStruct must beTreeEqual(func.ProjectKeyS(func.Hole, "results"))

          outerRepair must beTreeEqual(
            func.StaticMapS(
              "1" ->
                func.RightTarget,
              "0" ->
                func.ProjectKeyS(
                  func.AccessLeftTarget(Access.value(_)),
                  "original")))

          fm must beTreeEqual(
            func.ConcatArrays(
              func.MakeArray(
                func.Guard(
                  func.ProjectKeyS(func.Hole, "0"),
                  Type.AnyObject,
                  func.ProjectKeyS(
                    func.ProjectKeyS(func.Hole, "0"),
                    "a"),
                  func.Undefined)),
              func.MakeArray(func.ProjectKeyS(func.Hole, "1"))))
      }
    }

    // select [a, foo[*][*]] from afile
    "properly coalesce a more realistic double-shift example" in {
      val qsu0 = qsu.read(afile)
      val qsu2 = qsu.unreferenced()

      val qsu3 = qsu.map(qsu2, func.Undefined[Hole])

      val qsu1 =
        qsu.leftShift(
          qsu0,
          func.Hole,
          ExcludeId,
          OnUndefined.Omit,
          func.RightTarget,
          Rotation.ShiftMap)

      val qsu9 =
        qsu.map(qsu1, func.ProjectKeyS(func.Hole, "foo"))

      val qsu11 =
        qsu.leftShift(
          qsu9,
          func.Hole,
          ExcludeId,
          OnUndefined.Omit,
          func.RightTarget,
          Rotation.FlattenArray)

      // this is the problematic node here
      // it forces the inductive minimization, since the
      // shifts are not directly consecutive, only merged by
      // a mappable region
      val qsu12 =
        qsu.autojoin3((
          qsu11,
          qsu11,
          qsu3,
          _(MapFuncsCore.Guard(_, Type.AnyArray, _, _))))

      val qsu6 =
        qsu.map(qsu1, func.ProjectKeyS(func.Hole, "a"))

      val qsu13 =
        qsu.leftShift(
          qsu12,
          func.Hole,
          ExcludeId,
          OnUndefined.Omit,
          func.RightTarget,
          Rotation.FlattenArray)

      val qsu7 =
        qsu.map(qsu6, func.MakeArray(func.Hole))

      val qsu14 =
        qsu.map(qsu13, func.MakeArray(func.Hole))

      val qsu16 =
        qsu.autojoin2((
          qsu7,
          qsu14,
          _(MapFuncsCore.ConcatArrays(_, _))))

      val qgraph = QSUGraph.fromTree[Fix](qsu16)

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              LeftShift(
                LeftShift(Read(_), _, _, _, _, _),
                innerStruct,
                _,
                OnUndefined.Emit,
                innerRepair,
                _),
              outerStruct,
              _,
              OnUndefined.Emit,
              outerRepair,
              _),
            fm) => ok
      }
    }

    // a + b[*].c[*]
    "coalesce uneven shifts with an intervening map" in {
      val shiftedRead =
        qsu.leftShift(qsu.read(afile), func.Hole, ExcludeId, OnUndefined.Omit, func.RightTarget, Rotation.ShiftMap)

      val qgraph =
        QSUGraph.fromTree[Fix](
          qsu.autojoin2((
            qsu.map(shiftedRead, func.ProjectKeyS(func.Hole, "a")),
            qsu.leftShift(
              qsu.map(
                qsu.leftShift(
                  qsu.map(shiftedRead, func.ProjectKeyS(func.Hole, "b")),
                  func.Hole,
                  ExcludeId,
                  OnUndefined.Omit,
                  func.RightTarget,
                  Rotation.FlattenArray),
                func.ProjectKeyS(func.Hole, "c")),
              func.Hole,
              ExcludeId,
              OnUndefined.Omit,
              func.RightTarget,
              Rotation.FlattenArray),
            _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              LeftShift(
                LeftShift(Read(_), _, _, _, _, _),
                innerStruct,
                _,
                _,
                innerRepair,
                _),
              outerStruct,
              _,
              _,
              outerRepair,
              _),
            fm) =>

          innerStruct must beTreeEqual(func.ProjectKeyS(func.Hole, "b"))

          innerRepair must beTreeEqual(
            func.StaticMapS(
              "1" ->
                func.RightTarget,
              "0" ->
                func.AccessLeftTarget(Access.valueHole(_))))

          outerStruct must beTreeEqual(
            func.ProjectKeyS(func.ProjectKeyS(func.Hole, "1"), "c"))

          outerRepair must beTreeEqual(
            func.StaticMapS(
              "1" ->
                func.RightTarget,
              "0" ->
                func.ProjectKeyS(func.AccessLeftTarget(Access.valueHole(_)), "0")))

          fm must beTreeEqual(
            func.Add(
              func.ProjectKeyS(
                func.ProjectKeyS(func.Hole, "0"),
                "a"),
              func.ProjectKeyS(func.Hole, "1")))
      }
    }

    // b[*] or b[*][*]
    "correctly coalesce uneven shifts of the same source" in {
      val shiftedRead =
        qsu.tread(afile)

      val guardedRead =
        qsu.autojoin3((
          shiftedRead,
          shiftedRead,
          qsu.undefined(),
          _(MapFuncsCore.Guard(_, Type.AnyObject, _, _))))

      val projectB =
        qsu.autojoin2((
          guardedRead,
          qsu.cstr("b"),
          _(MapFuncsCore.ProjectKey(_, _))))

      val innerShift =
        qsu.transpose(
          qsu.autojoin3((
            projectB,
            projectB,
            qsu.undefined(),
            _(MapFuncsCore.Guard(_, Type.FlexArr(0, None, Type.FlexArr(0, None, Type.Bool)), _, _)))),
          Retain.Values,
          Rotation.FlattenArray)

      val qgraph =
        QSUGraph.fromTree[Fix](
          qsu.map(
            qsu.qsFilter(
              qsu._autojoin2((
                guardedRead,
                qsu.autojoin2((
                  qsu.transpose(
                    qsu.autojoin3((
                      projectB,
                      projectB,
                      qsu.undefined(),
                      _(MapFuncsCore.Guard(_, Type.FlexArr(0, None, Type.Bool), _, _)))),
                    Retain.Values,
                    Rotation.FlattenArray),
                  qsu.transpose(
                    qsu.autojoin3((
                      innerShift,
                      innerShift,
                      qsu.undefined(),
                      _(MapFuncsCore.Guard(_, Type.FlexArr(0, None, Type.Bool), _, _)))),
                    Retain.Values,
                    Rotation.FlattenArray),
                  _(MapFuncsCore.Or(_, _)))),
                func.StaticMapS(
                  "filter_source" -> func.LeftSide,
                  "filter_predicate" -> func.RightSide))),
              func.ProjectKeyS(func.Hole, "filter_predicate")),
            func.ProjectKeyS(func.Hole, "filter_source")))

      val leftShiftCount =
        runOn(qgraph).foldMapUp {
          case LeftShift(_, _, _, _, _) => 1
          case MultiLeftShift(_, ss, _) => ss.length
          case _                        => 0
        }

      // TODO: Should really be 3, but another bug is duplicating the inner
      //       shift common to both sides of the `Or`
      leftShiftCount must_= 4
    }
  }

  def runOn(qgraph: QSUGraph): QSUGraph =
    runOn_(qgraph).graph

  def runOn_(qgraph: QSUGraph): AuthenticatedQSU[Fix] = {
    val resultsF = for {
      agraph0 <- ApplyProvenance[Fix, F](qgraph)
      agraph <- ReifyBuckets[Fix, F](agraph0)
      back <- MinimizeAutoJoins[Fix, F](agraph)
    } yield back

    val results = resultsF.run.eval(0L).value.toEither
    results must beRight

    results.right.get
  }
}
