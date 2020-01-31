/*
 * Copyright 2014–2020 SlamData Inc.
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
import quasar.{Qspec, TreeMatchers, Type}
import quasar.IdStatus.{ExcludeId, IdOnly, IncludeId}
import quasar.contrib.iota._
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.{
  construction,
  Hole,
  LeftSide,
  MapFuncsCore,
  OnUndefined,
  PlannerError,
  ReduceFuncs,
  ReduceIndex,
  RightSide,
  SrcHole
}
import quasar.qsu.mra.ProvImpl

import matryoshka._
import matryoshka.data.Fix
import matryoshka.data.free._

import org.specs2.matcher.{Matcher, MatchersImplicits}

import pathy.Path
import Path.Sandboxed

import scalaz.{\/-, EitherT, Free, Need, StateT}
import scalaz.std.anyVal._
import scalaz.syntax.std.boolean._
import scalaz.syntax.equal._
//import scalaz.syntax.show._
import scalaz.syntax.tag._

import shims.{eqToScalaz, monoidToCats, orderToCats, orderToScalaz, showToCats, showToScalaz}

object MinimizeAutoJoinsSpec
    extends Qspec
    with MatchersImplicits
    with TreeMatchers
    with QSUTTypes[Fix] {

  import QSUGraph.Extractors._
  import ApplyProvenance.AuthenticatedQSU
  import QScriptUniform.{DTrans, Retain, Rotation}

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]

  val qsu = QScriptUniform.DslT[Fix]
  val func = construction.Func[Fix]
  val recFunc = construction.RecFunc[Fix]
  val qprov = ProvImpl[Fix[EJson], IdAccess, IdType]

  type J = Fix[EJson]
  val J = Fixed[Fix[EJson]]

  val afile = Path.rootDir[Sandboxed] </> Path.file("afile")
  val afile2 = Path.rootDir[Sandboxed] </> Path.file("afile2")

  val shiftedRead = qsu.read(afile, ExcludeId)

  import qprov.syntax._

  "linearize .foo + .bar" in {
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.map(
          qsu.read(afile, ExcludeId),
          recFunc.ProjectKeyS(recFunc.Hole, "foo")),
        qsu.map(
          qsu.read(afile, ExcludeId),
          recFunc.ProjectKeyS(recFunc.Hole, "bar")),
        _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case Map(Read(_, ExcludeId), fm) =>
        // must_=== doesn't work
        fm must beTreeEqual(
          recFunc.Add(
            recFunc.ProjectKeyS(recFunc.Hole, "foo"),
            recFunc.ProjectKeyS(recFunc.Hole, "bar")))
    }
  }

  "convert Typecheck to a Map(_, Guard)" in {
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin3((
        qsu.read(afile, ExcludeId),
        qsu.read(afile, ExcludeId),
        qsu.undefined(),
        _(MapFuncsCore.Guard(_, Type.AnyObject, _, _)))))

    runOn(qgraph) must beLike {
      case Map(Read(_, ExcludeId), fm) =>
        // must_=== doesn't work
        fm must beTreeEqual(recFunc.Guard(recFunc.Hole, Type.AnyObject, recFunc.Hole, recFunc.Undefined))
    }
  }

  "coalesce two summed reductions" in {
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.qsReduce(
          qsu.read(afile, ExcludeId),
          Nil,
          List(ReduceFuncs.Count(func.Hole)),
          func.ReduceIndex(\/-(0))),
        qsu.qsReduce(
          qsu.read(afile, ExcludeId),
          Nil,
          List(ReduceFuncs.Sum(func.Hole)),
          func.ReduceIndex(\/-(0))),
        _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case Map(
        QSReduce(
          Read(_, ExcludeId),
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
          recFunc.Add(
            recFunc.ProjectKeyS(recFunc.Hole, "0"),
            recFunc.ProjectKeyS(recFunc.Hole, "1")))
    }
  }

  "coalesce three summed reductions" in {
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.read(afile, ExcludeId),
            Nil,
            List(ReduceFuncs.Count(func.Hole)),
            func.ReduceIndex(\/-(0))),
          qsu.qsReduce(
            qsu.read(afile, ExcludeId),
            Nil,
            List(ReduceFuncs.Sum(func.Hole)),
            func.ReduceIndex(\/-(0))),
          _(MapFuncsCore.Add(_, _)))),
        qsu.qsReduce(
          qsu.read(afile, ExcludeId),
          Nil,
          List(ReduceFuncs.Max(func.Hole)),
          func.ReduceIndex(\/-(0))),
        _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case Map(
        QSReduce(
          Read(_, ExcludeId),
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
            "0" -> func.ReduceIndex(\/-(0)),
            "1" -> func.ReduceIndex(\/-(1)),
            "2" -> func.ReduceIndex(\/-(2))))

        fm must beTreeEqual(
          recFunc.Add(
            recFunc.Add(
              recFunc.ProjectKeyS(recFunc.Hole, "0"),
              recFunc.ProjectKeyS(recFunc.Hole, "1")),
            recFunc.ProjectKeyS(recFunc.Hole, "2")))
    }
  }

  "coalesce two summed reductions, one downstream of an autojoin" in {
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.qsReduce(
          qsu.autojoin2((
            qsu.read(afile, ExcludeId),
            qsu.map1((
              qsu.unreferenced(),
              MapFuncsCore.Constant[Fix, Hole](J.str("hey")))),
            _(MapFuncsCore.ConcatArrays(_, _)))),
          Nil,
          List(ReduceFuncs.Count(func.Hole)),
          Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
        qsu.qsReduce(
          qsu.read(afile, ExcludeId),
          Nil,
          List(ReduceFuncs.Sum(func.Hole)),
          Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
        _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case Map(
        QSReduce(
          Read(_, ExcludeId),
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
          recFunc.Add(
            recFunc.ProjectKeyS(recFunc.Hole, "0"),
            recFunc.ProjectKeyS(recFunc.Hole, "1")))
    }
  }

  "rewrite filter into cond only to avoid join" in {
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.read(afile, ExcludeId),
        qsu.qsFilter(
          qsu.read(afile, ExcludeId),
          recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("foo")))),
        _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case Map(Read(_, ExcludeId), fm) =>
        fm must beTreeEqual(
          recFunc.Add(
            recFunc.Hole,
            recFunc.Cond(
              recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("foo"))),
              recFunc.Hole,
              recFunc.Undefined)))
    }
  }

  "rewrite filter into cond when there are two filters" in {
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.qsFilter(
          qsu.read(afile, ExcludeId),
          recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("foo")))),
        qsu.qsFilter(
          qsu.read(afile, ExcludeId),
          recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("bar")))),
        _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case Map(Read(_, ExcludeId), fm) =>
        fm must beTreeEqual(
          recFunc.Add(
            recFunc.Cond(
              recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("foo"))),
              recFunc.Hole,
              recFunc.Undefined),
            recFunc.Cond(
              recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("bar"))),
              recFunc.Hole,
              recFunc.Undefined)))
    }
  }

  "rewrite filter into cond to avoid randomly creating more shifts" in {
    val shift = qsu.leftShift(
      shiftedRead,
      recFunc.Hole,
      ExcludeId,
      OnUndefined.Omit,
      func.RightSide,
      Rotation.ShiftArray)

    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        shift,
        qsu.qsFilter(
          shift,
          recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("foo")))),
        _(MapFuncsCore.Add(_, _)))))

    val results = runOn(qgraph)

    results must beLike {
      case
        Map(
          LeftShift(
            Read(`afile`, ExcludeId),
            _, _, _, _, _),
          fm) =>

        fm must beTreeEqual(
          recFunc.Add(
            recFunc.Hole,
            recFunc.Cond(
              recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("foo"))),
              recFunc.Hole,
              recFunc.Undefined)))
    }
  }

  "not rewrite filter acting as upstream source" in {
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.qsFilter(
          qsu.read(afile, ExcludeId),
          recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("foo")))),
        qsu.cint(42),
        _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case Map(QSFilter(_, _), fm) =>
        fm must beTreeEqual(
          recFunc.Add(recFunc.Hole, recFunc.Constant(J.int(42))))
    }
  }

  "not unnecessarily rewrite filter" in {
    val tread =
      qsu.tread(afile)

    val filter =
      qsu.qsFilter(
        qsu.autojoin3((
          tread,
          tread,
          qsu.undefined(),
          _(MapFuncsCore.Guard(_, Type.AnyObject, _, _)))),
        recFunc.Eq(
          recFunc.ProjectKeyS(recFunc.Hole, "city"),
          recFunc.ProjectKeyS(recFunc.Hole, "state")))

    val projLoc =
      qsu.autojoin2((
        filter,
        qsu.cstr("loc"),
        _(MapFuncsCore.ProjectKey(_, _))))

    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.autojoin2((
          qsu.cstr("city"),
          qsu.autojoin2((
            filter,
            qsu.cstr("city"),
            _(MapFuncsCore.ProjectKey(_, _)))),
          _(MapFuncsCore.MakeMap(_, _)))),
        qsu.autojoin2((
          qsu.cstr("loc"),
          qsu.transpose(
            qsu.autojoin3((
              projLoc,
              projLoc,
              qsu.undefined(),
              _(MapFuncsCore.Guard(_, Type.AnyArray, _, _)))),
            Retain.Values,
            Rotation.FlattenArray),
          _(MapFuncsCore.MakeMap(_, _)))),
      _(MapFuncsCore.ConcatMaps(_, _)))))

    runOn(qgraph).foldMapUp {
      case QSFilter(_, _) => true.disjunction
      case _              => false.disjunction
    }.unwrap must beTrue
  }.pendingUntilFixed

  "coalesce two summed bucketing reductions, inlining functions into the buckets" in {
    val readAndThings =
      qsu.map1((
        qsu.read(afile, ExcludeId),
        MapFuncsCore.Negate(SrcHole)))

    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.qsReduce(
          readAndThings,
          List(AccessHole[Fix]),
          List(ReduceFuncs.Count(func.Hole)),
          func.ReduceIndex(\/-(0))),
        qsu.qsReduce(
          readAndThings,
          List(AccessHole[Fix]),
          List(ReduceFuncs.Sum(func.Hole)),
          func.ReduceIndex(\/-(0))),
        _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case Map(
        QSReduce(
          Read(_, ExcludeId),
          List(bucket),
          List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
          repair),
        fm) =>

        // must_=== doesn't work
        bucket must beTreeEqual(func.Negate(AccessHole[Fix]))

        h1 must beTreeEqual(func.Negate(func.Hole))
        h2 must beTreeEqual(func.Negate(func.Hole))

        repair must beTreeEqual(
          func.StaticMapS(
            "0" ->
              func.ReduceIndex(\/-(0)),
            "1" ->
              func.ReduceIndex(\/-(1))))

        fm must beTreeEqual(
          recFunc.Add(
            recFunc.ProjectKeyS(recFunc.Hole, "0"),
            recFunc.ProjectKeyS(recFunc.Hole, "1")))
    }
  }

  "remap coalesced bucket references in dimensions" in {
    val readAndThings =
      qsu.map((
        qsu.dimEdit((
          qsu.read(afile, ExcludeId),
          DTrans.Group(func.ProjectKeyS(func.Hole, "label")))),
        recFunc.Negate(recFunc.ProjectKeyS(recFunc.Hole, "metric"))))

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
          qprov.empty
            .projectStatic(J.str("afile"), IdType.Dataset)
            .inflateExtend(IdAccess.bucket(r.root, 0), IdType.Expr)

        auth.dims(m.root) must_= expDims
    }
  }

  "leave uncoalesced reductions of different bucketing" in {
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.qsReduce(
          qsu.read(afile, ExcludeId),
          Nil,
          List(ReduceFuncs.Sum(func.Hole)),
          func.ReduceIndex(\/-(0))),
        qsu.qsReduce(
          qsu.read(afile, ExcludeId),
          List(func.ProjectKeyS(AccessHole[Fix], "state")),
          List(ReduceFuncs.Sum(func.Hole)),
          func.ReduceIndex(\/-(0))),
        _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case AutoJoin2C(
        QSReduce(Read(_, ExcludeId), Nil, _, _),
        QSReduce(Read(_, ExcludeId), List(_), _, _),
        MapFuncsCore.Add(LeftSide, RightSide)) => ok
    }
  }

  "minimize an autojoin after prior source failure" in {
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.autojoin2((
          qsu.read(afile, ExcludeId),
          qsu.read(afile2, ExcludeId),
          _(MapFuncsCore.Subtract(_, _)))),
        qsu.cint(42),
        _(MapFuncsCore.Add(_, _)))))

    val results = runOn(qgraph)

    results must beLike {
      case AutoJoin2(Read(`afile`, ExcludeId), Read(`afile2`, ExcludeId), fm) =>
        fm must beTreeEqual(
          func.Add(
            func.Subtract(
              func.LeftSide,
              func.RightSide),
            func.Constant(J.int(42))))
    }
  }

  "halt minimization at a grouped vertex" in {
    val groupKey =
      func.Lower(func.ProjectKeyS(func.Hole, "city"))

    val groupedGuardedRead =
      qsu.dimEdit(
        qsu.autojoin3((
          qsu.read(afile, ExcludeId),
          qsu.read(afile, ExcludeId),
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
      case AutoJoin2(
        Map(Read(_, ExcludeId), guardL),
        QSReduce(
          Map(Read(_, ExcludeId), guardR),
          bucket :: Nil,
          ReduceFuncs.Sum(prjPop) :: Nil,
          _),
        repair) =>

        guardL must beTreeEqual(guardR)

        repair must beTreeEqual(
          func.ConcatMaps(
            func.MakeMapS("city", func.ProjectKeyS(func.LeftSide, "city")),
            func.MakeMapS("1", func.RightSide)))

        prjPop must beTreeEqual(func.ProjectKeyS(func.Hole, "pop"))
    }
  }

  "avoid renaming dimensions when coalesced node appears on both sides" in {
    val ann = QScriptUniform.AnnotatedDsl[Fix, Symbol]

    val read =
      ann.read('read1, (afile, ExcludeId))

    val tree =
      ann.autojoin2(('n0, (
        ann.map(('n1, (
          read,
          recFunc.ProjectKeyS(recFunc.Hole, "b")))),
        ann.transpose(('n2, (
          ann.map(('n3, (
            read,
            recFunc.ProjectKeyS(recFunc.Hole, "a")))),
          Retain.Values,
          Rotation.ShiftArray))),
        _(MapFuncsCore.ConcatMaps(_, _)))))

    val (rename, qgraph) =
      QSUGraph.fromAnnotatedTree[Fix](tree.map(Some(_)))

    val AuthenticatedQSU(agraph, auth) = runOn_(qgraph)

    auth.dims exists {
      case (_, p) =>
        qprov.foldMapVectorIds {
          case (id, _) => IdAccess.symbols.exist(_ ≟ rename('read1))(id).disjunction
        }(p).unwrap
    }
  }

  "coalesce an autojoin on a single leftshift on a shared source" in {
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.transpose(
          shiftedRead,
          Retain.Values,
          Rotation.ShiftArray),
        shiftedRead,
        _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case
        LeftShift(
          Read(`afile`, ExcludeId),
          struct,
          ExcludeId,
          _,
          repair,
          _) =>

        struct must beTreeEqual(recFunc.Hole)

        repair must beTreeEqual(
          func.Add(
            func.RightSide,
            func.LeftSide))
    }
  }

  "coalesce an autojoin on a single leftshift on a shared source (RTL)" in {
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        shiftedRead,
        qsu.transpose(
          shiftedRead,
          Retain.Values,
          Rotation.ShiftArray),
        _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case
        LeftShift(
          Read(`afile`, ExcludeId),
          struct,
          ExcludeId,
          _,
          repair,
          _) =>

        struct must beTreeEqual(recFunc.Hole)

        repair must beTreeEqual(
          func.Add(
            func.LeftSide,
            func.RightSide))
    }
  }

  "inductively coalesce reduces on coalesced shifts" in {
    // count(a[*]) + sum(a)
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.qsReduce(
          qsu.transpose(
            shiftedRead,
            Retain.Values,
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
            Read(`afile`, ExcludeId),
            struct,
            ExcludeId,
            OnUndefined.Emit,
            repairInner,
            _),
          Nil,
          List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
          repairOuter),
        fm) =>

      struct must beTreeEqual(recFunc.Hole)

      repairInner must beTreeEqual(
        func.StaticMapS(
          "0" -> func.RightSide,
          "1" -> func.LeftSide))

      h1 must beTreeEqual(func.ProjectKeyS(func.Hole, "0"))
      h2 must beTreeEqual(func.ProjectKeyS(func.Hole, "1"))

      repairOuter must beTreeEqual(
        func.StaticMapS(
          "0" -> func.ReduceIndex(\/-(0)),
          "1" -> func.ReduceIndex(\/-(1))))

      fm must beTreeEqual(
        recFunc.Add(
          recFunc.ProjectKeyS(recFunc.Hole, "0"),
          recFunc.ProjectKeyS(recFunc.Hole, "1")))
    }
  }

  "inductively coalesce reductions on grouped shift" >> {
    val grouped =
      qsu.map(
        qsu.dimEdit(
          qsu._autojoin2((
            shiftedRead,
            qsu.transpose(
              qsu.map(
                shiftedRead,
                recFunc.ProjectKeyS(recFunc.Hole, "x")),
              Retain.Values,
              Rotation.ShiftArray),
            func.StaticMapS(
              "group_source" -> func.LeftSide,
              "group_key" -> func.RightSide))),
          DTrans.Group(func.ProjectKeyS(func.ProjectKeyS(func.Hole, "group_key"), "z"))),
        recFunc.ProjectKeyS(recFunc.Hole, "group_source"))

    // select x[_].y[_], count(*) from foo group by x[_].z
    val qgraph = QSUGraph.fromTree[Fix](
      qsu._autojoin2((
        qsu.transpose(
          qsu.map(
            grouped,
            recFunc.ProjectKeyS(recFunc.Hole, "y")),
          Retain.Values,
          Rotation.ShiftArray),
        qsu.lpReduce(
          grouped,
          ReduceFuncs.Count(())),
        func.StaticMapS(
          "y" -> func.LeftSide,
          "1" -> func.RightSide))))

   runOn(qgraph) must beLike {
      case AutoJoin2(
        Transpose(
          Map(
            Map(
              l @ LeftShift(_, struct, _, _, _, _),
              prjSrc),
            prjY),
          Retain.Values,
          Rotation.ShiftArray),
        QSReduce(
          r @ LeftShift(_, _, _, _, _, _),
          _,
          _,
          _),
        _) =>

      struct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "x"))
      prjSrc must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "group_source"))
      prjY must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "y"))

      l.root must_=== r.root
    }
  }

  "coalesce an autojoin on two leftshifts on a shared source" in {
    // a[*][*] + a
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.transpose(
          qsu.transpose(
            shiftedRead,
            Retain.Values,
            Rotation.ShiftArray),
          Retain.Values,
          Rotation.ShiftArray),
        shiftedRead,
        _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case
        LeftShift(
          LeftShift(
            Read(`afile`, ExcludeId),
            structInner,
            ExcludeId,
            OnUndefined.Emit,
            repairInner,
            _),
          structOuter,
          ExcludeId,
          OnUndefined.Emit,
          repairOuter,
          _) =>

        structInner must beTreeEqual(recFunc.Hole)

        repairInner must beTreeEqual(
          func.StaticMapS(
            "source" -> func.LeftSide,
            "cart0" -> func.RightSide))

        structOuter must beTreeEqual(
          recFunc.ProjectKeyS(recFunc.Hole, "cart0"))

        repairOuter must beTreeEqual(
          func.Add(
            func.RightSide,
            func.ProjectKeyS(func.LeftSide, "source")))
    }
  }

  "coalesce an autojoin on three leftshifts on a shared source" in {
    // a[*][*][*] + a
    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((
        qsu.transpose(
          qsu.transpose(
            qsu.transpose(
              shiftedRead,
              Retain.Values,
              Rotation.ShiftArray),
            Retain.Values,
            Rotation.ShiftMap),
          Retain.Values,
          Rotation.ShiftArray),
        shiftedRead,
        _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case
        LeftShift(
          LeftShift(
            LeftShift(
              Read(`afile`, ExcludeId),
              structInnerInner,
              ExcludeId,
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
          Rotation.ShiftArray) =>

        structInnerInner must beTreeEqual(recFunc.Hole)

        repairInnerInner must beTreeEqual(
          func.StaticMapS(
            "source" -> func.LeftSide,
            "cart0" -> func.RightSide))

        structInner must beTreeEqual(
          recFunc.ProjectKeyS(recFunc.Hole, "cart0"))

        repairInner must beTreeEqual(
          func.ConcatMaps(
            func.LeftSide,
            func.MakeMapS("cart0", func.RightSide)))

        structOuter must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "cart0"))

        repairOuter must beTreeEqual(
          func.Add(
            func.RightSide,
            func.ProjectKeyS(func.LeftSide, "source")))
    }
  }

  // c[*] / d[*][*]
  "coalesce uneven shifts" in {
    val cdivd =
      qsu.autojoin2((
        qsu.transpose(
          qsu.map(
            shiftedRead,
            recFunc.ProjectKeyS(recFunc.Hole, "c")),
          Retain.Values,
          Rotation.ShiftArray),
        qsu.transpose(
          qsu.transpose(
            qsu.map(
              shiftedRead,
              recFunc.ProjectKeyS(recFunc.Hole, "d")),
            Retain.Values,
            Rotation.ShiftArray),
          Retain.Values,
          Rotation.ShiftArray),
        _(MapFuncsCore.Divide(_, _))))

    val qgraph = QSUGraph.fromTree[Fix](cdivd)

    runOn(qgraph) must beLike {
      case
        LeftShift(
          LeftShift(
            LeftShift(
              Read(_, ExcludeId),
              cStruct,
              ExcludeId,
              OnUndefined.Emit,
              cRepair,
              Rotation.ShiftArray),
            dStruct,
            ExcludeId,
            OnUndefined.Emit,
            dRepair,
            Rotation.ShiftArray),
          ddStruct,
          ExcludeId,
          OnUndefined.Emit,
          ddRepair,
          Rotation.ShiftArray) =>

        cStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "c"))
        dStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "source"), "d"))
        ddStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "cart1"))

        cRepair must beTreeEqual(
          func.StaticMapS(
            "source" -> func.LeftSide,
            "cart0" -> func.RightSide))

        dRepair must beTreeEqual(
          func.ConcatMaps(
            func.LeftSide,
            func.MakeMapS("cart1", func.RightSide)))

        ddRepair must beTreeEqual(
          func.Divide(
            func.ProjectKeyS(func.LeftSide, "cart0"),
            func.RightSide))
    }
  }

  // a[*][*][*] + b - c[*] / d[*][*]
  "coalesce a thing that looks a lot like the search card" in {
    // a[*][*][*] + b
    val aplusb =
      qsu.autojoin2((
        qsu.transpose(
          qsu.transpose(
            qsu.transpose(
              qsu.map(
                shiftedRead,
                recFunc.ProjectKeyS(recFunc.Hole, "a")),
              Retain.Values,
              Rotation.ShiftArray),
            Retain.Values,
            Rotation.ShiftArray),
          Retain.Values,
          Rotation.ShiftArray),
        qsu.map((shiftedRead, recFunc.ProjectKeyS(recFunc.Hole, "b"))),
        _(MapFuncsCore.Add(_, _))))

    // c[*] / d[*][*]
    val cdivd =
      qsu.autojoin2((
        qsu.transpose(
          qsu.map(
            shiftedRead,
            recFunc.ProjectKeyS(recFunc.Hole, "c")),
          Retain.Values,
          Rotation.ShiftArray),
        qsu.transpose(
          qsu.transpose(
            qsu.map(
              shiftedRead,
              recFunc.ProjectKeyS(recFunc.Hole, "d")),
            Retain.Values,
            Rotation.ShiftArray),
          Retain.Values,
          Rotation.ShiftArray),
        _(MapFuncsCore.Divide(_, _))))

    val qgraph = QSUGraph.fromTree[Fix](
      qsu.autojoin2((aplusb, cdivd, _(MapFuncsCore.Subtract(_, _)))))

    runOn(qgraph) must beLike {
      case
        LeftShift(
          LeftShift(
            LeftShift(
              LeftShift(
                LeftShift(
                  LeftShift(
                    Read(`afile`, ExcludeId),
                      cstruct0,
                      ExcludeId,
                      OnUndefined.Emit,
                      crepair0,
                      Rotation.ShiftArray),
                    dstruct0,
                    ExcludeId,
                    OnUndefined.Emit,
                    drepair0,
                    Rotation.ShiftArray),
                  dstruct1,
                  ExcludeId,
                  OnUndefined.Emit,
                  drepair1,
                  Rotation.ShiftArray),
                astruct0,
                ExcludeId,
                OnUndefined.Emit,
                arepair0,
                Rotation.ShiftArray),
              astruct1,
              ExcludeId,
              OnUndefined.Emit,
              arepair1,
              Rotation.ShiftArray),
            astruct2,
            ExcludeId,
            OnUndefined.Emit,
            arepair2,
            Rotation.ShiftArray) =>

      cstruct0 must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "c"))
      dstruct0 must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "source"), "d"))
      dstruct1 must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "cart3"))
      astruct0 must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "source"), "a"))
      astruct1 must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "cart0"))
      astruct2 must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "cart0"))

      crepair0 must beTreeEqual(
        func.StaticMapS(
          "source" -> func.LeftSide,
          "cart2" -> func.RightSide))

      drepair0 must beTreeEqual(
        func.ConcatMaps(
          func.LeftSide,
          func.MakeMapS("cart3", func.RightSide)))

      drepair1 must beTreeEqual(
        func.ConcatMaps(
          func.LeftSide,
          func.MakeMapS("cart3", func.RightSide)))

      arepair0 must beTreeEqual(
        func.ConcatMaps(
          func.LeftSide,
          func.MakeMapS("cart0", func.RightSide)))

      arepair1 must beTreeEqual(
        func.ConcatMaps(
          func.LeftSide,
          func.MakeMapS("cart0", func.RightSide)))

      arepair2 must beTreeEqual(
        func.Subtract(
          func.Add(
            func.RightSide,
            func.ProjectKeyS(func.ProjectKeyS(func.LeftSide, "source"), "b")),
          func.Divide(
            func.ProjectKeyS(func.LeftSide, "cart2"),
            func.ProjectKeyS(func.LeftSide, "cart3"))))
    }
  }

  // [a, b[*][*]]]
  "coalesce with proper struct a contextual shift autojoin" in {
    val qgraph =
      QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.map(
            shiftedRead,
            recFunc.MakeArray(
              recFunc.Guard(
                recFunc.Hole,
                Type.AnyObject,
                recFunc.ProjectKeyS(recFunc.Hole, "a"),
                recFunc.Undefined))),
          qsu.map(
            qsu.transpose(
              qsu.transpose(
                qsu.map(
                  shiftedRead,
                  recFunc.Guard(
                    recFunc.Hole,
                    Type.AnyObject,
                    recFunc.ProjectKeyS(recFunc.Hole, "b"),
                    recFunc.Undefined)),
                Retain.Values,
                Rotation.ShiftArray),
              Retain.Values,
              Rotation.ShiftArray),
            recFunc.MakeArray(recFunc.Hole)),
          _(MapFuncsCore.ConcatArrays(_, _)))))

    runOn(qgraph) must beLike {
      case
        LeftShift(
          LeftShift(
            Read(_, ExcludeId),
            innerStruct,
            _,
            OnUndefined.Emit,
            innerRepair,
            _),
          outerStruct,
          _,
          OnUndefined.Emit,
          outerRepair,
          _) =>

        innerStruct must beTreeEqual(
          recFunc.Guard(
            recFunc.Hole,
            Type.AnyObject,
            recFunc.ProjectKeyS(recFunc.Hole, "b"),
            recFunc.Undefined))

        innerRepair must beTreeEqual(
          func.StaticMapS(
            "source" -> func.LeftSide,
            "cart2" -> func.RightSide))

        outerStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "cart2"))

        outerRepair must beTreeEqual(
          func.ConcatArrays(
            func.MakeArray(
              func.Guard(
                func.ProjectKeyS(func.LeftSide, "source"),
                Type.AnyObject,
                func.ProjectKeyS(
                  func.ProjectKeyS(func.LeftSide, "source"),
                  "a"),
                func.Undefined)),
            func.MakeArray(func.RightSide)))
    }
  }

  // select [a, foo[*][*]] from afile
  "properly coalesce a more realistic double-shift example" in {
    val qsu0 = qsu.read(afile, ExcludeId)
    val qsu2 = qsu.unreferenced()

    val qsu3 = qsu.map(qsu2, recFunc.Undefined[Hole])

    val qsu9 =
      qsu.map(qsu0, recFunc.ProjectKeyS(recFunc.Hole, "foo"))

    val qsu11 =
      qsu.transpose(
        qsu9,
        Retain.Values,
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
      qsu.map(qsu0, recFunc.ProjectKeyS(recFunc.Hole, "a"))

    val qsu13 =
      qsu.transpose(
        qsu12,
        Retain.Values,
        Rotation.FlattenArray)

    val qsu7 =
      qsu.map(qsu6, recFunc.MakeArray(recFunc.Hole))

    val qsu14 =
      qsu.map(qsu13, recFunc.MakeArray(recFunc.Hole))

    val qsu16 =
      qsu.autojoin2((
        qsu7,
        qsu14,
        _(MapFuncsCore.ConcatArrays(_, _))))

    val qgraph = QSUGraph.fromTree[Fix](qsu16)

    runOn(qgraph) must beLike {
      case
        LeftShift(
          LeftShift(
            Read(_, ExcludeId),
            innerStruct,
            _,
            OnUndefined.Emit,
            innerRepair,
            _),
          outerStruct,
          _,
          OnUndefined.Emit,
          outerRepair,
          _) => ok
    }
  }

  // a + b[*].c[*]
  "coalesce uneven shifts with an intervening map" in {
    val qgraph =
      QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.map(shiftedRead, recFunc.ProjectKeyS(recFunc.Hole, "a")),
          qsu.transpose(
            qsu.map(
              qsu.transpose(
                qsu.map(shiftedRead, recFunc.ProjectKeyS(recFunc.Hole, "b")),
                Retain.Values,
                Rotation.FlattenArray),
              recFunc.ProjectKeyS(recFunc.Hole, "c")),
            Retain.Values,
            Rotation.FlattenArray),
          _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case
        LeftShift(
          LeftShift(
            Read(_, ExcludeId),
            innerStruct,
            _,
            _,
            innerRepair,
            _),
          outerStruct,
          _,
          _,
          outerRepair,
          _) =>

        innerStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "b"))

        innerRepair must beTreeEqual(
          func.StaticMapS(
            "source" -> func.LeftSide,
            "cart1" -> func.RightSide))

        outerStruct must beTreeEqual(
          recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "cart1"), "c"))

        outerRepair must beTreeEqual(
          func.Add(
            func.ProjectKeyS(
              func.ProjectKeyS(func.LeftSide, "source"),
              "a"),
            func.RightSide))
    }
  }

  // a[*] + b[*].c[*]
  "coalesce a shift with uneven shifts with an intervening map" in {
    val qgraph =
      QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.transpose(
            qsu.map(shiftedRead, recFunc.ProjectKeyS(recFunc.Hole, "a")),
            Retain.Values,
            Rotation.FlattenArray),
          qsu.transpose(
            qsu.map(
              qsu.transpose(
                qsu.map(shiftedRead, recFunc.ProjectKeyS(recFunc.Hole, "b")),
                Retain.Values,
                Rotation.FlattenArray),
              recFunc.ProjectKeyS(recFunc.Hole, "c")),
            Retain.Values,
            Rotation.FlattenArray),
          _(MapFuncsCore.Add(_, _)))))

    runOn(qgraph) must beLike {
      case
        LeftShift(
          LeftShift(
            LeftShift(
              Read(_, ExcludeId),
              aStruct,
              _,
              _,
              aRepair,
              _),
            bStruct,
            _,
            _,
            bRepair,
            _),
          cStruct,
          _,
          _,
          cRepair,
          _) =>

        aStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "a"))
        bStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "source"), "b"))
        cStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "cart1"), "c"))

        aRepair must beTreeEqual(
          func.StaticMapS(
            "source" -> func.LeftSide,
            "cart0" -> func.RightSide))

        bRepair must beTreeEqual(
          func.ConcatMaps(
            func.LeftSide,
            func.MakeMapS("cart1", func.RightSide)))

        cRepair must beTreeEqual(
          func.Add(
            func.ProjectKeyS(func.LeftSide, "cart0"),
            func.RightSide))
    }
  }

  // select * from foo where b[*] or b[*][*]
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
          _(MapFuncsCore.Guard(_, Type.FlexArr(0, None, Type.Bool), _, _)))),
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
            recFunc.ProjectKeyS(recFunc.Hole, "filter_predicate")),
          recFunc.ProjectKeyS(recFunc.Hole, "filter_source")))

    runOn(qgraph) must haveShiftCount(2)
  }

  "create a single AutoJoin2 when there are exactly two sources" in {
    val reduce =
      qsu.qsReduce(
        shiftedRead,
        Nil,
        List(ReduceFuncs.Arbitrary(func.ProjectKeyS(func.Hole, "b"))),
        Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0))))

    val twoSources =
      QSUGraph.fromTree[Fix](
        qsu._autojoin2(
          (qsu._autojoin2(
            (qsu.map(shiftedRead, recFunc.MakeMapS("a", recFunc.ProjectKeyS(recFunc.Hole, "a"))),
              qsu.map(reduce, recFunc.MakeMapS("b", recFunc.Hole)),
              func.ConcatMaps(func.LeftSide, func.RightSide))),
            qsu.map(shiftedRead, recFunc.MakeMapS("c", recFunc.ProjectKeyS(recFunc.Hole, "c"))),
            func.ConcatMaps(func.LeftSide, func.RightSide))))

    runOn(twoSources) must beLike {
      case AutoJoin2(
        Read(_, ExcludeId),
        QSReduce(_, _, _, _),
        repair) =>

        repair must beTreeEqual(
          func.StaticMapS(
            "a" -> func.ProjectKeyS(func.LeftSide, "a"),
            "b" -> func.RightSide,
            "c" -> func.ProjectKeyS(func.LeftSide, "c")))
    }
  }

  // a{:_}, a{_:}
  "shifts keys and values with a single left-shift" in {
    val shifts = QSUGraph.fromTree[Fix](
      qsu._autojoin2((
        qsu.transpose(
          qsu.map(
            shiftedRead,
            recFunc.ProjectKeyS(recFunc.Hole, "a")),
          Retain.Identities,
          Rotation.ShiftMap),
        qsu.transpose(
          qsu.map(
            shiftedRead,
            recFunc.ProjectKeyS(recFunc.Hole, "a")),
          Retain.Values,
          Rotation.ShiftMap),
        func.StaticMapS(
          "0" -> func.LeftSide,
          "1" -> func.RightSide))))

    runOn(shifts) must beLike {
      case
        LeftShift(
          Read(_, ExcludeId),
          struct,
          IncludeId,
          OnUndefined.Emit,
          repair,
          Rotation.ShiftMap) =>

        struct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "a"))

        repair must beTreeEqual(
          func.StaticMapS(
            "0" -> func.ProjectIndexI(func.RightSide, 0),
            "1" -> func.ProjectIndexI(func.RightSide, 1)))
    }
  }

  // a{_:}, b[_:], a{:_}
  "reorders candidates to coalesce compatible shifts" in {
    val qgraph = QSUGraph.fromTree[Fix](
      qsu._autojoin3((
        qsu.transpose(
          qsu.map(
            shiftedRead,
            recFunc.ProjectKeyS(recFunc.Hole, "a")),
          Retain.Identities,
          Rotation.ShiftMap),
        qsu.transpose(
          qsu.map(
            shiftedRead,
            recFunc.ProjectKeyS(recFunc.Hole, "b")),
          Retain.Identities,
          Rotation.ShiftArray),
        qsu.transpose(
          qsu.map(
            shiftedRead,
            recFunc.ProjectKeyS(recFunc.Hole, "a")),
          Retain.Values,
          Rotation.ShiftMap),
        func.StaticMapS(
          "0" -> func.LeftSide3,
          "1" -> func.Center,
          "2" -> func.RightSide3))))

    runOn(qgraph) must beLike {
      case LeftShift(
        LeftShift(
          Read(_, ExcludeId),
          structB,
          IdOnly,
          OnUndefined.Emit,
          repairB,
          Rotation.ShiftArray),
        structA,
        IncludeId,
        OnUndefined.Emit,
        repairA,
        Rotation.ShiftMap) =>

        structB must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "b"))
        structA must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "source"), "a"))

        repairB must beTreeEqual(
          func.StaticMapS(
            "source" -> func.LeftSide,
            "cart1" -> func.RightSide))

        repairA must beTreeEqual(
          func.StaticMapS(
            "0" -> func.ProjectIndexI(func.RightSide, 0),
            "1" -> func.ProjectKeyS(func.LeftSide, "cart1"),
            "2" -> func.ProjectIndexI(func.RightSide, 1)))
    }
  }

  "rewrites type-filters into Typecheck in FreeMaps" in {
    val qgraph = QSUGraph.fromTree[Fix](
      qsu._autojoin2((
        qsu.qsFilter(
          qsu.map((
            qsu.transpose(
              qsu.map((qsu.read(afile, ExcludeId), recFunc.ProjectKeyS(recFunc.Hole, "previous_addresses"))),
              Retain.Values,
              Rotation.ShiftArray),
            recFunc.ProjectKeyS(recFunc.Hole, "city"))),
          recFunc.Eq(recFunc.TypeOf(recFunc.Hole), recFunc.Constant(J.str("string")))),
        qsu.qsFilter(
          qsu.map((qsu.read(afile, ExcludeId), recFunc.ProjectKeyS(recFunc.Hole, "last_visit"))),
          recFunc.Eq(recFunc.TypeOf(recFunc.Hole), recFunc.Constant(J.str("string")))),
        func.ConcatMaps(
          func.MakeMapS("left", func.LeftSide),
          func.MakeMapS("right", func.RightSide)))))

    runOn(qgraph) must beLike {
      case Map(
        LeftShift(
          Read(_, ExcludeId),
          struct,
          ExcludeId,
          OnUndefined.Emit,
          repair,
          Rotation.ShiftArray), outerMap) =>

        struct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "previous_addresses"))

        repair must beTreeEqual(
          func.StaticMapS(
            "0" -> func.ProjectKeyS(func.RightSide, "city"),
            "1" -> func.ProjectKeyS(func.LeftSide, "last_visit")))

        outerMap must beTreeEqual(
          recFunc.StaticMapS(
            "left" -> recFunc.Typecheck(recFunc.ProjectKeyS(recFunc.Hole, "0"), Type.Str),
            "right" -> recFunc.Typecheck(recFunc.ProjectKeyS(recFunc.Hole, "1"), Type.Str)))
    }
  }

  // a[*], b[*], a[*][*], b[*]
  "optimally reassociate shifts during coalescence" in {
    val read = qsu.read(afile, ExcludeId)

    val as = qsu.transpose(
      qsu.map((read, recFunc.ProjectKeyS(recFunc.Hole, "a"))),
      Retain.Values,
      Rotation.ShiftArray)

    val bs = qsu.transpose(
      qsu.map((read, recFunc.ProjectKeyS(recFunc.Hole, "b"))),
      Retain.Values,
      Rotation.ShiftArray)

    val ass = qsu.transpose(
      qsu.transpose(
        qsu.map((read, recFunc.ProjectKeyS(recFunc.Hole, "a"))),
        Retain.Values,
        Rotation.ShiftArray),
      Retain.Values,
      Rotation.ShiftArray)

    val qgraph = QSUGraph.fromTree[Fix](
      qsu._autojoin2((
        qsu._autojoin2((
          qsu._autojoin2((
            qsu.map((
              as,
              recFunc.MakeArray(recFunc.Hole))),
            qsu.map((
              bs,
              recFunc.MakeArray(recFunc.Hole))),
            func.ConcatArrays(func.LeftSide, func.RightSide))),
          qsu.map((
            ass,
            recFunc.MakeArray(recFunc.Hole))),
          func.ConcatArrays(func.LeftSide, func.RightSide))),
        qsu.map((
          bs,
          recFunc.MakeArray(recFunc.Hole))),
        func.ConcatArrays(func.LeftSide, func.RightSide))))

    runOn(qgraph) must haveShiftCount(3)
  }

  // a[*][*], a[*][*].b[*]
  "collapses three-tier shift without re-coalescing with self" in {
    val read = qsu.read(afile, ExcludeId)

    val as = qsu.transpose(
      qsu.transpose(
        qsu.map((read, recFunc.ProjectKeyS(recFunc.Hole, "a"))),
        Retain.Values,
        Rotation.ShiftArray),
      Retain.Values,
      Rotation.ShiftArray)

    val bs = qsu.transpose(
      qsu.map((as, recFunc.ProjectKeyS(recFunc.Hole, "b"))),
      Retain.Values,
      Rotation.ShiftArray)

    val qgraph = QSUGraph.fromTree[Fix](
      qsu._autojoin2((
        as,
        bs,
        func.ConcatMaps(func.LeftSide, func.RightSide))))

    runOn(qgraph) must haveShiftCount(3)
  }

  // a[_:], a[_][_:], a[_][_]
  "collapses two-tier id-varying array shift without re-coalescing with self" in {
    val read = qsu.read(afile, ExcludeId)

    val ase = qsu.transpose(
      qsu.map((read, recFunc.ProjectKeyS(recFunc.Hole, "a"))),
      Retain.Values,
      Rotation.ShiftArray)

    val asi = qsu.transpose(
      qsu.map((read, recFunc.ProjectKeyS(recFunc.Hole, "a"))),
      Retain.Identities,
      Rotation.ShiftArray)

    val asesi = qsu.transpose(
      ase,
      Retain.Identities,
      Rotation.ShiftArray)

    val asese = qsu.transpose(
      ase,
      Retain.Values,
      Rotation.ShiftArray)

    val qgraph = QSUGraph.fromTree[Fix](
      qsu._autojoin2((
        qsu._autojoin2((
          asi,
          asesi,
          func.ConcatMaps(func.LeftSide, func.RightSide))),
        asese,
        func.ConcatMaps(func.LeftSide, func.RightSide))))

    val results = runOn(qgraph)

    results must haveShiftCount(2)

    results must beLike {
      case
        LeftShift(
          LeftShift(
            Read(`afile`, ExcludeId),
            _,
            _,
            _,
            repairInner,
            _),
          _,
          _,
          _,
          repair,
          _) =>

      repairInner must beTreeEqual(
        func.StaticMapS(
          "cart1" -> func.ProjectIndexI(func.RightSide, 1),
          "simpl9" -> func.ProjectIndexI(func.RightSide, 0)))

      repair must beTreeEqual(
        func.ConcatMaps(
          func.ConcatMaps(
            func.ProjectKeyS(func.LeftSide, "simpl9"),
            func.ProjectIndexI(func.RightSide, 0)),
          func.ProjectIndexI(func.RightSide, 1)))
    }
  }

  // a{_:}, a{_}{_:}, a{_}{_}
  "collapses two-tier id-varying object shift without re-coalescing with self" in {
    val read = qsu.read(afile, ExcludeId)

    val ase = qsu.transpose(
      qsu.map((read, recFunc.ProjectKeyS(recFunc.Hole, "a"))),
      Retain.Values,
      Rotation.ShiftMap)

    val asi = qsu.transpose(
      qsu.map((read, recFunc.ProjectKeyS(recFunc.Hole, "a"))),
      Retain.Identities,
      Rotation.ShiftMap)

    val asesi = qsu.transpose(
      ase,
      Retain.Identities,
      Rotation.ShiftMap)

    val asese = qsu.transpose(
      ase,
      Retain.Values,
      Rotation.ShiftMap)

    val qgraph = QSUGraph.fromTree[Fix](
      qsu._autojoin2((
        qsu._autojoin2((
          asi,
          asesi,
          func.StaticMapS(
            "k1" -> func.LeftSide,
            "k2" -> func.RightSide))),
        asese,
        func.ConcatMaps(func.LeftSide, func.MakeMapS("v2", func.RightSide)))))

    runOn(qgraph) must haveShiftCount(2)
  }

  // a[*][*], b[*][*]
  "avoid collapsing incompatible trailing shifts" in {
    val read = qsu.read(afile, ExcludeId)

    val ass = qsu.transpose(
      qsu.transpose(
        qsu.map(
          read,
          recFunc.ProjectKeyS(recFunc.Hole, "a")),
        Retain.Values,
        Rotation.ShiftMap),
      Retain.Values,
      Rotation.ShiftMap)

    val bss = qsu.transpose(
      qsu.transpose(
        qsu.map(
          read,
          recFunc.ProjectKeyS(recFunc.Hole, "b")),
        Retain.Values,
        Rotation.ShiftMap),
      Retain.Values,
      Rotation.ShiftMap)

    val qgraph = QSUGraph.fromTree[Fix](
      qsu._autojoin2((
        ass,
        bss,
        func.ConcatMaps(func.LeftSide, func.RightSide))))

    runOn(qgraph) must haveShiftCount(4)
  }

  // foo[_].baz[_], foo[_].qux[_], bar[_][_]
  "ensure necessary deconstruction is created in downstream shifts" in {
    val read = qsu.read(afile, ExcludeId)

    val barss = qsu.transpose(
      qsu.transpose(
        qsu.map(
          read,
          recFunc.ProjectKeyS(recFunc.Hole, "bar")),
        Retain.Values,
        Rotation.ShiftArray),
      Retain.Values,
      Rotation.ShiftArray)

    val foos = qsu.transpose(
      qsu.map(
        read,
        recFunc.ProjectKeyS(recFunc.Hole, "foo")),
      Retain.Values,
      Rotation.ShiftArray)

    val quxs = qsu.transpose(
      qsu.map(
        foos,
        recFunc.ProjectKeyS(recFunc.Hole, "qux")),
      Retain.Values,
      Rotation.ShiftArray)

    val bazs = qsu.transpose(
      qsu.map(
        foos,
        recFunc.ProjectKeyS(recFunc.Hole, "baz")),
      Retain.Values,
      Rotation.ShiftArray)

    val qgraph = QSUGraph.fromTree[Fix](
      qsu._autojoin2((
        qsu._autojoin2((
          bazs,
          quxs,
          func.StaticMapS(
            "0" -> func.LeftSide,
            "1" -> func.RightSide))),
        barss,
        func.ConcatMaps(
          func.LeftSide,
          func.MakeMapS("2", func.RightSide)))))

    runOn(qgraph) must beLike {
      case
        LeftShift(
          LeftShift(
            LeftShift(
              LeftShift(
                LeftShift(
                  Read(_, ExcludeId),
                  struct1,
                  ExcludeId, _, _, Rotation.ShiftArray),
                struct2,
                ExcludeId, _, _, Rotation.ShiftArray),
              struct3,
              ExcludeId, _, _, Rotation.ShiftArray),
            struct4,
            ExcludeId, _, _, Rotation.ShiftArray),
          struct5,
          ExcludeId, _, _, Rotation.ShiftArray) =>

        struct1 must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "bar"))
        struct2 must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "cart2"))
        struct3 must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "source"), "foo"))
        struct4 must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "simpl12"), "baz"))
        struct5 must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "simpl12"), "qux"))
    }
  }

  // r11{_}{_}{_}{_}{_}{_:}, r11{_}{_}{_:}, r11{_}{_:}, r11{_:}, r11{_}{_}{_}{_:}
  "ensure downstream compatible structs are adjusted for upstream compatible wrapping" in {
    val rlp0 = qsu.read(afile, ExcludeId)

    val rlp3 = qsu.transpose(
      rlp0,
      Retain.Values,
      Rotation.ShiftMap)

    val rlp4 = qsu.transpose(
      rlp3,
      Retain.Values,
      Rotation.ShiftMap)

    val rlp5 = qsu.transpose(
      rlp4,
      Retain.Values,
      Rotation.ShiftMap)

    val rlp23 = qsu.transpose(
      rlp5,
      Retain.Identities,
      Rotation.ShiftMap)

    val rlp19 = qsu.transpose(
      rlp0,
      Retain.Identities,
      Rotation.ShiftMap)

    val rlp15 = qsu.transpose(
      rlp3,
      Retain.Identities,
      Rotation.ShiftMap)

    val rlp11 = qsu.transpose(
      rlp4,
      Retain.Identities,
      Rotation.ShiftMap)

    val rlp6 = qsu.transpose(
      rlp5,
      Retain.Values,
      Rotation.ShiftMap)

    val rlp7 = qsu.transpose(
      rlp6,
      Retain.Values,
      Rotation.ShiftMap)

    val rlp8 = qsu.transpose(
      rlp7,
      Retain.Identities,
      Rotation.ShiftMap)

    val rlp13 = qsu._autojoin2((
      rlp8,
      rlp11,
      func.StaticMapS(
        "0" -> func.LeftSide,
        "1" -> func.RightSide)))

    val rlp17 = qsu._autojoin2((
      rlp13,
      rlp15,
      func.ConcatMaps(
        func.LeftSide,
        func.MakeMapS("2", func.RightSide))))

    val rlp21 = qsu._autojoin2((
      rlp17,
      rlp19,
      func.ConcatMaps(
        func.LeftSide,
        func.MakeMapS("3", func.RightSide))))

    val rlp26 = qsu._autojoin2((
      rlp21,
      rlp23,
      func.ConcatMaps(
        func.LeftSide,
        func.MakeMapS("4", func.RightSide))))

    val qgraph = QSUGraph.fromTree[Fix](rlp26)

    runOn(qgraph) must beLike {
      case
        LeftShift(
          LeftShift(
            LeftShift(
              LeftShift(
                LeftShift(
                  LeftShift(
                    Read(`afile`, ExcludeId),
                    _,
                    _,
                    _,
                    _,
                    _),
                  struct,
                  _,
                  _,
                  _,
                  _),
                _,
                _,
                _,
                _,
                _),
              _,
              _,
              _,
              _,
              _),
            _,
            _,
            _,
            _,
            _),
          _,
          _,
          _,
          _,
          _) =>

        struct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "cart0"))
    }
  }

  // r11{_}{_}{_}{_:}, r11{_}{_:}, r11{_}{_}{_:}
  "detect compatibility across certain complex ternary structures" in {
    val read = qsu.read(afile, ExcludeId)

    val rs = qsu.transpose(
      read,
      Retain.Values,
      Rotation.ShiftMap)

    val rsse = qsu.transpose(
      rs,
      Retain.Values,
      Rotation.ShiftMap)

    val rssi = qsu.transpose(
      rs,
      Retain.Identities,
      Rotation.ShiftMap)

    val rsssi = qsu.transpose(
      rsse,
      Retain.Identities,
      Rotation.ShiftMap)

    val rssse = qsu.transpose(
      rsse,
      Retain.Values,
      Rotation.ShiftMap)

    val rssssi = qsu.transpose(
      rssse,
      Retain.Identities,
      Rotation.ShiftMap)

    val qgraph = QSUGraph.fromTree[Fix](
      qsu._autojoin2((
        qsu._autojoin2((
          rssssi,
          rssi,
          func.StaticMapS(
            "0" -> func.LeftSide,
            "1" -> func.RightSide))),
        rsssi,
        func.ConcatMaps(
          func.LeftSide,
          func.MakeMapS("3", func.RightSide)))))

    runOn(qgraph) must haveShiftCount(4)
  }

  // select *{_}.z[_], *{_}.y, *{_}.q{_}[_] from foo
  "all cartouche share a common prefix on shifted root" in {
    val shiftRoot =
      qsu.transpose(
        shiftedRead,
        Retain.Values,
        Rotation.ShiftMap)

    val qgraph = QSUGraph.fromTree[Fix](
      qsu._autojoin2(
        qsu._autojoin2(
          shiftRoot,
          qsu.transpose(
            qsu.map(
              shiftRoot,
              recFunc.ProjectKeyS(recFunc.Hole, "z")),
            Retain.Values,
            Rotation.ShiftArray),
          func.StaticMapS(
            "y" -> func.ProjectKeyS(func.LeftSide, "y"),
            "z" -> func.RightSide)),
        qsu.transpose(
          qsu.transpose(
            qsu.map(
              shiftRoot,
              recFunc.ProjectKeyS(recFunc.Hole, "q")),
            Retain.Values,
            Rotation.ShiftMap),
          Retain.Values,
          Rotation.ShiftArray),
        func.ConcatMaps(
          func.LeftSide,
          func.MakeMapS("q", func.RightSide))))

    runOn(qgraph) must beLike {
      case LeftShift(
        LeftShift(
          LeftShift(
            LeftShift(
              Read(_, ExcludeId),
              rootStruct,
              ExcludeId,
              _,
              rootRepair,
              Rotation.ShiftMap),
            zStruct,
            ExcludeId,
            _,
            zRepair,
            Rotation.ShiftArray),
          qStruct,
          ExcludeId,
          _,
          qRepair,
          Rotation.ShiftMap),
        qStruct2,
        ExcludeId,
        _,
        joiner,
        Rotation.ShiftArray) =>

      rootStruct must beTreeEqual(recFunc.Hole)
      rootRepair must beTreeEqual(func.MakeMapS("simpl6", func.RightSide))

      zStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "simpl6"), "z"))
      zRepair must beTreeEqual(func.ConcatMaps(func.LeftSide, func.MakeMapS("cart1", func.RightSide)))

      qStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "simpl6"), "q"))
      qRepair must beTreeEqual(func.ConcatMaps(func.LeftSide, func.MakeMapS("cart2", func.RightSide)))

      qStruct2 must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "cart2"))

      joiner must beTreeEqual(func.StaticMapS(
        "y" -> func.ProjectKeyS(func.ProjectKeyS(func.LeftSide, "simpl6"), "y"),
        "z" -> func.ProjectKeyS(func.LeftSide, "cart1"),
        "q" -> func.RightSide))
    }
  }

  // select *{_}.z[_], *{_:} as y, *{_}.q{_}[_] from foo
  "all cartouche share a common prefix on shifted root including identities" in {
    val shiftRoot =
      qsu.transpose(
        shiftedRead,
        Retain.Values,
        Rotation.ShiftMap)

    val qgraph = QSUGraph.fromTree[Fix](
      qsu._autojoin2(
        qsu._autojoin2(
          qsu.transpose(
            shiftedRead,
            Retain.Identities,
            Rotation.ShiftMap),
          qsu.transpose(
            qsu.map(
              shiftRoot,
              recFunc.ProjectKeyS(recFunc.Hole, "z")),
            Retain.Values,
            Rotation.ShiftArray),
          func.StaticMapS(
            "y" -> func.ProjectKeyS(func.LeftSide, "y"),
            "z" -> func.RightSide)),
        qsu.transpose(
          qsu.transpose(
            qsu.map(
              shiftRoot,
              recFunc.ProjectKeyS(recFunc.Hole, "q")),
            Retain.Values,
            Rotation.ShiftMap),
          Retain.Values,
          Rotation.ShiftArray),
        func.ConcatMaps(
          func.LeftSide,
          func.MakeMapS("q", func.RightSide))))

    runOn(qgraph) must beLike {
      case LeftShift(
        LeftShift(
          LeftShift(
            LeftShift(
              Read(_, ExcludeId),
              rootStruct,
              IncludeId,
              _,
              rootRepair,
              Rotation.ShiftMap),
            zStruct,
            ExcludeId,
            _,
            zRepair,
            Rotation.ShiftArray),
          qStruct,
          ExcludeId,
          _,
          qRepair,
          Rotation.ShiftMap),
        qStruct2,
        ExcludeId,
        _,
        joiner,
        Rotation.ShiftArray) =>

      rootStruct must beTreeEqual(recFunc.Hole)
      rootRepair must beTreeEqual(func.StaticMapS(
        "simpl7" -> func.ProjectIndexI(func.RightSide, 1),
        "simpl6" -> func.ProjectIndexI(func.RightSide, 0)))

      zStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "simpl7"), "z"))
      zRepair must beTreeEqual(func.ConcatMaps(func.LeftSide, func.MakeMapS("cart1", func.RightSide)))

      qStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "simpl7"), "q"))
      qRepair must beTreeEqual(func.ConcatMaps(func.LeftSide, func.MakeMapS("cart2", func.RightSide)))

      qStruct2 must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "cart2"))

      joiner must beTreeEqual(func.StaticMapS(
        "y" -> func.ProjectKeyS(func.ProjectKeyS(func.LeftSide, "simpl6"), "y"),
        "z" -> func.ProjectKeyS(func.LeftSide, "cart1"),
        "q" -> func.RightSide))
    }
  }

  // select x[_].y{_}.z, x[_].y{_:}, x[_].q, x[_].y{_}.v[_].w from foo
  "all cartouche share a common prefix and reference deep identities" in {
    val xshift =
      qsu.transpose(
        qsu.map(
          shiftedRead,
          recFunc.ProjectKeyS(recFunc.Hole, "x")),
        Retain.Values,
        Rotation.ShiftArray)

    val y =
      qsu.map(
        xshift,
        recFunc.ProjectKeyS(recFunc.Hole, "y"))

    val yvalues =
      qsu.transpose(
        y,
        Retain.Values,
        Rotation.ShiftMap)

    val vvalues =
      qsu.transpose(
        qsu.map(
          yvalues,
          recFunc.ProjectKeyS(recFunc.Hole, "v")),
        Retain.Values,
        Rotation.ShiftArray)

    val yids =
      qsu.transpose(
        y,
        Retain.Identities,
        Rotation.ShiftMap)

    val qgraph = QSUGraph.fromTree[Fix](
      qsu._autojoin2(
        qsu._autojoin2(
          qsu._autojoin2(
            yvalues,
            yids,
            func.StaticMapS(
              "z" -> func.ProjectKeyS(func.LeftSide, "z"),
              "y" -> func.RightSide)),
          xshift,
          func.ConcatMaps(
            func.LeftSide,
            func.MakeMapS("q", func.ProjectKeyS(func.RightSide, "q")))),
        vvalues,
        func.ConcatMaps(
          func.LeftSide,
          func.MakeMapS("w", func.ProjectKeyS(func.RightSide, "w")))))

    runOn(qgraph) must beLike {
      case LeftShift(
        LeftShift(
          LeftShift(
            Read(_, _),
            xstruct,
            ExcludeId,
            _,
            xrepair,
            Rotation.ShiftArray),
          ystruct,
          IncludeId,
          _,
          yrepair,
          Rotation.ShiftMap),
        vstruct,
        ExcludeId,
        _,
        joiner,
        Rotation.ShiftArray) =>

      xstruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "x"))
      xrepair must beTreeEqual(func.MakeMapS("simpl19", func.RightSide))

      ystruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "simpl19"), "y"))
      yrepair must beTreeEqual(
        func.ConcatMaps(
          func.ConcatMaps(
            func.LeftSide,
            func.MakeMapS("simpl18", func.ProjectIndexI(func.RightSide, 1))),
          func.MakeMapS("simpl17", func.ProjectIndexI(func.RightSide, 0))))

      vstruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "simpl18"), "v"))

      joiner must beTreeEqual(func.StaticMapS(
        "z" -> func.ProjectKeyS(func.ProjectKeyS(func.LeftSide, "simpl18"), "z"),
        "y" -> func.ProjectKeyS(func.LeftSide, "simpl17"),
        "q" -> func.ProjectKeyS(func.ProjectKeyS(func.LeftSide, "simpl19"), "q"),
        "w" -> func.ProjectKeyS(func.RightSide, "w")))
    }
  }

  // select x[_].y{_}.z{_:} as zk, x[_].y{_:}, x[_].y{_}.z{_}, x[_].q, x[_].y{_}.v[_].w from foo
  "all cartouche share a common prefix and reference multiple deep identities" in {
    val xshift =
      qsu.transpose(
        qsu.map(
          shiftedRead,
          recFunc.ProjectKeyS(recFunc.Hole, "x")),
        Retain.Values,
        Rotation.ShiftArray)

    val y =
      qsu.map(
        xshift,
        recFunc.ProjectKeyS(recFunc.Hole, "y"))

    val yvalues =
      qsu.transpose(
        y,
        Retain.Values,
        Rotation.ShiftMap)

    val z =
      qsu.map(
        yvalues,
        recFunc.ProjectKeyS(recFunc.Hole, "z"))

    val zvalues =
      qsu.transpose(
        z,
        Retain.Values,
        Rotation.ShiftMap)

    val zids =
      qsu.transpose(
        z,
        Retain.Identities,
        Rotation.ShiftMap)

    val vvalues =
      qsu.transpose(
        qsu.map(
          yvalues,
          recFunc.ProjectKeyS(recFunc.Hole, "v")),
        Retain.Values,
        Rotation.ShiftArray)

    val yids =
      qsu.transpose(
        y,
        Retain.Identities,
        Rotation.ShiftMap)

    val qgraph = QSUGraph.fromTree[Fix](
      qsu._autojoin2(
        qsu._autojoin2(
          qsu._autojoin2(
            qsu._autojoin2(
              zids,
              yids,
              func.StaticMapS(
                "zk" -> func.LeftSide,
                "y" -> func.RightSide)),
            zvalues,
            func.ConcatMaps(
              func.LeftSide,
              func.MakeMapS("z", func.RightSide))),
          xshift,
          func.ConcatMaps(
            func.LeftSide,
            func.MakeMapS("q", func.ProjectKeyS(func.RightSide, "q")))),
        vvalues,
        func.ConcatMaps(
          func.LeftSide,
          func.MakeMapS("w", func.ProjectKeyS(func.RightSide, "w")))))

    runOn(qgraph) must beLike {
      case LeftShift(
        LeftShift(
          LeftShift(
            LeftShift(
              Read(_, ExcludeId),
              xstruct,
              ExcludeId,
              _,
              xrepair,
              Rotation.ShiftArray),
            ystruct,
            IncludeId,
            _,
            yrepair,
            Rotation.ShiftMap),
          vstruct,
          ExcludeId,
          _,
          vrepair,
          Rotation.ShiftArray),
        zstruct,
        IncludeId,
        _,
        joiner,
        Rotation.ShiftMap) =>

      xstruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "x"))
      xrepair must beTreeEqual(func.MakeMapS("simpl28", func.RightSide))

      ystruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "simpl28"), "y"))
      yrepair must beTreeEqual(
        func.ConcatMaps(
          func.ConcatMaps(
            func.LeftSide,
            func.MakeMapS("simpl27", func.ProjectIndexI(func.RightSide, 1))),
          func.MakeMapS("simpl25", func.ProjectIndexI(func.RightSide, 0))))

      vstruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "simpl27"), "v"))
      vrepair must beTreeEqual(func.ConcatMaps(func.LeftSide, func.MakeMapS("cart4", func.RightSide)))

      zstruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "simpl27"), "z"))

      joiner must beTreeEqual(func.StaticMapS(
        "zk" -> func.ProjectIndexI(func.RightSide, 0),
        "y" -> func.ProjectKeyS(func.LeftSide, "simpl25"),
        "z" -> func.ProjectIndexI(func.RightSide, 1),
        "q" -> func.ProjectKeyS(func.ProjectKeyS(func.LeftSide, "simpl28"), "q"),
        "w" -> func.ProjectKeyS(func.ProjectKeyS(func.LeftSide, "cart4"), "w")))
    }
  }

  def runOn(qgraph: QSUGraph): QSUGraph =
    runOn_(qgraph).graph

  def runOn_(qgraph: QSUGraph): AuthenticatedQSU[Fix, qprov.P] = {
    val resultsF =
      ApplyProvenance[Fix, F](qprov, qgraph)
        .flatMap(ReifyBuckets[Fix, F](qprov))
        .flatMap(MinimizeAutoJoins[Fix, F](qprov))

    val results = resultsF.run.eval(0L).value.toEither
    results must beRight

    results.right.get
  }

  def haveShiftCount(count: Int): Matcher[QSUGraph] = { graph: QSUGraph =>
    val actual = graph.foldMapUp {
      case LeftShift(_, _, _, _, _, _) => 1
      case _ => 0
    }

    (actual == count, s"expected $count shifts, got $actual")
  }
}
